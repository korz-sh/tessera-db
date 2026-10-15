"""Exact Turso image-delta recovery over the single-host journal spike."""

import base64
import json
from pathlib import Path
import tempfile
import threading
import uuid

from .owner import IdentityConflict, Request, check_sql, receipt
from tessera.experimental.turso.snapshot import UnknownOutcome, canonical, connect, digest
from tessera.experimental.journal import Conflict, Mutation

BLOCK = 4096
FORMAT = "turso-journal-image-1"


# @spec turso/native-recovery-error
class NativeRecoveryError(ValueError):
    pass


# @spec turso/native-journal-owner
class NativeJournalOwner:
    """Keep disposable Turso databases derived from one durable journal.

    Each journal mutation contains changed blocks from a sealed native database
    image. Multi-group changes share one journal pack and are only exposed after
    that pack commits. Any failed preparation discards all local state.
    """

    def __init__(self, journal, groups, *, schema=(), epoch=1, cache_root=None):
        groups = list(groups)
        if (not 1 <= len(groups) <= 64 or len(set(groups)) != len(groups)
                or type(epoch) is not int or epoch < 1):
            raise ValueError("owner requires 1..64 unique groups and a positive epoch")
        self.journal, self.groups, self.schema = journal, tuple(groups), tuple(schema)
        self.epoch = epoch
        self.cache_root = None if cache_root is None else Path(cache_root)
        if self.cache_root is not None:
            self.cache_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = self._broken = False
        self._tmp, self._connections, self._images = None, {}, {}
        try:
            current = [journal.epochs.get(group, 0) for group in groups]
            if any(value > epoch for value in current):
                raise Conflict("journal has a newer owner epoch")
            claims = [(group, epoch) for group, value in zip(groups, current) if value < epoch]
            if claims:
                journal.claim_many(claims)
            self._reload()
        except BaseException:
            self.close()
            raise

    def _create_base(self, path):
        conn = connect(path)
        try:
            for sql in self.schema:
                if "_tessera_" in sql.lower():
                    raise ValueError("schema references reserved receipt namespace")
                conn.execute(sql).fetchall()
            conn.execute("CREATE TABLE _tessera_receipts(id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL, result TEXT NOT NULL)").fetchall()
            return conn
        except BaseException:
            conn.close()
            raise

    def _seal(self, conn):
        target = Path(self._tmp.name) / f"seal-{uuid.uuid4().hex}.db"
        try:
            conn.execute("VACUUM INTO '" + str(target).replace("'", "''") + "'").fetchall()
            return target.read_bytes()
        finally:
            target.unlink(missing_ok=True)

    @staticmethod
    def _install(path, image):
        for suffix in ("", "-wal", "-shm"):
            Path(str(path) + suffix).unlink(missing_ok=True)
        path.write_bytes(image)

    @staticmethod
    def _encode_delta(before, after, fingerprint):
        changed = []
        for offset in range(0, len(after), BLOCK):
            block = after[offset:offset + BLOCK]
            if block != before[offset:offset + len(block)]:
                changed.append(offset // BLOCK)
        data = b"".join(after[index * BLOCK:(index + 1) * BLOCK] for index in changed)
        return canonical(dict(format=FORMAT, before=digest(before), after=digest(after),
                              size=len(after), blocks=changed,
                              data=base64.b64encode(data).decode(), fingerprint=fingerprint))

    @staticmethod
    def _decode_delta(before, payload):
        try:
            envelope = json.loads(payload)
            blocks = envelope["blocks"]
            if (envelope["format"] != FORMAT or envelope["before"] != digest(before)
                    or not isinstance(blocks, list) or blocks != sorted(set(blocks))
                    or not all(type(index) is int and index >= 0 for index in blocks)
                    or type(envelope["size"]) is not int or envelope["size"] < 0
                    or not isinstance(envelope["fingerprint"], str)):
                raise ValueError("invalid delta metadata")
            result = bytearray(before[:envelope["size"]])
            result.extend(b"\0" * (envelope["size"] - len(result)))
            raw = base64.b64decode(envelope["data"], validate=True)
            consumed = 0
            for index in blocks:
                offset = index * BLOCK
                length = min(BLOCK, envelope["size"] - offset)
                if length <= 0 or consumed + length > len(raw):
                    raise ValueError("delta block is outside the image")
                result[offset:offset + length] = raw[consumed:consumed + length]
                consumed += length
            if consumed != len(raw) or digest(result) != envelope["after"]:
                raise ValueError("delta content mismatch")
            return bytes(result), envelope["fingerprint"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise NativeRecoveryError("invalid native image delta") from exc

    @staticmethod
    def _apply(conn, request, version):
        conn.execute("BEGIN CONCURRENT")
        try:
            rows = [conn.execute(sql, params).fetchall()
                    for sql, params in json.loads(request.payload)]
            conn.execute("INSERT INTO _tessera_receipts VALUES(?,?,?,?)",
                         (request.operation_id, request.fingerprint, version,
                          canonical(rows).decode())).fetchall()
            conn.execute("COMMIT").fetchall()
            return receipt(conn, request.operation_id)
        except BaseException:
            try:
                conn.execute("ROLLBACK").fetchall()
            except Exception:
                pass
            raise

    def _reload(self):
        for conn in self._connections.values():
            conn.close()
        self._connections, self._images = {}, {}
        if self._tmp is not None:
            self._tmp.cleanup()
        self._tmp = tempfile.TemporaryDirectory(prefix="turso-journal-", dir=self.cache_root)
        try:
            for group in self.groups:
                path = Path(self._tmp.name) / f"{group}.db"
                base = self._create_base(Path(self._tmp.name) / f"{group}-base.db")
                image = self._seal(base)
                base.close()
                records = self.journal.scan(group)
                for expected, record in enumerate(records, 1):
                    raw = base64.b64decode(record["payload"], validate=True)
                    if digest(raw) != record["sha256"] or record["version"] != expected:
                        raise NativeRecoveryError("journal record identity/version mismatch")
                    image, fingerprint = self._decode_delta(image, raw)
                    record["fingerprint"] = fingerprint
                self._install(path, image)
                conn = connect(path)
                if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise NativeRecoveryError("replayed database failed integrity check")
                if self.journal.versions.get(group, 0) != len(records):
                    raise NativeRecoveryError("journal group version mismatch")
                for record in records:
                    native = receipt(conn, record["operation_id"])
                    if (native is None or native.version != record["version"]
                            or native.fingerprint != record["fingerprint"]):
                        raise NativeRecoveryError("native receipt disagrees with journal")
                self._connections[group], self._images[group] = conn, image
        except BaseException as exc:
            for conn in self._connections.values():
                conn.close()
            self._connections = {}
            if isinstance(exc, NativeRecoveryError):
                raise
            raise NativeRecoveryError("cannot replay committed journal into Turso") from exc

    def commit(self, operations):
        operations = list(operations)
        if not 1 <= len(operations) <= 64:
            raise ValueError("commit requires 1..64 group requests")
        if any(group not in self._connections or not isinstance(request, Request)
               for group, request in operations):
            raise ValueError("commit references an unmanaged group or invalid request")
        with self._lock:
            if self._closed or self._broken:
                raise RuntimeError("owner closed or recovery required")
            unique, results = {}, {}
            for group, request in operations:
                key = (group, request.operation_id)
                old = unique.get(key)
                if old is not None and old.fingerprint != request.fingerprint:
                    raise IdentityConflict("conflicting operation IDs in one commit")
                unique[key] = request
            pending, versions = [], dict(self.journal.versions)
            for (group, operation_id), request in unique.items():
                old = self.journal.lookup(group, operation_id)
                if old is not None:
                    actual = receipt(self._connections[group], operation_id)
                    if actual is None or actual.fingerprint != request.fingerprint:
                        raise IdentityConflict("operation ID has different durable content")
                    results[(group, operation_id)] = actual
                    continue
                version = versions.get(group, 0) + 1
                versions[group] = version
                pending.append((group, request, version))
            payloads = []
            try:
                images = dict(self._images)
                for group, request, version in pending:
                    results[(group, request.operation_id)] = self._apply(
                        self._connections[group], request, version)
                    after = self._seal(self._connections[group])
                    payloads.append(self._encode_delta(images[group], after, request.fingerprint))
                    images[group] = after
                if pending:
                    self.journal.append([
                        Mutation(group, request.operation_id, self.epoch, version - 1, payload)
                        for (group, request, version), payload in zip(pending, payloads)
                    ])
                    self._images = images
            except BaseException as exc:
                if self.journal._broken:
                    self._broken = True
                    raise UnknownOutcome("journal outcome unknown; reopen and replay") from exc
                try:
                    self._reload()
                except Exception as recovery:
                    self._broken = True
                    raise NativeRecoveryError("owner cannot restore the committed prefix") from recovery
                raise
            return [results[(group, request.operation_id)] for group, request in operations]

    def read(self, group, sql, params=()):
        check_sql(sql, read=True)
        with self._lock:
            if self._closed or self._broken or group not in self._connections:
                raise RuntimeError("owner closed, unmanaged or recovery required")
            return self._connections[group].execute(sql, params).fetchall()

    def lookup(self, group, operation_id):
        with self._lock:
            if self._closed or self._broken or group not in self._connections:
                raise RuntimeError("owner closed, unmanaged or recovery required")
            return receipt(self._connections[group], operation_id)

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for conn in self._connections.values():
                conn.close()
            self._connections = {}
            if self._tmp is not None:
                self._tmp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
