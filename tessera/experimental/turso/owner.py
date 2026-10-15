"""Managed concurrent SQL owner for the Turso experiment."""

import hashlib
import json
import math
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tessera.backends.base import LeaseLost, NotFound
from tessera.experimental.turso.api import (
    DatabaseQuotaExceeded,
    OperationError,
    OperationLimitExceeded,
    Receipt,
    Request,
    check_sql,
)
from tessera.experimental.turso.identity import (
    GroupBoundary,
    InvalidIdentity,
    require_group_access,
)
from tessera.experimental.turso.snapshot import (
    SnapshotStore,
    UnknownOutcome,
    canonical,
    connect,
)


# @spec turso/identity-conflict
class IdentityConflict(OperationError, ValueError):
    pass


# @spec turso/receipt
def receipt(conn, operation_id):
    row = conn.execute("SELECT digest,version,result FROM _tessera_receipts WHERE id=?",
                       (operation_id,)).fetchone()
    return None if row is None else Receipt(operation_id, row[0], row[1], json.loads(row[2]))


# @spec turso/batch-owner
class BatchOwner:
    def __init__(self, backend, group, *, schema=(), workers=4, cache_root=None,
                 incremental=False, native=False, lease_ttl_ms=60000,
                 max_result_bytes=None, max_sql_seconds=None,
                 stage_callback=None,
                 boundary=None, principal=None, authorize=None,
                 max_frontier_bytes=None, max_operation_bytes=None):
        if type(workers) is not int or not 1 <= workers <= 32:
            raise ValueError("workers must be 1..32")
        if type(lease_ttl_ms) is not int or not 60 <= lease_ttl_ms <= 300000:
            raise ValueError("lease_ttl_ms must be 60..300000")
        if (max_result_bytes is not None
                and (type(max_result_bytes) is not int or max_result_bytes < 1)):
            raise ValueError("max_result_bytes must be a positive integer or None")
        if (max_sql_seconds is not None
                and (not isinstance(max_sql_seconds, (int, float))
                     or isinstance(max_sql_seconds, bool) or max_sql_seconds <= 0)):
            raise ValueError("max_sql_seconds must be positive or None")
        boundary = GroupBoundary.isolated(group) if boundary is None else boundary
        if not isinstance(boundary, GroupBoundary) or boundary.group != group:
            raise InvalidIdentity("boundary must match the requested group")
        require_group_access(boundary, principal=principal, authorize=authorize)
        from tessera.experimental.turso.incremental import IncrementalStore
        from tessera.experimental.turso.native import NativeLogStore
        if type(incremental) is not bool or type(native) is not bool:
            raise ValueError("incremental and native must be booleans")
        if incremental and native:
            raise ValueError("incremental and native modes are mutually exclusive")
        self.boundary = boundary
        store_type = NativeLogStore if native else IncrementalStore if incremental else SnapshotStore
        self.storage = store_type(
            backend, group, boundary=boundary
        )
        self.configure_limits(max_frontier_bytes=max_frontier_bytes,
                              max_operation_bytes=max_operation_bytes)
        self.workers, self._lock = workers, threading.RLock()
        self._max_result_bytes = max_result_bytes
        self._max_sql_seconds = max_sql_seconds
        self._stage_callback = stage_callback
        self._lease_ttl_ms = lease_ttl_ms
        self._lease_guard = threading.Lock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat_error = None
        self._heartbeat_thread = None
        self._closed = self._broken = False
        self.created = False
        self._conn = self._tmp = None
        self._cache_root = None if cache_root is None else Path(cache_root)
        if self._cache_root is not None:
            self._cache_root.mkdir(parents=True, exist_ok=True)
        try:
            floor = self.storage.manifest()[0]["epoch"]
        except NotFound:
            floor = 0
        # LocalBackend holds a lifetime flock; remote publication checks this lease atomically.
        self._lease = backend.acquire_lease(
            "turso-experiment-" + group, lease_ttl_ms, min_epoch=floor
        )
        self.storage.lease = self._lease
        # Initialization can download or publish several remote artifacts.
        # Start renewal as soon as authority is acquired so a short lease
        # cannot expire before the owner becomes ready.
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat, name=f"tessera-lease-{group}", daemon=True
        )
        self._heartbeat_thread.start()
        try:
            try:
                self._reload()
            except NotFound:
                self._conn = connect(self._path)
                for sql in schema:
                    if "_tessera_" in sql.lower():
                        raise ValueError("schema references reserved receipt namespace")
                    self._conn.execute(sql).fetchall()
                self._conn.execute("CREATE TABLE _tessera_receipts(id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL, result TEXT NOT NULL)")
                self._manifest, self._etag = self.storage.publish(
                    self._conn, version=0, epoch=self._lease.epoch, etag=None,
                    source=self._path)
                self.created = True
        except BaseException:
            self.close()
            raise

    def _heartbeat(self):
        interval = max(0.01, self._lease_ttl_ms / 3000)
        while not self._heartbeat_stop.wait(interval):
            try:
                with self._lease_guard:
                    if self._heartbeat_stop.is_set():
                        return
                    self._lease = self.storage.backend.renew_lease(
                        self._lease, self._lease_ttl_ms
                    )
                    self.storage.lease = self._lease
            except BaseException as exc:
                self._heartbeat_error = exc
                return

    def configure_limits(self, *, max_frontier_bytes=None, max_operation_bytes=None):
        """Set the catalog policy after acquiring the group's writer lease."""
        for name, value in (("max_frontier_bytes", max_frontier_bytes),
                            ("max_operation_bytes", max_operation_bytes)):
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"{name} must be a positive integer or None")
        self.storage.max_frontier_bytes = max_frontier_bytes
        self._max_operation_bytes = max_operation_bytes

    def _require_live_heartbeat(self):
        if self._heartbeat_error is not None:
            raise LeaseLost("owner lease heartbeat failed; reopen before writing") from self._heartbeat_error

    def _reload(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        if self._tmp is not None:
            self._tmp.cleanup()
        self._tmp = tempfile.TemporaryDirectory(prefix="turso-owner-", dir=self._cache_root)
        self._path = Path(self._tmp.name) / "work.db"
        self._manifest, self._etag = self.storage.restore(self._path)
        self._conn = connect(self._path)
        # Validate required receipt schema before accepting work.
        self._conn.execute("SELECT id,digest,version,result FROM _tessera_receipts LIMIT 0").fetchall()
        self.storage.validate(self._conn, self._manifest)

    def _execute(self, request, version):
        c = connect(self._path)
        started = time.monotonic()
        try:
            if self._stage_callback is not None:
                self._stage_callback(request.operation_id, "executing")
            if self._max_sql_seconds is not None:
                c.set_query_timeout(max(1, math.ceil(self._max_sql_seconds * 1000)))
            c.execute("PRAGMA foreign_keys=ON").fetchall()
            c.execute("BEGIN CONCURRENT")
            if self._max_result_bytes is None:
                rows = [c.execute(sql, params).fetchall()
                        for sql, params in json.loads(request.payload)]
            else:
                rows = []
                result_bytes = 2  # Outer JSON array brackets.
                for sql, params in json.loads(request.payload):
                    cursor = c.execute(sql, params)
                    statement_rows = []
                    result_bytes += 2 + bool(rows)  # Inner brackets and comma.
                    while (row := cursor.fetchone()) is not None:
                        result_bytes += len(canonical(row)) + bool(statement_rows)
                        if result_bytes > self._max_result_bytes:
                            raise OperationLimitExceeded("operation result exceeds byte limit")
                        statement_rows.append(row)
                    rows.append(statement_rows)
            encoded = canonical(rows).decode()
            if (self._max_sql_seconds is not None
                    and time.monotonic() - started > self._max_sql_seconds):
                raise OperationLimitExceeded("operation exceeded SQL runtime limit")
            if (self._max_result_bytes is not None
                    and len(encoded.encode()) > self._max_result_bytes):
                raise OperationLimitExceeded("operation result exceeds byte limit")
            c.execute("INSERT INTO _tessera_receipts VALUES(?,?,?,?)",
                      (request.operation_id, request.fingerprint, version, encoded))
            c.execute("COMMIT")
            if self._stage_callback is not None:
                self._stage_callback(request.operation_id, "committed_local")
            return receipt(c, request.operation_id)
        except Exception as exc:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            if (self._max_sql_seconds is not None
                    and time.monotonic() - started >= self._max_sql_seconds):
                return OperationLimitExceeded("operation exceeded SQL runtime limit")
            return exc
        finally:
            c.close()

    def commit(self, requests):
        requests = list(requests)
        if not 1 <= len(requests) <= 128 or not all(isinstance(r, Request) for r in requests):
            raise ValueError("batch needs 1..128 Request objects")
        with self._lock:
            if self._closed or self._broken:
                raise RuntimeError("owner closed or recovery required")
            self._require_live_heartbeat()
            unique, results = {}, {}
            for r in requests:
                previous = unique.get(r.operation_id)
                if previous and previous.fingerprint != r.fingerprint:
                    raise IdentityConflict("conflicting operation IDs in one batch")
                unique[r.operation_id] = r
            pending = []
            for r in unique.values():
                old = receipt(self._conn, r.operation_id)
                if old is None:
                    pending.append(r)
                else:
                    results[r.operation_id] = (old if old.fingerprint == r.fingerprint else
                                                IdentityConflict("operation ID has different durable content"))
            if pending:
                if (self._max_operation_bytes is not None
                        and any(len(request.payload) > self._max_operation_bytes
                                for request in pending)):
                    raise DatabaseQuotaExceeded("operation payload quota exceeded")
                try:
                    version = self._manifest["version"] + 1
                    with ThreadPoolExecutor(max_workers=self.workers) as pool:
                        outcomes = list(pool.map(lambda r: self._execute(r, version), pending))
                    changed = False
                    for r, outcome in zip(pending, outcomes):
                        # The transaction's receipt is authoritative even if a local reply failed.
                        actual = receipt(self._conn, r.operation_id)
                        results[r.operation_id] = actual if actual is not None else outcome
                        if actual is None and not isinstance(outcome, Exception):
                            raise RuntimeError("transaction returned without its receipt")
                        changed |= actual is not None
                    if changed:
                        self._require_live_heartbeat()
                        if self._stage_callback is not None:
                            for request in pending:
                                if receipt(self._conn, request.operation_id) is not None:
                                    self._stage_callback(request.operation_id, "publishing")
                        with self._lease_guard:
                            self._require_live_heartbeat()
                            self._lease = self.storage.backend.renew_lease(
                                self._lease, self._lease_ttl_ms
                            )
                            self.storage.lease = self._lease
                            self._manifest, self._etag = self.storage.publish(
                                self._conn, version=version, epoch=self._lease.epoch,
                                etag=self._etag, source=self._path)
                except Exception as error:
                    try:
                        self._reload()
                    except Exception as exc:
                        self._broken = True
                        raise UnknownOutcome("owner cannot recover; reopen and resolve receipts") from exc
                    raise error
            return [results[r.operation_id] for r in requests]

    def apply_callback(self, callback):
        """Bridge a legacy SQLite callback to one fenced v2 publication.

        Callback IDs are unique invocation markers, not retry identities. A
        caller must still make retries safe at its own business boundary.
        """
        if not callable(callback):
            raise TypeError("callback required")
        with self._lock:
            if self._closed or self._broken:
                raise RuntimeError("owner closed or recovery required")
            self._require_live_heartbeat()
            version = self._manifest["version"] + 1
            marker = "callback-" + uuid.uuid4().hex
            fingerprint = hashlib.sha256(marker.encode()).hexdigest()
            published = False
            try:
                changes_before = self._conn.execute("SELECT total_changes()").fetchone()[0]
                schema_before = self._conn.execute("PRAGMA schema_version").fetchone()[0]
                result = callback(self._conn)
                if self._conn.in_transaction:
                    raise ValueError("v2 callback must commit its SQL transaction")
                changes_after = self._conn.execute("SELECT total_changes()").fetchone()[0]
                schema_after = self._conn.execute("PRAGMA schema_version").fetchone()[0]
                if changes_after == changes_before and schema_after == schema_before:
                    return result, self._manifest["version"]
                self._conn.execute("INSERT INTO _tessera_receipts VALUES(?,?,?,?)",
                                   (marker, fingerprint, version, "[]"))
                self._require_live_heartbeat()
                with self._lease_guard:
                    self._require_live_heartbeat()
                    self._lease = self.storage.backend.renew_lease(
                        self._lease, self._lease_ttl_ms)
                    self.storage.lease = self._lease
                    self._manifest, self._etag = self.storage.publish(
                        self._conn, version=version, epoch=self._lease.epoch,
                        etag=self._etag, source=self._path)
                published = True
                return result, version
            except Exception as error:
                try:
                    self._reload()
                except Exception as exc:
                    self._broken = True
                    raise UnknownOutcome("owner cannot recover callback frontier") from exc
                if not published and receipt(self._conn, marker) is not None:
                    return result, version
                raise error

    def read(self, sql, params=()):
        check_sql(sql, read=True)
        with self.storage.open() as (c, _):
            return c.execute(sql, params).fetchall()

    def lookup(self, operation_id):
        with self.storage.open() as (c, _):
            return receipt(c, operation_id)

    def lifecycle(self, *, policy=None):
        from .lifecycle import LifecycleManager
        return LifecycleManager(self, policy=policy)

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._heartbeat_stop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=max(1, self._lease_ttl_ms / 1000))
            try:
                if self._conn is not None:
                    self._conn.close()
                if self._tmp is not None:
                    self._tmp.cleanup()
            finally:
                try:
                    with self._lease_guard:
                        self.storage.backend.release_lease(self._lease)
                except LeaseLost:
                    # Takeover is expected after remote expiry; never release its successor.
                    pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
