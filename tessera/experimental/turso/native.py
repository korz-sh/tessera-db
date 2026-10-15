"""Engine-pinned opaque MVCC-log sealing for the Turso experiment."""

import hashlib
import json
import re
import uuid
from pathlib import Path

from tessera.backends.base import NotFound
from tessera.experimental.turso.api import DatabaseQuotaExceeded
from tessera.experimental.turso.identity import GroupBoundary, InvalidIdentity
from tessera.experimental.turso.snapshot import (
    ENGINE,
    InvalidSnapshot,
    SnapshotStore,
    UnknownOutcome,
    canonical,
    digest,
)

FORMAT = "tessera-turso-native-log-1"


# @spec turso/native-log-store
class NativeLogStore(SnapshotStore):
    """Publish a checkpoint plus opaque, ordered `.db-log` suffixes.

    The sidecar is deliberately never parsed. Its name and replay behavior are
    pinned to pyturso 0.7.2 and must be requalified before an engine upgrade.
    """

    format = FORMAT
    max_segments = 8

    def __init__(self, backend, group, *, boundary=None):
        super().__init__(backend, group, boundary=boundary)
        self.prefix = f"turso-native/{group}/"
        self.key = self.prefix + "manifest.json"
        self.max_frontier_bytes = None

    def _check_frontier_quota(self, candidate_bytes, previous=None):
        limit = self.max_frontier_bytes
        if limit is None:
            return
        previous_bytes = (0 if previous is None else
                          previous["checkpoint"]["bytes"] + previous["log_bytes"])
        if candidate_bytes > limit and candidate_bytes > previous_bytes:
            raise DatabaseQuotaExceeded("published frontier byte quota exceeded")

    def _object_valid(self, value):
        return (isinstance(value, dict)
                and re.fullmatch(re.escape(self.prefix) + r"images/[0-9a-f]{32}\.db",
                                 value.get("key", ""))
                and type(value.get("bytes")) is int and value["bytes"] >= 0
                and re.fullmatch(r"[0-9a-f]{64}", value.get("sha256", "")))

    def manifest(self):
        raw, etag = self.backend.get(self.key)
        return self.parse_manifest(raw, etag)

    def parse_manifest(self, raw, etag=None):
        """Validate a live or immutable historical native manifest."""
        try:
            m = json.loads(raw)
            published = GroupBoundary.from_dict(
                m.get("boundary", GroupBoundary.isolated(self.group).to_dict())
            )
            checkpoint, segments = m["checkpoint"], m["segments"]
            valid = (
                m["format"] == self.format
                and m["engine"] == ENGINE
                and m["group"] == self.group
                and published == self.boundary
                and all(type(m[k]) is int and m[k] >= 0
                        for k in ("version", "epoch", "log_bytes", "receipt_count"))
                and re.fullmatch(r"[0-9a-f]{64}", m["log_sha256"])
                and re.fullmatch(r"[0-9a-f]{64}", m["receipt_sha256"])
                and self._object_valid(checkpoint)
                and isinstance(segments, list)
                and len(segments) <= self.max_segments
            )
            offset = 0
            for segment in segments:
                valid = (valid and self._object_valid(segment)
                         and segment.get("offset") == offset
                         and re.fullmatch(r"[0-9a-f]{64}", segment.get("result_sha256", "")))
                offset += segment.get("bytes", 0) if isinstance(segment, dict) else 0
            valid = (valid and offset == m["log_bytes"]
                     and (segments[-1]["result_sha256"] if segments else digest(b""))
                     == m["log_sha256"])
            if not valid:
                raise ValueError("invalid fields")
        except (KeyError, TypeError, ValueError, InvalidIdentity) as exc:
            raise InvalidSnapshot("invalid native-log manifest") from exc
        return m, etag

    def _load(self, reference):
        try:
            data, _ = self.backend.get(reference["key"])
        except NotFound as exc:
            raise InvalidSnapshot("native artifact is missing") from exc
        if len(data) != reference["bytes"] or digest(data) != reference["sha256"]:
            raise InvalidSnapshot("native artifact length/hash mismatch")
        return data

    @staticmethod
    def _receipt_frontier(conn):
        rows = conn.execute(
            "SELECT id,digest,version FROM _tessera_receipts ORDER BY id"
        ).fetchall()
        return len(rows), digest(canonical(rows))

    @staticmethod
    def _log_path(path):
        # pyturso 0.7.2 replaces the database suffix (`db.sqlite` becomes
        # `db.db-log`) instead of blindly appending to the full filename.
        return Path(path).with_suffix(".db-log")

    def restore(self, path):
        m, etag = self.manifest()
        self.restore_manifest(m, path)
        return m, etag

    def restore_manifest(self, manifest, path):
        m = manifest
        path = Path(path)
        with path.open("xb") as output:
            output.write(self._load(m["checkpoint"]))
        log = bytearray()
        for segment in m["segments"]:
            if segment["offset"] != len(log):
                raise InvalidSnapshot("native log has a gap or reordering")
            log.extend(self._load(segment))
            if digest(log) != segment["result_sha256"]:
                raise InvalidSnapshot("native log cumulative hash mismatch")
        if len(log) != m["log_bytes"] or digest(log) != m["log_sha256"]:
            raise InvalidSnapshot("native log frontier mismatch")
        if log:
            self._log_path(path).write_bytes(log)

    @staticmethod
    def _copy_verified_local_file(path, output, reference):
        """Hash the exact cached bytes copied into the new restore."""
        path = Path(path)
        checksum = hashlib.sha256()
        size = 0
        try:
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    output.write(chunk)
                    checksum.update(chunk)
                    size += len(chunk)
        except FileNotFoundError as exc:
            raise InvalidSnapshot("cached native artifact is missing") from exc
        if size != reference["bytes"] or checksum.hexdigest() != reference["sha256"]:
            raise InvalidSnapshot("cached native artifact length/hash mismatch")
        return checksum

    def extend_restored(self, previous, previous_path, manifest, path):
        """Restore an append-only published suffix from a verified local base.

        Return False when the new frontier rebases or is not an exact extension.
        Every local and fetched byte is checked against both manifests before
        the caller opens or publishes the reconstructed cache entry.
        """
        previous, _ = self.parse_manifest(canonical(previous))
        manifest, _ = self.parse_manifest(canonical(manifest))
        old_segments = previous["segments"]
        if (manifest["version"] <= previous["version"]
                or manifest["checkpoint"] != previous["checkpoint"]
                or len(manifest["segments"]) <= len(old_segments)
                or manifest["segments"][:len(old_segments)] != old_segments):
            return False

        previous_path, path = Path(previous_path), Path(path)
        old_log_path = self._log_path(previous_path)
        with path.open("xb") as output:
            self._copy_verified_local_file(previous_path, output, previous["checkpoint"])
        offset = previous["log_bytes"]
        with self._log_path(path).open("xb") as output:
            if offset:
                checksum = self._copy_verified_local_file(
                    old_log_path, output,
                    {"bytes": offset, "sha256": previous["log_sha256"]})
            else:
                checksum = hashlib.sha256()
                if checksum.hexdigest() != previous["log_sha256"]:
                    raise InvalidSnapshot("cached native empty log hash mismatch")
            for segment in manifest["segments"][len(old_segments):]:
                if segment["offset"] != offset:
                    raise InvalidSnapshot("native log extension has a gap")
                data = self._load(segment)
                output.write(data)
                checksum.update(data)
                offset += len(data)
                if checksum.hexdigest() != segment["result_sha256"]:
                    raise InvalidSnapshot("native log extension hash mismatch")
        if offset != manifest["log_bytes"] or checksum.hexdigest() != manifest["log_sha256"]:
            raise InvalidSnapshot("native log extension frontier mismatch")
        return True

    def validate(self, conn, manifest):
        try:
            if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise InvalidSnapshot("native frontier failed integrity check")
            expected = manifest["receipt_count"], manifest["receipt_sha256"]
            if self._receipt_frontier(conn) != expected:
                raise InvalidSnapshot("native receipt frontier mismatch")
        except InvalidSnapshot:
            raise
        except Exception as exc:
            raise InvalidSnapshot("native frontier cannot be opened") from exc

    def _put(self, data):
        key = self.prefix + "images/" + uuid.uuid4().hex + ".db"
        self.backend.put(key, data, if_none_match=True)
        return {"key": key, "bytes": len(data), "sha256": digest(data)}

    def _advance(self, manifest, etag):
        raw = canonical(manifest)
        if etag is not None:
            self.archive_current()
        try:
            if self._fenced:
                new_etag = self.backend.publish_pointer(self.key, raw, etag, self.lease)
            else:
                new_etag = self.backend.put(
                    self.key, raw, if_match=etag, if_none_match=etag is None
                )
        except Exception as error:
            try:
                actual, new_etag = self.backend.get(self.key)
            except NotFound:
                raise error
            except Exception as exc:
                raise UnknownOutcome("cannot reconcile native-log publication") from exc
            if actual != raw:
                raise error
        return new_etag

    def artifact_keys(self, manifest):
        return {manifest["checkpoint"]["key"],
                *(segment["key"] for segment in manifest["segments"])}

    def publish_restored(self, conn, *, version, epoch, etag, source):
        """Publish a restored database as a fresh checkpoint."""
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        data = Path(source).read_bytes()
        previous = self.manifest()[0] if etag is not None else None
        self._check_frontier_quota(len(data), previous)
        checkpoint = self._put(data)
        receipt_count, receipt_sha256 = self._receipt_frontier(conn)
        manifest = dict(
            format=self.format, engine=ENGINE, group=self.group,
            boundary=self.boundary.to_dict(), version=version, epoch=epoch,
            checkpoint=checkpoint, segments=[], log_bytes=0,
            log_sha256=digest(b""), receipt_count=receipt_count,
            receipt_sha256=receipt_sha256,
        )
        return manifest, self._advance(manifest, etag)

    def publish(self, conn, *, version, epoch, etag, source=None):
        if source is None:
            raise ValueError("native-log publication requires the working database path")
        source = Path(source)
        previous = self.manifest()[0] if etag is not None else None
        rebase = previous is None or len(previous["segments"]) >= self.max_segments
        if rebase:
            # BatchOwner holds its publication lock and all worker connections
            # have closed before this quiesced checkpoint begins.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            data = source.read_bytes()
            self._check_frontier_quota(len(data), previous)
            checkpoint = self._put(data)
            segments, log = [], b""
        else:
            checkpoint = previous["checkpoint"]
            segments = list(previous["segments"])
            log_path = self._log_path(source)
            log = log_path.read_bytes() if log_path.exists() else b""
            old_bytes = previous["log_bytes"]
            if len(log) < old_bytes or digest(log[:old_bytes]) != previous["log_sha256"]:
                # The engine can rewrite its local log after a large commit.
                # Seal the complete committed image instead of publishing a
                # suffix against bytes that no longer exist locally.
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
                if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise InvalidSnapshot("working native image failed integrity check")
                data = source.read_bytes()
                self._check_frontier_quota(len(data), previous)
                checkpoint = self._put(data)
                segments, log = [], b""
            else:
                suffix = log[old_bytes:]
                if not suffix:
                    raise InvalidSnapshot("changed native frontier has no new log segment")
                self._check_frontier_quota(checkpoint["bytes"] + len(log), previous)
                segment = self._put(suffix)
                segment.update(offset=old_bytes, result_sha256=digest(log))
                segments.append(segment)
        receipt_count, receipt_sha256 = self._receipt_frontier(conn)
        manifest = dict(
            format=self.format,
            engine=ENGINE,
            group=self.group,
            boundary=self.boundary.to_dict(),
            version=version,
            epoch=epoch,
            checkpoint=checkpoint,
            segments=segments,
            log_bytes=len(log),
            log_sha256=digest(log),
            receipt_count=receipt_count,
            receipt_sha256=receipt_sha256,
        )
        return manifest, self._advance(manifest, etag)
