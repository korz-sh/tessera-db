"""Experimental native Turso snapshot protocol. Excluded from the Tessera wheel."""

from contextlib import closing, contextmanager
import hashlib
from importlib.metadata import version as installed_version
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import uuid

import turso
from tessera import LocalBackend
from tessera.backends.base import CASFailed, NotFound
from tessera.experimental.turso.api import OperationOutcomeUnknown
from tessera.experimental.turso.identity import GroupBoundary, InvalidIdentity

ENGINE = "0.7.2"
FORMAT = "tessera-turso-experiment-1"


# @spec turso/invalid-snapshot
class InvalidSnapshot(ValueError):
    pass


# @spec turso/unknown-outcome
class UnknownOutcome(OperationOutcomeUnknown):
    """Publication may have committed; resolve through durable receipt lookup."""


# @spec turso/canonical
def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


# @spec turso/digest
def digest(data):
    return hashlib.sha256(data).hexdigest()


# @spec turso/connect
def connect(path):
    if installed_version("pyturso") != ENGINE:
        raise RuntimeError(f"experimental adapter requires pyturso=={ENGINE}")
    conn = turso.connect(str(path), isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=mvcc").fetchall()
        conn.execute("PRAGMA synchronous=FULL").fetchall()
        return conn
    except BaseException:
        conn.close()
        raise


# @spec turso/snapshot-store
class SnapshotStore:
    format = FORMAT
    needs_previous = False

    def __init__(self, backend, group, *, boundary=None):
        self._fenced = False
        self.lease = None
        if not isinstance(backend, LocalBackend):
            from .fencing import FencedBackend
            if not isinstance(backend, FencedBackend):
                raise TypeError("LocalBackend or experimental FencedBackend required")
            self._fenced = True
        boundary = GroupBoundary.isolated(group) if boundary is None else boundary
        if not isinstance(boundary, GroupBoundary) or boundary.group != group:
            raise InvalidIdentity("boundary must match the requested group")
        self.backend, self.group = backend, group
        self.boundary = boundary
        self.prefix = f"turso-experiment/{group}/"
        self.key = self.prefix + "manifest.json"

    def manifest(self):
        raw, etag = self.backend.get(self.key)
        try:
            m = json.loads(raw)
            published = GroupBoundary.from_dict(
                m.get("boundary", GroupBoundary.isolated(self.group).to_dict())
            )
            valid = (m["format"] == self.format and m["engine"] == ENGINE and m["group"] == self.group
                     and published == self.boundary
                     and all(type(m[k]) is int and m[k] >= 0 for k in ("version", "epoch", "bytes"))
                     and re.fullmatch(re.escape(self.prefix) + r"images/[0-9a-f]{32}\.db", m["image"])
                     and re.fullmatch(r"[0-9a-f]{64}", m["sha256"]))
            if not valid:
                raise ValueError("invalid fields")
        except (KeyError, TypeError, ValueError, InvalidIdentity) as exc:
            raise InvalidSnapshot("invalid experimental manifest") from exc
        return m, etag

    def restore(self, path):
        m, etag = self.manifest()
        self.restore_manifest(m, path)
        return m, etag

    def restore_manifest(self, manifest, path):
        """Materialize exactly the already validated manifest supplied."""
        m = manifest
        data = self.decode_image(self.load_image(m))
        with Path(path).open("xb") as output:
            output.write(data)

    def load_image(self, m):
        try:
            data, _ = self.backend.get(m["image"])
        except NotFound as exc:
            raise InvalidSnapshot("published image is missing") from exc
        if len(data) != m["bytes"] or digest(data) != m["sha256"]:
            raise InvalidSnapshot("published image length/hash mismatch")
        return data

    def encode_image(self, data, previous):
        return data

    def decode_image(self, data):
        return data

    @contextmanager
    def open(self):
        with tempfile.TemporaryDirectory(prefix="turso-published-") as root:
            path = Path(root) / "db.sqlite"
            m, _ = self.restore(path)
            with closing(connect(path)) as c:
                self.validate(c, m)
                yield c, m

    def validate(self, conn, manifest):
        if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise InvalidSnapshot("published image failed integrity check")

    def publish(self, conn, *, version, epoch, etag, source=None):
        previous = self.manifest()[0] if self.needs_previous and etag is not None else None
        with tempfile.TemporaryDirectory(prefix="turso-seal-") as root:
            image = Path(root) / "sealed.db"
            conn.execute("VACUUM INTO '" + str(image).replace("'", "''") + "'").fetchall()
            data = self.encode_image(image.read_bytes(), previous)
        key = self.prefix + "images/" + uuid.uuid4().hex + ".db"
        self.backend.put(key, data, if_none_match=True)
        m = dict(format=self.format, engine=ENGINE, group=self.group,
                 boundary=self.boundary.to_dict(), version=version,
                 epoch=epoch, image=key, bytes=len(data), sha256=digest(data))
        raw = canonical(m)
        if etag is not None:
            self.archive_current()
        try:
            if self._fenced:
                new_etag = self.backend.publish_pointer(self.key, raw, etag, self.lease)
            else:
                new_etag = self.backend.put(self.key, raw, if_match=etag, if_none_match=etag is None)
        except Exception as error:
            try:
                actual, new_etag = self.backend.get(self.key)
            except NotFound:
                raise error
            except Exception as exc:
                raise UnknownOutcome("cannot reconcile manifest publication") from exc
            if actual != raw:
                raise error
        return m, new_etag

    def archive_current(self):
        """Create an immutable copy of the currently published frontier.

        Archiving happens before replacement.  A crash can therefore leave an
        extra archive, but can never replace a head without preserving it.
        """
        old, _ = self.manifest()
        raw = canonical(old)
        key = f"{self.prefix}history/{old['version']:020d}.json"
        try:
            self.backend.put(key, raw, if_none_match=True)
        except CASFailed:
            actual, _ = self.backend.get(key)
            if actual != raw:
                raise InvalidSnapshot("history version has conflicting content")
        return key

    def artifact_keys(self, manifest):
        return {manifest["image"]}

    def export(self, destination):
        destination = Path(destination).absolute()
        if os.path.lexists(destination):
            raise FileExistsError(destination)
        # Conversion happens on a verified isolated copy, never on the owner.
        with tempfile.TemporaryDirectory(prefix="turso-export-", dir=destination.parent) as root:
            path = Path(root) / "db.sqlite"
            self.restore(path)
            with closing(connect(path)) as c:
                c.execute("PRAGMA journal_mode=wal").fetchall()
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            with closing(sqlite3.connect(path)) as c:
                if c.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise InvalidSnapshot("export failed SQLite integrity check")
            os.link(path, destination)
        return destination
