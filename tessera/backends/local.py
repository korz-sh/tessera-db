# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Local filesystem object-storage backend for tessera-db.

Objects are files at ``root_dir / key`` (keys use "/" separators); the etag is
the hex sha256 of the content. Leases are JSON state files under
``_leases/`` guarded by ``fcntl.flock``: the flock is held for the lifetime of
the lease on an fd the backend keeps open, so the OS releases it if the
process dies — that is what makes leases crash-safe.

CAS notes:
- ``put`` evaluates its condition and writes inside a per-key flock (a lock
  file under ``_locks/``). Atomic ``os.replace`` alone does NOT make
  concurrent CAS puts single-winner (two threads can both observe the old
  etag before either renames); the flock serializes check+write so exactly
  one CAS put wins.
- Writes are atomic: temp file in the same parent directory (mkstemp), fsync,
  then ``os.replace`` onto the final path.
"""

import fcntl
import hashlib
import json
import os
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from tessera.backends.base import (
    Backend,
    CASFailed,
    Lease,
    LeaseLost,
    NotFound,
    TenantBusy,
)

__all__ = ["LocalBackend"]

_LEASES_DIR = "_leases"
_LOCKS_DIR = "_locks"
_TMP_PREFIX = ".tessera-tmp-"
_MISSING = (FileNotFoundError, IsADirectoryError, NotADirectoryError)


def _validate_key(key: str) -> None:
    if not isinstance(key, str) or not key:
        raise ValueError("object key must be a non-empty string")
    if key.startswith("/"):
        raise ValueError(f"object key must not start with '/': {key!r}")
    if ".." in key:
        raise ValueError(f"object key must not contain '..': {key!r}")


def _validate_tenant(tenant: str) -> None:
    if not isinstance(tenant, str) or not tenant:
        raise ValueError("tenant must be a non-empty string")
    for part in tenant.split("/"):
        if part in ("", ".", ".."):
            raise ValueError(f"invalid tenant name: {tenant!r}")


def _read_state(fd: int) -> dict:
    """Parse the JSON object at *fd* from offset 0; {} if missing/corrupt."""
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
        obj = json.loads(b"".join(chunks).decode("utf-8"))
        return obj if isinstance(obj, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_state(fd: int, obj: dict) -> None:
    """Atomically-enough rewrite of the JSON *obj* at *fd* (caller holds flock)."""
    payload = json.dumps(obj).encode("utf-8")
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, len(payload))
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        view = view[written:]
    os.fsync(fd)


# @spec backends/local-backend
class LocalBackend(Backend):
    """Filesystem-backed implementation of :class:`Backend`.

    Layout under ``root``::

        <key>              object content (etag = hex sha256 of content)
        _leases/<t>.json   lease state (flock-guarded)
        _locks/<sha>.lock  per-key put/delete lock files (internal)

    The lease fd stays open for the lifetime of the lease; dropping the
    backend without releasing keeps the flock until the process exits.
    """

    def __init__(self, root_dir: str | Path) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self._lease_fds: dict[str, int] = {}  # owner -> open fd (flock held)
        self._lease_fds_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Object store
    # ------------------------------------------------------------------

    def _path(self, key: str) -> Path:
        _validate_key(key)
        return self.root / key

    @staticmethod
    def _etag_of(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _current_etag(self, path: Path) -> str | None:
        try:
            return self._etag_of(path.read_bytes())
        except _MISSING:
            return None

    def get(self, key: str) -> tuple[bytes, str]:
        path = self._path(key)
        try:
            data = path.read_bytes()
        except _MISSING as exc:
            raise NotFound(f"object not found: {key}") from exc
        return data, self._etag_of(data)

    def head(self, key: str) -> str | None:
        return self._current_etag(self._path(key))

    def put(
        self,
        key: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> str:
        path = self._path(key)
        data = bytes(data)
        new_etag = self._etag_of(data)
        with self._key_lock(key):
            current = self._current_etag(path)
            if if_match is not None and current != if_match:
                raise CASFailed(
                    f"put if_match failed for {key!r}: "
                    f"expected {if_match!r}, current is {current!r}"
                )
            if if_none_match and current is not None:
                raise CASFailed(f"put if_none_match failed for {key!r}: key already exists")
            self._atomic_write(path, data)
        return new_etag

    def delete(self, key: str) -> None:
        path = self._path(key)
        with self._key_lock(key):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    supports_fenced_gc = True

    def gc_delete(self, key: str, lease: Lease) -> None:
        # The lifetime flock excludes other owners. Prevent release from
        # closing that descriptor until the delete has actually completed.
        with self._lease_fds_lock:
            if lease.owner not in self._lease_fds:
                raise LeaseLost(f"GC lease for {lease.tenant!r} is no longer held")
            if not key.startswith(f"{lease.tenant}/"):
                raise ValueError("GC key must belong to the leased tenant")
            self.delete(key)

    def list(self, prefix: str) -> list[str]:
        if not isinstance(prefix, str):
            raise ValueError("prefix must be a string")
        found: list[str] = []
        for dirpath, dirnames, filenames in os.walk(self.root):
            # Prune internal directories so they never leak as keys.
            dirnames[:] = [d for d in dirnames if d not in (_LEASES_DIR, _LOCKS_DIR)]
            for name in filenames:
                if name.startswith(_TMP_PREFIX):
                    continue  # leftover temp file from a crashed put
                rel = os.path.relpath(os.path.join(dirpath, name), self.root)
                key = rel.replace(os.sep, "/")
                if key.startswith(prefix):
                    found.append(key)
        return sorted(found)

    def mtime(self, key: str) -> float | None:
        """Last-modified unix seconds of the object file, or ``None``.

        Any stat failure (missing key, permission problem) maps to ``None``:
        the GC contract treats unknown age as "never delete".
        """
        try:
            return os.stat(self._path(key)).st_mtime
        except OSError:
            return None

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        parent = path.parent
        parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=parent, prefix=_TMP_PREFIX)
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)  # atomic within the same filesystem
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        LocalBackend._fsync_dir(parent)

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    @contextmanager
    def _key_lock(self, key: str) -> Iterator[None]:
        """Per-key flock so the CAS condition check + write are one critical
        section. Exclusivity is per open file description, so separate
        ``os.open`` calls (threads or processes) conflict correctly."""
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        lock_path = self.root / _LOCKS_DIR / f"{digest}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    # ------------------------------------------------------------------
    # Leases (JSON state file + fcntl.flock)
    # ------------------------------------------------------------------

    def _lease_path(self, tenant: str) -> Path:
        _validate_tenant(tenant)
        return self.root / _LEASES_DIR / f"{tenant}.json"

    def acquire_lease(
        self,
        tenant: str,
        ttl_ms: int,
        endpoint: str = "",
        session_token: str = "",
        min_epoch: int = 0,
    ) -> Lease:
        # State lives on disk and no sweeper removes it, but the floor is
        # honored anyway: the Backend contract requires it and it costs one
        # max().
        path = self._lease_path(tenant)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise TenantBusy(
                    f"lease for tenant {tenant!r} is held by another owner"
                ) from exc
            # Flock acquired: mutual exclusion is ours until we release/close
            # the fd, or the process dies (OS drops the flock either way).
            state = _read_state(fd)
            epoch = max(int(state.get("epoch", 0)), int(min_epoch)) + 1
            owner = uuid.uuid4().hex
            expires_at = time.time() + ttl_ms / 1000.0
            _write_state(
                fd,
                {
                    "tenant": tenant,
                    "owner": owner,
                    "epoch": epoch,
                    "expires_at": expires_at,
                    "endpoint": endpoint,
                    "session_token": session_token,
                },
            )
        except BaseException:
            os.close(fd)
            raise
        with self._lease_fds_lock:
            self._lease_fds[owner] = fd
        return Lease(
            tenant=tenant,
            owner=owner,
            epoch=epoch,
            expires_at=expires_at,
            endpoint=endpoint,
            session_token=session_token,
        )

    def renew_lease(self, lease: Lease, ttl_ms: int) -> Lease:
        with self._lease_fds_lock:
            fd = self._lease_fds.get(lease.owner)
        if fd is None:
            raise LeaseLost(
                f"lease for tenant {lease.tenant!r} is no longer owned "
                f"by {lease.owner!r}"
            )
        expires_at = time.time() + ttl_ms / 1000.0
        # The flock from acquire is still held on this fd, so no one else
        # can be writing the state file concurrently. Preserve the endpoint
        # recorded at acquire time: the state file is authoritative, the
        # passed lease is the fallback for pre-endpoint state files.
        state = _read_state(fd)
        endpoint = str(state.get("endpoint") or lease.endpoint or "")
        # The session token is preserved the same way as the endpoint: a
        # renewal does NOT rotate the credential (rotating it would leave
        # clients holding a valid lease view receiving 401 for no reason).
        session_token = str(state.get("session_token") or lease.session_token or "")
        _write_state(
            fd,
            {
                "tenant": lease.tenant,
                "owner": lease.owner,
                "epoch": lease.epoch,
                "expires_at": expires_at,
                "endpoint": endpoint,
                "session_token": session_token,
            },
        )
        return Lease(
            tenant=lease.tenant,
            owner=lease.owner,
            epoch=lease.epoch,
            expires_at=expires_at,
            endpoint=endpoint,
            session_token=session_token,
        )

    def release_lease(self, lease: Lease) -> None:
        with self._lease_fds_lock:
            fd = self._lease_fds.pop(lease.owner, None)
        if fd is None:
            # Idempotency: a "released" tombstone written by this same owner
            # means the lease was already released — that is fine.
            tombstone = self._load_lease_file(lease.tenant)
            if tombstone.get("released") and tombstone.get("owner") == lease.owner:
                return
            raise LeaseLost(
                f"lease for tenant {lease.tenant!r} is no longer owned "
                f"by {lease.owner!r}"
            )
        _write_state(
            fd,
            {
                "tenant": lease.tenant,
                "owner": lease.owner,
                "epoch": lease.epoch,
                "expires_at": 0.0,
                "released": True,
            },
        )
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _load_lease_file(self, tenant: str) -> dict:
        try:
            fd = os.open(self._lease_path(tenant), os.O_RDONLY)
        except OSError:
            return {}
        try:
            return _read_state(fd)
        finally:
            os.close(fd)
