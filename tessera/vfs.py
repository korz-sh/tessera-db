# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Page-level read/write VFS for tessera-db (milestones M6 and M7-A1).

:class:`TesseraVFS` is an apsw VFS that serves a tenant's SQLite database
directly from object storage, WITHOUT downloading anything in full. Each
page read resolves by precedence, newest to oldest:

1. **Delta overlay**: the manifest's ``deltas[]`` (TSD1 frames) are
   downloaded and applied in order onto a ``pgno -> bytes`` dict at
   registration time; the most recent page wins (a page in two deltas ->
   the last applied wins).
2. **Content-addressed local chunk cache** (:class:`ChunkCacheLRU`): the
   cache is GLOBAL across tenants (the file name IS the sha256 of the
   contents), unlike the per-tenant hydration cache.
3. **Whole-chunk download**: a page miss downloads the ENTIRE chunk
   containing it (``backend.get(entry.key)``), verifies the sha256 of the
   decoded contents against ``entry.hash``, caches it and serves.

Locking is a no-op: the logical file is private to the process, so
``xLock``/``xUnlock``/``xCheckReservedLock`` do nothing. The
``-wal``/``-journal``/``-shm`` sidecars never exist: ``xAccess`` answers
``False`` and ``xOpen`` returns a null file (size 0, zero reads). Page 1
is ALWAYS served with bytes 18/19 (file format) set to 1, so SQLite sees a
normal rollback-journal database even when the stored pages record WAL
mode. Cached or stored bytes are NEVER mutated: only the buffer returned
to the caller.

WRITABLE sessions (M7 parts A1/A2): ``register(..., writable=True,
sandbox_dir=...)`` gives the session a local *sandbox* where the sidecars
(``-wal``/``-shm``/``-journal``) are real filesystem files. Page 1 is
served with bytes 18/19 FORCED to 2,2 (WAL) —only in the delivered buffer,
never in the stored bytes— so SQLite enters WAL mode DIRECTLY on open,
even if the base chunk records rollback mode (a ``VACUUM INTO`` base
carries 1/1 and the 1/1->WAL switch would rewrite page 1 through the main
file, which the VFS rejects). ALL user writes land in the LOCAL ``-wal``;
the main logical file still accepts no writes
(``xWrite``/``xTruncate`` raise :class:`apsw.ReadOnlyError`), so the
checkpoint can never mutate it and the frames stay in the local WAL, ready
to be extracted with :func:`tessera.delta.read_wal_frames`. Note: without
``xShmMap`` on the main file, SQLite requires
``PRAGMA locking_mode=EXCLUSIVE`` for WAL (heap-memory wal-index),
consistent with the lease's one-writer-per-tenant model.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path

import apsw

from .backends.base import Backend, BackendError
from .chunks import decode_chunk
from .delta import Delta
from .delta import parse as parse_delta
from .manifest import Manifest

__all__ = ["ChunkCacheLRU", "Overlay", "TesseraVFS"]


_SIDECAR_SUFFIXES = ("-wal", "-journal", "-shm")
_CACHE_TMP_PREFIX = ".tessera-cache-"
_DEFAULT_CACHE_DIR = Path("/tmp/tessera/chunks/")


def _is_sha256_hex(value: str) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        c in "0123456789abcdef" for c in value
    )


def _is_valid_page_size(page_size: int) -> bool:
    return (
        isinstance(page_size, int)
        and 512 <= page_size <= 65536
        and page_size & (page_size - 1) == 0
    )


def _name_of(name: object) -> str | None:
    """Normalize an xOpen/xAccess name argument to a plain string.

    apsw passes ``str``, an :class:`apsw.URIFilename` (which carries URI
    parameters), or ``None`` (temporary file). ``None`` stays ``None``.
    """
    if name is None:
        return None
    filename = getattr(name, "filename", None)
    if callable(filename):
        return filename()
    return str(name)


def _is_sidecar(name: str | None) -> bool:
    return name is not None and name.endswith(_SIDECAR_SUFFIXES)


# ---------------------------------------------------------------------------
# ChunkCacheLRU: content-addressed chunk cache (global, cross-tenant)
# ---------------------------------------------------------------------------


# @spec tessera/chunk-cache-lru
class ChunkCacheLRU:
    """Disk cache of chunk blobs keyed by the sha256 hex of their content.

    ``dir/<sha256-hex>`` files, written atomically (temp file + ``os.replace``,
    so a concurrent reader never sees a partial blob). The filename IS the
    hash: :meth:`get` trusts it (content was verified before :meth:`put`),
    and :meth:`put` validates the mapping to keep that trust honest.

    Eviction is mtime-based LRU: :meth:`evict` deletes the files with the
    oldest mtime until the directory fits in ``max_bytes``. :meth:`put` and
    :meth:`touch` refresh mtime, so a just-used chunk is never the victim.

    Single-process assumption, but a lock serializes puts/eviction and the
    atomic replace keeps reads safe; losing a read race just re-downloads.
    """

    def __init__(self, dir: Path, max_bytes: int = 1 << 30) -> None:
        if max_bytes < 0:
            raise ValueError(f"max_bytes must be >= 0, got {max_bytes}")
        self.dir = Path(dir)
        self.max_bytes = int(max_bytes)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # -- paths ---------------------------------------------------------

    def _path(self, h: str) -> Path:
        return self.dir / h

    # -- core operations -------------------------------------------------

    def get(self, h: str) -> bytes | None:
        """Return the cached content for hash *h*, or ``None`` on a miss.

        Trusted read: the name is the hash and the mapping was validated on
        put, so the content is returned as-is (no re-hashing). A hit also
        refreshes the file's mtime (touch on use).
        """
        try:
            data = self._path(h).read_bytes()
        except OSError:
            return None
        self.touch(h)
        return data

    def touch(self, h: str) -> None:
        """Mark *h* as recently used (mtime = now). Best-effort."""
        try:
            os.utime(self._path(h), None)
        except OSError:
            pass

    def put(self, h: str, data: bytes) -> None:
        """Atomically cache *data* under its content hash *h*.

        Validates that *h* is the sha256 hex of *data* (the trust contract of
        :meth:`get`), writes to a temp file, fsyncs and ``os.replace``s it
        into place, then lazily evicts old entries until the cache fits.
        """
        if not _is_sha256_hex(h):
            raise ValueError(f"cache key must be a sha256 hex string, got {h!r}")
        if hashlib.sha256(data).hexdigest() != h:
            raise ValueError(f"cache key {h} does not match content sha256")
        self.dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            fd, tmp_name = tempfile.mkstemp(dir=self.dir, prefix=_CACHE_TMP_PREFIX)
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self._path(h))  # atomic; fresh mtime
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            self._evict_locked()

    def evict(self) -> int:
        """Drop oldest-mtime files until the cache fits ``max_bytes``.

        Returns the number of files deleted. Best-effort: files that cannot
        be deleted are skipped and never retried within the same pass.
        """
        with self._lock:
            return self._evict_locked()

    def _evict_locked(self) -> int:
        deleted = 0
        failed: set[Path] = set()
        while True:
            entries: list[tuple[int, Path, int]] = []
            total = 0
            try:
                children = list(self.dir.iterdir())
            except OSError:
                return deleted
            for path in children:
                if path.name.startswith(_CACHE_TMP_PREFIX):
                    continue  # in-flight temp write of another put
                try:
                    st = path.stat(follow_symlinks=False)
                except OSError:
                    continue
                if not path.is_file():
                    continue
                entries.append((st.st_mtime_ns, path, st.st_size))
                total += st.st_size
            if total <= self.max_bytes or not entries:
                return deleted
            victims = [e for e in entries if e[1] not in failed]
            if not victims:
                return deleted  # everything left is undeletable: stop
            mtime_ns, victim, _size = min(victims, key=lambda e: (e[0], e[1].name))
            try:
                victim.unlink()
                deleted += 1
            except OSError:
                failed.add(victim)

    def stats(self) -> dict:
        """Return ``{"files": <count>, "bytes": <total size>}`` (best-effort)."""
        files = 0
        total = 0
        try:
            children = list(self.dir.iterdir())
        except OSError:
            return {"files": 0, "bytes": 0}
        for path in children:
            if path.name.startswith(_CACHE_TMP_PREFIX):
                continue
            try:
                st = path.stat(follow_symlinks=False)
            except OSError:
                continue
            if path.is_file():
                files += 1
                total += st.st_size
        return {"files": files, "bytes": total}


# ---------------------------------------------------------------------------
# Overlay: delta pages applied in order (newest wins)
# ---------------------------------------------------------------------------


# @spec tessera/overlay
class Overlay:
    """In-memory dict of pages coming from the manifest's delta chain.

    Built by applying each delta's frames IN ORDER into one ``pgno -> bytes``
    dict, so a page present in two deltas ends up with the newest value, and
    a page present in a delta shadows the same page in any chunk.
    """

    def __init__(self) -> None:
        self._pages: dict[int, bytes] = {}

    @classmethod
    def build(cls, deltas: list[Delta], page_size: int) -> "Overlay":
        """Apply *deltas* in order (later = newer = wins) at *page_size*."""
        overlay = cls()
        for delta in deltas:
            if delta.page_size != page_size:
                raise ValueError(
                    f"delta page_size {delta.page_size} != manifest page_size {page_size}"
                )
            for pgno, page in delta.frames:
                overlay._pages[pgno] = page
        return overlay

    def get(self, pgno: int) -> bytes | None:
        """Return the overlaid page *pgno* (1-based), or ``None``."""
        return self._pages.get(pgno)

    def set(self, pgno: int, page: bytes) -> None:
        """Overlay page *pgno* (1-based) with *page* in place (newest wins).

        Write-session support (M7-A1): :meth:`_Session.apply_delta` merges
        live WAL frames into the overlay without rebuilding it.
        """
        self._pages[pgno] = page

    def __contains__(self, pgno: int) -> bool:
        return pgno in self._pages

    def __len__(self) -> int:
        return len(self._pages)


# ---------------------------------------------------------------------------
# Session + file objects
# ---------------------------------------------------------------------------


@dataclass
class _Session:
    """One registered tenant snapshot: manifest view + live resources.

    ``page_count`` is the LOGICAL page count served to SQLite: the manifest's
    ``page_count`` grown by any delta that extends the database (and later by
    :meth:`apply_delta` for live write-session merges). Read-only sessions
    keep it fixed for their lifetime; writable sessions may grow it.
    """

    tenant: str
    manifest: Manifest
    backend: Backend
    page_size: int
    chunk_pages: int
    overlay: Overlay
    pool: ThreadPoolExecutor | None = None
    # Logical (served) page count: >= manifest.page_count.
    page_count: int = 0
    # Write-session state (M7-A1): sidecars are real local files under
    # sandbox_dir, page 1 is served RAW, main file stays read-only.
    writable: bool = False
    sandbox_dir: Path | None = None

    def apply_delta(self, delta: Delta) -> None:
        """Merge *delta*'s committed frames into the overlay IN PLACE.

        Later frames win (a page in two deltas keeps the newest value), and
        the session's logical page count grows to
        ``max(current, delta.page_count)`` so pages the delta appended are
        readable. Raises :class:`ValueError` when the delta's page size does
        not match the session's.
        """
        if delta.page_size != self.page_size:
            raise ValueError(
                f"delta page_size {delta.page_size} != session page_size {self.page_size}"
            )
        for pgno, page in delta.frames:
            self.overlay.set(pgno, page)
        if delta.page_count > self.page_count:
            self.page_count = delta.page_count


class _TesseraFile:
    """The main logical database file: read-only, page-level reads.

    Duck-typed apsw VFSFile: no need to subclass :class:`apsw.VFSFile` (that
    is only for inheriting from an existing VFS implementation). Note the
    apsw argument order in :meth:`xRead`: ``(amount, offset)``.

    The file NEVER accepts writes (not even on writable sessions: user writes
    must land in the local ``-wal`` sidecar, and a checkpoint must never be
    able to push WAL pages back into the logical main file). Writable
    sessions differ in that page 1 is served with the file-format bytes
    forced to 2/2 (WAL — see :meth:`TesseraVFS._read_page`) and the device is
    not advertised as IMMUTABLE (SQLite must believe the database is writable
    so it opens it in WAL mode and writes to the sidecar).
    """

    def __init__(self, vfs: "TesseraVFS", session: _Session, filename: str) -> None:
        self._vfs = vfs
        self._session = session
        self._filename = filename

    # -- reads -----------------------------------------------------------

    def xRead(self, amount: int, offset: int) -> bytes:
        """Read *amount* bytes at *offset*, page by page.

        SQLite issues page-aligned, page-sized requests; any other shape is
        tolerated by fetching the spanning pages and slicing. Reads beyond
        EOF are zero-filled (short reads are fatal for SQLite).
        """
        session = self._session
        size = session.page_count * session.page_size
        if amount < 0 or offset < 0:
            raise apsw.IOError(f"invalid read amount={amount} offset={offset}")
        end = min(offset + amount, size)
        out = bytearray()
        pos = offset
        try:
            while pos < end:
                pgno = pos // session.page_size + 1
                page_start = (pgno - 1) * session.page_size
                page = self._vfs._read_page(session, pgno)
                lo = pos - page_start
                hi = min(page_start + session.page_size, end) - page_start
                out += page[lo:hi]
                pos = page_start + hi
        except BackendError as exc:
            raise apsw.IOError(str(exc)) from exc
        if len(out) < amount:
            out += b"\x00" * (amount - len(out))
        return bytes(out)

    # -- metadata --------------------------------------------------------

    def xFileSize(self) -> int:
        return self._session.page_count * self._session.page_size

    def xSectorSize(self) -> int:
        return self._session.page_size

    def xDeviceCharacteristics(self) -> int:
        if self._session.writable:
            # A writable session must look like ordinary writable storage:
            # IMMUTABLE would make SQLite treat the file as read-only media
            # and refuse WAL writes. The file still rejects xWrite — all
            # writes go through the local -wal sidecar instead.
            return 0
        # IMMUTABLE: the logical file never changes while open (a fresh
        # manifest is a new session), so SQLite can skip locking and
        # change detection entirely.
        return apsw.SQLITE_IOCAP_IMMUTABLE

    # -- locking is meaningless: process-private logical file -------------

    def xLock(self, level: int) -> None:
        pass

    def xUnlock(self, level: int) -> None:
        pass

    def xCheckReservedLock(self) -> bool:
        return False

    # -- writes are impossible --------------------------------------------

    def xWrite(self, data, offset: int) -> None:
        raise apsw.ReadOnlyError(f"{type(self).__name__} is read-only")

    def xTruncate(self, newsize: int) -> None:
        raise apsw.ReadOnlyError(f"{type(self).__name__} is read-only")

    def xSync(self, flags: int) -> None:
        pass  # nothing to flush on a read-only file

    def xClose(self) -> None:
        pass  # process-private logical file: nothing to release

    def xFileControl(self, op: int, ptr: int) -> bool:
        return False  # no file-control ops understood


class _NullFile:
    """Null sidecar (-wal / -journal / -shm): empty, unreadable, read-only."""

    def xRead(self, amount: int, offset: int) -> bytes:
        return b"\x00" * amount

    def xWrite(self, data, offset: int) -> None:
        raise apsw.ReadOnlyError("tessera sidecars are null files")

    def xTruncate(self, newsize: int) -> None:
        pass  # truncating nothing to nothing

    def xSync(self, flags: int) -> None:
        pass

    def xFileSize(self) -> int:
        return 0

    def xLock(self, level: int) -> None:
        pass

    def xUnlock(self, level: int) -> None:
        pass

    def xCheckReservedLock(self) -> bool:
        return False

    def xSectorSize(self) -> int:
        return 512

    def xDeviceCharacteristics(self) -> int:
        return 0

    def xClose(self) -> None:
        pass

    def xFileControl(self, op: int, ptr: int) -> bool:
        return False


class _LocalFile:
    """Real local sidecar file (-wal / -shm / -journal) for writable sessions.

    Plain Python file I/O backing SQLite's WAL for a write session: the file
    lives under the session's ``sandbox_dir`` and is opened in place
    (``"r+b"``), created on first open when it does not exist yet (``"w+b"``
    fallback — SQLite opens the WAL with READWRITE|CREATE before the first
    frame exists). Short reads are zero-padded: SQLite treats the zero tail
    as free WAL space, exactly as the null sidecar of the read path does.

    All user writes land here; the logical main file is never touched (its
    ``xWrite`` keeps raising :class:`apsw.ReadOnlyError`, so a checkpoint can
    never push WAL pages back into the main file).
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        try:
            # buffering=0: SQLite relies on its writes being visible to the
            # OS immediately (xFileSize, external readers of the -wal such as
            # tessera.delta.read_wal_frames, crash semantics). Buffered
            # Python I/O would hide the WAL tail in userspace until a flush
            # — read_wal_frames then captures a TRUNCATED delta (M7-A2
            # finding: silently missing committed frames with integrity
            # still "ok").
            self._fh = open(self._path, "r+b", buffering=0)
        except FileNotFoundError:
            self._fh = open(self._path, "w+b", buffering=0)  # READWRITE|CREATE
        except OSError as exc:
            raise apsw.CantOpenError(f"cannot open sidecar {self._path}: {exc}") from exc

    def xRead(self, amount: int, offset: int) -> bytes:
        if amount < 0 or offset < 0:
            raise apsw.IOError(f"invalid read amount={amount} offset={offset}")
        self._fh.seek(offset)
        data = self._fh.read(amount)
        if len(data) < amount:  # past EOF: zero-fill (fresh WAL space)
            data += b"\x00" * (amount - len(data))
        return data

    def xWrite(self, data, offset: int) -> None:
        if offset < 0:
            raise apsw.IOError(f"invalid write offset={offset}")
        self._fh.seek(offset)
        self._fh.write(bytes(data))

    def xTruncate(self, newsize: int) -> None:
        if newsize < 0:
            raise apsw.IOError(f"invalid truncate size={newsize}")
        self._fh.truncate(newsize)

    def xSync(self, flags: int) -> None:
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def xFileSize(self) -> int:
        return os.fstat(self._fh.fileno()).st_size

    def xLock(self, level: int) -> None:
        pass  # process-private sandbox file: no cross-process contention

    def xUnlock(self, level: int) -> None:
        pass

    def xCheckReservedLock(self) -> bool:
        return False

    def xSectorSize(self) -> int:
        return 4096

    def xDeviceCharacteristics(self) -> int:
        return 0

    def xClose(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    def xFileControl(self, op: int, ptr: int) -> bool:
        return False


class _TempFile:
    """In-memory temp file for name-less opens (SQLite temp storage)."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def xRead(self, amount: int, offset: int) -> bytes:
        if offset < 0 or amount < 0:
            raise apsw.IOError(f"invalid read amount={amount} offset={offset}")
        chunk = bytes(self._buf[offset : offset + amount])
        return chunk + b"\x00" * (amount - len(chunk))

    def xWrite(self, data, offset: int) -> None:
        if offset < 0:
            raise apsw.IOError(f"invalid write offset={offset}")
        if offset + len(data) > len(self._buf):
            self._buf.extend(b"\x00" * (offset + len(data) - len(self._buf)))
        self._buf[offset : offset + len(data)] = bytes(data)

    def xTruncate(self, newsize: int) -> None:
        if newsize < 0:
            raise apsw.IOError(f"invalid truncate size={newsize}")
        del self._buf[newsize:]

    def xSync(self, flags: int) -> None:
        pass

    def xFileSize(self) -> int:
        return len(self._buf)

    def xLock(self, level: int) -> None:
        pass

    def xUnlock(self, level: int) -> None:
        pass

    def xCheckReservedLock(self) -> bool:
        return False

    def xSectorSize(self) -> int:
        return 512

    def xDeviceCharacteristics(self) -> int:
        return 0

    def xClose(self) -> None:
        self._buf = bytearray()

    def xFileControl(self, op: int, ptr: int) -> bool:
        return False


# ---------------------------------------------------------------------------
# TesseraVFS
# ---------------------------------------------------------------------------


# @spec tessera/tessera-vfs
class TesseraVFS(apsw.VFS):
    """apsw VFS serving tenants as chunked base + delta overlay.

    Sessions are registered by key (``"{tenant}/v{manifest.version}"``);
    re-registering the same key replaces the session with fresh manifest
    state. Open connections keep their bound session: each logical file is
    process-private and immutable.

    Args:
        name: VFS registration name (pass to ``apsw.Connection(..., vfs=name)``).
        cache_dir: chunk cache directory (default ``/tmp/tessera/chunks/`` —
            GLOBAL and content-addressed, shared across tenants; not the
            per-tenant hydration cache of M5).
        cache_max_bytes: soft cap of the chunk cache.

    Sessions registered with ``writable=True`` (M7-A1) additionally expose
    real local sidecar files under ``sandbox_dir`` so SQLite can run the
    logical database in WAL mode: all user writes land in the local
    ``-wal`` file and the main logical file stays read-only.
    """

    def __init__(
        self,
        name: str = "tessera",
        cache_dir: Path | None = None,
        cache_max_bytes: int = 1 << 30,
    ) -> None:
        super().__init__(name)
        self.vfs_name = name
        self._sessions: dict[str, _Session] = {}
        self._sessions_lock = threading.Lock()
        self._cache = ChunkCacheLRU(
            Path(cache_dir) if cache_dir is not None else _DEFAULT_CACHE_DIR,
            max_bytes=cache_max_bytes,
        )

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        tenant: str,
        manifest: Manifest,
        backend: Backend,
        *,
        hydrate: bool = False,
        hydrate_concurrency: int = 2,
        writable: bool = False,
        sandbox_dir: Path | str | None = None,
    ) -> str:
        """Register a session for *tenant* from its format-2 *manifest*.

        Downloads and applies all deltas up front (they are small). With
        ``hydrate=True`` a bounded ``ThreadPoolExecutor`` prefetched every
        chunk into the cache in the background; failures there never surface
        to the caller — those chunks simply fetch on demand.

        With ``writable=True`` the session becomes a WRITE session (M7-A1):
        ``sandbox_dir`` is required (:class:`ValueError` otherwise) and
        created if missing; the session's sidecar names (``-wal``/``-shm``/
        ``-journal``) open as REAL LOCAL FILES under it and page 1 is served
        with the file-format bytes forced to 2/2 (WAL), so SQLite can open
        the logical database in WAL mode directly and write — every write
        lands in the local ``-wal`` file; the main logical file never accepts
        writes.

        Returns the session key ``"{tenant}/v{manifest.version}"``.
        Re-registering an existing key replaces the session.
        """
        if manifest.format != 2:
            raise ValueError(
                f"TesseraVFS serves format-2 manifests, got format {manifest.format!r}"
            )
        if not manifest.chunks:
            raise ValueError("manifest references no chunks")
        chunk_pages = manifest.chunk_pages
        if not isinstance(chunk_pages, int) or chunk_pages <= 0:
            raise ValueError(f"manifest chunk_pages must be > 0, got {chunk_pages!r}")

        sandbox: Path | None = None
        if writable:
            if sandbox_dir is None:
                raise ValueError(
                    "writable=True requires sandbox_dir (local directory for "
                    "the -wal/-shm/-journal sidecar files)"
                )
            sandbox = Path(sandbox_dir)
            sandbox.mkdir(parents=True, exist_ok=True)

        # Delta chain first: small, downloaded eagerly at register. Stored
        # payloads go through the manifest's chunk codec before TSD1 parse.
        deltas: list[Delta] = []
        for entry in manifest.deltas:
            raw, _etag = backend.get(entry.key)
            deltas.append(parse_delta(decode_chunk(raw, manifest.chunk_codec)))

        # Page size from the stored chunk size — no download needed (codec
        # "none" stores exactly chunk_pages * page_size bytes). Compressed
        # codecs have a stored size that says nothing about the page size,
        # so fall back to the delta header when the derivation is not a
        # valid page size.
        page_size = manifest.chunks[0].size // chunk_pages
        if not _is_valid_page_size(page_size) and deltas:
            page_size = deltas[0].page_size
        if not _is_valid_page_size(page_size):
            raise ValueError(
                f"derived page size {page_size!r} is invalid (chunk size "
                f"{manifest.chunks[0].size} / chunk_pages {chunk_pages}); "
                "compressed chunk sets must carry at least one delta"
            )
        overlay = Overlay.build(deltas, page_size)

        # Effective served size: the manifest's page_count describes the
        # CHUNK BASE, but a committed delta that grew the database records
        # the true post-image size in its TSD1 header (the WAL commit frame's
        # db-size field). Serving the smaller base size would truncate the
        # logical file under SQLite (pages beyond it are unreadable ->
        # CorruptError), so the session serves the maximum. The session binds
        # a private manifest copy; stored manifest objects are not mutated.
        served_page_count = manifest.page_count
        for delta in deltas:
            served_page_count = max(served_page_count, delta.page_count)
        if served_page_count != manifest.page_count:
            manifest = replace(manifest, page_count=served_page_count)

        key = f"{tenant}/v{manifest.version}"
        session = _Session(
            tenant=tenant,
            manifest=manifest,
            backend=backend,
            page_size=page_size,
            chunk_pages=chunk_pages,
            overlay=overlay,
            page_count=served_page_count,
            writable=writable,
            sandbox_dir=sandbox,
        )
        with self._sessions_lock:
            previous = self._sessions.get(key)
            self._sessions[key] = session
        if previous is not None and previous.pool is not None:
            previous.pool.shutdown(wait=True, cancel_futures=True)

        if hydrate:
            pool = ThreadPoolExecutor(
                max_workers=max(1, int(hydrate_concurrency)),
                thread_name_prefix="tessera-hydrate",
            )
            session.pool = pool
            for entry in manifest.chunks:
                pool.submit(self._prefetch_chunk, session, entry)
        return key

    def release(self, key: str) -> None:
        """Stop *key*'s hydrator pool (if any) and drop the session.

        Idempotent for unknown keys. Open connections keep reading their
        bound session — release only detaches the VFS-level state.
        """
        with self._sessions_lock:
            session = self._sessions.pop(key, None)
        if session is not None and session.pool is not None:
            session.pool.shutdown(wait=True, cancel_futures=True)

    # ------------------------------------------------------------------
    # Page resolution
    # ------------------------------------------------------------------

    def _read_page(self, session: _Session, pgno: int) -> bytes:
        """Return page *pgno* (1-based) of *session*: overlay -> cache -> backend.

        The returned buffer is always a fresh copy (stored/cached data is
        never handed out nor mutated). Page 1 is delivered with the file
        format bytes (18/19) FORCED at serve time — stored/cached chunk bytes
        never change: read-only sessions get ``1, 1`` so SQLite treats the
        logical file as a plain rollback-journal database, while WRITABLE
        sessions get ``2, 2`` (WAL) regardless of what the stored base
        records. The forced 2/2 is what lets a write session open WAL mode
        directly even on a rollback-flagged ``VACUUM INTO`` base (M7-A2):
        without it, ``PRAGMA journal_mode=WAL`` would switch the header
        through a rollback-journal transaction — direct main-file writes the
        VFS correctly rejects (M7-A1 finding).
        """
        page_count = session.page_count
        if pgno < 1 or pgno > page_count:
            raise BackendError(f"page {pgno} out of range 1..{page_count}")
        page = session.overlay.get(pgno)
        if page is None:
            chunk_idx = (pgno - 1) // session.chunk_pages
            entry = session.manifest.chunks[chunk_idx]
            blob = self._cache.get(entry.hash)
            if blob is None:
                # Page miss: download the WHOLE chunk containing the page.
                raw, _etag = session.backend.get(entry.key)
                blob = decode_chunk(raw, session.manifest.chunk_codec)
                # The hash is over the UNCOMPRESSED content (see
                # tessera.chunks), so verify after decoding.
                if hashlib.sha256(blob).hexdigest() != entry.hash:
                    raise BackendError(f"chunk hash mismatch: {entry.key}")
                self._cache.put(entry.hash, blob)
            offset = ((pgno - 1) % session.chunk_pages) * session.page_size
            page = blob[offset : offset + session.page_size]
        page = bytes(page)  # copy: never hand out stored buffers
        if pgno == 1:
            mutated = bytearray(page)
            if session.writable:
                # Writable session (M7-A2): lie about the header — force the
                # file format bytes to 2/2 (WAL) so SQLite opens WAL mode
                # DIRECTLY. A VACUUM INTO base (migrate_to_v2) stores 1/1;
                # serving it unmodified would make the WAL switch rewrite
                # page 1 through a ROLLBACK-JOURNAL transaction (direct
                # main-file writes -> ReadOnlyError, M7-A1 finding). Serve
                # time only: stored/cached chunk bytes never mutate.
                mutated[18] = 2  # file format write version: WAL
                mutated[19] = 2  # file format read version: WAL
            else:
                mutated[18] = 1  # file format write version: rollback journal
                mutated[19] = 1  # file format read version: rollback journal
            page = bytes(mutated)
        return page

    def _prefetch_chunk(self, session: _Session, entry) -> None:
        """Background prefetch of one chunk. Never raises."""
        try:
            if self._cache.get(entry.hash) is not None:
                return  # already cached (get also refreshed its mtime)
            raw, _etag = session.backend.get(entry.key)
            blob = decode_chunk(raw, session.manifest.chunk_codec)
            if hashlib.sha256(blob).hexdigest() != entry.hash:
                raise BackendError(f"chunk hash mismatch: {entry.key}")
            self._cache.put(entry.hash, blob)
        except Exception:
            # Hydration is best-effort: a failure just leaves the chunk
            # uncached; it will be fetched (and verified) on demand.
            pass

    # ------------------------------------------------------------------
    # apsw VFS methods
    # ------------------------------------------------------------------

    def _sidecar_session(self, name: str) -> "_Session | None":
        """Resolve the session owning sidecar name *name* (``key`` + suffix).

        Sidecar names arrive as the registered key plus the suffix (e.g.
        ``"t/v5-wal"`` for key ``"t/v5"``); stripping the suffix yields the
        key. ``None`` when the name carries no known suffix or no session is
        registered under the base key.
        """
        for suffix in _SIDECAR_SUFFIXES:
            if name.endswith(suffix):
                with self._sessions_lock:
                    return self._sessions.get(name[: -len(suffix)])
        return None

    def xOpen(self, name, flags):
        filename = _name_of(name)
        if filename is None:
            return _TempFile()  # SQLite temporary storage
        if _is_sidecar(filename):
            session = self._sidecar_session(filename)
            if (
                session is not None
                and session.writable
                and session.sandbox_dir is not None
            ):
                # Write session (M7-A1): the sidecar is a REAL LOCAL FILE in
                # the sandbox — this is where SQLite's WAL (and every user
                # write) lands. Names use the last path component.
                return _LocalFile(session.sandbox_dir / Path(filename).name)
            return _NullFile()  # -wal / -journal / -shm never exist
        with self._sessions_lock:
            session = self._sessions.get(filename)
        if session is None:
            raise apsw.CantOpenError(
                f"no tessera session registered for {filename!r} "
                "(register the tenant first)"
            )
        return _TesseraFile(self, session, filename)

    def xDelete(self, filename: str, syncdir: bool) -> None:
        pass  # nothing managed by this VFS is ever deleted through it

    def xAccess(self, pathname, flags: int) -> bool:
        name = _name_of(pathname)
        if _is_sidecar(name):
            # Read sessions: no journals / WAL / shm ever exist. Writable
            # sessions: report the sandbox file's actual existence so SQLite
            # can find a left-behind -wal on reopen.
            session = self._sidecar_session(name) if name is not None else None
            if (
                session is not None
                and session.writable
                and session.sandbox_dir is not None
            ):
                return (session.sandbox_dir / Path(name).name).exists()
            return False
        if flags == apsw.SQLITE_ACCESS_READWRITE:
            with self._sessions_lock:
                session = self._sessions.get(name)
            # Writable sessions are logically writable (through their local
            # -wal sidecar); read-only sessions enforce read-only semantics.
            return bool(session is not None and session.writable)
        return True  # the logical main file always exists and is readable

    def xFullPathname(self, name: str) -> str:
        return str(name)  # the registered key is the full logical name

    def xCurrentTime(self) -> float:
        return 2440587.5 + time.time() / 86400.0  # Julian day number

    def xCurrentTimeInt64(self) -> int:
        return int((2440587.5 + time.time() / 86400.0) * 86400000)

    def xSleep(self, microseconds: int) -> int:
        time.sleep(microseconds / 1_000_000.0)
        return microseconds

    def xRandomness(self, numbytes: int) -> bytes:
        return os.urandom(numbytes)

    def xGetLastError(self) -> tuple[int, str]:
        return (0, "")
