# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Local hydration of a tenant database.

:func:`ensure_local` materializes a tenant's committed state — the base
object(s) plus the manifest's ordered delta chain — as a plain SQLite file
under a local directory. Small marker files make repeated calls cheap and
crash-safe:

- ``.snapshot_etag``  JSON ``{"key": ..., "etag": ...}`` of the last hydrated
  snapshot (format 1). When it matches the manifest's snapshot key *and* the
  cached ``snap-<etag[:16]>.sqlite`` copy is still present, the snapshot is
  not re-downloaded.
- ``.chunks_marker``  JSON ``{"keys": [...], "page_count": ...,
  "chunk_pages": ..., "codec": ...}`` of the last hydrated chunk base
  (format 2). It replaces the ``.snapshot_etag`` semantics for v2 tenants:
  when its key list matches the manifest's chunk keys exactly *and*
  ``db.sqlite`` exists, the chunked base is not rebuilt (zero downloads).
- ``.applied.json``   JSON list of delta etags already applied to
  ``db.sqlite``. Persisted after every successful apply, *before* the next
  delta starts, so a crash between deltas resumes exactly where it stopped.

Error contract (documented choice):

- ``BackendError`` — object-store / pointer problems: a fetched delta's etag
  differs from the manifest entry (object drifted or corrupt at rest), the
  object is missing (:class:`~tessera.backends.base.NotFound`), or a chunk
  fails its hash check inside :func:`tessera.chunks.get_chunk`.
- ``ValueError`` — structurally corrupt delta payloads propagate UNWRAPPED
  from :func:`tessera.delta.parse` (its sha256 trailer guards against
  bit-rot). Transport problems are BackendError; payload corruption is
  ValueError.
"""

import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tessera.backends.base import Backend, BackendError
from tessera.chunks import assemble, get_chunk
from tessera.delta import apply_delta, parse
from tessera.manifest import Manifest

__all__ = ["ensure_local", "is_fully_local"]

_SNAPSHOT_MARKER = ".snapshot_etag"
_CHUNKS_MARKER = ".chunks_marker"
_APPLIED_MARKER = ".applied.json"
_DB_NAME = "db.sqlite"
_TMP_PREFIX = ".tessera-hydrate-"
# Dropped by the hot Owner inside its live directory: an owner-managed dir is
# NOT a disposable cache — it holds the open connection and the unpublished
# WAL — so cache_lock's dirty-marker sweep and the Store LRU must skip it.
_OWNER_MARKER = ".tessera-owner"

# Bound on concurrent object fetches during hydration. Deltas and chunks
# are independent write-once objects; concurrency shortens wall time only.
_FETCH_WORKERS = 16


def _snap_file(dest: Path, etag: str) -> Path:
    """Content-addressed cache path for a hydrated snapshot."""
    return dest / f"snap-{etag[:16]}.sqlite"


def _chunk_cache(dest: Path, chunk_hash: str) -> Path:
    """Content-addressed cache path for one chunk blob (format 2)."""
    return dest / "chunks" / chunk_hash


def _default_dest(tenant: str) -> Path:
    if not isinstance(tenant, str) or not tenant:
        raise ValueError("tenant must be a non-empty string")
    for part in tenant.split("/"):
        if part in ("", ".", ".."):
            raise ValueError(f"invalid tenant name: {tenant!r}")
    return Path(tempfile.gettempdir()) / "tessera" / tenant


def _atomic_write(path: Path, data: bytes) -> None:
    """Write *data* to *path* atomically (temp file + fsync + rename)."""
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=parent, prefix=_TMP_PREFIX)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    _fsync_dir(parent)


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


def _atomic_copy(src: Path, dst: Path) -> None:
    """Atomically place a copy of *src* at *dst* (fsynced)."""
    _atomic_write(dst, src.read_bytes())


def _read_snapshot_marker(path: Path) -> dict | None:
    """Return ``{"key": ..., "etag": ...}`` from the marker, or None."""
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    key, etag = obj.get("key"), obj.get("etag")
    if not isinstance(key, str) or not isinstance(etag, str) or not etag:
        return None
    return {"key": key, "etag": etag}


def _read_chunks_marker(path: Path) -> dict | None:
    """Return the v2 chunks-marker fields from *path*, or None.

    Missing, corrupt or malformed markers all read as None: the caller then
    rebuilds the chunked base, which is always safe (just slower).
    """
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    keys = obj.get("keys")
    if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
        return None
    page_count = obj.get("page_count")
    chunk_pages = obj.get("chunk_pages")
    codec = obj.get("codec")
    if isinstance(page_count, bool) or not isinstance(page_count, int):
        return None
    if isinstance(chunk_pages, bool) or not isinstance(chunk_pages, int):
        return None
    if not isinstance(codec, str):
        return None
    return {
        "keys": keys,
        "page_count": page_count,
        "chunk_pages": chunk_pages,
        "codec": codec,
    }


def _read_applied(path: Path) -> list[str]:
    """Read the applied-etag list. Missing/corrupt marker -> [].

    An empty list is always SAFE, not just optimistic: applying a delta to an
    already up-to-date ``db.sqlite`` is a byte-identical page rewrite
    (apply_delta is idempotent), so a lost marker merely costs a re-apply.
    """
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(obj, list):
        return []
    return [etag for etag in obj if isinstance(etag, str)]


def _hydrate_snapshot_base(
    backend: Backend, manifest: Manifest, dest: Path
) -> list[str]:
    """Build the format-1 base under *dest*; return the applied-etag list.

    Downloads the snapshot unless the ``.snapshot_etag`` marker matches the
    manifest's snapshot key and the cached copy is still present. On a
    lineage change (fresh dest, or a re-snapshotted manifest) the applied
    list resets BEFORE the new marker is published — a crash in between just
    redoes this block on the next call.
    """
    snap_marker = dest / _SNAPSHOT_MARKER
    applied_marker = dest / _APPLIED_MARKER
    db_path = dest / _DB_NAME

    prev = _read_snapshot_marker(snap_marker)
    same_lineage = prev is not None and prev["key"] == manifest.snapshot
    if same_lineage and _snap_file(dest, prev["etag"]).exists():
        snap_etag = prev["etag"]  # cached copy present: skip the download
    else:
        data, etag = backend.get(manifest.snapshot)
        cached = _snap_file(dest, etag)
        if not cached.exists():
            _atomic_write(cached, data)
        snap_etag = etag
    snap_file = _snap_file(dest, snap_etag)

    # ---- applied-delta state ----------------------------------------------
    applied = _read_applied(applied_marker)
    if not same_lineage:
        # New snapshot lineage: db.sqlite must be rebuilt from it, so no
        # previously applied delta counts. Reset the applied marker BEFORE
        # publishing the new snapshot marker — a crash in between just
        # redoes this block on the next call.
        applied = []
        _atomic_write(applied_marker, b"[]")
    _atomic_write(snap_marker, json.dumps({"key": manifest.snapshot, "etag": snap_etag}).encode("utf-8"))

    if not same_lineage or not db_path.exists():
        _atomic_copy(snap_file, db_path)
    return applied


def _hydrate_chunks_base(
    backend: Backend, tenant: str, manifest: Manifest, dest: Path
) -> list[str]:
    """Build the format-2 chunked base under *dest*; return the applied list.

    The v2 base is assembled by concatenation (no page-level VFS). Each
    chunk entry is fetched in ``manifest.chunks`` order (hash-verified inside
    :func:`tessera.chunks.get_chunk`) into the content-addressed cache
    ``dest/chunks/<hash>`` unless already cached there (cache copies are
    trusted: chunk objects are write-once). :func:`tessera.chunks.assemble`
    then concatenates the blobs into ``db.sqlite`` and truncates to
    ``page_count * page_size`` — the last chunk's zero padding never reaches
    the database. The page size is derived as ``len(blob0) // chunk_pages``
    (every chunk is padded to exactly ``chunk_pages`` pages).

    When the ``.chunks_marker`` key list matches the manifest's chunk keys
    exactly and ``db.sqlite`` exists, the base is already current and the
    call performs zero downloads; deltas resume from ``.applied.json``.
    Otherwise the whole base is rebuilt and the applied list resets BEFORE
    the new marker is published — the same crash-safe order as the v1
    snapshot swap (a crash in between just redoes the rebuild).
    """
    chunks_marker = dest / _CHUNKS_MARKER
    applied_marker = dest / _APPLIED_MARKER
    db_path = dest / _DB_NAME

    want_keys = [entry.key for entry in manifest.chunks]
    if not want_keys:
        raise ValueError("format-2 manifest must reference at least one chunk")

    marker = _read_chunks_marker(chunks_marker)
    if marker is not None and marker["keys"] == want_keys and db_path.exists():
        return _read_applied(applied_marker)  # cached base: zero downloads

    # Fetch missing chunks concurrently in batches of _FETCH_WORKERS —
    # peak memory stays bounded at one batch of blobs regardless of the
    # backlog size; assemble still runs in manifest order.
    missing = [
        entry for entry in manifest.chunks
        if not _chunk_cache(dest, entry.hash).exists()
    ]
    with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        for i in range(0, len(missing), _FETCH_WORKERS):
            batch = missing[i : i + _FETCH_WORKERS]
            for entry, blob in zip(
                batch,
                pool.map(lambda e: get_chunk(backend, tenant, e.hash), batch),
            ):
                _atomic_write(_chunk_cache(dest, entry.hash), blob)
    blobs = [
        _chunk_cache(dest, entry.hash).read_bytes() for entry in manifest.chunks
    ]
    page_size = len(blobs[0]) // manifest.chunk_pages
    assemble(blobs, manifest.page_count, page_size, db_path)

    # Crash-safe order: applied reset BEFORE publishing the marker — a
    # crash in between just redoes this block on the next call.
    _atomic_write(applied_marker, b"[]")
    _atomic_write(
        chunks_marker,
        json.dumps(
            {
                "keys": want_keys,
                "page_count": manifest.page_count,
                "chunk_pages": manifest.chunk_pages,
                "codec": manifest.chunk_codec,
            }
        ).encode("utf-8"),
    )
    return []


# @spec tessera/ensure-local
def ensure_local(
    backend: Backend,
    tenant: str,
    manifest: Manifest,
    dest: str | Path | None = None,
) -> Path:
    """Hydrate the database described by *manifest* into a local directory.

    Format 1: downloads the snapshot (cached by etag as
    ``snap-<etag[:16]>.sqlite``) and copies it to ``db.sqlite``. Format 2:
    rebuilds the compacted base by concatenating chunks (no VFS).
    Both formats then apply each not-yet-applied delta of
    ``manifest.deltas`` in order, persisting ``.applied.json`` after every
    apply. Returns the absolute path to ``db.sqlite``; after it returns,
    ``db.sqlite`` exists and equals base + all deltas.

    *dest* defaults to ``<tempdir>/tessera/<tenant>/`` (i.e. ``/tmp/tessera/
    <tenant>/`` on standard Linux) and is created if missing.

    Lineage: the destination is rebound automatically when the base changes
    — format 1: the manifest's snapshot KEY changes (fresh dest, or a
    re-snapshotted manifest); format 2: the manifest's chunk key list
    changes. In both cases the db is rebuilt from the new base and the
    applied list resets. Within one base the cached content is trusted
    (base objects are write-once by design).
    """
    dest = _default_dest(tenant) if dest is None else Path(dest)
    dest = dest.expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)

    # Restore may remove/reorder deltas without changing content-addressed
    # base chunks. Cached later pages cannot be undone by skipping deltas.
    applied_before = _read_applied(dest / _APPLIED_MARKER)
    desired = [entry.etag for entry in manifest.deltas]
    if applied_before != desired[:len(applied_before)]:
        # Reject a tampered manifest before replacing a valid cached image.
        for entry in manifest.deltas:
            if backend.head(entry.key) != entry.etag:
                raise BackendError(f"delta object {entry.key!r} does not match manifest etag")
        for name in (_SNAPSHOT_MARKER, _CHUNKS_MARKER, "db.sqlite-wal",
                     "db.sqlite-shm", "db.sqlite-journal", _DB_NAME, _APPLIED_MARKER):
            (dest / name).unlink(missing_ok=True)

    # ---- base (format-specific) --------------------------------------------
    if manifest.format == 2:
        applied = _hydrate_chunks_base(backend, tenant, manifest, dest)
    else:
        applied = _hydrate_snapshot_base(backend, manifest, dest)

    applied_marker = dest / _APPLIED_MARKER
    db_path = dest / _DB_NAME

    # ---- deltas (shared) ----------------------------------------------------
    applied_set = set(applied)
    # Fetch the missing delta objects concurrently in batches of
    # _FETCH_WORKERS (write-once + pinned etags make order irrelevant at
    # fetch time). Batching bounds peak memory to one batch of payloads
    # regardless of the backlog; apply stays sequential, in manifest
    # order, with the marker persisted after every apply.
    missing = [e for e in manifest.deltas if e.etag not in applied_set]
    with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        for i in range(0, len(missing), _FETCH_WORKERS):
            batch = missing[i : i + _FETCH_WORKERS]
            fetched = list(pool.map(lambda e: backend.get(e.key), batch))
            for entry, (data, etag) in zip(batch, fetched):
                if etag != entry.etag:
                    raise BackendError(
                        f"delta object {entry.key!r} does not match the manifest: "
                        f"expected etag {entry.etag!r}, object store returned {etag!r} "
                        f"(object drifted or corrupt)"
                    )
                delta = parse(data)  # corrupt payload: ValueError propagates (see module docstring)
                apply_delta(db_path, delta)
                applied.append(entry.etag)
                applied_set.add(entry.etag)
                # Persist BEFORE the next delta so a crash never loses progress.
                _atomic_write(applied_marker, json.dumps(applied).encode("utf-8"))

    return db_path


# @spec tessera/is-fully-local
def is_fully_local(manifest: Manifest, dest: str | Path) -> bool:
    """True when *dest* already holds exactly the committed state of *manifest*.

    Pure predicate used to pick the read path: reads of a fully-local tenant
    hydrate nothing and can go straight to ``dest/db.sqlite``, while reads of
    a not-fully-local tenant can skip hydration entirely (VFS). The check is
    marker reads and ``stat`` calls ONLY — it never downloads anything:

    - format 1: ``db.sqlite`` exists, the ``.snapshot_etag`` marker matches
      the manifest's snapshot key and every manifest delta etag is recorded
      in ``.applied.json``.
    - format 2: ``db.sqlite`` exists, the ``.chunks_marker`` key list matches
      the manifest's chunk keys exactly and every manifest delta etag is
      recorded in ``.applied.json``.

    Missing, corrupt or malformed markers read as ``False`` (the safe
    answer: the caller then falls back to the full hydration path, which is
    always correct, only slower). A ``True`` answer means
    :func:`ensure_local` would perform zero downloads for this manifest.
    """
    dest = Path(dest)
    if not (dest / _DB_NAME).is_file():
        return False
    applied = _read_applied(dest / _APPLIED_MARKER)
    if applied != [entry.etag for entry in manifest.deltas]:
        return False
    if manifest.format == 2:
        marker = _read_chunks_marker(dest / _CHUNKS_MARKER)
        if marker is None:
            return False
        return marker["keys"] == [entry.key for entry in manifest.chunks]
    marker = _read_snapshot_marker(dest / _SNAPSHOT_MARKER)
    if marker is None:
        return False
    return marker["key"] == manifest.snapshot
