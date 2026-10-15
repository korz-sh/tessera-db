# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Compaction - migrate_to_v2, rechunk, full and incremental compaction."""

import hashlib
import json
import logging
import sqlite3

from ..backends.base import BackendError, CASFailed, NotFound
from ..cache_lock import cache_access
from ..chunks import (
    assemble,
    decode_chunk,
    encode_chunk,
    page_size_of,
    put_if_absent,
    split,
)
from ..delta import parse as parse_delta
from ..lease import LeaseHandle
from ..local import (
    _APPLIED_MARKER,
    _CHUNKS_MARKER,
    _SNAPSHOT_MARKER,
    _atomic_write,
    _snap_file,
    ensure_local,
)
from ..manifest import ChunkEntry, Manifest
from ..publication import publish_manifest
from .exceptions import Conflict, TenantNotFound
from .utils import _canonicalize_vacuum_header, _manifest_key, _snapshot_key


def _chunk_pages() -> int:
    """Resolve ``CHUNK_PAGES`` through the ``tessera.store`` package.

    Tests shrink the chunk size via ``monkeypatch.setattr(
    "tessera.store.CHUNK_PAGES", N)``; resolving through the package
    namespace at call time preserves that seam — a module-level ``from
    ..chunks import CHUNK_PAGES`` would freeze the value at import time.
    """
    from . import CHUNK_PAGES

    return CHUNK_PAGES

logger = logging.getLogger(__name__)


# @spec store/compact-mixin
class CompactMixin:
    """Compaction operations mixed into :class:`Store`."""

    @cache_access
    def migrate_to_v2(self, tenant: str) -> Manifest:
        """Migrate *tenant* to manifest format 2.

        Full compaction + rechunk under a lease; format 2 reads are
        concatenation until M6's VFS. Under a live lease: fetch the
        manifest, hydrate it locally, run ``VACUUM INTO`` for a fully
        compacted copy, split it into ``CHUNK_PAGES``-page chunks, upload
        each create-only (content-addressed, deduped) and publish a
        format-2 manifest whose ``chunks`` replace the snapshot
        (``snapshot=""``, ``deltas=[]``, ``compacted_version`` = the
        migrated v1 version).

        Idempotent: a tenant already on format 2 returns its current
        manifest unchanged (no version bump, no re-chunk). CAS failure
        raises :class:`Conflict` with no retry; crash-safety is
        :meth:`_rechunk`'s (shared with the format-2 branch of
        :meth:`compact`).

        Raises :class:`TenantNotFound` (no manifest), :class:`Conflict`
        (stale epoch or a lost manifest CAS) and
        :class:`~tessera.backends.base.LeaseLost`.
        """
        handle = self._acquire(tenant)
        handle.ensure_alive()

        manifest_key = _manifest_key(tenant)
        try:
            data, etag = self._backend.get(manifest_key)
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        manifest = Manifest.from_json(data.decode("utf-8"))
        if manifest.format == 2:
            return manifest  # already migrated: idempotent no-op
        if manifest.last_epoch > handle.lease.epoch:
            raise Conflict(
                f"stale writer for tenant {tenant!r}: manifest last_epoch "
                f"{manifest.last_epoch} > lease epoch {handle.lease.epoch}"
            )
        return self._rechunk(manifest, etag, handle, compacted_version=manifest.version)

    def _rechunk(
        self,
        manifest: Manifest,
        etag: str,
        handle: LeaseHandle,
        *,
        compacted_version: int,
    ) -> Manifest:
        """Shared compaction + rechunk core (migrate_to_v2 / compact on v2).

        The lease must already be alive and the epoch fence passed. Steps:

        1. Local sync of *manifest*'s state into the tenant directory.
        2. ``VACUUM INTO`` a temp file in the tenant dir (a stale temp from
           a crashed attempt is deleted first); ``page_count`` is derived
           from the compacted file size.
        3. ``split`` into ``CHUNK_PAGES``-page chunks and upload each
           create-only via :func:`tessera.chunks.put_if_absent` (codec
           ``"none"``). Unchanged chunks produce identical hashes, so
           re-chunking an unmodified database uploads nothing new.
        4. Manifest CAS (``if_match`` on *etag*) publishing format 2 with
           ``compacted_version`` as given by the caller. Failure raises
           :class:`Conflict` and does NOT retry: the old manifest stays
           current and consistent (v1 snapshot + deltas, or the previous
           chunk set); uploaded-but-unreferenced chunks fall to ``gc``.
        5. Local refresh LAST (crash-safe, mirroring
           :func:`~tessera.local.ensure_local`'s own order): ``db.sqlite``
           rebuilt from the chunk blobs so it becomes the chunk base itself
           (identical page layout — the next write's WAL frames must be
           captured against the base readers apply deltas onto), applied
           list reset, ``.chunks_marker`` published, chunk cache seeded so
           the next hydration does zero downloads, stale ``.snapshot_etag``
           removed.

        Old chunks become unreferenced by the new manifest — deleting them
        is ``gc``'s job, never this method's.
        """
        tenant = handle.lease.tenant
        epoch = handle.lease.epoch
        dest = self._root / tenant
        chunk_pages = _chunk_pages()

        # 1. Local sync: current committed state -> db.sqlite.
        db_path = ensure_local(self._backend, tenant, manifest, dest=dest)

        # 2. Fully compacted copy. VACUUM INTO fails if the target exists,
        # so a stale temp (crashed previous attempt) is deleted first.
        tmp_target = dest / f".rechunk-{manifest.version}.sqlite"
        try:
            tmp_target.unlink()
        except FileNotFoundError:
            pass
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("VACUUM INTO ?", (str(tmp_target),))
        finally:
            conn.close()
        try:
            # Pin free-running header counters (schema cookie et al) BEFORE
            # hashing: an unchanged database must re-chunk to identical
            # chunk hashes (dedupe) even though db.sqlite now holds the
            # previous chunk base itself (see step 5) and every VACUUM
            # bumps the cookie.
            _canonicalize_vacuum_header(tmp_target)
            page_size = page_size_of(tmp_target)
            page_count = tmp_target.stat().st_size // page_size
            chunk_pairs = split(tmp_target, chunk_pages=chunk_pages)
        finally:
            tmp_target.unlink(missing_ok=True)

        # 3. Chunk upload, create-only, dedup by content address.
        # put_if_absent hides the fresh-put etag, so the object's current
        # etag is resolved with one HEAD per chunk (always cheap, honest for
        # any backend; on the dedupe path this is the spec'd behavior).
        codec = "none"
        chunk_entries: list[ChunkEntry] = []
        for chunk_hash, blob in chunk_pairs:
            key, _existed = put_if_absent(
                self._backend, tenant, encode_chunk(blob, codec)
            )
            chunk_etag = self._backend.head(key)
            if chunk_etag is None:
                raise BackendError(
                    f"chunk {key!r} missing right after upload for tenant {tenant!r}"
                )
            chunk_entries.append(
                ChunkEntry(key=key, etag=chunk_etag, size=len(blob), hash=chunk_hash)
            )

        # 4. Manifest CAS (if_match). Failure leaves the old manifest —
        # which still references its old base + deltas — consistent.
        new_version = manifest.version + 1
        new_manifest = Manifest(
            format=2,
            version=new_version,
            snapshot="",
            deltas=[],
            last_epoch=epoch,
            chunks=chunk_entries,
            chunk_pages=chunk_pages,
            page_count=page_count,
            compacted_version=compacted_version,
            chunk_codec=codec,
            # A rebase covers exactly the commits the old manifest covered:
            # the object version advances (history uniqueness) but the
            # commit frontier does not.
            data_version=manifest.data_version,
        )
        try:
            publish_manifest(
                self._backend, tenant, manifest, new_manifest, etag,
                before_cas=handle.ensure_alive,
            )
        except CASFailed:
            self._manifest_cache.pop(tenant, None)
            raise Conflict(
                f"manifest CAS failed for tenant {tenant!r} "
                f"(version {manifest.version} -> {new_version})"
            ) from None
        self._manifest_cache.pop(tenant, None)

        # 5. Local refresh LAST (crash-safe): applied reset BEFORE the
        # marker swap (ensure_local's own order), chunk cache seeded for
        # zero-download hydration, stale v1 snapshot marker dropped.
        #
        # db.sqlite is rebuilt from the chunk blobs FIRST so it BECOMES the
        # chunk base (byte-identical page layout), not merely a logically
        # equivalent file left over from before the VACUUM: the next write
        # runs its transaction on db.sqlite and captures WAL frames there,
        # and readers apply those frames on top of the chunked base — M5's
        # apply_delta and M6's page-level VFS both require identical page
        # numbering between the delta and the base it lands on. Crash before
        # the marker swap: the old marker does not match the new manifest, so
        # the next ensure_local simply rebuilds db.sqlite from the chunks.
        assemble(
            [blob for _chunk_hash, blob in chunk_pairs],
            page_count,
            page_size,
            db_path,
        )
        _atomic_write(dest / _APPLIED_MARKER, b"[]")
        _atomic_write(
            dest / _CHUNKS_MARKER,
            json.dumps(
                {
                    "keys": [entry.key for entry in chunk_entries],
                    "page_count": page_count,
                    "chunk_pages": chunk_pages,
                    "codec": codec,
                }
            ).encode("utf-8"),
        )
        for entry, (_chunk_hash, blob) in zip(chunk_entries, chunk_pairs):
            _atomic_write(dest / "chunks" / entry.hash, blob)
        (dest / _SNAPSHOT_MARKER).unlink(missing_ok=True)

        self._stamp_used(tenant, handle)
        return new_manifest

    @cache_access
    def compact(self, tenant: str) -> Manifest:
        """Fold the tenant's delta chain into a fresh write-once base.

        Format 1: sync the local database, run ``VACUUM INTO`` to produce a
        compacted copy, upload it create-only as
        ``<tenant>/snap/<version + 1>.sqlite`` and publish a manifest CAS
        that points at the new snapshot with an empty ``deltas`` list.
        Format 2: full rechunk — delegate to :meth:`_rechunk` (new version,
        ``compacted_version`` pinned to the version being committed, i.e.
        the manifest version whose chunks reflect a full compaction); the
        chunk set replaces the snapshot, ``deltas`` empties, and unchanged
        chunk hashes dedupe on upload (re-chunking an unmodified database
        uploads nothing new). Returns the new
        :class:`~tessera.manifest.Manifest`.

        Crash safety:

        - The snapshot upload is create-only. If the key already exists (a
          previous attempt crashed between the snapshot PUT and the manifest
          CAS), the stored object is verified against our compacted bytes;
          equal content is reused (its stored etag becomes the snapshot
          etag), anything else raises :class:`Conflict`.
        - A failed manifest CAS raises :class:`Conflict` and does NOT retry:
          the old manifest stays current and still references the old
          snapshot + full delta chain, so everything remains consistent.
          The orphaned snapshot is reclaimed by ``gc``.
        - Local markers move LAST, in :func:`~tessera.local.ensure_local`'s
          own crash-safe order (applied list resets before the snapshot
          marker swaps): ``db.sqlite`` is the VACUUM source, i.e. logically
          equivalent to the new snapshot, so the next ``ensure_local`` sees
          the marker matching the current manifest and does no work.

        Raises :class:`TenantNotFound` (no manifest), :class:`Conflict`
        (stale epoch, snapshot key collision with different content, or a
        lost manifest CAS) and :class:`~tessera.backends.base.LeaseLost`.
        """
        handle = self._acquire(tenant)
        handle.ensure_alive()
        epoch = handle.lease.epoch

        manifest_key = _manifest_key(tenant)
        try:
            data, etag = self._backend.get(manifest_key)
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        manifest = Manifest.from_json(data.decode("utf-8"))
        if manifest.last_epoch > epoch:
            raise Conflict(
                f"stale writer for tenant {tenant!r}: manifest last_epoch "
                f"{manifest.last_epoch} > lease epoch {epoch}"
            )

        if manifest.format == 2:
            return self._rechunk(
                manifest, etag, handle, compacted_version=manifest.version + 1
            )

        # 1. Local sync: snapshot + delta chain -> db.sqlite.
        dest = self._root / tenant
        db_path = ensure_local(self._backend, tenant, manifest, dest=dest)

        # 2. Compacted copy. VACUUM INTO fails if the target exists, so a
        # stale temp (crashed previous attempt) is deleted first; the temp
        # lives in the same directory and is removed after reading.
        new_version = manifest.version + 1
        snap_key = _snapshot_key(tenant, new_version)
        tmp_target = dest / f".compact-{new_version}.sqlite"
        try:
            tmp_target.unlink()
        except FileNotFoundError:
            pass
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("VACUUM INTO ?", (str(tmp_target),))
        finally:
            conn.close()
        try:
            snapshot_data = tmp_target.read_bytes()
        finally:
            tmp_target.unlink(missing_ok=True)

        # 3. Snapshot PUT write-once; CASFailed = key already exists (the
        # crash-recovery case above): verify content, reuse the stored etag.
        try:
            snap_etag = self._backend.put(snap_key, snapshot_data, if_none_match=True)
        except CASFailed:
            try:
                existing, existing_etag = self._backend.get(snap_key)
            except NotFound:
                raise Conflict(
                    f"snapshot key {snap_key!r} vanished while verifying "
                    f"it for tenant {tenant!r}"
                ) from None
            if existing != snapshot_data:
                raise Conflict(
                    f"snapshot key {snap_key!r} already exists with "
                    f"different content for tenant {tenant!r}"
                ) from None
            snap_etag = existing_etag

        # 4. Manifest CAS (if_match). Failure leaves the old manifest —
        # which still references the old snapshot + deltas — consistent.
        new_manifest = Manifest(
            version=new_version, snapshot=snap_key, deltas=[], last_epoch=epoch,
            data_version=manifest.data_version,
        )
        try:
            publish_manifest(
                self._backend, tenant, manifest, new_manifest, etag,
                before_cas=handle.ensure_alive,
            )
        except CASFailed:
            self._manifest_cache.pop(tenant, None)
            raise Conflict(
                f"manifest CAS failed for tenant {tenant!r} "
                f"(version {manifest.version} -> {new_version})"
            ) from None
        self._manifest_cache.pop(tenant, None)

        # 5. Local markers LAST (crash-safe order, mirroring ensure_local):
        # seed the content-addressed snapshot cache so subsequent reads and
        # writes hydrate with zero downloads, reset the applied list, then
        # swap the snapshot marker.
        _atomic_write(_snap_file(dest, snap_etag), snapshot_data)
        _atomic_write(dest / _APPLIED_MARKER, b"[]")
        _atomic_write(
            dest / _SNAPSHOT_MARKER,
            json.dumps({"key": snap_key, "etag": snap_etag}).encode("utf-8"),
        )

        self._stamp_used(tenant, handle)
        return new_manifest

    @cache_access
    def compact_incremental(self, tenant: str) -> Manifest:
        """Fold the tenant's delta chain into its chunk base INCREMENTALLY.

        Format 2 only: every delta's frames are applied in order onto an
        in-memory ``pgno -> page`` overlay (newest wins), and only the chunks
        TOUCHED by at least one overlaid page are rebuilt from their stored
        base plus the overlay — untouched chunks are carried into the new
        manifest verbatim and are never re-uploaded (content addressing:
        unchanged content hashes to the same chunk address, so
        :func:`tessera.chunks.put_if_absent` dedupes and a compaction that
        changes nothing uploads zero bytes).

        - Growth: a delta that extended the database (``delta.page_count``
          beyond ``manifest.page_count``) grows the chunk list to
          ``ceil(page_count_new / chunk_pages)``; trailing chunks the deltas
          never wrote are stored as all-zero pages.
        - Shrink: a delta whose committed image is SMALLER than the current
          base (a user ``VACUUM`` run inside a write fn wrote a smaller
          image to the WAL) drops the chunk entries beyond the new
          ``ceil(page_count_new / chunk_pages)`` coverage.
        - Format 1 tenants have no chunk base to merge into and take the
          FULL compaction path (:meth:`compact`); a v2 tenant with no deltas
          is an idempotent no-op (manifest returned unchanged).

        The result is CONTENT-equivalent to a full ``VACUUM`` compaction but
        NOT byte-equivalent: pages keep their identity and no VACUUM
        reorganization happens (free-space reshuffling is
        :meth:`compact`/:meth:`_rechunk`'s job).

        Crash safety mirrors :meth:`_rechunk`: chunk uploads are create-only
        and content-addressed (orphans fall to ``gc``); a failed
        manifest CAS raises :class:`Conflict` and does NOT retry — nothing
        local changed, and the old manifest still references its old chunks
        plus the full delta chain, so everything remains consistent. On
        success the tenant's warm VFS sessions are released (their delta
        overlay is folded into the base, so the next registration starts
        from ``deltas=[]`` with zero downloads) and the local
        ``.chunks_marker``/``.applied.json`` markers are dropped best-effort
        (``db.sqlite`` is kept as-is: the next
        :func:`~tessera.local.ensure_local` rebuilds it from the new chunk
        set).

        Raises :class:`TenantNotFound` (no manifest), :class:`Conflict`
        (stale epoch or a lost manifest CAS) and
        :class:`~tessera.backends.base.LeaseLost`.
        """
        handle = self._acquire(tenant)
        handle.ensure_alive()
        epoch = handle.lease.epoch

        manifest_key = _manifest_key(tenant)
        try:
            data, etag = self._backend.get(manifest_key)
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        manifest = Manifest.from_json(data.decode("utf-8"))
        if manifest.last_epoch > epoch:
            raise Conflict(
                f"stale writer for tenant {tenant!r}: manifest last_epoch "
                f"{manifest.last_epoch} > lease epoch {epoch}"
            )

        if manifest.format == 1:
            # No chunk base to merge into: full compaction (VACUUM INTO +
            # rechunk). compact() re-fences under the same retained lease.
            return self.compact(tenant)
        if not manifest.deltas:
            return manifest  # nothing to fold: idempotent no-op

        # 1. Download + parse the whole delta chain in order; the overlay is
        # a plain pgno -> page dict applied in order (newest wins). page_size
        # is derived from the stored chunk size (exact for codec "none"),
        # and every delta must agree with it or the delta does not belong to
        # this base.
        deltas = []
        for entry in manifest.deltas:
            raw, _delta_etag = self._backend.get(entry.key)
            deltas.append(parse_delta(decode_chunk(raw, manifest.chunk_codec)))
        page_size = manifest.chunks[0].size // manifest.chunk_pages
        for delta in deltas:
            if delta.page_size != page_size:
                raise BackendError(
                    f"tenant {tenant!r}: delta page_size {delta.page_size} "
                    f"!= chunk base page_size {page_size} (delta does not "
                    f"belong to this chunk base)"
                )
        overlay: dict[int, bytes] = {}
        for delta in deltas:
            for pgno, page in delta.frames:
                overlay[pgno] = page
        # The FINAL committed image governs the new page count: each delta's
        # TSD1 commit frame records the database size after that commit, so
        # the newest delta's page_count is the true post-image size. It may
        # exceed manifest.page_count (growth) or fall below it (a user
        # VACUUM inside a write fn wrote a smaller image to the WAL — the
        # SHRINK case below). An earlier delta's larger page_count belongs
        # to a superseded image and must not resurrect pages the final image
        # dropped, and manifest.page_count must not floor the result — that
        # would make a shrunk chain never drop its tail chunks.
        page_count_new = deltas[-1].page_count

        # 2. Touched chunk indices (ascending); on shrink, chunks beyond the
        # new coverage are dropped, never rebuilt.
        needed = -(-page_count_new // manifest.chunk_pages)  # ceil coverage
        touched = sorted(
            idx
            for idx in {(pgno - 1) // manifest.chunk_pages for pgno in overlay}
            if idx < needed
        )
        # Overlay pages grouped per touched chunk, in page order.
        pages_by_chunk: dict[int, list[tuple[int, bytes]]] = {}
        for pgno in sorted(overlay):
            pages_by_chunk.setdefault(
                (pgno - 1) // manifest.chunk_pages, []
            ).append((pgno, overlay[pgno]))

        def _publish(chunk_bytes: bytes) -> ChunkEntry:
            """Encode + upload one chunk create-only; return its entry.

            put_if_absent hides the fresh-put etag, so the object's current
            etag is resolved with one HEAD per chunk (always cheap, honest
            for any backend; on the dedupe path this is the spec'd behavior
            — same contract as :meth:`_rechunk`). The entry hash is the
            sha256 of the UNCOMPRESSED content (zero padding included), per
            the ``ChunkEntry.hash`` contract the VFS verifies on read.
            """
            blob = encode_chunk(chunk_bytes, manifest.chunk_codec)
            key, _existed = put_if_absent(self._backend, tenant, blob)
            chunk_etag = self._backend.head(key)
            if chunk_etag is None:
                raise BackendError(
                    f"chunk {key!r} missing right after upload for tenant {tenant!r}"
                )
            return ChunkEntry(
                key=key,
                etag=chunk_etag,
                size=len(blob),
                hash=hashlib.sha256(chunk_bytes).hexdigest(),
            )

        # 3. Rebuild each touched chunk: stored base (hash-verified after
        # decode) or zeros beyond the old base, overlay pages spliced in at
        # their page offsets, then padded/truncated to exactly
        # chunk_pages * page_size. Unchanged content hashes identically and
        # dedupes on upload (zero bytes for untouched-equivalent chunks).
        rebuilt: dict[int, ChunkEntry] = {}
        chunk_len = manifest.chunk_pages * page_size
        for idx in touched:
            if idx < len(manifest.chunks):
                entry = manifest.chunks[idx]
                raw, _chunk_etag = self._backend.get(entry.key)
                base = decode_chunk(raw, manifest.chunk_codec)
                if hashlib.sha256(base).hexdigest() != entry.hash:
                    raise BackendError(f"chunk hash mismatch: {entry.key}")
            else:
                base = bytes(chunk_len)  # growth tail: all-zero base
            buf = bytearray(base)
            for pgno, page in pages_by_chunk[idx]:
                off = ((pgno - 1) % manifest.chunk_pages) * page_size
                buf[off : off + page_size] = page
            del buf[chunk_len:]  # exactly chunk_pages * page_size (zero-pad)
            buf.extend(b"\x00" * (chunk_len - len(buf)))
            rebuilt[idx] = _publish(bytes(buf))

        # 4. Merge: rebuilt touched chunks + carried untouched chunks, in
        # index order; growth appends zero chunks the deltas never wrote,
        # shrink has already cut the loop short at ``needed``.
        new_chunks: list[ChunkEntry] = []
        for idx in range(needed):
            if idx in rebuilt:
                new_chunks.append(rebuilt[idx])
            elif idx < len(manifest.chunks):
                new_chunks.append(manifest.chunks[idx])
            else:
                new_chunks.append(_publish(bytes(chunk_len)))

        # 5. Manifest CAS (if_match). Failure leaves the old manifest —
        # which still references its old chunks + full delta chain —
        # consistent; nothing local was touched.
        new_version = manifest.version + 1
        new_manifest = Manifest(
            format=2,
            version=new_version,
            snapshot="",
            deltas=[],
            last_epoch=epoch,
            chunks=new_chunks,
            chunk_pages=manifest.chunk_pages,
            page_count=page_count_new,
            compacted_version=new_version,
            chunk_codec=manifest.chunk_codec,
            data_version=manifest.data_version,
        )
        try:
            publish_manifest(
                self._backend, tenant, manifest, new_manifest, etag,
                before_cas=handle.ensure_alive,
            )
        except CASFailed:
            self._manifest_cache.pop(tenant, None)
            raise Conflict(
                f"manifest CAS failed for tenant {tenant!r} "
                f"(version {manifest.version} -> {new_version})"
            ) from None
        self._manifest_cache.pop(tenant, None)

        # 6. Success: the delta chain is folded into the base, so warm VFS
        # sessions (old base + live overlay) are stale — release them all;
        # the next registration starts from deltas=[] (zero downloads).
        # Local markers describe the OLD base + deltas, so drop them
        # best-effort (db.sqlite is KEPT: the next ensure_local rebuilds it
        # from the new chunk set).
        if self._vfs is not None:
            for cache_key in [k for k in self._vfs_keys if k[0] == tenant]:
                session_key = self._vfs_keys.pop(cache_key, None)
                if session_key is not None:
                    try:
                        self._vfs.release(session_key)
                    except Exception:
                        pass
        dest = self._root / tenant
        (dest / _CHUNKS_MARKER).unlink(missing_ok=True)
        (dest / _APPLIED_MARKER).unlink(missing_ok=True)

        self._stamp_used(tenant, handle)
        return new_manifest
