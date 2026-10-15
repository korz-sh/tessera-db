# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Tenant-facing store for tessera-db.

:class:`Store` glues the object-storage backend, per-tenant leases, the TSD1
delta codec and local hydration into the two operations callers use:
:meth:`Store.write` (append one committed SQLite transaction) and
:meth:`Store.read` (hydrated access to the committed state).

Object layout (one backend namespace per deployment)::

    <tenant>/manifest.json              the ONE mutable object (CAS if-match)
    <tenant>/snap/<version>.sqlite      write-once base snapshot (format 1)
    <tenant>/wal/<epoch>-<seq>.delta    write-once deltas (TSD1 frame sets)
    <tenant>/chunks/<hh>/<hash>         write-once compacted-base chunks (format 2)

Manifest format 2 replaces the base snapshot with a content-addressed chunk
set (``tessera.chunks``): writes still append deltas on top; reads of v2
tenants that are not fully local are served PAGE BY PAGE through the
M6 :class:`~tessera.vfs.TesseraVFS` (base chunks downloaded on demand into a
global content-addressed cache, deltas overlaid in memory), while v1 tenants
and fully-local v2 tenants keep the M5 hydration path
(:func:`~tessera.local.ensure_local` + :mod:`sqlite3`).

Write-path order (design-l0 §7 semantics):

1. ``ensure_alive`` — fence on a live lease (or use a caller-supplied
   :class:`~tessera.backends.base.Lease` and skip lease ops entirely).
2. manifest fetch + epoch fence — fetch the current manifest and reject a
   stale writer when ``manifest.last_epoch > lease.epoch`` with
   :class:`Conflict`.
3. local sync — :func:`~tessera.local.ensure_local` hydrates the snapshot +
   delta chain into ``<root>/<tenant>/db.sqlite``.
4. transaction — run the caller's ``fn(conn)`` on a WAL-mode connection; the
   caller commits inside ``fn``.
5. delta PUT write-once — read the committed frames straight out of the
   ``-wal`` file (BEFORE the connection closes: the last connection to close
   checkpoints and deletes the WAL) and upload them create-only.
6. manifest CAS — publish the new manifest with ``if_match`` on the previous
   etag and ``last_epoch`` set to the lease epoch.
7. On CAS failure the local directory is discarded (``rmtree``) and
   :class:`Conflict` is raised — ``fn`` is NEVER retried, and the local
   database is never allowed to drift ahead of the committed manifest.

Reads never touch lease ops: they use a single-HEAD etag cache per tenant
(the If-None-Match pattern), optionally poll until ``min_version`` becomes
visible (:class:`StaleRead` on deadline), then serve ``fn`` a connection —
through the VFS (read-only) for v2 tenants that are not fully local, or via
local hydration for v1 tenants and fully-local v2 tenants.

An LRU pass runs at the start of every write/read: when the cache root
exceeds ``lru_bytes``, the least-recently-used tenant directories are
removed (never the tenant currently in flight).

Compaction (:meth:`Store.compact`) folds the delta chain into a fresh
write-once snapshot (format 1) or fully re-chunks the compacted base
(format 2) under the lease; :meth:`Store.compact_incremental` folds the
delta chain of a format-2 tenant into its chunk base WITHOUT a VACUUM
rewrite — only delta-touched chunks are rebuilt and re-uploaded
(content addressing dedupes everything else); :meth:`Store.migrate_to_v2`
converts a v1 tenant to format 2 (total compaction + rechunk). Garbage
collection (:meth:`Store.gc`) is a mark & sweep over the LIVE manifests —
the current one plus the archived history of replaced manifests under
``<tenant>/history/<version>.json`` (the newest ``gc_keep_history`` kept):
every object any of them references survives; what remains is deleted past
the ``gc_delta_age_s`` grace window, so readers holding any recently
published manifest keep their objects. Successful writes trigger compaction
inline when the chain grows past ``inline_compact_deltas`` deltas or
``inline_compact_bytes`` bytes.
"""

from ..chunks import CHUNK_PAGES
from .core import Store
from .exceptions import (
    Conflict,
    StaleRead,
    TenantExists,
    TenantNotFound,
    VersionUnavailable,
)
from .utils import _delta_key, _manifest_key, _snapshot_key

__all__ = [
    "Conflict",
    "StaleRead",
    "Store",
    "TenantExists",
    "TenantNotFound",
    "VersionUnavailable",
]
