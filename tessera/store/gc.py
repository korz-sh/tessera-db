# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Garbage collection - mark & sweep over live manifests and history."""

import logging
from concurrent.futures import ThreadPoolExecutor

from ..backends.base import BackendError, NotFound
from ..cache_lock import cache_access
from ..manifest import Manifest
from .exceptions import TenantNotFound
from .utils import _history_version, _manifest_key, _snapshot_version

logger = logging.getLogger(__name__)

# Deletes flush to the backend in chunks of this size: one ensure_alive
# plus one bulk request per chunk instead of per object.
_GC_DELETE_BATCH = 500

# Object fetches during the mark phase are independent and write-once;
# concurrency only shortens wall time, it cannot change the result set.
_FETCH_WORKERS = 16


# @spec store/gcmixin
# @implements durable-write-contract/maintenance-preserves-committed-results
# @implements durable-write-contract/consistent-recoverable-history
# @implements bounded-write-cost/grace-window-protects-young-objects
# @implements bounded-write-cost/retained-storage-is-bounded
# @implements tenant-recovery/complete-historical-state
class GCMixin:
    """Garbage collection mixed into :class:`Store`."""

    @cache_access
    # @implements bounded-write-cost/fenced-garbage-collection-on-object-storage
    def gc(self, tenant: str, *, dry_run: bool = False) -> dict[str, list[str]]:
        """Collect unreferenced objects while holding the tenant lease.

        Mark snapshots, deltas and chunks from the current manifest and
        EVERY archived manifest before sweeping. If any listed history
        cannot be read or parsed, abort before deleting anything.

        Unreferenced objects are eligible only after ``gc_delta_age_s``;
        unknown mtimes are preserved. The newest ``gc_keep_snapshots``
        snapshot versions are additionally retained. Object age is not a
        reader pin: an old object can be collected after its last history
        reference expires, even if a reader only just fetched that history.

        History retention keeps the UNION of the newest ``gc_keep_history``
        versions and archives younger than ``history_retention_s`` (unknown
        archive mtimes are preserved when time retention is enabled).
        Pruning occurs after marking: references from pruned archives become
        collectible on a subsequent run. Retained history protects v1 and
        v2 objects equally, independently of the snapshot count floor.

        Destructive GC requires a backend satisfying the deletion fencing
        contract (``supports_fenced_gc``): LocalBackend serializes deletes
        atomically against lease takeover; S3DynamoBackend bounds every
        delete by the lease skew margin — valid because the mark below
        covers the COMPLETE closure of committed manifests (current +
        every archived history, all of which are CAS-ordered before their
        successors), and manifests only evolve forward through CAS, so no
        successor can reference a swept object. ``dry_run=True`` works on
        any backend, takes no lease and reports advisory ``*_candidates``
        lists instead of ``*_deleted`` (and ``history_candidates`` instead
        of ``history_pruned``). A plan is not authorization to execute its
        candidates later.

        Returns ``deltas_deleted``, ``snapshots_deleted``, ``chunks_deleted``
        and, when nonempty, ``history_pruned`` lists. Backend, manifest and
        lease errors propagate; already completed deletes are not undone.
        """
        # Async history archives must land before marking (they ARE the
        # reference set that protects recent readers).
        if not dry_run and not self._backend.supports_fenced_gc:
            raise BackendError("backend cannot fence GC deletion; use dry_run=True")
        self._drain_history_archives()
        handle = None if dry_run else self._acquire(tenant)
        if handle is not None:
            handle.ensure_alive()

        # Deletes go out in batches: one ensure_alive per batch (the skew
        # margin dwarfs a batch's duration, so liveness once per batch is
        # the same bound as once per delete) and one bulk request on
        # backends that offer it — thousands of one-by-one S3 DELETEs
        # would dominate a sweep's cost.
        pending: list[str] = []

        def flush_pending() -> None:
            if handle is not None and pending:
                handle.ensure_alive()
                self._backend.gc_delete_batch(list(pending), handle.lease)
            pending.clear()

        def delete_candidate(key):
            pending.append(key)
            if len(pending) >= _GC_DELETE_BATCH:
                flush_pending()

        try:
            data, _etag = self._backend.get(_manifest_key(tenant))
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        manifest = Manifest.from_json(data.decode("utf-8"))

        now = self._clock()
        report: dict[str, list[str]] = {
            "deltas_deleted": [],
            "snapshots_deleted": [],
            "chunks_deleted": [],
        }

        # ---- MARK ---------------------------------------------------------
        # Every object referenced by the current manifest OR by any archived
        # history manifest is live: readers may hold any published version,
        # and a compaction only drops references from the CURRENT manifest.
        referenced_deltas = {entry.key for entry in manifest.deltas}
        referenced_snapshots = {manifest.snapshot} if manifest.format == 1 else set()
        referenced_chunks = (
            {entry.key for entry in manifest.chunks} if manifest.format == 2 else set()
        )
        history_keys = self._backend.list(f"{tenant}/history/")
        # The archives are independent write-once objects: fetch them
        # concurrently — same requests, bounded workers. A fetch/parse
        # failure still propagates here, before ANY delete is issued.
        with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
            history_raws = list(
                pool.map(lambda k: self._backend.get(k)[0], history_keys)
            )
        for raw in history_raws:
            old = Manifest.from_json(raw.decode("utf-8"))
            referenced_deltas.update(entry.key for entry in old.deltas)
            if old.format == 1:
                referenced_snapshots.add(old.snapshot)
            if old.format == 2:
                referenced_chunks.update(entry.key for entry in old.chunks)

        # ---- SWEEP (grace/mtime rules unchanged) --------------------------
        # Deltas: anything under wal/ not referenced by any live manifest,
        # older than the grace window.
        for key in self._backend.list(f"{tenant}/wal/"):
            if key in referenced_deltas:
                continue
            if not self._past_grace(key, now):
                continue
            delete_candidate(key)
            report["deltas_deleted"].append(key)

        # Snapshots: history references are live regardless of the separate
        # keep-K floor, including v1 history after migration to v2.
        snap_keys = self._backend.list(f"{tenant}/snap/")
        versions = sorted(
            {v for k in snap_keys if (v := _snapshot_version(k)) is not None},
            reverse=True,
        )
        keep = set(versions[: max(self._gc_keep_snapshots, 0)])
        current_version = _snapshot_version(manifest.snapshot)
        if current_version is not None:
            keep.add(current_version)
        for key in snap_keys:
            version = _snapshot_version(key)
            if version is None or version in keep or key in referenced_snapshots:
                continue
            if not self._past_grace(key, now):
                continue
            delete_candidate(key)
            report["snapshots_deleted"].append(key)

        # Chunks: anything under chunks/ not referenced by any live manifest
        # (current + history), past the grace window.
        for key in self._backend.list(f"{tenant}/chunks/"):
            if key in referenced_chunks:
                continue
            if not self._past_grace(key, now):
                continue
            delete_candidate(key)
            report["chunks_deleted"].append(key)

        # ---- PRUNE history (metadata: count-based, immediate) -------------
        # Keep the newest gc_keep_history versions; delete older history
        # objects right away. History is never age-swept — this is the only
        # way history shrinks. From the NEXT gc on, objects referenced only
        # by pruned versions are collectible (this run already marked them).
        versions = sorted(
            {v for k in history_keys if (v := _history_version(k)) is not None},
            reverse=True,
        )
        keep_versions = set(versions[: max(self._gc_keep_history, 0)])
        # TIME-based retention in addition to count-based. Counting versions
        # makes the window depend on traffic: with 24 versions, a tenant
        # writing 10 times per second keeps ~2 seconds of history, and the
        # accidental DELETE someone discovers 20 minutes later is already
        # unrecoverable. Whatever EITHER criterion saves is kept: the count
        # is the floor (an idle tenant does not lose its history to age) and
        # the time is the real ceiling promised to a human.
        if self._history_retention_s > 0:
            cutoff = self._clock() - self._history_retention_s
            for key in history_keys:
                version = _history_version(key)
                if version is None or version in keep_versions:
                    continue
                mtime = self._backend.mtime(key)
                if mtime is None or mtime >= cutoff:
                    # No mtime means keep it: deleting something because it
                    # cannot be dated would destroy the only copy of a
                    # recoverable state.
                    keep_versions.add(version)
        pruned: list[str] = []
        for key in history_keys:
            version = _history_version(key)
            if version is None or version in keep_versions:
                continue
            delete_candidate(key)
            pruned.append(key)
        if pruned:
            # Present only when non-empty: the three legacy report keys keep
            # their exact shape for existing consumers.
            report["history_pruned"] = pruned

        flush_pending()
        if dry_run:
            return {key.replace("_deleted", "_candidates").replace("_pruned", "_candidates"): value
                    for key, value in report.items()}
        self._stamp_used(tenant, handle)
        return report

    def _past_grace(self, key: str, now: float) -> bool:
        """True when *key*'s backend mtime is older than ``gc_delta_age_s``.

        Unknown mtime (``None``) is never past the grace window: the
        conservative GC contract deletes nothing whose age it cannot prove.
        """
        mtime = self._backend.mtime(key)
        if mtime is None:
            return False
        return now - mtime >= self._gc_delta_age_s
