# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Store core - shared state, lifecycle and cross-cutting helpers.

The operation bodies live in the mixins this class composes:
:class:`~.write.WriteMixin` (create/write/VFS write),
:class:`~.read.ReadMixin` (read/VFS read/history/restore),
:class:`~.compact.CompactMixin` (migrate/compact/rechunk) and
:class:`~.gc.GCMixin` (mark & sweep).
"""

import logging
import os
import shutil
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

from ..backends.base import Backend, BackendError, LeaseLost, NotFound
from ..cache_lock import cache_access
from ..lease import LeaseHandle
from ..local import _OWNER_MARKER
from ..manifest import Manifest
from .adapters import apsw, TesseraVFS
from .compact import CompactMixin
from .exceptions import TenantNotFound
from .gc import GCMixin
from .read import ReadMixin
from .utils import VFS_CHUNKS_DIRNAME, VFS_SESSIONS_PER_TENANT, _manifest_key
from .write import WriteMixin

logger = logging.getLogger(__name__)


# @spec store/store
class Store(WriteMixin, ReadMixin, CompactMixin, GCMixin):
    """Per-tenant transactional store over an object-storage backend.

    Owns the shared mutable state (lease handles, seq counters, manifest
    cache, VFS session registry) and every cross-cutting helper (lease
    retention, LRU eviction, manifest fetch, lifecycle). Write/read/compact/
    gc operations are provided by the mixins in the same package.

    Args:
        backend: object store + lease manager (see
            :class:`~tessera.backends.base.Backend`).
        ttl_ms: lease TTL in milliseconds (keep >= 3000: the lease counts as
            expired 1 s early for clock-skew safety).
        retain_ms: how long an acquired lease handle is reused across
            operations before being released and re-acquired.
        read_wait_ms: how long :meth:`read` waits for ``min_version``.
        read_poll_ms: interval between manifest re-heads while waiting.
        lru_bytes: soft cap for the total size of the local cache root.
        root: local cache root directory (default ``/tmp/tessera``); tenant
            directories are created on demand. The VFS read path additionally
            keeps the GLOBAL content-addressed chunk cache under
            ``<root>/chunks`` (reserved name, never evicted — see
            :meth:`_lru_evict`).
        use_vfs: serve reads of format-2 tenants that are NOT fully local
            through :class:`~tessera.vfs.TesseraVFS` (page-level reads, no
            full download; requires the optional ``apsw`` package). V1
            tenants and fully-local v2 tenants are unaffected, and with
            ``use_vfs=False`` (or without apsw) v2 reads fall back to full
            hydration exactly as in M5 — a missing apsw logs one warning.
        clock: injectable ``float``-returning callable used for lease
            retention, LRU ordering and read deadlines (defaults to
            :func:`time.time`).
        inline_compact: run :meth:`compact` automatically at the end of a
            successful :meth:`write` when the delta chain exceeds either
            threshold below.
        inline_compact_deltas: delta-count threshold for the inline trigger.
        inline_compact_bytes: total delta-bytes threshold for the inline
            trigger.
        gc_delta_age_s: grace window for :meth:`gc`; objects younger than
            this are never deleted (see :meth:`gc` for the invariant).
        gc_keep_snapshots: how many of the newest snapshot versions
            :meth:`gc` retains (the current snapshot is always kept).
        history_retention_s: TIME-based recovery window (default 7 days).
            GC retains a historical manifest if it is among the
            ``gc_keep_history`` newest OR if it is newer than this window.
            0 disables the time-based criterion.
        gc_keep_history: how many of the most recently REPLACED manifest
            versions :meth:`gc` keeps archived under ``<tenant>/history/``
            and marks as live. Readers holding a manifest within the last
            K published versions are protected by GC; older ones may lose
            their objects once they age past the grace window (documented
            limit — K bounds the protection window).
    """

    def __init__(
        self,
        backend: Backend,
        *,
        ttl_ms: int = 10_000,
        retain_ms: int = 2_000,
        read_wait_ms: int = 500,
        read_poll_ms: int = 50,
        lru_bytes: int = 400 * 1024 * 1024,
        root: str | Path | None = None,
        clock: Callable[[], float] = time.time,
        inline_compact: bool = True,
        inline_compact_deltas: int = 200,
        inline_compact_bytes: int = 16 * 1024 * 1024,
        gc_delta_age_s: float = 3600.0,
        gc_keep_snapshots: int = 3,
        gc_keep_history: int = 24,
        history_retention_s: float = 7 * 86400.0,
        use_vfs: bool = True,
    ) -> None:
        self._backend = backend
        self._ttl_ms = int(ttl_ms)
        self._retain_ms = int(retain_ms)
        self._read_wait_ms = int(read_wait_ms)
        self._read_poll_ms = int(read_poll_ms)
        self._lru_bytes = int(lru_bytes)
        self._root = Path(root) if root is not None else Path("/tmp/tessera")
        self._clock = clock
        self._inline_compact = bool(inline_compact)
        self._inline_compact_deltas = int(inline_compact_deltas)
        self._inline_compact_bytes = int(inline_compact_bytes)
        self._gc_delta_age_s = float(gc_delta_age_s)
        self._gc_keep_snapshots = int(gc_keep_snapshots)
        self._gc_keep_history = int(gc_keep_history)
        self._history_retention_s = float(history_retention_s)
        # Live lease handles, retained (not released) after writes.
        self._handles: dict[str, LeaseHandle] = {}
        self._handle_used_at: dict[str, float] = {}
        # Next delta seq per (tenant, epoch); starts at 1.
        self._seqs: dict[tuple[str, int], int] = {}
        # Single-HEAD manifest cache: (etag, manifest) per tenant.
        self._manifest_cache: dict[str, tuple[str, Manifest]] = {}
        # In-flight async history-archive threads (see _drain_history_archives).
        self._history_threads: list[threading.Thread] = []
        # Last-use timestamps of tenant cache directories (LRU input).
        self._dir_used_at: dict[str, float] = {}
        # VFS read path (format 2, not fully local): lazy TesseraVFS singleton
        # plus a warm-session cache keyed by (tenant, manifest.version) ->
        # session key. Sessions are registered once per version so deltas are
        # downloaded at most once; released by close() (or when a tenant
        # exceeds _VFS_SESSIONS_PER_TENANT warm versions).
        self._use_vfs = bool(use_vfs)
        self._vfs: "TesseraVFS | None" = None
        self._vfs_lock = threading.Lock()
        self._vfs_keys: dict[tuple[str, int], str] = {}
        self._vfs_warned = False

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"<Store backend={type(self._backend).__name__} "
            f"root={str(self._root)!r} handles={len(self._handles)}>"
        )

    # ------------------------------------------------------------------
    # Lease retention
    # ------------------------------------------------------------------

    def _acquire(self, tenant: str) -> LeaseHandle:
        """Return a live :class:`LeaseHandle` for *tenant*.

        Reuses the cached handle while it is alive and was used within
        ``retain_ms``; otherwise releases the old one (best-effort) and
        acquires a fresh lease. Raises :class:`~tessera.backends.base.TenantBusy`
        (via the backend) when another owner holds the lease. A cached handle
        that is provably ``"lost"`` is never silently replaced: the operation
        fails fast with :class:`~tessera.backends.base.LeaseLost` BEFORE any
        storage is touched, and the dead handle is dropped so the NEXT
        operation starts a clean lease session.
        """
        now = self._clock()
        handle = self._handles.get(tenant)
        used_at = self._handle_used_at.get(tenant)
        if (
            handle is not None
            and used_at is not None
            and handle.state == "alive"
            and now - used_at < self._retain_ms / 1000.0
        ):
            self._handle_used_at[tenant] = now  # sliding retention
            self._last_acquire_reused = True
            return handle
        self._last_acquire_reused = False
        if handle is not None and handle.state == "lost":
            # Fencing fail-fast: the retained lease provably died (expired,
            # stolen or released behind this store's back). Minting a fresh
            # lease here would silently launder the loss, so surface
            # LeaseLost before any storage mutation instead.
            try:
                handle.release()  # idempotent; frees backend lease resources
            except Exception:
                pass
            self._handles.pop(tenant, None)
            self._handle_used_at.pop(tenant, None)
            raise LeaseLost(
                f"lease for tenant {tenant!r} was lost (expired, stolen or "
                f"released behind this store's back); the next operation "
                f"acquires a fresh lease session"
            )
        if handle is not None:
            try:
                handle.release()
            except Exception:
                pass
        # Epoch floor from the CACHED manifest: if the TTL sweeper removed
        # the lease item, the backend would restart at epoch 1 and the tenant
        # would become unwritable (existing delta keys -> perpetual
        # Conflict). Only the cache is used: reading the manifest here would
        # cost one GET per acquire, and the cached manifest is already what
        # the write will use for the CAS. Without a cache the floor is 0 and
        # behaviour is as before.
        cached = self._manifest_cache.get(tenant)
        min_epoch = cached[1].last_epoch if cached is not None else 0
        handle = LeaseHandle.acquire(
            self._backend, tenant, self._ttl_ms, min_epoch=min_epoch
        )
        self._handles[tenant] = handle
        self._handle_used_at[tenant] = now
        return handle

    def _stamp_used(self, tenant: str, handle: LeaseHandle | None) -> None:
        """Refresh lease-retention and LRU timestamps after a use."""
        now = self._clock()
        if handle is not None:
            self._handle_used_at[tenant] = now
        self._dir_used_at[tenant] = now

    @cache_access
    def close(self) -> None:
        """Release every retained lease handle and VFS read session.

        Idempotent, best-effort. Releasing a VFS session drops the registered
        manifest state and in-memory delta overlay (and shuts down any
        background hydrator); the global chunk cache on disk is KEPT — the
        next read re-uses it. Connections handed to read fns are closed when
        each read returns, so there is nothing else to tear down.
        """
        self._drain_history_archives()
        for handle in list(self._handles.values()):
            try:
                handle.release()
            except Exception:
                pass
        self._handles.clear()
        self._handle_used_at.clear()
        if self._vfs is not None:
            for key in list(self._vfs_keys.values()):
                try:
                    self._vfs.release(key)
                except Exception:
                    pass
            self._vfs_keys.clear()

    # ------------------------------------------------------------------
    # Manifest helpers
    # ------------------------------------------------------------------

    def _get_manifest(self, tenant: str) -> Manifest:
        """Fetch and parse the current manifest; NotFound -> TenantNotFound."""
        try:
            data, _etag = self._backend.get(_manifest_key(tenant))
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        return Manifest.from_json(data.decode("utf-8"))

    @cache_access
    def _read_manifest_cached(self, tenant: str) -> Manifest:
        """One HEAD + conditional get: the If-None-Match pattern.

        A HEAD whose etag equals the cached one satisfies the read with a
        single round trip; only an etag change triggers a full fetch (and
        cache update).
        """
        etag = self._backend.head(_manifest_key(tenant))
        if etag is None:
            raise TenantNotFound(f"tenant {tenant!r} does not exist")
        cached = self._manifest_cache.get(tenant)
        if cached is not None and cached[0] == etag:
            return cached[1]
        data, etag = self._backend.get(_manifest_key(tenant))
        manifest = Manifest.from_json(data.decode("utf-8"))
        self._manifest_cache[tenant] = (etag, manifest)
        return manifest

    @contextmanager
    def _postcommit(self, tenant: str):
        """Maintenance cannot turn an already durable publication into failure."""
        try:
            yield
        except Exception:
            logger.exception("tenant %s: postcommit maintenance failed", tenant)
            self._manifest_cache.pop(tenant, None)
            shutil.rmtree(self._root / tenant, ignore_errors=True)
            for key in [key for key in self._vfs_keys if key[0] == tenant]:
                session_key = self._vfs_keys.pop(key)
                try:
                    if self._vfs is not None:
                        self._vfs.release(session_key)
                except Exception:
                    logger.debug("failed to release stale VFS session", exc_info=True)

    def _drain_history_archives(self, timeout_s: float = 5.0) -> None:
        """Wait for in-flight async history archives (bounded, best-effort)."""
        deadline = time.monotonic() + timeout_s
        for thread in self._history_threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
        self._history_threads = [t for t in self._history_threads if t.is_alive()]

    # ------------------------------------------------------------------
    # Local cache management
    # ------------------------------------------------------------------

    @staticmethod
    def _dir_size(path: Path) -> int:
        """Total size in bytes of every file under *path* (0 on errors)."""
        total = 0
        for dirpath, _dirnames, filenames in os.walk(path):
            for name in filenames:
                try:
                    total += os.stat(os.path.join(dirpath, name)).st_size
                except OSError:
                    pass
        return total

    @cache_access
    def _lru_evict(self, keep: str | None = None) -> None:
        """Rmtree tenant dirs until the cache root fits in ``lru_bytes``.

        The least-recently-used directory is evicted first; *keep* (the
        tenant currently in flight) is never evicted. Directories with no
        recorded use are evicted first. Stops when nothing evictable
        remains. A missing root is a no-op.

        The reserved ``chunks`` directory under the root (the GLOBAL
        content-addressed VFS chunk cache) is never evicted nor counted:
        eviction must not pull the cache out from under open VFS sessions,
        and the cache has its own ``cache_max_bytes`` LRU inside
        :class:`~tessera.vfs.ChunkCacheLRU`. Tenant directories are the only
        eviction candidates — and the VFS does not use them at all, so
        evicting one can never break a live session either. (A tenant
        literally named ``chunks`` shares the reserved directory and thus
        the protection.) Owner-managed directories (``_OWNER_MARKER``) are
        never evicted either: a live Owner holds an open connection and the
        unpublished WAL there — it is not a disposable cache.
        """
        try:
            children = [
                p
                for p in self._root.iterdir()
                if p.is_dir() and p.name != VFS_CHUNKS_DIRNAME
            ]
        except OSError:
            return
        sizes = {p: self._dir_size(p) for p in children}
        total = sum(sizes.values())
        while total > self._lru_bytes:
            evictable = [
                p
                for p in sizes
                if p.name != keep and not (p / _OWNER_MARKER).exists()
            ]
            if not evictable:
                break
            victim = min(
                evictable,
                key=lambda p: (self._dir_used_at.get(p.name, 0.0), p.name),
            )
            shutil.rmtree(victim, ignore_errors=True)
            total -= sizes.pop(victim)
            self._dir_used_at.pop(victim.name, None)
            self._manifest_cache.pop(victim.name, None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def exists(self, tenant: str) -> bool:
        """True when *tenant* has a manifest."""
        return self._backend.head(_manifest_key(tenant)) is not None

    @cache_access
    def delete(self, tenant: str) -> None:
        """Delete *tenant* and every object it owns.

        The manifest is deleted first so the tenant becomes invisible to
        readers immediately; the snapshot, delta and any remaining objects
        are then removed best-effort. Raises :class:`TenantNotFound` when
        there is no manifest.
        """
        handle = self._acquire(tenant)
        try:
            data, _etag = self._backend.get(_manifest_key(tenant))
        except NotFound:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
        manifest = Manifest.from_json(data.decode("utf-8"))

        # Manifest FIRST: from here on readers see no tenant.
        self._delete_quiet(_manifest_key(tenant))
        if manifest.snapshot:  # format 2 has no snapshot (chunks replace it)
            self._delete_quiet(manifest.snapshot)
        for entry in manifest.deltas:
            self._delete_quiet(entry.key)
        for key in self._backend.list(f"{tenant}/"):
            self._delete_quiet(key)

        try:
            handle.release()
        except Exception:
            pass
        self._handles.pop(tenant, None)
        self._handle_used_at.pop(tenant, None)
        self._manifest_cache.pop(tenant, None)
        self._dir_used_at.pop(tenant, None)
        for seq_key in [k for k in self._seqs if k[0] == tenant]:
            del self._seqs[seq_key]
        shutil.rmtree(self._root / tenant, ignore_errors=True)

    def _delete_quiet(self, key: str) -> None:
        """Delete *key*, swallowing :class:`NotFound` (best-effort cleanup)."""
        try:
            self._backend.delete(key)
        except NotFound:
            pass

    # ------------------------------------------------------------------
    # VFS support (format 2, not fully local)
    # ------------------------------------------------------------------

    def _get_vfs(self) -> "TesseraVFS":
        """Lazy :class:`~tessera.vfs.TesseraVFS` singleton for this Store.

        The VFS name is unique per Store instance (``tessera-<id(self):x>``):
        apsw VFS names are process-global, so two Stores in one process over
        different backends must never share one. The chunk cache is the
        GLOBAL content-addressed cache under ``<root>/chunks`` — shared
        across tenants of this store, distinct from the per-tenant hydration
        directories, and never evicted by :meth:`_lru_evict` (reserved name).

        Only called when ``apsw`` is importable, so the lazy import cannot
        fail here.
        """
        if apsw is None or TesseraVFS is None:  # pragma: no cover - caller guards
            raise BackendError("the VFS read path requires the optional 'apsw' package")
        if self._vfs is None:
            with self._vfs_lock:
                if self._vfs is None:
                    self._root.mkdir(parents=True, exist_ok=True)
                    self._vfs = TesseraVFS(
                        name=f"tessera-{id(self):x}",
                        cache_dir=self._root / VFS_CHUNKS_DIRNAME,
                    )
        return self._vfs

    def _cap_vfs_sessions(self, tenant: str) -> None:
        """Keep at most ``VFS_SESSIONS_PER_TENANT`` warm VFS sessions per tenant.

        ``_vfs_keys`` preserves insertion order (oldest first); the oldest
        sessions of *tenant* beyond the cap are released best-effort and
        dropped, so a manifest that keeps advancing cannot accumulate warm
        sessions unboundedly. Other tenants' sessions are untouched.
        """
        own = [k for k in self._vfs_keys if k[0] == tenant]
        for stale in own[: max(0, len(own) - VFS_SESSIONS_PER_TENANT)]:
            key = self._vfs_keys.pop(stale, None)
            if key is not None:
                try:
                    self._vfs.release(key)  # type: ignore[union-attr]
                except Exception:
                    pass
