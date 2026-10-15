# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
# @implements tenant-recovery/independently-owned-clones
"""Read path - reads, VFS reads, history and restore/clone/export."""

import logging
import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..backends.base import BackendError, CASFailed, NotFound
from ..cache_lock import cache_access
from ..local import ensure_local, is_fully_local
from ..manifest import Manifest
from ..publication import publish_manifest
from .adapters import _ApswConnAdapter, apsw
from .exceptions import (
    Conflict,
    StaleRead,
    TenantExists,
    TenantNotFound,
    VersionUnavailable,
)
from .utils import _history_version, _manifest_key

logger = logging.getLogger(__name__)


# @spec store/read-mixin
# @implements tenant-recovery/existing-readers-observe-restored-state
class ReadMixin:
    """Read-path operations mixed into :class:`Store`."""

    # ------------------------------------------------------------------
    # VFS read path (format 2, not fully local)
    # ------------------------------------------------------------------

    def _read_vfs(self, tenant: str, manifest: Manifest, fn: Callable[[Any], Any]) -> Any:
        """Run ``fn(conn)`` on a read-only VFS connection (M6 read path).

        Registers a VFS session for ``(tenant, manifest.version)`` on first
        use — deltas download once per version, never per read — and opens a
        read-only apsw connection on the session key. ``fn`` receives a
        :class:`_ApswConnAdapter` (sqlite3-shaped, writes raise). The session
        stays WARM after the read: the connection is closed in ``finally``
        but the registration is kept (released by :meth:`close`, or capped
        per tenant by :meth:`_cap_vfs_sessions`).

        The session cache is SHARED with the M7-A2 write path (the VFS serves
        exactly one session per key ``"{tenant}/v{version}"``). If the warm
        session is a WRITABLE one (a write replaced the read session at this
        key), it is re-registered read-only: a read-only connection cannot
        open a WAL-mode write session (its sandbox ``-wal`` is wiped after
        every write and no shared-memory wal-index can be created), and
        serving page 1 with 1/1 flags is what makes reads work. Replacing
        costs a delta re-download (deltas are small by design); the write
        path's own hit check re-registers writable the same way.
        """
        if apsw is None:  # pragma: no cover - caller guards
            raise BackendError("the VFS read path requires the optional 'apsw' package")
        vfs = self._get_vfs()
        cache_key = (tenant, manifest.version)
        session_key = self._vfs_keys.get(cache_key)
        if session_key is not None:
            warm = vfs._sessions.get(session_key)
            if warm is None or warm.writable:
                session_key = None  # stale entry or a write session: re-register
        if session_key is None:
            session_key = vfs.register(tenant, manifest, self._backend, hydrate=False)
            self._vfs_keys[cache_key] = session_key
            self._cap_vfs_sessions(tenant)
        adapted: _ApswConnAdapter | None = None
        try:
            # Opening the connection may already read page 1 (and hit the
            # backend), so it shares the error translation below.
            conn = apsw.Connection(
                session_key, flags=apsw.SQLITE_OPEN_READONLY, vfs=vfs.vfs_name
            )
            adapted = _ApswConnAdapter(conn)
            return fn(adapted)
        except apsw.IOError as exc:
            # The VFS converts BackendError (missing or hash-mismatched chunk
            # object) into apsw.IOError inside xRead, the only place apsw
            # lets storage errors surface. Restore the Store-level contract:
            # storage problems raise BackendError (cause chain preserved).
            raise BackendError(str(exc)) from exc
        finally:
            if adapted is not None:
                adapted.close()  # closes the connection, never the warm session

    # ------------------------------------------------------------------
    # Point-in-time recovery
    # ------------------------------------------------------------------

    # @implements operator-readiness/read-only-tenant-diagnostics
    def diagnostics(self, tenant: str) -> dict:
        """Inspect durable metadata without leases, writes, or cache hydration.

        Owner identity and publication lag are unknown: the portable backend
        contract does not expose owner inspection. This is not a health probe.
        """
        try:
            data, _etag = self._backend.get(_manifest_key(tenant))
        except NotFound as exc:
            raise TenantNotFound(f"tenant {tenant!r} does not exist") from exc
        current = Manifest.from_json(data.decode("utf-8"))
        versions = {current.version}
        for key in self._backend.list(f"{tenant}/history/"):
            version = _history_version(key)
            if version is not None:
                versions.add(version)
        return {
            "version": current.version,
            "format": current.format,
            "delta_count": len(current.deltas),
            "delta_bytes": sum(delta.size for delta in current.deltas),
            "recovery_versions": sorted(versions, reverse=True),
            "owner": None,
            "publication_lag_s": None,
        }

    def history(self, tenant: str) -> list[dict]:
        """Available recovery points, newest to oldest.

        Returns ``[{"version": int, "timestamp": float | None}, ...]``,
        including the CURRENT version in addition to the archived ones. It
        is the first thing needed during an incident: knowing how far back
        one can go.

        Data is write-once, so each entry is a complete and consistent
        state, not a diff that must be reconstructed.
        """
        # Draining ensures a just-replaced manifest is already in history/
        # and does not look unrecoverable right after the incident, which
        # is exactly when it is needed.
        self._drain_history_archives()
        current = self._get_manifest(tenant)  # validates existence
        entries: list[dict] = [
            {
                "version": current.version,
                "timestamp": self._backend.mtime(_manifest_key(tenant)),
                "current": True,
            }
        ]
        for key in self._backend.list(f"{tenant}/history/"):
            version = _history_version(key)
            if version is None or version == current.version:
                continue
            entries.append(
                {
                    "version": version,
                    "timestamp": self._backend.mtime(key),
                    "current": False,
                }
            )
        entries.sort(key=lambda e: e["version"], reverse=True)
        return entries

    def _manifest_at(self, tenant: str, version: int) -> Manifest:
        """Manifest at *version*, current or archived."""
        current = self._get_manifest(tenant)
        if current.version == version:
            return current
        try:
            data, _etag = self._backend.get(f"{tenant}/history/{version}.json")
        except NotFound:
            # An archive upload may still be in flight (a version replaced
            # moments ago). Drain and retry ONCE before declaring it
            # unrecoverable: telling someone their state does not exist when
            # it does is the worst error this API can make.
            self._drain_history_archives()
            try:
                data, _etag = self._backend.get(
                    f"{tenant}/history/{version}.json"
                )
            except NotFound:
                data = None
        if data is None:
            available = [e["version"] for e in self.history(tenant)]
            oldest = min(available) if available else None
            raise VersionUnavailable(
                f"version {version} of tenant {tenant!r} is no longer "
                f"recoverable (GC collected it); the oldest available "
                f"version is {oldest}."
            )
        return Manifest.from_json(data.decode("utf-8"))

    def _resolve_version(
        self, tenant: str, version: int | None, at: float | None
    ) -> int:
        """Requested version by number, or the newest <= *at*."""
        if version is not None and at is not None:
            raise ValueError("pass either 'version' or 'at', not both")
        if version is not None:
            return version
        if at is None:
            raise ValueError("either 'version' or 'at' is required")
        # An entry's timestamp is when that manifest STOPPED being the
        # current one (when it was archived); the current one carries its
        # own mtime. For "as it was at T" we need the last state that was
        # ALREADY in force at T, i.e. the smallest version whose
        # replacement happened AFTER T. Comparing with <= would return the
        # already-modified state.
        entries = self.history(tenant)
        candidates = [
            e for e in entries if e["timestamp"] is not None and e["timestamp"] > at
        ]
        if not candidates:
            # Nothing was replaced after T: the state in force at T is the
            # current one (nobody wrote since then).
            return max(e["version"] for e in entries)
        chosen = min(candidates, key=lambda e: e["version"])
        oldest = min(e["version"] for e in entries)
        if chosen["version"] == oldest and oldest > 1:
            # The requested point may predate what is retained: it cannot be
            # asserted that this is the state at T.
            raise VersionUnavailable(
                f"the state of tenant {tenant!r} at {at} is no longer "
                f"recoverable; oldest available version is {oldest}."
            )
        return chosen["version"]

    def read_at(
        self,
        tenant: str,
        fn: Callable[[sqlite3.Connection], Any],
        *,
        version: int | None = None,
        at: float | None = None,
    ) -> Any:
        """Read the tenant as it was at *version* (or at instant *at*).

        Takes no lease and does not touch current state: objects are
        write-once, i.e. immutable, so reading the past is as safe as
        reading the present. *at* is a unix timestamp (``time.time()`` or
        ``datetime.timestamp()``), the way a human thinks about the problem
        ("as it was at 14:32").

        Raises :class:`VersionUnavailable` if GC already collected that
        point, including how far back one CAN go.
        """
        resolved = self._resolve_version(tenant, version, at)
        manifest = self._manifest_at(tenant, resolved)
        return self._read_with_manifest(tenant, manifest, fn, historical=True)

    def _recovery_target(self, tenant, version, at):
        if version is None and at is None:
            return self._get_manifest(tenant)
        return self._manifest_at(tenant, self._resolve_version(tenant, version, at))

    def export(
        self, tenant: str, dest: str | Path, *,
        version: int | None = None, at: float | None = None,
    ) -> Path:
        """Export a standalone SQLite image; never replace an existing path."""
        from ..recovery import materialize

        destination = Path(dest).expanduser().absolute()
        if os.path.lexists(destination):
            raise FileExistsError(destination)
        manifest = self._recovery_target(tenant, version, at)
        with materialize(self._backend, tenant, manifest, destination.parent) as image:
            # Same-filesystem hard link is atomic and create-only, including
            # when another process creates a file/symlink after our precheck.
            os.link(image, destination)
        return destination

    @cache_access
    def clone(
        self, source: str, destination: str, *,
        version: int | None = None, at: float | None = None,
    ) -> Manifest:
        """Create a tenant with its own copy of a selected source database."""
        from ..recovery import image_manifest, materialize

        if self.exists(destination):
            raise TenantExists(f"tenant {destination!r} already exists")
        selected = self._recovery_target(source, version, at)
        with materialize(self._backend, source, selected, self._root) as image:
            new = image_manifest(self._backend, destination, image, selected, 1, 0)
            try:
                self._backend.put(_manifest_key(destination),
                                  new.to_json().encode("utf-8"), if_none_match=True)
            except CASFailed:
                raise TenantExists(f"tenant {destination!r} already exists") from None
        return new

    @cache_access
    def restore(
        self,
        tenant: str,
        *,
        version: int | None = None,
        at: float | None = None,
    ) -> Manifest:
        """Publish the complete historical image as a new fenced version."""
        from ..recovery import image_manifest, materialize

        # Prime the epoch floor before acquisition, including cold recovery
        # after an external lease-record sweep.
        self._read_manifest_cached(tenant)
        handle = self._acquire(tenant)
        handle.ensure_alive()
        data, etag = self._backend.get(_manifest_key(tenant))
        current = Manifest.from_json(data.decode("utf-8"))
        if current.last_epoch > handle.lease.epoch:
            raise Conflict(f"stale recovery lease for tenant {tenant!r}")
        resolved = self._resolve_version(tenant, version, at)
        old = self._manifest_at(tenant, resolved)
        with materialize(self._backend, tenant, old, self._root) as image:
            new = image_manifest(self._backend, tenant, image, current,
                                 current.version + 1, handle.lease.epoch)
            try:
                new_etag = publish_manifest(
                    self._backend, tenant, current, new, etag,
                    before_cas=handle.ensure_alive,
                )
            except CASFailed:
                self._manifest_cache.pop(tenant, None)
                raise Conflict(f"restore CAS failed for tenant {tenant!r}") from None
        self._manifest_cache[tenant] = (new_etag, new)
        with self._postcommit(tenant):
            self._stamp_used(tenant, handle)
        return new

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def read(
        self,
        tenant: str,
        fn: Callable[[sqlite3.Connection], Any],
        *,
        min_version: int | None = None,
    ) -> Any:
        """Run ``fn(conn)`` against the tenant's committed state.

        Resolves the manifest with a single HEAD (etag cache), optionally
        polling every ``read_poll_ms`` — deadline ``read_wait_ms`` — until
        the manifest reaches *min_version*, raising :class:`StaleRead` when
        the deadline passes. Never touches lease operations.

        Connection choice:

        - V1 tenants, and v2 tenants that are fully local
          (:func:`~tessera.local.is_fully_local`), hydrate via
          :func:`~tessera.local.ensure_local` and run ``fn`` on a plain
          writable :class:`sqlite3.Connection` (M5 behaviour, unchanged).
        - V2 tenants that are NOT fully local go through the TesseraVFS when
          ``use_vfs`` is on and ``apsw`` is importable: ``fn`` receives a
          read-only, sqlite3-shaped adapter over the VFS connection — page
          reads are served from the delta overlay, the global chunk cache
          and on-demand whole-chunk downloads, so nothing is fully
          downloaded and ``dest/db.sqlite`` is not created. Without apsw the
          read silently falls back to hydration (one logged warning).

        In both cases ``fn`` commits nothing: the VFS path is read-only and
        raises on writes; the hydration path opens a fresh local copy.
        """
        self._lru_evict(keep=tenant)
        manifest = self._read_manifest_cached(tenant)
        # min_version is a COMMIT frontier: resolve against data_version,
        # never the object generation — a compaction CAS can push
        # manifest.version past an unpublished write's number without the
        # manifest containing that write's data.
        if min_version is not None and manifest.data_version < min_version:
            deadline = self._clock() + self._read_wait_ms / 1000.0
            while manifest.data_version < min_version:
                if self._clock() >= deadline:
                    raise StaleRead(
                        f"tenant {tenant!r} manifest data_version stuck at "
                        f"{manifest.data_version} < required {min_version} "
                        f"after {self._read_wait_ms} ms"
                    )
                time.sleep(self._read_poll_ms / 1000.0)
                manifest = self._read_manifest_cached(tenant)
        return self._read_with_manifest(tenant, manifest, fn)

    @cache_access
    def _read_with_manifest(
        self,
        tenant: str,
        manifest: Manifest,
        fn: Callable[[sqlite3.Connection], Any],
        *,
        historical: bool = False,
    ) -> Any:
        """Run ``fn`` over the state described by *manifest* (current or
        historical).

        The body shared by ``read`` and the historical reads: since objects
        are write-once, an old manifest describes a state just as valid as
        the current one and is served through the same path (VFS or
        hydration).

        ``historical=True`` hydrates into a SEPARATE directory. Without
        that, the ``ensure_local`` marker at ``root/<tenant>`` sees the
        current state already materialized, accepts it, and the past read
        returns the present — silently, which is the worst failure mode for
        a recovery tool.
        """
        dest = (
            self._root / f"{tenant}@v{manifest.version}"
            if historical
            else self._root / tenant
        )
        if (
            manifest.format == 2
            and self._use_vfs
            and apsw is not None
            and not is_fully_local(manifest, dest)
        ):
            result = self._read_vfs(tenant, manifest, fn)
        else:
            if (
                manifest.format == 2
                and self._use_vfs
                and apsw is None
                and not self._vfs_warned
            ):
                # Warn once per Store: use_vfs was requested but apsw is
                # missing, so v2 reads silently pay full hydration.
                self._vfs_warned = True
                logger.warning(
                    "use_vfs=True but apsw is not installed: reads of "
                    "format-2 tenants fall back to full hydration "
                    "(install the 'vfs' extra to enable page-level reads)"
                )
            db_path = ensure_local(self._backend, tenant, manifest, dest=dest)
            conn = sqlite3.connect(db_path)
            try:
                result = fn(conn)
            finally:
                conn.close()
        if not historical:
            self._dir_used_at[tenant] = self._clock()
        return result
