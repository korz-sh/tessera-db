# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Owner lifecycle: start/stop/serve, bootstrap, main loop and teardown."""

import logging
import random
import secrets
import shutil
import sqlite3
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

from ..backends.base import NotFound, TenantBusy
from ..lease import LeaseHandle
from ..local import (
    _CHUNKS_MARKER,
    _OWNER_MARKER,
    _SNAPSHOT_MARKER,
    _read_chunks_marker,
    _read_snapshot_marker,
    ensure_local,
)
from ..manifest import Manifest
from ..store import Store, TenantNotFound, _manifest_key
from .http import _make_handler_class
from .utils import (
    _ACQUIRE_ATTEMPTS,
    _ACQUIRE_BASE_S,
    _DB_NAME,
    _TICK_S,
)

logger = logging.getLogger(__name__)


# @spec owner/lifecycle-mixin
# @implements durable-write-contract/uncertain-owner-state-cannot-publish-later
# @implements bounded-write-cost/fencing-invariants-are-unchanged
class LifecycleMixin:
    """Lifecycle operations mixed into :class:`Owner`."""

    def start(self) -> bool:
        """Run the owner until idle timeout, :meth:`stop` or fencing loss.

        Blocking. Binds the HTTP server first (the lease ``endpoint`` needs
        the bound port), acquires the tenant lease with full-jitter backoff,
        bootstraps the warm local state, then loops: publication while
        dirty, incremental compaction check, idle shutdown.

        Returns ``True`` on a clean exit (idle timeout or ``stop()`` —
        process exit code 0) and ``False`` on fencing loss (lease no longer
        alive or a lost manifest CAS — process exit code 1).

        Raises :class:`TenantBusy` when another owner still holds the lease
        after the backoff budget, and :class:`TenantNotFound` when the tenant
        has no manifest. Single-run: call ``start()`` once per Owner.
        """
        # ---- 1. HTTP server FIRST: the lease endpoint needs the port. ----
        server = ThreadingHTTPServer(
            (self._http_host, self._http_port_requested),
            _make_handler_class(self),
        )
        self._server = server
        self._http_port_value = int(server.server_address[1])
        server_thread = threading.Thread(
            target=server.serve_forever,
            name=f"tessera-owner-http-{self._tenant}",
            daemon=True,
        )
        server_thread.start()
        logger.info(
            "owner %s: http listening on %s:%d",
            self._tenant,
            self._http_host,
            self._http_port_value,
        )
        try:
            # ---- 2. Lease (full-jitter backoff while the previous holder
            # exits). The handle's renewal thread keeps it alive for the
            # owner's whole life; state != "alive" => fencing loss.
            endpoint = f"http://{self._advertise_host}:{self._http_port_value}"
            # Session token: 256 bits of os.urandom, generated HERE and
            # published in the same conditional UpdateItem as the acquire.
            # Not passed via env or containerOverrides (visible through
            # ecs:DescribeTasks) and not fetched from Secrets Manager: the
            # auto-promoted owner on Fargate must be serving within seconds
            # and cannot depend on another AWS call.
            self._session_token = secrets.token_urlsafe(32)

            # ---- 2a. Pre-hydrate BEFORE contending for the lease. The
            # download is pure unfenced reads — waiting for a busy
            # predecessor's TTL doubles as hydration time, so takeover wall
            # time is the lease wait alone. Best-effort: any failure just
            # means the post-acquire path below does the same work.
            manifest_key = _manifest_key(self._tenant)
            dest = self._root / self._tenant
            self._dest = dest
            # Mark the dir owner-managed before any hydration so a cache
            # sweep never treats the live dir as disposable (see below).
            dest.mkdir(parents=True, exist_ok=True)
            (dest / _OWNER_MARKER).touch()
            try:
                warm_data, _warm_etag = self._backend.get(manifest_key)
                warm_manifest = Manifest.from_json(warm_data.decode("utf-8"))
                self._guard_stale_wal(dest, warm_manifest)
                ensure_local(
                    self._backend, self._tenant, warm_manifest, dest=dest
                )
            except Exception:  # noqa: BLE001 — warm-up is best-effort
                logger.info(
                    "owner %s: pre-hydrate skipped; post-acquire path "
                    "hydrates as usual",
                    self._tenant,
                )

            # ---- 2b. Lease.
            handle = self._acquire_with_backoff(endpoint)
            self._handle = handle
            self._epoch = handle.lease.epoch

            # ---- 3. Bootstrap local state: fetch manifest, hydrate once,
            # open the ONE warm connection. Warm restarts: matching
            # ensure_local markers mean zero downloads. The refetch also
            # catches up whatever the predecessor published while we
            # waited for the lease.
            try:
                data, _etag = self._backend.get(manifest_key)
            except NotFound:
                raise TenantNotFound(
                    f"tenant {self._tenant!r} does not exist"
                ) from None
            manifest = Manifest.from_json(data.decode("utf-8"))
            with self._lock:
                self._last_published_version = manifest.version
                # Prime the publish path's cached head with the committed
                # manifest just fetched — the first publish then needs no
                # manifest GET either.
                self._cached_manifest = manifest
                self._cached_etag = _etag
                # The commit sequence lives on the DATA frontier axis: a
                # manifest stamped by compaction has a higher ``version``
                # but covers the same commits, so seeding from
                # ``data_version`` keeps new write numbers above every
                # covered commit — never colliding with object versions a
                # min_version read would wrongly resolve on.
                self._commit_seq = manifest.data_version
                self._published_commit_seq = manifest.data_version
            # The dir is already owner-marked and pre-hydrated; re-guard the
            # WAL against the POST-acquire manifest and catch up whatever the
            # predecessor committed during the lease wait.
            self._guard_stale_wal(dest, manifest)
            db_path = ensure_local(self._backend, self._tenant, manifest, dest=dest)
            # check_same_thread=False: HTTP handler threads use this ONE
            # connection; every access is serialized by the owner lock.
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=0")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._conn = conn

            # ---- 4. Store for COMPACTION ONLY. Seed the owner's live lease
            # handle into the store's retention cache so
            # store.compact_incremental() reuses THE owner's lease instead of
            # acquiring a second one (which would TenantBusy against
            # ourselves — or worse, release ours once the retention window
            # lapses; _maybe_compact refreshes _handle_used_at right before
            # every call to keep the reuse window open). The store gets a
            # scratch root of its own: compaction is pure backend work, and
            # routing its cache-lock markers/sweeps through a private root
            # keeps them away from the live dir even on a shared cache root.
            store = Store(
                self._backend,
                root=self._root / ".tessera-owner-scratch",
                ttl_ms=self._ttl_ms,
            )
            store._handles[self._tenant] = handle
            store._handle_used_at[self._tenant] = time.time()
            self._store = store

            # ---- 5. Main loop.
            with self._lock:
                self._serving = True
            logger.info(
                "owner %s: serving epoch=%d version=%d db=%s",
                self._tenant,
                self._epoch,
                manifest.version,
                db_path,
            )
            return self._main_loop()
        finally:
            self._teardown()

    def serve_in_background(self) -> "Owner":
        """Run :meth:`start` on a daemon thread; returns self immediately."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("owner is already running in the background")
        self._thread = threading.Thread(
            target=self.start,
            name=f"tessera-owner-{self._tenant}",
            daemon=True,
        )
        self._thread.start()
        return self

    def join(self, timeout: float | None = None) -> None:
        """Wait for a :meth:`serve_in_background` thread to finish (no-op
        when the owner runs on the caller's own thread)."""
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)

    def stop(self, publish: bool = True) -> None:
        """Thread-safe shutdown request.

        Sets the stop event; the main loop performs the teardown. With
        ``publish=True`` (default) and dirty writes pending, ONE final
        publish runs before the lease is released (best-effort — a failed
        final publish leaves the local directory to be discarded, see
        :meth:`_teardown`). With ``publish=False`` pending local commits are
        discarded. Safe to call from tests and signal handlers; a no-op if
        the owner already exited.
        """
        # No _lock here on purpose: it is a plain bool assignment (atomic
        # under the GIL) and taking the lock would block the shutdown
        # request behind a publish inside its transient-error budget —
        # exactly the moment an operator wants to stop.
        self._stop_publish = bool(publish)
        self._stop_event.set()
        if publish and self._loop_done.is_set():
            # The loop already exited (e.g. a clean idle exit raced with this
            # call): nothing else will publish — try once, best-effort. The
            # call self-gates (no-op when fenced, released or not dirty).
            try:
                self._publish_once()
            except Exception:  # noqa: BLE001 - best-effort by contract
                pass

    # ------------------------------------------------------------------
    # Bootstrap helpers
    # ------------------------------------------------------------------

    def _acquire_with_backoff(self, endpoint: str) -> LeaseHandle:
        """Acquire the tenant lease with full-jitter exponential backoff.

        ``_ACQUIRE_ATTEMPTS`` attempts total, sleeping ``uniform(0, 50ms * 2^i)``
        between them; the last :class:`TenantBusy` propagates. When
        ``acquire_wait_s > 0`` (ephemeral owners launched INTO contention —
        the current holder yields politely but not instantly), the whole
        attempt cycle repeats with a capped 1 s jitter until the deadline.
        """
        deadline = time.monotonic() + self._acquire_wait_s
        while True:
            delay = _ACQUIRE_BASE_S
            last_busy: TenantBusy | None = None
            for attempt in range(_ACQUIRE_ATTEMPTS):
                try:
                    try:
                        data, _ = self._backend.get(_manifest_key(self._tenant))
                    except NotFound:
                        raise TenantNotFound(f"tenant {self._tenant!r} does not exist") from None
                    floor = Manifest.from_json(data.decode("utf-8")).last_epoch
                    return LeaseHandle.acquire(
                        self._backend,
                        self._tenant,
                        self._ttl_ms,
                        endpoint=endpoint,
                        session_token=self._session_token,
                        min_epoch=floor,
                    )
                except TenantBusy as exc:
                    last_busy = exc
                    if attempt == _ACQUIRE_ATTEMPTS - 1:
                        break
                    time.sleep(random.uniform(0.0, delay))  # full jitter
                    delay *= 2
            assert last_busy is not None  # only reachable after >= 1 failure
            if time.monotonic() >= deadline:
                raise last_busy
            logger.info(
                "owner %s: lease busy; retrying (%.0f s left)",
                self._tenant,
                deadline - time.monotonic(),
            )
            time.sleep(random.uniform(0.2, 1.0))

    @staticmethod
    def _base_matches(manifest: Manifest, dest: Path) -> bool:
        """True when the local base marker matches *manifest*'s base."""
        if manifest.format == 2:
            marker = _read_chunks_marker(dest / _CHUNKS_MARKER)
            return (
                marker is not None
                and marker["keys"] == [entry.key for entry in manifest.chunks]
            )
        marker = _read_snapshot_marker(dest / _SNAPSHOT_MARKER)
        return marker is not None and marker["key"] == manifest.snapshot

    def _guard_stale_wal(self, dest: Path, manifest: Manifest) -> None:
        """Remove stale ``-wal``/``-shm`` sidecars before a base rebuild.

        A leftover WAL from a previous owner run is safe to keep only when
        the base lineage is unchanged: SQLite then recovers it onto the SAME
        base, replaying frames that are either already published (idempotent
        page writes) or unpublished local commits that this owner will
        re-publish. When the base does NOT match (fresh destination, or the
        manifest was re-based by a compaction while we were gone) the
        recovered frames would land on a REBUILT base with unrelated page
        numbering and corrupt it — the sidecars are deleted so hydration
        starts from a clean file.
        """
        dest.mkdir(parents=True, exist_ok=True)
        db_path = dest / _DB_NAME
        if self._base_matches(manifest, dest) and db_path.exists():
            return
        for suffix in ("-wal", "-shm"):
            Path(f"{db_path}{suffix}").unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _main_loop(self) -> bool:
        """Publish/compact/idle loop. ``True`` = clean exit, ``False`` = fenced."""
        last_publish = time.monotonic()
        last_compact = time.monotonic()
        while not self._stop_event.wait(_TICK_S):
            now = time.monotonic()

            # Fencing gate: the renewal thread owns liveness; a handle that
            # is no longer "alive" means the lease was lost or released
            # behind our back — stop serving and exit with code 1.
            if self._handle is None or self._handle.state != "alive":
                with self._lock:
                    self._fenced = True
                    self._serving = False
                logger.error(
                    "owner %s: lease lost (fencing) — stopping", self._tenant
                )
                return False

            with self._lock:
                dirty = self._dirty
            if dirty and now - last_publish >= self._publish_every_s:
                try:
                    ok = self._publish_once()
                except Exception:  # noqa: BLE001 - transient backend failure:
                    logger.exception(
                        "owner %s: publish outcome uncertain; stopping",
                        self._tenant,
                    )
                    ok = False
                last_publish = time.monotonic()
                if not ok:
                    return False  # fencing loss inside the publish path

            now = time.monotonic()
            if now - last_compact >= self._compact_every_s:
                last_compact = now
                self._maybe_compact()
                if self._fenced:
                    return False

            now = time.monotonic()
            with self._lock:
                idle_for = now - self._last_write
                dirty = self._dirty
            if idle_for > self._idle_s and not dirty:
                logger.info(
                    "owner %s: idle for %.1fs — clean exit", self._tenant, idle_for
                )
                return True
        if self._fenced:
            # A /write handler detected fencing before this tick: the stop
            # request must not mask it — start() returns False on fencing loss.
            return False
        return True  # stop() requested: clean shutdown (exit code 0)

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def _teardown(self) -> None:
        """One-shot cleanup for every exit path (idle, stop, fencing, error).

        Order matters: stop serving FIRST (no write can slip in past the
        dirty snapshot below — the admission check runs under the same
        lock), stop the HTTP server, run the optional final publish, then
        release the lease and close resources. Idempotent; never raises.
        """
        with self._lock:
            self._serving = False
            dirty = self._dirty
            fenced = self._fenced
            want_final_publish = self._stop_publish and dirty and not fenced

        server = self._server
        if server is not None:
            try:
                server.shutdown()  # stops serve_forever
            except Exception:  # noqa: BLE001 - teardown never raises
                pass
            try:
                server.server_close()
            except Exception:  # noqa: BLE001
                pass
            self._server = None

        # Final publish for stop(publish=True) with pending writes — the
        # lease is still alive here and must never run when fenced.
        if (
            want_final_publish
            and self._handle is not None
            and self._handle.state == "alive"
            and self._conn is not None
        ):
            try:
                self._publish_once()
            except Exception:  # noqa: BLE001 - best-effort by contract
                logger.exception(
                    "owner %s: final publish failed; local dir will be discarded",
                    self._tenant,
                )

        if self._handle is not None:
            try:
                self._handle.release()  # idempotent; swallows LeaseLost
            except Exception:  # noqa: BLE001
                pass
        if self._store is not None:
            try:
                self._store.close()  # releases the seeded handle again (no-op)
            except Exception:  # noqa: BLE001
                pass
        conn = self._conn
        if conn is not None:
            try:
                conn.close()  # checkpoints + deletes the WAL
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

        # Invariant: the local database must never drift ahead of the
        # committed manifest. conn.close() checkpointed everything — if
        # dirty writes were never published (publish=False, a failed final
        # publish, or a fencing loss), they would otherwise survive in
        # db.sqlite as phantom committed data. Discard the directory; the
        # next owner hydrates cold from the manifest.
        with self._lock:
            still_dirty = self._dirty
        if still_dirty and self._dest is not None:
            shutil.rmtree(self._dest, ignore_errors=True)
            logger.warning(
                "owner %s: discarded local dir holding unpublished commits",
                self._tenant,
            )
        self._loop_done.set()
        logger.info(
            "owner %s: stopped (fenced=%s)", self._tenant, self._fenced
        )
