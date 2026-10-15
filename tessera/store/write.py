# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Write path - create, write and the VFS write session (M7-A2)."""

import json
import logging
import shutil
import sqlite3
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..backends.base import BackendError, CASFailed, Lease, NotFound
from ..cache_lock import cache_access
from ..delta import read_wal_frames
from ..delta import serialize as serialize_delta
from ..lease import LeaseHandle
from ..local import (
    _APPLIED_MARKER,
    _atomic_write,
    _read_applied,
    ensure_local,
    is_fully_local,
)
from ..manifest import DeltaEntry, Manifest
from ..publication import publish_manifest
from .adapters import apsw, _ApswWriteConnAdapter
from .exceptions import Conflict, TenantExists, TenantNotFound
from .utils import _delta_key, _manifest_key, _snapshot_key

logger = logging.getLogger(__name__)


# @spec store/write-mixin
# @implements bounded-write-cost/write-receipt-exposes-the-durability-contract
class WriteMixin:
    """Write-path operations mixed into :class:`Store`.

    All shared mutable state (lease handles, seq counters, manifest cache,
    VFS session registry) lives on the Store instance — these methods only
    add behavior, no state of their own.
    """

    @staticmethod
    def _connect_wal(db_path: Path) -> sqlite3.Connection:
        """Open *db_path* with the store's WAL connection pragmas."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @cache_access
    def create(self, tenant: str, schema: str | None = None) -> Manifest:
        """Create *tenant* with a fresh version-1 snapshot.

        Builds an empty SQLite database (optionally executing *schema*),
        uploads it write-once as ``<tenant>/snap/1.sqlite`` and publishes the
        initial manifest with create-only CAS. No lease is needed: the
        ``if_none_match`` conditions make concurrent creators single-winner.

        Raises :class:`TenantExists` if the tenant already exists.
        """
        manifest_key = _manifest_key(tenant)
        if self._backend.head(manifest_key) is not None:
            raise TenantExists(f"tenant {tenant!r} already exists")
        self._root.mkdir(parents=True, exist_ok=True)
        tmp = Path(tempfile.mkdtemp(prefix="tessera-create-", dir=self._root))
        try:
            db_path = tmp / "db.sqlite"
            conn = self._connect_wal(db_path)
            try:
                if schema:
                    conn.executescript(schema)
                    conn.commit()
                # Fold the WAL into the main file so the uploaded bytes are
                # self-contained (snapshot objects have no WAL of their own).
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                conn.close()
            data = db_path.read_bytes()

            snapshot_key = _snapshot_key(tenant, 1)
            try:
                self._backend.put(snapshot_key, data, if_none_match=True)
            except CASFailed:
                raise TenantExists(f"tenant {tenant!r} already exists") from None
            manifest = Manifest(
                version=1, snapshot=snapshot_key, deltas=[], last_epoch=0
            )
            try:
                self._backend.put(
                    manifest_key,
                    manifest.to_json().encode("utf-8"),
                    if_none_match=True,
                )
            except CASFailed:
                raise TenantExists(f"tenant {tenant!r} already exists") from None
            return manifest
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    @cache_access
    def write(
        self,
        tenant: str,
        fn: Callable[[sqlite3.Connection], Any],
        *,
        lease: Lease | None = None,
    ) -> tuple[Any, int]:
        """Run ``fn(conn)`` as one committed transaction on *tenant*'s DB.

        ``fn`` receives an open WAL-mode :class:`sqlite3.Connection` on the
        hydrated local database and must commit inside itself. Committed
        frames are then read from the ``-wal`` file, uploaded write-once and
        published via a manifest CAS. Returns ``(fn's result, new manifest
        version)``; a transaction that produced no committed frames is a
        no-op returning the *current* version.

        Connection choice (mirrors :meth:`read`): v2 tenants that are NOT
        fully local go through the VFS WRITE path (``_write_vfs``) when
        ``use_vfs`` is on and ``apsw`` is importable — ``fn`` receives a
        writable sqlite3-shaped adapter over an apsw connection on a writable
        TesseraVFS session (all writes land in a local ``-wal`` sidecar;
        nothing is fully hydrated). V1 tenants and fully-local v2 tenants
        keep the plain hydration write path; without apsw the VFS branch is
        skipped entirely.

        Pass *lease* (a plain :class:`~tessera.backends.base.Lease`) to skip
        lease operations entirely — the lease's epoch fences the write.

        Fencing: a live lease (``ensure_alive``) gates the whole operation;
        a manifest whose ``last_epoch`` exceeds the effective epoch, a
        delta-key collision, or a failed manifest CAS all raise
        :class:`Conflict` and never retry ``fn``.
        """
        self._lru_evict(keep=tenant)

        # 1. Fencing: live lease, or a caller-supplied epoch.
        handle: LeaseHandle | None = None
        if lease is not None:
            effective_epoch = lease.epoch
        else:
            handle = self._acquire(tenant)
            handle.ensure_alive()
            effective_epoch = handle.lease.epoch

        # 2. Manifest fetch + epoch fence.
        manifest_key = _manifest_key(tenant)
        #
        # Retained-lease fast path (A2.4 of docs/SERVERLESS-LATENCY-PLAN.md):
        # while THIS store holds the tenant's live lease, no other writer can
        # publish (their CAS would need the epoch, which is ours), so a
        # manifest this store itself published is authoritative — the GET is
        # skipped. Safety net: if the cache is somehow stale, the CAS below
        # fails on the old etag and surfaces Conflict (never corruption); the
        # cache is invalidated on that path so the next write re-fetches.
        cached = (
            self._manifest_cache.get(tenant)
            if handle is not None and self._last_acquire_reused
            else None
        )
        if cached is not None:
            etag, manifest = cached
        else:
            try:
                data, etag = self._backend.get(manifest_key)
            except NotFound:
                raise TenantNotFound(f"tenant {tenant!r} does not exist") from None
            manifest = Manifest.from_json(data.decode("utf-8"))
        # Recovery from a SWEPT lease: if the just-acquired epoch does not
        # exceed the `last_epoch` the manifest already committed, the lease
        # record was lost (TTL sweeper on an idle tenant) and numbering
        # restarted from zero. Without this, the reused epoch regenerates
        # existing delta keys and the tenant becomes unwritable forever.
        # Re-acquire ONCE with the manifest as the floor; the cost is one
        # extra call only on the acquire after a sweep, never on the hot
        # path. With an explicit `lease=` it is untouched: that epoch
        # belongs to the caller and the fence below decides.
        if (
            handle is not None
            and not self._last_acquire_reused
            and manifest.last_epoch >= effective_epoch
        ):
            handle.release()
            self._handles.pop(tenant, None)
            self._handle_used_at.pop(tenant, None)
            handle = LeaseHandle.acquire(
                self._backend,
                tenant,
                self._ttl_ms,
                min_epoch=manifest.last_epoch,
            )
            self._handles[tenant] = handle
            self._handle_used_at[tenant] = self._clock()
            handle.ensure_alive()
            effective_epoch = handle.lease.epoch

        if manifest.last_epoch > effective_epoch:
            raise Conflict(
                f"stale writer for tenant {tenant!r}: manifest last_epoch "
                f"{manifest.last_epoch} > lease epoch {effective_epoch}"
            )

        # 3. VFS write path (format 2, not fully local, M7-A2): fn runs on a
        # writable apsw connection over a writable TesseraVFS session; the
        # committed frames are extracted from the session's local -wal. The
        # plain hydration path below is untouched for v1 tenants and
        # fully-local v2 tenants.
        dest = self._root / tenant
        if (
            manifest.format == 2
            and self._use_vfs
            and apsw is not None
            and not is_fully_local(manifest, dest)
        ):
            out = self._write_vfs(
                tenant, fn, manifest, etag, effective_epoch,
                before_cas=handle.ensure_alive if handle is not None else None,
            )
            with self._postcommit(tenant):
                self._stamp_used(tenant, handle)
            return out

        # 3b. Plain path local sync: snapshot + delta chain -> db.sqlite.
        db_path = ensure_local(self._backend, tenant, manifest, dest=dest)
        wal_path = Path(f"{db_path}-wal")

        # 4. Transaction: caller commits inside fn. The WAL is read while
        # the connection is still open — the last connection to close
        # checkpoints and deletes the WAL.
        conn = self._connect_wal(db_path)
        try:
            result = fn(conn)
            # 5. Collect committed frames; none -> no-op publish.
            try:
                delta = read_wal_frames(wal_path)
            except (ValueError, OSError):
                self._stamp_used(tenant, handle)
                return result, manifest.version
        except BaseException:
            conn.close()
            shutil.rmtree(dest, ignore_errors=True)
            raise
        finally:
            conn.close()

        payload = serialize_delta(delta)

        # 6. Delta PUT write-once (seq is per (tenant, epoch), starts at 1).
        seq = self._seqs.get((tenant, effective_epoch), 0) + 1
        delta_key = _delta_key(tenant, effective_epoch, seq)
        self._seqs[(tenant, effective_epoch)] = seq
        try:
            delta_etag = self._backend.put(delta_key, payload, if_none_match=True)
        except Exception as exc:
            shutil.rmtree(dest, ignore_errors=True)
            if not isinstance(exc, CASFailed):
                raise
            raise Conflict(
                f"delta key {delta_key!r} already exists for tenant {tenant!r}"
            ) from None

        # 7. Manifest CAS (if_match) + last_epoch fence. Failure means
        # another writer won: discard local state, never retry fn.
        # A write only appends a delta: format-2 manifests carry their chunk
        # base forward unchanged, while format-1 manifests must never grow
        # format-2 fields.
        v2_fields: dict[str, Any] = {}
        if manifest.format == 2:
            v2_fields = {
                "format": 2,
                "chunks": list(manifest.chunks),
                "chunk_pages": manifest.chunk_pages,
                "page_count": manifest.page_count,
                "compacted_version": manifest.compacted_version,
                "chunk_codec": manifest.chunk_codec,
            }
        new_manifest = Manifest(
            version=manifest.version + 1,
            snapshot=manifest.snapshot,
            deltas=[
                *manifest.deltas,
                DeltaEntry(
                    key=delta_key,
                    etag=delta_etag,
                    size=len(payload),
                    epoch=effective_epoch,
                ),
            ],
            last_epoch=effective_epoch,
            **v2_fields,
        )
        try:
            new_etag = publish_manifest(
                self._backend, tenant, manifest, new_manifest, etag,
                before_cas=handle.ensure_alive if handle is not None else None,
            )
        except Exception as exc:
            self._manifest_cache.pop(tenant, None)
            # Burn the seq: the delta object was already uploaded write-once,
            # so a retry under the SAME epoch must use the next key (the
            # orphaned delta is unreferenced; gc() collects it by age).
            self._seqs[(tenant, effective_epoch)] = seq
            shutil.rmtree(dest, ignore_errors=True)
            if not isinstance(exc, CASFailed):
                raise
            raise Conflict(
                f"manifest CAS failed for tenant {tenant!r} "
                f"(version {manifest.version} -> {manifest.version + 1})"
            ) from None
        self._manifest_cache[tenant] = (new_etag, new_manifest)

        # 8. Success: advance the seq counter, keep the lease retained, and
        # record the just-published delta in the local applied marker: the
        # transaction ran on db.sqlite and conn.close() checkpointed it, so
        # the local file now equals base + every delta of the NEW manifest.
        # The next read (or write's ensure_local) needs no re-apply of our
        # own delta. Crash before this append costs one idempotent re-apply
        # on the next ensure_local (apply_delta is byte-identical).
        self._seqs[(tenant, effective_epoch)] = seq
        with self._postcommit(tenant):
            applied = _read_applied(dest / _APPLIED_MARKER)
            if delta_etag not in applied:
                applied.append(delta_etag)
                _atomic_write(
                    dest / _APPLIED_MARKER, json.dumps(applied).encode("utf-8")
                )
            self._stamp_used(tenant, handle)

            # 9. Inline compaction trigger: fold the chain when it grows past
            # either configured threshold. The lease is retained and alive, so
            # _acquire reuses it. compact() never appends deltas, so the trigger
            # cannot re-fire recursively. Failures are logged by _postcommit;
            # the write receipt remains authoritative.
            if self._inline_compact and (
                len(new_manifest.deltas) > self._inline_compact_deltas
                or sum(entry.size for entry in new_manifest.deltas)
                > self._inline_compact_bytes
            ):
                self.compact(tenant)

        return result, new_manifest.version

    # ------------------------------------------------------------------
    # VFS write path (format 2, not fully local)
    # ------------------------------------------------------------------

    def _write_vfs(
        self,
        tenant: str,
        fn: Callable[[sqlite3.Connection], Any],
        manifest: Manifest,
        etag: str,
        effective_epoch: int,
        *,
        before_cas: Callable[[], None] | None = None,
    ) -> tuple[Any, int]:
        """One committed write through a writable TesseraVFS session (M7-A2).

        Called from :meth:`write` for format-2 tenants that are not fully
        local; the epoch fence has already passed and *etag* is the current
        manifest's etag for the CAS. Steps:

        1. Writable session from the ``_vfs_keys`` cache, keyed
           ``(tenant, manifest.version)``. On a miss the session is
           registered with ``writable=True`` and a versioned sandbox dir
           ``<root>/<tenant>-sandbox/v<manifest.version>`` (the version the
           session was REGISTERED at — its birth version). On a hit the
           session is reused; since every write wipes the sandbox afterwards,
           the SAME dir is wiped + recreated before the connection opens (the
           simplest correct option: no VFS session state is mutated from the
           Store, and sidecars of a crashed previous write cannot leak into
           this one).
        2. Writable apsw connection on the session key with the M7-A1 pragma
           contract: ``PRAGMA locking_mode=EXCLUSIVE`` FIRST (no ``xShmMap``
           on the main file -> heap-memory wal-index), then
           ``PRAGMA journal_mode=WAL`` (reports "wal" with NO main-file
           writes — the VFS serves page 1 with the file-format bytes forced
           to 2/2), ``wal_autocheckpoint=0``, ``synchronous=NORMAL``, and
           ``SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`` so ``close()`` is clean (a
           close-time checkpoint could never succeed on the read-only main
           file).
        3. ``fn`` runs on a :class:`_ApswWriteConnAdapter`; on ANY exception
           the connection is closed and the sandbox wiped — no manifest
           change, ``fn`` never retried here.
        4. Post-commit: the ``-wal`` under the sandbox is located (it exists
           once any read/write transaction ran; a pure-read ``fn`` may leave
           none) and its committed frames are extracted WHILE the connection
           is open. No WAL / no committed frames -> no-op: the current
           version is returned unchanged.
        5. Delta PUT create-only (seq per ``(tenant, effective_epoch)``),
           then the manifest CAS with the v2 fields carried forward verbatim
           (``page_count`` stays the CHUNK BASE size — the delta's TSD1
           header records the true post-image size, exactly as on the plain
           v2 write path). A CAS failure closes the connection, wipes the
           sandbox and raises :class:`Conflict` — the warm session stays
           registered for the next attempt (its overlay is only updated
           AFTER a successful CAS).
        6. Success: the delta is merged into the warm session's overlay
           (:meth:`~tessera.vfs._Session.apply_delta`), the session is
           re-pointed at the new version in ``_vfs_keys`` (carry-forward: no
           re-register, no delta re-download on the next write at the new
           version), the connection is closed and the sandbox wiped.

        Note: inline compaction is a plain-path trigger only — a VFS write
        never compacts (re-chunk + session bookkeeping is future work).
        Storage failures surfaced by the VFS (a chunk fetch inside a page
        read) are translated back to :class:`BackendError`, matching the
        read path's contract.
        """
        if apsw is None:  # pragma: no cover - caller guards
            raise BackendError(
                "the VFS write path requires the 'vfs' extra "
                "(pip install 'tessera-db[vfs]')"
            )
        vfs = self._get_vfs()
        cache_key = (tenant, manifest.version)
        conn: "apsw.Connection | None" = None

        def _discard() -> None:
            shutil.rmtree(sandbox_dir, ignore_errors=True)

        def _close_quiet() -> None:
            # NO_CKPT_ON_CLOSE makes this clean (M7-A1 fact 5); kept
            # best-effort so a close-time surprise can never mask the real
            # outcome after the manifest was published.
            if conn is None:
                return
            try:
                conn.close()
            except Exception:
                pass

        # 1. Writable session (warm-session cache) + freshly wiped sandbox.
        session_key = self._vfs_keys.get(cache_key)
        session = vfs._sessions.get(session_key) if session_key is not None else None
        if session is not None and session.writable and session.sandbox_dir is not None:
            # Session hit: reuse ITS sandbox dir (the birth-version dir).
            sandbox_dir = Path(session.sandbox_dir)
        else:
            # Miss (or a stale/foreign cache entry): fresh versioned dir.
            session_key = None
            sandbox_dir = self._root / f"{tenant}-sandbox" / f"v{manifest.version}"
        shutil.rmtree(sandbox_dir, ignore_errors=True)
        sandbox_dir.mkdir(parents=True, exist_ok=True)
        if session_key is None:
            session_key = vfs.register(
                tenant,
                manifest,
                self._backend,
                writable=True,
                sandbox_dir=sandbox_dir,
            )
            self._vfs_keys[cache_key] = session_key

        # 2. Writable connection with the empirical M7-A1 pragma contract.
        try:
            conn = apsw.Connection(
                session_key, flags=apsw.SQLITE_OPEN_READWRITE, vfs=vfs.vfs_name
            )
            # locking_mode=EXCLUSIVE MUST come before any other statement
            # (M7-A1: WAL without xShmMap needs the heap-memory wal-index).
            conn.execute("PRAGMA locking_mode=EXCLUSIVE").fetchone()
            # The VFS serves page 1 with 2/2 file-format bytes (M7-A2), so
            # SQLite enters WAL directly: journal_mode=WAL reports "wal" and
            # writes NOTHING to the main file — even on a 1/1 VACUUM base.
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            entered_wal = mode == "wal"
            conn.execute("PRAGMA wal_autocheckpoint=0")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.config(apsw.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1)
        except apsw.Error as exc:
            _close_quiet()
            _discard()
            raise BackendError(str(exc)) from exc
        if not entered_wal:
            _close_quiet()
            _discard()
            raise BackendError(
                f"tenant {tenant!r}: VFS write session could not enter WAL "
                f"mode (journal_mode reported {mode!r})"
            )

        # 3. fn on the write adapter: real commit(), DML allowed. Any failure
        # discards the sandbox and propagates — the manifest is untouched.
        try:
            result = fn(_ApswWriteConnAdapter(conn))
        except apsw.IOError as exc:
            # The VFS converts BackendError (missing/corrupt chunk object)
            # into apsw.IOError inside xRead; restore the Store-level
            # contract (cause chain preserved), mirroring _read_vfs.
            _close_quiet()
            _discard()
            raise BackendError(str(exc)) from exc
        except BaseException:
            _close_quiet()
            _discard()
            raise

        # 4. Post-commit: committed frames out of the sandbox -wal while the
        # connection is still open. Absent WAL (pure-read fn) or a WAL with
        # no committed transactions -> no-op publish.
        wal_files = list(sandbox_dir.glob("*-wal"))
        delta = None
        if wal_files:
            try:
                delta = read_wal_frames(wal_files[0])
            except (ValueError, OSError):
                delta = None  # empty / uncommitted WAL: nothing to publish
        if delta is None:
            _close_quiet()
            _discard()
            return result, manifest.version

        payload = serialize_delta(delta)

        # 5a. Delta PUT write-once (seq is per (tenant, epoch), starts at 1).
        seq = self._seqs.get((tenant, effective_epoch), 0) + 1
        delta_key = _delta_key(tenant, effective_epoch, seq)
        self._seqs[(tenant, effective_epoch)] = seq
        try:
            delta_etag = self._backend.put(delta_key, payload, if_none_match=True)
        except Exception as exc:
            _close_quiet()
            _discard()
            if not isinstance(exc, CASFailed):
                raise
            raise Conflict(
                f"delta key {delta_key!r} already exists for tenant {tenant!r}"
            ) from None

        # 5b. Manifest CAS (if_match) + last_epoch fence. Failure means
        # another writer won: discard the sandbox, never retry fn.
        new_manifest = Manifest(
            format=2,
            version=manifest.version + 1,
            snapshot=manifest.snapshot,
            deltas=[
                *manifest.deltas,
                DeltaEntry(
                    key=delta_key,
                    etag=delta_etag,
                    size=len(payload),
                    epoch=effective_epoch,
                ),
            ],
            last_epoch=effective_epoch,
            chunks=list(manifest.chunks),
            chunk_pages=manifest.chunk_pages,
            page_count=manifest.page_count,
            compacted_version=manifest.compacted_version,
            chunk_codec=manifest.chunk_codec,
        )
        try:
            new_etag = publish_manifest(
                self._backend, tenant, manifest, new_manifest, etag,
                before_cas=before_cas,
            )
        except Exception as exc:
            self._manifest_cache.pop(tenant, None)
            # Burn the seq (same reasoning as the plain path): the delta is
            # already uploaded; a same-epoch retry needs the next key.
            self._seqs[(tenant, effective_epoch)] = seq
            _close_quiet()
            _discard()
            if not isinstance(exc, CASFailed):
                raise
            raise Conflict(
                f"manifest CAS failed for tenant {tenant!r} "
                f"(version {manifest.version} -> {manifest.version + 1})"
            ) from None
        self._manifest_cache[tenant] = (new_etag, new_manifest)

        # 6. Success: absorb the delta into the warm session
        # (overlay pages + logical page_count), carry the session forward to
        # the new version (same key, new cache entry; the old-version entry
        # is dropped), then close cleanly and wipe the sandbox (A2: wiped
        # after EVERY write).
        self._seqs[(tenant, effective_epoch)] = seq
        with self._postcommit(tenant):
            session = vfs._sessions.get(session_key)
            if session is not None:  # defensive: registered/hit above
                session.apply_delta(delta)
            self._vfs_keys.pop(cache_key, None)
            self._vfs_keys[(tenant, new_manifest.version)] = session_key
        _close_quiet()
        _discard()
        return result, new_manifest.version
