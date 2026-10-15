# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Publish path: committed WAL -> one delta + one manifest CAS, plus the
background incremental-compaction check."""

import json
import logging
import random
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from ..backends.base import (
    BackendError,
    CASFailed,
    LeaseLost,
    NotFound,
    TenantBusy,
)
from ..delta import serialize as serialize_delta
from ..local import _APPLIED_MARKER, _atomic_write, _read_applied
from ..manifest import DeltaEntry, Manifest, ManifestError
from ..publication import (
    HistoryCollisionError,
    archive_predecessor,
    cas_manifest,
    publish_manifest,
)
from ..store import Conflict, _delta_key, _manifest_key
from .utils import _COMPACT_BYTES, _COMPACT_DELTAS, _DB_NAME

logger = logging.getLogger(__name__)


# @implements durable-write-contract/durable-acknowledgement-and-visibility
# @implements bounded-write-cost/windowed-publication-amortizes-backend-writes
# @implements tenant-recovery/fenced-monotonic-recovery-publication
class _LocalPublishError(Exception):
    """Deterministic local fault in the publish path (missing/corrupt
    WAL, unserializable delta). No backend retry can heal it — the owner
    fences on the first attempt instead of burning the error budget."""


# Definitive fencing signals — they never heal by retrying, so they
# propagate immediately. Everything else a backend op can raise
# (transient network/5xx errors, lost responses) is retryable within
# the error budget: the publish protocol is idempotent (write-once
# delta/history objects with content reconcile, conditional manifest
# CAS), so a retry can only converge or hit the same fence.
_NO_RETRY = (
    CASFailed,
    LeaseLost,
    TenantBusy,
    NotFound,
    ManifestError,
    HistoryCollisionError,
    _LocalPublishError,
)
_PUBLISH_RETRY_BASE_S = 0.25
_PUBLISH_RETRY_CAP_S = 2.0


def _read_wal_frames(wal_path):
    """Extract committed frames through the package namespace.

    Resolved via ``tessera.owner`` at call time so tests can patch
    ``tessera.owner.read_wal_frames`` (same seam the original single-module
    layout provided).
    """
    from .. import owner as _owner_pkg

    return _owner_pkg.read_wal_frames(wal_path)


# @spec owner/publish-mixin
class PublishMixin:
    """Publication and compaction operations mixed into :class:`Owner`."""

    def _publish_once(self) -> bool:
        """Publish under the owner lock; exhausted retries retire the owner."""
        with self._lock:
            return self._publish_locked()

    def _publish_locked(self) -> bool:
        """Retry wrapper; the caller already holds ``_lock``.

        A transient backend error used to fence the owner permanently
        (issue #149 — proven on real WAN by the 8h AWS soak). The lock
        is held across the whole retry loop: committing writes cannot
        move anyway while the backend is down (their ack IS the
        publish), and letting the WAL change mid-retry would break the
        write-once reconcile that makes retries safe.
        """
        deadline = time.monotonic() + self._publish_error_budget_s
        delay = _PUBLISH_RETRY_BASE_S
        while True:
            try:
                return self._publish_attempt_locked()
            except _NO_RETRY:
                # Definitive fencing signal — same semantics as before:
                # flag and exit, never retry.
                self._fenced = True
                self._serving = False
                self._stop_event.set()
                raise
            except Exception:
                if time.monotonic() >= deadline:
                    self._fenced = True
                    self._serving = False
                    self._stop_event.set()
                    raise
                logger.info(
                    "owner %s: publish transient error; retrying "
                    "(%.1fs budget left)",
                    self._tenant,
                    deadline - time.monotonic(),
                    exc_info=True,
                )
                time.sleep(random.uniform(0.0, delay))
                delay = min(delay * 2, _PUBLISH_RETRY_CAP_S)

    def _publish_attempt_locked(self) -> bool:
        """Publish one dirty transaction; the caller already holds _lock."""
        if self._fenced:
            return False
        if not self._dirty or self._conn is None or self._dest is None:
            # Nothing outstanding: every committed write is already durable
            # (or the owner is torn down and the floor is moot anyway).
            self._published_commit_seq = max(
                self._published_commit_seq, self._commit_seq
            )
            return True  # torn-down owner (or never started): nothing to do
        if self._handle is None:
            self._fenced = True
            self._serving = False
            return False

        self._handle.ensure_alive()

        # Committed frames out of the local -wal WHILE the connection is
        # open (the last connection to close would checkpoint and delete
        # it). Missing or corrupt WAL while dirty is a publication failure —
        # LOCAL and deterministic, so it is wrapped as _LocalPublishError to
        # fence on the first attempt rather than retry the error budget.
        wal_path = Path(f"{self._dest / _DB_NAME}-wal")
        try:
            delta = _read_wal_frames(wal_path)
            payload = serialize_delta(delta)
        except Exception as exc:
            raise _LocalPublishError(
                f"local WAL read/serialize failed: {exc}"
            ) from exc

        # Delta PUT write-once; seq is per (tenant, epoch), in-memory from 1.
        epoch = self._epoch
        seq = self._seq + 1
        delta_key = _delta_key(self._tenant, epoch, seq)
        manifest_key = _manifest_key(self._tenant)

        # The owner knows the manifest it CAS'd last: the cached head makes
        # the manifest GET redundant, so a hot publish is delta PUT ∥
        # history PUT → CAS (two round trips) instead of four. A stale
        # cache can only LOSE the CAS — the conditional write validates it —
        # and the conflict path below refetches and retries, exactly like a
        # lost CAS always did.
        cached_manifest = self._cached_manifest
        cached_etag = self._cached_etag

        if cached_manifest is not None:
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_delta = pool.submit(
                    self._backend.put, delta_key, payload, if_none_match=True
                )
                f_hist = pool.submit(
                    archive_predecessor, self._backend, self._tenant,
                    cached_manifest,
                )
                try:
                    delta_etag = f_delta.result()
                except CASFailed:
                    delta_etag = self._reconcile_delta_collision(delta_key, payload)
                    if delta_etag is None:
                        return False
                f_hist.result()
            manifest = cached_manifest
            etag = cached_etag
            archived = True
        else:
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_delta = pool.submit(
                    self._backend.put, delta_key, payload, if_none_match=True
                )
                f_man = pool.submit(self._backend.get, manifest_key)
                try:
                    delta_etag = f_delta.result()
                except CASFailed:
                    delta_etag = self._reconcile_delta_collision(delta_key, payload)
                    if delta_etag is None:
                        return False
                try:
                    data, etag = f_man.result()
                except NotFound:
                    self._fenced = True
                    self._serving = False
                    logger.error(
                        "owner %s: manifest vanished (fencing) — stopping",
                        self._tenant,
                    )
                    return False
            manifest = Manifest.from_json(data.decode("utf-8"))
            archived = False

        if manifest.last_epoch > epoch:
            self._fenced = True
            self._serving = False
            logger.error(
                "owner %s: stale epoch %d < manifest last_epoch %d (fencing) "
                "— stopping",
                self._tenant,
                epoch,
                manifest.last_epoch,
            )
            return False
        new_manifest = self._manifest_after_publish(
            manifest,
            DeltaEntry(
                key=delta_key,
                etag=delta_etag,
                size=len(payload),
                epoch=epoch,
            ),
            epoch,
            self._commit_seq,
        )
        try:
            self._handle.ensure_alive()
            if archived:
                new_etag = cas_manifest(
                    self._backend, self._tenant, new_manifest, etag,
                    before_cas=self._handle.ensure_alive,
                )
            else:
                new_etag = publish_manifest(
                    self._backend, self._tenant, manifest, new_manifest, etag,
                    before_cas=self._handle.ensure_alive,
                )
        except CASFailed:
            # A lost CAS at our epoch can only be our own background
            # compaction rebasing the base (it runs outside the lock by
            # design): rebuild the manifest on the fresh committed state
            # and retry once — the delta object is already uploaded, we
            # only re-reference it. A manifest stamped with a NEWER epoch
            # is real fencing.
            fresh, fresh_etag = self._fetch_manifest_fencing_check(
                manifest_key, epoch
            )
            if fresh is None:
                return False
            if any(e.key == delta_key for e in fresh.deltas):
                # A previous attempt's CAS LANDED but its response was
                # lost (the transient-error retry can reach exactly
                # here): the committed head already contains this delta
                # — adopt it instead of re-appending a duplicate entry.
                new_manifest, new_etag = fresh, fresh_etag
            else:
                new_manifest = self._manifest_after_publish(
                    fresh, DeltaEntry(
                        key=delta_key, etag=delta_etag, size=len(payload),
                        epoch=epoch,
                    ), epoch, self._commit_seq,
                )
                try:
                    self._handle.ensure_alive()
                    new_etag = publish_manifest(
                        self._backend, self._tenant, fresh, new_manifest,
                        fresh_etag,
                        before_cas=self._handle.ensure_alive,
                    )
                except CASFailed:
                    self._fenced = True
                    self._serving = False
                    logger.error(
                        "owner %s: manifest CAS failed twice (fencing) "
                        "— stopping",
                        self._tenant,
                    )
                    return False

        # Success: bump seq, expose the new version, clear the flag. The
        # durable floor is the published manifest's DATA frontier (the
        # commits folded in), not its object generation — a compaction CAS
        # may have moved ``version`` past commits this publish now covers.
        self._seq = seq
        self._last_published_version = new_manifest.version
        self._published_commit_seq = new_manifest.data_version
        self._commit_seq = max(self._commit_seq, new_manifest.data_version)
        self._dirty = False
        # The committed head is exactly what we just CAS'd — cache it so the
        # next publish skips the manifest GET. Compaction (which runs
        # outside this lock) invalidates the cache after its own CAS.
        self._cached_manifest = new_manifest
        self._cached_etag = new_etag

        # Checkpoint the WAL back into db.sqlite (best-effort): single
        # connection, lock held, so this cannot race a write. Keeps the
        # local file equal to base + every published delta, so the next
        # publish captures only NEW frames and warm restarts hydrate
        # with zero downloads.
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        except sqlite3.Error:
            pass

        # Record the published etag in the applied marker (best-effort,
        # mirroring Store.write): warm restarts then skip re-applying it.
        try:
            applied = _read_applied(self._dest / _APPLIED_MARKER)
            if delta_etag not in applied:
                applied.append(delta_etag)
                _atomic_write(
                    self._dest / _APPLIED_MARKER,
                    json.dumps(applied).encode("utf-8"),
                )
        except OSError:
            pass

        logger.info(
            "owner %s: published version %d (%d frames, %d bytes) as %s",
            self._tenant,
            new_manifest.version,
            len(delta.frames),
            len(payload),
            delta_key,
        )
        return True

    @staticmethod
    def _manifest_after_publish(
        manifest: Manifest, delta_entry: DeltaEntry, epoch: int,
        commit_seq: int,
    ) -> Manifest:
        """New manifest = *manifest* + one delta, v2 fields carried verbatim.

        Two distinct counters are stamped: ``version`` = object generation
        (``max(manifest.version + 1, commit_seq)`` — monotonic even when a
        compaction CAS raced ahead) and ``data_version`` = the commit
        frontier actually covered (``commit_seq``; this publish folds every
        locally committed write). A ``min_version`` read resolves against
        ``data_version`` — never against the bare object version, which
        compaction can advance without containing those commits.
        """
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
        return Manifest(
            version=max(manifest.version + 1, commit_seq),
            snapshot=manifest.snapshot,
            deltas=[*manifest.deltas, delta_entry],
            last_epoch=epoch,
            data_version=commit_seq,
            **v2_fields,
        )

    def _reconcile_delta_collision(
        self, delta_key: str, payload: bytes
    ) -> str | None:
        """CASFailed on a write-once delta key; return its etag or fence.

        Only this owner ever uses its epoch, so the realistic cause is OUR
        OWN orphan from an attempt that died between the delta PUT and the
        manifest CAS (a retry after a transient error): identical content
        reconciles to the stored object, anything else is real fencing.
        """
        try:
            existing, existing_etag = self._backend.get(delta_key)
        except NotFound:
            existing, existing_etag = None, ""
        if existing != payload:
            self._fenced = True
            self._serving = False
            logger.error(
                "owner %s: delta key %r collision (fencing) — stopping",
                self._tenant,
                delta_key,
            )
            return None
        return existing_etag

    def _fetch_manifest_fencing_check(
        self, manifest_key: str, epoch: int
    ) -> "tuple[Manifest | None, str]":
        """Re-fetch the manifest and classify a lost CAS.

        Same epoch means our own sibling won (publish vs compaction —
        both live in this process): the caller retries. A NEWER epoch is
        real fencing: flag the owner and return ``(None, "")``.
        """
        try:
            data, etag = self._backend.get(manifest_key)
        except NotFound:
            self._fenced = True
            self._serving = False
            logger.error(
                "owner %s: manifest vanished (fencing) — stopping", self._tenant
            )
            return None, ""
        fresh = Manifest.from_json(data.decode("utf-8"))
        if fresh.last_epoch > epoch:
            self._fenced = True
            self._serving = False
            logger.error(
                "owner %s: stale epoch %d < manifest last_epoch %d (fencing) "
                "— stopping",
                self._tenant,
                epoch,
                fresh.last_epoch,
            )
            return None, ""
        return fresh, etag

    # ------------------------------------------------------------------
    # Compaction path (background, format 2 only)
    # ------------------------------------------------------------------

    def _maybe_compact(self) -> None:
        """Fold the delta chain into the chunk base when it grew too big.

        Format-2 tenants only, thresholds mirroring the Store's inline
        trigger. Safe for the warm connection: while we hold the lease no
        external writer can exist, and ``compact_incremental`` never touches
        ``db.sqlite`` (it merges deltas into the chunk base in memory) — the
        full ``compact``/``_rechunk`` path that rewrites ``db.sqlite`` is
        never invoked from here (a v1 manifest is skipped, and the format
        cannot change under our lease).
        """
        store = self._store
        if store is None:
            return
        try:
            data, _etag = self._backend.get(_manifest_key(self._tenant))
        except NotFound:
            return  # tenant deleted; the publish path will fence us out
        except BackendError:
            logger.exception(
                "owner %s: compaction check failed transiently; skipping cycle",
                self._tenant,
            )
            return
        try:
            manifest = Manifest.from_json(data.decode("utf-8"))
        except ManifestError:
            logger.exception("owner %s: unparseable manifest; skipping cycle", self._tenant)
            return
        if manifest.format != 2:
            return
        total_bytes = sum(entry.size for entry in manifest.deltas)
        if not (
            len(manifest.deltas) > _COMPACT_DELTAS or total_bytes > _COMPACT_BYTES
        ):
            return

        # Keep the seeded handle inside the store's retention window so
        # Store._acquire reuses THE owner's lease (see the seeding in start).
        store._handle_used_at[self._tenant] = time.time()
        try:
            result = store.compact_incremental(self._tenant)
            # The compaction CAS moved the manifest's object generation —
            # and replaced the published head, so the publish path's cached
            # manifest/etag is stale now. Drop it WITHOUT taking ``_lock``:
            # this path deliberately never takes it (a publish holds it
            # across its WAN CAS — waiting here could deadlock the pair),
            # and a stale cache can only LOSE the conditional CAS anyway:
            # the publish path refetches and retries like any lost CAS.
            # ``data_version`` carried the commit frontier unchanged, so no
            # commit-side counter needs updating either.
            self._cached_manifest = None
            self._cached_etag = ""
            logger.info(
                "owner %s: incremental compaction done (version %d)",
                self._tenant,
                result.version,
            )
        except Conflict:
            # Distinguish a real fencing loss (a NEWER epoch moved in) from
            # losing the manifest CAS to our own /write publish, which holds
            # the owner lock while this path deliberately does not: a
            # same-epoch manifest means our sibling won — skip this cycle
            # and refetch next tick instead of killing a healthy owner.
            try:
                data, _ = self._backend.get(_manifest_key(self._tenant))
                fresh_epoch = Manifest.from_json(
                    data.decode("utf-8")
                ).last_epoch
            except (NotFound, BackendError, ManifestError):
                fresh_epoch = None
            if fresh_epoch is not None and fresh_epoch <= self._epoch:
                logger.info(
                    "owner %s: compaction lost the CAS to our own publish "
                    "— skipping cycle",
                    self._tenant,
                )
                return
            with self._lock:
                self._fenced = True
                self._serving = False
            logger.error(
                "owner %s: compaction lost the manifest CAS (fencing) — stopping",
                self._tenant,
            )
        except LeaseLost:
            with self._lock:
                self._fenced = True
                self._serving = False
            logger.error(
                "owner %s: lease lost during compaction — stopping", self._tenant
            )
        except TenantBusy:
            # Cannot happen with the seeded handle, but a lost race in the
            # retention window would surface here: skip this cycle only.
            logger.warning(
                "owner %s: compaction skipped (tenant busy)", self._tenant
            )
        except BackendError:
            logger.exception(
                "owner %s: compaction failed transiently; skipping cycle",
                self._tenant,
            )
