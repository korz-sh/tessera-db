"""Bounded history, point-in-time recovery, export, and conservative GC."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from tessera import LocalBackend
from tessera.backends.base import CASFailed, LeaseLost, NotFound

from .snapshot import InvalidSnapshot, canonical, connect


@dataclass(frozen=True)
# @spec turso/retention-policy
class RetentionPolicy:
    """Operator retention; independent of any object-store lifecycle rules."""

    keep_last: int = 8
    retain_seconds: float = 7 * 24 * 3600
    artifact_grace_seconds: float = 3600

    def __post_init__(self):
        if type(self.keep_last) is not int or self.keep_last < 0:
            raise ValueError("keep_last must be a nonnegative integer")
        for name in ("retain_seconds", "artifact_grace_seconds"):
            value = getattr(self, name)
            if (not isinstance(value, (int, float)) or isinstance(value, bool)
                    or value < 0):
                raise ValueError(f"{name} must be nonnegative")


# @spec turso/metadata-limit-exceeded
class MetadataLimitExceeded(RuntimeError):
    """A complete GC reachability scan exceeded its per-step metadata budget."""


# @spec turso/retention-session
class RetentionSession:
    """Hold a fenced group lease for metadata-only GC, without restoring SQLite."""

    def __init__(self, backend, group, *, policy=None, lease_ttl_ms=25_000):
        if not 1_000 <= lease_ttl_ms <= 300_000:
            raise ValueError("invalid maintenance lease lifetime")
        from .native import NativeLogStore

        self.storage = NativeLogStore(backend, group)
        self._lease_ttl_ms = lease_ttl_ms
        self._lease_guard = threading.Lock()
        self._lock = threading.RLock()
        self._lease = None
        self.policy = policy

    def __enter__(self):
        current, _ = self.storage.manifest()
        self._lease = self.storage.backend.acquire_lease(
            "turso-experiment-" + self.storage.group, self._lease_ttl_ms,
            min_epoch=current["epoch"])
        self.storage.lease = self._lease
        return LifecycleManager(self, policy=self.policy)

    def _require_live_heartbeat(self):
        if self._lease is None or time.time() >= self._lease.expires_at - 1:
            raise LeaseLost("maintenance lease expired")

    def __exit__(self, _kind, _error, _traceback):
        if self._lease is not None:
            try:
                self.storage.backend.release_lease(self._lease)
            except Exception:
                if _error is None:
                    raise
            finally:
                self._lease = None


def _owner_locked(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self.owner._lock:
            return method(self, *args, **kwargs)
    return locked


# @spec turso/lifecycle-manager
class LifecycleManager:
    def __init__(self, owner, *, policy=None, clock=time.time):
        self.owner = owner
        self.storage = owner.storage
        self.backend = self.storage.backend
        self.policy = RetentionPolicy() if policy is None else policy
        if not isinstance(self.policy, RetentionPolicy):
            raise TypeError("policy must be RetentionPolicy")
        self._clock = clock

    def incremental_mark_step(self, *, max_metadata=32, max_chunk_bytes=1_048_576,
                              max_seconds=15, restart=False):
        """Advance a durable native mark page; this method never deletes images."""
        from .gc_mark import mark_step

        return mark_step(self, max_metadata=max_metadata,
                         max_chunk_bytes=max_chunk_bytes, max_seconds=max_seconds,
                         restart=restart)

    def exact_index_step(self, *, max_chunks=1024, max_input_bytes=16_777_216,
                         max_index_bytes=8_388_608, max_seconds=20):
        """Verify a completed native mark and publish one exact index; no sweep."""
        from .gc_reduce import exact_index_step

        return exact_index_step(self, max_chunks=max_chunks,
                                max_input_bytes=max_input_bytes,
                                max_index_bytes=max_index_bytes,
                                max_seconds=max_seconds)

    def incremental_exact_index_step(self, *, max_chunks=32,
                                     max_input_bytes=4_194_304,
                                     max_index_bytes=8_388_608,
                                     max_seconds=20):
        """Reduce a marked native cycle in fenced, resumable batches."""
        from .gc_reduce import incremental_exact_index_step

        return incremental_exact_index_step(
            self, max_chunks=max_chunks, max_input_bytes=max_input_bytes,
            max_index_bytes=max_index_bytes, max_seconds=max_seconds)

    def stale_gc_cleanup_step(self, *, cursor=None, max_scan=100,
                              max_objects=100, max_bytes=16_777_216,
                              max_seconds=15):
        """Delete bounded temporary GC objects outside the active native cycle."""
        from .gc_cleanup import stale_gc_cleanup_step

        return stale_gc_cleanup_step(self, cursor=cursor, max_scan=max_scan,
                                     max_objects=max_objects, max_bytes=max_bytes,
                                     max_seconds=max_seconds)

    def verify_gc_pins_now(self, *, max_pins=1000, page_size=100,
                           max_input_bytes=16_777_216, max_seconds=20):
        """Check the live pin set against an indexed native mark; no sweep permit."""
        from .gc_pins import verify_gc_pins_now

        return verify_gc_pins_now(self, max_pins=max_pins, page_size=page_size,
                                  max_input_bytes=max_input_bytes,
                                  max_seconds=max_seconds)

    def experimental_image_sweep_step(self, *, max_scan=100, max_deletes=100,
                                      max_bytes=16_777_216, max_seconds=20,
                                      max_pins=1000):
        """Run one fenced native image sweep page; explicitly unscheduled alpha path."""
        from .gc_sweep import image_sweep_step

        return image_sweep_step(self, max_scan=max_scan, max_deletes=max_deletes,
                                max_bytes=max_bytes, max_seconds=max_seconds,
                                max_pins=max_pins)

    def native_gc_cycle_step(self, *, max_seconds=12, max_steps=6,
                             restart_after_seconds=86_400):
        """Advance a fenced native mark/index/sweep cycle within one call budget.

        The caller still chooses when this experimental flow is scheduled. A
        completed cycle is refreshed after a day so late orphan uploads and
        newly expired history are eventually reconsidered without a new head.
        """
        from .gc_mark import _read_state
        from .gc_pins import PinInventoryChanged

        if (not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
                or not 0 < max_seconds <= 20 or type(max_steps) is not int
                or not 1 <= max_steps <= 20
                or not isinstance(restart_after_seconds, (int, float))
                or isinstance(restart_after_seconds, bool)
                or not 60 <= restart_after_seconds <= 31_536_000):
            raise ValueError("invalid native GC cycle budget")
        deadline = time.monotonic() + max_seconds
        state, _ = _read_state(self.backend, self.storage.group)
        restart = (state is not None and state["phase"] == "complete"
                   and time.time() - state["started_at"] >= restart_after_seconds)
        deleted = removed_bytes = steps = 0
        last = None
        while steps < max_steps and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            initial_probe = last is None
            if initial_probe or last["phase"] in {"history", "pins", "deadline"}:
                last = self.incremental_mark_step(
                    max_seconds=min(15, remaining), restart=restart)
                restart = False
                if initial_probe and last["phase"] in {
                        "marked_unverified", "indexed_unverified", "sweep"}:
                    # A closed mark returns its phase without advancing work.
                    # The next operation still fits this visit's step budget.
                    continue
            elif last["phase"] == "marked_unverified":
                last = self.incremental_exact_index_step(max_seconds=min(20, remaining))
            elif last["phase"] in {"indexed_unverified", "sweep"}:
                try:
                    last = self.experimental_image_sweep_step(
                        max_seconds=min(20, remaining))
                except PinInventoryChanged:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    last = self.incremental_mark_step(max_seconds=min(15, remaining),
                                                      restart=True)
            elif last["phase"] in {"complete", "budget"}:
                break
            else:
                raise RuntimeError("unexpected native GC phase")
            steps += 1
            deleted += last.get("deleted", 0)
            removed_bytes += last.get("bytes", 0)
            if last["phase"] in {"complete", "budget", "deadline"}:
                break
        return {"phase": "native_gc", "stage": last["phase"] if last else "deadline",
                "reason": last.get("reason") if last else None,
                "steps": steps, "deleted": deleted, "bytes": removed_bytes,
                "next_cursor": None}

    def _history(self):
        found = {}
        for key in self.backend.list(self.storage.prefix + "history/"):
            raw, _ = self.backend.get(key)
            try:
                manifest = json.loads(raw)
                version = manifest["version"]
                if type(version) is not int or version < 0:
                    raise ValueError
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise InvalidSnapshot(f"invalid history object {key!r}") from exc
            found[version] = (manifest, key)
        return found

    def versions(self):
        history = self._history()
        current, _ = self.storage.manifest()
        history[current["version"]] = (current, self.storage.key)
        return sorted(history)

    def manifest_at(self, version):
        if type(version) is not int or version < 0:
            raise ValueError("version must be a nonnegative integer")
        current, _ = self.storage.manifest()
        if current["version"] == version:
            return current
        if hasattr(self.storage, "parse_manifest"):
            key = f"{self.storage.prefix}history/{version:020d}.json"
            try:
                raw, etag = self.backend.get(key)
            except NotFound:
                raise NotFound(f"published version {version} is not retained") from None
            manifest, _ = self.storage.parse_manifest(raw, etag)
            if manifest["version"] != version:
                raise InvalidSnapshot("history key and published version differ")
            return manifest
        try:
            return self._history()[version][0]
        except KeyError:
            raise NotFound(f"published version {version} is not retained") from None

    @_owner_locked
    def inspect(self, version, sql, params=()):
        from .api import check_sql
        check_sql(sql, read=True)
        manifest = self.manifest_at(version)
        with tempfile.TemporaryDirectory(prefix="turso-inspect-") as root:
            path = Path(root) / "db.sqlite"
            self.storage.restore_manifest(manifest, path)
            with closing(connect(path)) as conn:
                self.storage.validate(conn, manifest)
                return conn.execute(sql, params).fetchall()

    @_owner_locked
    def export(self, version, destination):
        destination = Path(destination).absolute()
        if os.path.lexists(destination):
            raise FileExistsError(destination)
        manifest = self.manifest_at(version)
        with tempfile.TemporaryDirectory(
            prefix="turso-export-", dir=destination.parent
        ) as root:
            path = Path(root) / "db.sqlite"
            self.storage.restore_manifest(manifest, path)
            with closing(connect(path)) as conn:
                self.storage.validate(conn, manifest)
                conn.execute("PRAGMA journal_mode=wal").fetchall()
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            with closing(sqlite3.connect(path)) as conn:
                if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise InvalidSnapshot("export failed SQLite integrity check")
            os.link(path, destination)
        return destination

    def restore(self, version):
        """Restore an old frontier by publishing it at a new version."""
        with self.owner._lock:
            self.owner._require_live_heartbeat()
            source = self.manifest_at(version)
            pin = self.storage.prefix + "restore-pins/" + uuid.uuid4().hex + ".json"
            self.backend.put(pin, canonical(source), if_none_match=True)
            try:
                with tempfile.TemporaryDirectory(prefix="turso-restore-") as root:
                    path = Path(root) / "work.db"
                    self.storage.restore_manifest(source, path)
                    with closing(connect(path)) as conn:
                        self.storage.validate(conn, source)
                        publish = getattr(
                            self.storage, "publish_restored", self.storage.publish
                        )
                        with self.owner._lease_guard:
                            self.owner._require_live_heartbeat()
                            self.owner._lease = self.backend.renew_lease(
                                self.owner._lease, self.owner._lease_ttl_ms
                            )
                            self.storage.lease = self.owner._lease
                            manifest, etag = publish(
                                conn, version=self.owner._manifest["version"] + 1,
                                epoch=self.owner._lease.epoch,
                                etag=self.owner._etag, source=path,
                            )
                self.owner._manifest, self.owner._etag = manifest, etag
                self.owner._reload()
                return manifest
            finally:
                self.backend.delete(pin)

    @_owner_locked
    def clone(self, version, target_group, *, backend=None, reset_receipts=False,
              max_frontier_bytes=None):
        """Create an independent group from one retained frontier."""
        if type(reset_receipts) is not bool:
            raise ValueError("reset_receipts must be a boolean")
        if (max_frontier_bytes is not None
                and (type(max_frontier_bytes) is not int or max_frontier_bytes < 1)):
            raise ValueError("max_frontier_bytes must be a positive integer or None")
        self.owner._require_live_heartbeat()
        target_backend = self.backend if backend is None else backend
        source = self.manifest_at(version)
        pin = self.storage.prefix + "restore-pins/" + uuid.uuid4().hex + ".json"
        self.backend.put(pin, canonical(source), if_none_match=True)
        try:
            target = type(self.storage)(target_backend, target_group)
            target.max_frontier_bytes = max_frontier_bytes
            try:
                target.manifest()
            except NotFound:
                pass
            else:
                raise FileExistsError(f"target group {target_group!r} already exists")
            lease = target_backend.acquire_lease(
                "turso-experiment-" + target_group, self.owner._lease_ttl_ms
            )
            target.lease = lease
            try:
                with tempfile.TemporaryDirectory(prefix="turso-clone-") as root:
                    path = Path(root) / "work.db"
                    self.storage.restore_manifest(source, path)
                    with closing(connect(path)) as conn:
                        self.storage.validate(conn, source)
                        if reset_receipts:
                            conn.execute("DELETE FROM _tessera_receipts").fetchall()
                        self.owner._require_live_heartbeat()
                        manifest, _ = target.publish(
                            conn, version=0, epoch=lease.epoch, etag=None, source=path
                        )
                return manifest
            finally:
                target_backend.release_lease(lease)
        finally:
            self.backend.delete(pin)

    def _retained(self, history, now):
        newest = set(sorted(history, reverse=True)[:self.policy.keep_last])
        for version, (_, key) in history.items():
            modified = self.backend.mtime(key)
            if modified is None or now - modified < self.policy.retain_seconds:
                newest.add(version)
        return newest

    def _bounded_metadata_page(self, prefix, max_metadata, deadline, *, allow_overflow=False):
        """Prove a complete metadata listing before any GC decision."""
        collected = []
        cursor = None
        while True:
            if time.monotonic() >= deadline:
                return None
            page = self.backend.list_page(
                prefix, max_metadata + 1 - len(collected), start_after=cursor)
            if not page:
                return collected
            for key, _ in page:
                if not key.startswith(prefix) or (cursor is not None and key <= cursor):
                    raise RuntimeError("GC metadata listing did not advance")
                cursor = key
            collected.extend(page)
            if len(collected) > max_metadata:
                if allow_overflow:
                    return collected
                raise MetadataLimitExceeded("GC metadata limit exceeded; no objects deleted")

    def _checked_history_version(self, key, current_version):
        prefix = self.storage.prefix + "history/"
        suffix = key[len(prefix):]
        if (not key.startswith(prefix) or len(suffix) != 25
                or not suffix.endswith(".json") or not suffix[:20].isascii()
                or not suffix[:20].isdigit()):
            raise InvalidSnapshot(f"invalid history key {key!r}")
        version = int(suffix[:20])
        if version >= current_version:
            raise InvalidSnapshot(f"history is not older than the published head: {key!r}")
        raw, etag = self.backend.get(key)
        try:
            manifest = (self.storage.parse_manifest(raw, etag)[0]
                        if hasattr(self.storage, "parse_manifest") else json.loads(raw))
            manifest_version = manifest["version"]
            if type(manifest_version) is not int or manifest_version != version:
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, InvalidSnapshot) as exc:
            raise InvalidSnapshot(f"invalid history object {key!r}") from exc
        return version

    def prune_overflow_history_step(self, *, max_objects=100, max_bytes=16_777_216,
                                    max_seconds=15, max_metadata=999):
        """Prune proven-expired history even when a full image mark cannot fit.

        The first `max_metadata + 1` history keys prove that every candidate
        before the last `keep_last` keys has enough newer history to survive a
        policy count check. Images are never touched in this overflow phase.
        """
        if (not 1 <= max_objects <= 1000 or max_bytes <= 0
                or not 0 < max_seconds <= 20 or not 1 <= max_metadata <= 999):
            raise ValueError("invalid GC budget")
        if not hasattr(self.backend, "list_page"):
            raise RuntimeError("bounded GC requires paged object listing")
        deadline = time.monotonic() + max_seconds
        with self.owner._lock:
            self.owner._require_live_heartbeat()
            now = self._clock()
            current, current_etag = self.storage.manifest()
            pins_page = self._bounded_metadata_page(
                self.storage.prefix + "restore-pins/", max_metadata, deadline)
            if pins_page is None:
                return {"phase": "deadline", "deleted": 0, "bytes": 0,
                        "next_cursor": None}
            pinned_versions = set()
            for key, _ in pins_page:
                if time.monotonic() >= deadline:
                    return {"phase": "deadline", "deleted": 0, "bytes": 0,
                            "next_cursor": None}
                raw, _ = self.backend.get(key)
                try:
                    version = json.loads(raw)["version"]
                    if type(version) is not int or version < 0:
                        raise ValueError
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise InvalidSnapshot(f"invalid restore pin {key!r}") from exc
                pinned_versions.add(version)
            history_prefix = self.storage.prefix + "history/"
            history_page = self._bounded_metadata_page(
                history_prefix, max_metadata, deadline, allow_overflow=True)
            if history_page is None:
                return {"phase": "deadline", "deleted": 0, "bytes": 0,
                        "next_cursor": None}
            # A complete short listing belongs to the ordinary GC path. The
            # count proof needs `keep_last` newer keys inside this page.
            if len(history_page) <= max_metadata:
                return {"phase": "history_overflow", "deleted": 0, "bytes": 0,
                        "next_cursor": None}
            candidates = history_page[:max(0, len(history_page) - self.policy.keep_last)]
            if candidates:
                # Malformed newer objects must not count toward `keep_last`.
                for key, _ in history_page[len(candidates):]:
                    if time.monotonic() >= deadline:
                        return {"phase": "deadline", "deleted": 0, "bytes": 0,
                                "next_cursor": None}
                    self._checked_history_version(key, current["version"])
            deleted = removed_bytes = 0
            for key, size in candidates:
                if (time.monotonic() >= deadline or deleted >= max_objects
                        or (deleted and removed_bytes + size > max_bytes)):
                    break
                version = self._checked_history_version(key, current["version"])
                if version in pinned_versions:
                    continue
                modified = self.backend.mtime(key)
                if modified is None or now - modified < self.policy.retain_seconds:
                    continue
                self._gc_delete_checked(key, current_etag, image=False)
                deleted += 1
                removed_bytes += size
                if removed_bytes >= max_bytes:
                    break
            return {"phase": "history_overflow", "deleted": deleted,
                    "bytes": removed_bytes, "next_cursor": None}

    def gc_step(self, *, cursor=None, max_objects=100, max_bytes=16_777_216,
                max_seconds=15, max_metadata=999):
        """Run one bounded retention pass; return a cursor for the image scan.

        Metadata must fit in a bounded complete listing. If it does not, stop
        without deleting anything rather than using incomplete reachability.
        The caller restarts from the beginning after a completed image cycle.
        """
        if (not 1 <= max_objects <= 1000 or max_bytes <= 0
                or not 0 < max_seconds <= 20 or not 1 <= max_metadata <= 999):
            raise ValueError("invalid GC budget")
        if cursor is not None and (not isinstance(cursor, str)
                                   or not cursor.startswith(self.storage.prefix + "images/")):
            raise ValueError("invalid GC cursor")
        if not hasattr(self.backend, "list_page"):
            raise RuntimeError("bounded GC requires paged object listing")
        deadline = time.monotonic() + max_seconds
        with self.owner._lock:
            self.owner._require_live_heartbeat()
            now = self._clock()
            current, current_etag = self.storage.manifest()
            history_page = self._bounded_metadata_page(
                self.storage.prefix + "history/", max_metadata, deadline)
            if history_page is None:
                return {"phase": "deadline", "deleted": 0, "bytes": 0,
                        "next_cursor": cursor}
            pins_page = self._bounded_metadata_page(
                self.storage.prefix + "restore-pins/", max_metadata, deadline)
            if pins_page is None:
                return {"phase": "deadline", "deleted": 0, "bytes": 0,
                        "next_cursor": cursor}
            history = {}
            for key, _ in history_page:
                if time.monotonic() >= deadline:
                    return {"phase": "deadline", "deleted": 0, "bytes": 0,
                            "next_cursor": cursor}
                raw, _ = self.backend.get(key)
                try:
                    manifest = json.loads(raw)
                    version = manifest["version"]
                    if type(version) is not int or version < 0:
                        raise ValueError
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise InvalidSnapshot(f"invalid history object {key!r}") from exc
                history[version] = (manifest, key)
            pinned = []
            for key, _ in pins_page:
                if time.monotonic() >= deadline:
                    return {"phase": "deadline", "deleted": 0, "bytes": 0,
                            "next_cursor": cursor}
                raw, _ = self.backend.get(key)
                pinned.append(json.loads(raw))
            retained_versions = self._retained(history, now)
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "deleted": 0, "bytes": 0,
                        "next_cursor": cursor}
            retained_versions.update(manifest["version"] for manifest in pinned
                                     if manifest["version"] in history)
            prune = [key for version, (_, key) in history.items()
                     if version not in retained_versions]
            if prune:
                selected = []
                removed_bytes = 0
                sizes = dict(history_page)
                for key in sorted(prune):
                    size = sizes[key]
                    if (time.monotonic() >= deadline
                            or len(selected) >= max_objects
                            or (selected and removed_bytes + size > max_bytes)):
                        break
                    selected.append(key)
                    removed_bytes += size
                    if removed_bytes >= max_bytes:
                        break
                for key in selected:
                    self._gc_delete_checked(key, current_etag, image=False)
                return {"phase": "history", "deleted": len(selected),
                        "bytes": removed_bytes, "next_cursor": None}
            reachable = set()
            for manifest in [current, *(item[0] for item in history.values()), *pinned]:
                reachable.update(self.storage.artifact_keys(manifest))
            page = self.backend.list_page(self.storage.prefix + "images/",
                                          max_objects, start_after=cursor)
            if not page:
                return {"phase": "images", "deleted": 0, "bytes": 0,
                        "next_cursor": None, "wrapped": True}
            removed = removed_bytes = 0
            next_cursor = cursor
            for key, size in page:
                if time.monotonic() >= deadline:
                    break
                if key not in reachable:
                    modified = self.backend.mtime(key)
                    if (modified is not None
                            and now - modified >= self.policy.artifact_grace_seconds):
                        if removed and removed_bytes + size > max_bytes:
                            break
                        self._gc_delete_checked(key, current_etag, image=True)
                        removed += 1
                        removed_bytes += size
                next_cursor = key
                if removed_bytes >= max_bytes:
                    break
            return {"phase": "images", "deleted": removed, "bytes": removed_bytes,
                    "next_cursor": next_cursor, "wrapped": False}

    def _gc_delete_checked(self, key, current_etag, *, image):
        with self.owner._lease_guard:
            self.owner._require_live_heartbeat()
            if not isinstance(self.backend, LocalBackend):
                self.owner._lease = self.backend.renew_lease(
                    self.owner._lease, self.owner._lease_ttl_ms)
                self.storage.lease = self.owner._lease
            if self.storage.manifest()[1] != current_etag:
                raise CASFailed("published frontier changed during GC")
            if image and not isinstance(self.backend, LocalBackend):
                self.backend.delete_retired_image(key, self.owner._lease)
            else:
                self.backend.delete(key)

    def gc(self, *, dry_run=False):
        """Prune expired history, then sweep unreferenced artifacts.

        On S3/DynamoDB, every deletion rechecks the live lease and the exact
        published pointer. If the owner was displaced, GC stops with any
        already-deleted *retired* objects remaining deleted. Current and
        retained frontiers are never candidates.
        """
        with self.owner._lock:
            self.owner._require_live_heartbeat()
            now = self._clock()
            current, current_etag = self.storage.manifest()
            history = self._history()
            retained_versions = self._retained(history, now)
            pin_keys = self.backend.list(self.storage.prefix + "restore-pins/")
            pinned = []
            for key in pin_keys:
                raw, _ = self.backend.get(key)
                pinned.append(json.loads(raw))
            retained_versions.update(
                manifest["version"] for manifest in pinned
                if manifest["version"] in history
            )
            manifests = ([current] + [history[v][0] for v in retained_versions]
                         + pinned)
            reachable = set()
            for manifest in manifests:
                reachable.update(self.storage.artifact_keys(manifest))
            candidates = []
            for key in self.backend.list(self.storage.prefix + "images/"):
                modified = self.backend.mtime(key)
                if (key not in reachable and modified is not None
                        and now - modified >= self.policy.artifact_grace_seconds):
                    candidates.append(key)
            prune = [key for version, (_, key) in history.items()
                     if version not in retained_versions]
            if not dry_run:
                # Remove history first: a crash between passes leaks images,
                # which is safe. A later pass can collect them after grace.
                for key in prune + candidates:
                    if not isinstance(self.backend, LocalBackend):
                        # A concurrent owner cannot publish under our live
                        # lease. The head check also rejects an expired owner
                        # resuming after takeover. Each mutation is checked
                        # independently because a long S3 sweep may outlive
                        # one lease period.
                        with self.owner._lease_guard:
                            self.owner._require_live_heartbeat()
                            self.owner._lease = self.backend.renew_lease(
                                self.owner._lease, self.owner._lease_ttl_ms)
                            self.storage.lease = self.owner._lease
                            if self.storage.manifest()[1] != current_etag:
                                raise CASFailed("published frontier changed during GC")
                            if key in candidates:
                                self.backend.delete_retired_image(key, self.owner._lease)
                            else:
                                self.backend.delete(key)
                    else:
                        self.backend.delete(key)
            suffix = "candidates" if dry_run else "deleted"
            return {
                f"artifacts_{suffix}": sorted(candidates),
                ("history_candidates" if dry_run else "history_pruned"): sorted(prune),
                "retained_versions": sorted(
                    {current["version"], *retained_versions}
                ),
                "restore_pins": sorted(pin_keys),
            }
