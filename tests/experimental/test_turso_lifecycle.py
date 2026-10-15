import json
import sqlite3
from contextlib import closing

import pytest

pytest.importorskip("turso")

from tessera import LocalBackend
from tessera.backends.base import LeaseLost
from tessera.experimental.turso import BatchOwner, Request, RetentionPolicy
from tessera.experimental.turso.api import DatabaseQuotaExceeded
from tessera.experimental.turso.snapshot import InvalidSnapshot, canonical

SCHEMA = ["CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)"]


class PagedLocalBackend(LocalBackend):
    def list_page(self, prefix, limit=1000, *, start_after=None):
        keys = [key for key in self.list(prefix)
                if start_after is None or key > start_after]
        return [(key, self._path(key).stat().st_size) for key in keys[:limit]]


def test_bounded_gc_rejects_short_truncated_metadata_pages(tmp_path, monkeypatch):
    backend = PagedLocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        old_image = owner.storage.manifest()[0]["checkpoint"]["key"]
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        original = backend.list_page

        def short_page(prefix, limit=1000, *, start_after=None):
            if "/history/" in prefix or "/restore-pins/" in prefix:
                limit = min(limit, 1)
            return original(prefix, limit, start_after=start_after)

        monkeypatch.setattr(backend, "list_page", short_page)
        with pytest.raises(RuntimeError, match="metadata limit"):
            owner.lifecycle(policy=RetentionPolicy(
                keep_last=1, retain_seconds=0, artifact_grace_seconds=0)
            ).gc_step(max_metadata=1)
        assert old_image in backend.list(owner.storage.prefix + "images/")


def test_bounded_gc_keeps_history_reachable_after_short_pages(tmp_path, monkeypatch):
    backend = PagedLocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        historical_image = owner.lifecycle().manifest_at(1)["image"]
        original = backend.list_page

        def short_page(prefix, limit=1000, *, start_after=None):
            if "/history/" in prefix:
                limit = min(limit, 1)
            return original(prefix, limit, start_after=start_after)

        monkeypatch.setattr(backend, "list_page", short_page)
        policy = RetentionPolicy(keep_last=2, retain_seconds=0, artifact_grace_seconds=0)
        owner.lifecycle(policy=policy).gc_step(max_metadata=3)
        assert historical_image in backend.list(owner.storage.prefix + "images/")


def test_overflow_prunes_expired_history_without_losing_pin_or_recent_versions(
        tmp_path, monkeypatch):
    backend = PagedLocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=2, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        for number in range(1, 13):
            owner.commit([add(f"op-{number}", number)])
        lifecycle = owner.lifecycle(policy=policy)
        first = lifecycle.manifest_at(0)
        pin = owner.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(first), if_none_match=True)
        original = backend.list_page

        def short_page(prefix, limit=1000, *, start_after=None):
            if "/history/" in prefix or "/restore-pins/" in prefix:
                limit = min(limit, 2)
            return original(prefix, limit, start_after=start_after)

        monkeypatch.setattr(backend, "list_page", short_page)
        with pytest.raises(RuntimeError, match="metadata limit"):
            lifecycle.gc_step(max_metadata=5)

        deleted = 0
        for _ in range(12):
            result = lifecycle.prune_overflow_history_step(
                max_metadata=5, max_objects=2)
            assert result["phase"] == "history_overflow"
            assert result["next_cursor"] is None
            deleted += result["deleted"]
            if len(backend.list(owner.storage.prefix + "history/")) <= 5:
                break
        assert deleted > 0
        assert lifecycle.manifest_at(0)["version"] == 0
        assert lifecycle.manifest_at(10)["version"] == 10
        assert lifecycle.manifest_at(11)["version"] == 11
        assert owner.storage.artifact_keys(first) <= set(
            backend.list(owner.storage.prefix + "images/"))
        assert lifecycle.gc_step(max_metadata=5)["phase"] in {"history", "images"}
        assert owner.read("SELECT count(*) FROM items") == [(12,)]


def test_overflow_keeps_unexpired_history(tmp_path):
    backend = PagedLocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=1, retain_seconds=86400,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        for number in range(1, 8):
            owner.commit([add(f"op-{number}", number)])
        result = owner.lifecycle(policy=policy).prune_overflow_history_step(
            max_metadata=3)
        assert result["deleted"] == 0
        assert len(backend.list(owner.storage.prefix + "history/")) == 7


def test_overflow_rejects_malformed_newer_history_before_pruning(tmp_path):
    backend = PagedLocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        for number in range(1, 8):
            owner.commit([add(f"op-{number}", number)])
        keys = backend.list(owner.storage.prefix + "history/")
        backend._path(keys[3]).write_bytes(b"not a manifest")
        with pytest.raises(InvalidSnapshot, match="invalid history object"):
            owner.lifecycle(policy=policy).prune_overflow_history_step(
                max_metadata=3)
        assert keys[0] in backend.list(owner.storage.prefix + "history/")


def add(name, item_id):
    return Request(name, [("INSERT INTO items VALUES(?, ?)", [item_id, name])])


def test_history_inspect_restore_is_new_reversible_version(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        lifecycle = owner.lifecycle()

        assert lifecycle.versions() == [0, 1, 2]
        assert lifecycle.inspect(1, "SELECT value FROM items") == [("one",)]
        restored = lifecycle.restore(1)
        assert restored["version"] == 3
        assert owner.read("SELECT value FROM items") == [("one",)]
        assert owner.lookup("one").version == 1
        assert owner.lookup("two") is None

        # The replaced v2 frontier remains an immutable recovery point.
        assert lifecycle.inspect(2, "SELECT value FROM items ORDER BY id") == [
            ("one",), ("two",)
        ]
        assert lifecycle.restore(2)["version"] == 4
        assert owner.lookup("two").version == 2


def test_native_history_lookup_validates_exact_group_without_listing(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        history_key = owner.storage.prefix + "history/00000000000000000001.json"
        raw, etag = backend.get(history_key)
        monkeypatch.setattr(backend, "list", lambda *_: pytest.fail("history lookup listed S3"))
        assert owner.lifecycle().manifest_at(1)["version"] == 1
        changed = json.loads(raw)
        changed["group"] = "different"
        backend.put(history_key, canonical(changed), if_match=etag)
        with pytest.raises(InvalidSnapshot):
            owner.lifecycle().manifest_at(1)


def test_native_clone_pins_source_and_stops_after_lease_loss(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "source", schema=SCHEMA, native=True) as owner:
        owner.commit([add("one", 1)])
        original = owner.storage.restore_manifest

        def lose_during_copy(*args):
            original(*args)
            assert backend.list(owner.storage.prefix + "restore-pins/")
            owner._heartbeat_error = LeaseLost("source lease lost")

        monkeypatch.setattr(owner.storage, "restore_manifest", lose_during_copy)
        with pytest.raises(LeaseLost):
            owner.lifecycle().clone(1, "copy", reset_receipts=True)
        assert backend.list(owner.storage.prefix + "restore-pins/") == []
        assert backend.list("turso-native/copy/") == []


def test_native_clone_quota_rejects_before_target_publication(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "source", schema=SCHEMA, native=True) as owner:
        owner.commit([add("one", 1)])
        with pytest.raises(DatabaseQuotaExceeded):
            owner.lifecycle().clone(1, "copy", reset_receipts=True,
                                    max_frontier_bytes=1)
        assert backend.list("turso-native/copy/") == []
        assert backend.list(owner.storage.prefix + "restore-pins/") == []


@pytest.mark.parametrize("mode", ["snapshot", "incremental", "native"])
def test_historical_export_is_create_only_stock_sqlite(tmp_path, mode):
    backend = LocalBackend(tmp_path / "objects")
    options = {"incremental": mode == "incremental", "native": mode == "native"}
    with BatchOwner(backend, "g", schema=SCHEMA, **options) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        output = owner.lifecycle().export(1, tmp_path / f"{mode}.sqlite")
        with pytest.raises(FileExistsError):
            owner.lifecycle().export(1, output)
    with closing(sqlite3.connect(output)) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert conn.execute("SELECT value FROM items").fetchall() == [("one",)]


def test_gc_retention_and_interrupted_restore_pin(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        lifecycle = owner.lifecycle(policy=policy)
        v0 = lifecycle.manifest_at(0)
        pin = owner.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(v0), if_none_match=True)

        plan = lifecycle.gc(dry_run=True)
        assert v0["image"] not in plan["artifacts_candidates"]
        assert plan["restore_pins"] == [pin]
        report = lifecycle.gc()
        assert v0["image"] not in report["artifacts_deleted"]
        assert lifecycle.inspect(0, "SELECT count(*) FROM items") == [(0,)]

        backend.delete(pin)
        report = lifecycle.gc()
        assert v0["image"] in report["artifacts_deleted"]


@pytest.mark.parametrize("mode", ["snapshot", "incremental", "native"])
def test_clone_creates_independent_group(tmp_path, mode):
    backend = LocalBackend(tmp_path / "objects")
    options = {"incremental": mode == "incremental", "native": mode == "native"}
    with BatchOwner(backend, "source", schema=SCHEMA, **options) as owner:
        owner.commit([add("one", 1)])
        owner.commit([add("two", 2)])
        cloned = owner.lifecycle().clone(1, "copy")
        assert cloned["version"] == 0
        with pytest.raises(FileExistsError):
            owner.lifecycle().clone(1, "copy")
    with BatchOwner(backend, "copy", **options) as copy:
        assert copy.read("SELECT value FROM items") == [("one",)]
        copy.commit([add("copy-only", 3)])
    with BatchOwner(backend, "source", **options) as source:
        assert source.read("SELECT value FROM items ORDER BY id") == [
            ("one",), ("two",)
        ]


def test_gc_bounds_sustained_snapshot_growth(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=2, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA) as owner:
        for number in range(1, 13):
            owner.commit([add(f"op-{number}", number)])
            owner.lifecycle(policy=policy).gc()
        assert len(backend.list("turso-experiment/g/history/")) == 2
        assert len(backend.list("turso-experiment/g/images/")) == 3


@pytest.mark.parametrize("native", [False, True])
def test_bounded_gc_preserves_head_and_pin_across_pages(tmp_path, native):
    backend = PagedLocalBackend(tmp_path / "objects")
    policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=SCHEMA, native=native) as owner:
        for number in range(1, 13):
            owner.commit([add(f"op-{number}", number)])
        lifecycle = owner.lifecycle(policy=policy)
        first = lifecycle.manifest_at(0)
        pin = owner.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(first), if_none_match=True)
        with pytest.raises(RuntimeError, match="metadata limit"):
            lifecycle.gc_step(max_metadata=1)
        assert lifecycle.inspect(0, "SELECT count(*) FROM items") == [(0,)]

        for _ in range(20):
            result = lifecycle.gc_step(max_objects=1)
            if result["phase"] != "history":
                break
        assert result["phase"] == "images"
        assert owner.storage.artifact_keys(first) <= set(
            backend.list(owner.storage.prefix + "images/"))
        cursor = result["next_cursor"]
        for _ in range(30):
            result = lifecycle.gc_step(cursor=cursor, max_objects=1)
            cursor = result["next_cursor"]
            if result.get("wrapped"):
                break
        assert result["wrapped"] is True
        assert lifecycle.inspect(0, "SELECT count(*) FROM items") == [(0,)]
        backend.delete(pin)
        for _ in range(20):
            result = lifecycle.gc_step(max_objects=1)
            if result["phase"] != "history":
                break
        cursor = None
        for _ in range(30):
            result = lifecycle.gc_step(cursor=cursor, max_objects=1)
            cursor = result["next_cursor"]
            if result.get("wrapped"):
                break
        assert result["wrapped"] is True
        assert not (owner.storage.artifact_keys(first) & set(
            backend.list(owner.storage.prefix + "images/")))
        assert owner.read("SELECT count(*) FROM items") == [(12,)]
