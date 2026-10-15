import shutil
import threading
import time

import pytest

pytest.importorskip("turso")

from tessera import LocalBackend
from tessera.experimental.turso import (
    Operation,
    PublishedVersionUnavailable,
    RemoteFrontierCorrupt,
    Runtime,
)


SCHEMA = ["CREATE TABLE items(id INTEGER PRIMARY KEY)"]


class PublicationBarrier(LocalBackend):
    def __init__(self, root):
        super().__init__(root)
        self.pause = False
        self.entered = threading.Event()
        self.proceed = threading.Event()

    def put(self, key, data, **kwargs):
        if self.pause and key.endswith("manifest.json"):
            self.entered.set()
            if not self.proceed.wait(5):
                raise TimeoutError("publication barrier expired")
        return super().put(key, data, **kwargs)


def insert(number):
    return Operation(str(number), [("INSERT INTO items VALUES(?)", [number])])


def test_read_during_publication_stays_on_previous_complete_frontier(tmp_path):
    backend = PublicationBarrier(tmp_path / "objects")
    runtime = Runtime(backend, "g", schema=SCHEMA, read_cache_root=tmp_path / "reads")
    assert runtime.read("SELECT * FROM items") == []
    backend.pause = True
    result = []
    writing = threading.Thread(target=lambda: result.append(runtime.submit(insert(1), timeout=5)))
    writing.start()
    try:
        assert backend.entered.wait(5)
        assert runtime.read("SELECT * FROM items") == []
    finally:
        backend.proceed.set()
        writing.join(5)
        runtime.close()
    assert result[0].version == 1


def test_receipt_or_min_version_provides_read_your_writes(tmp_path):
    runtime = Runtime(LocalBackend(tmp_path / "objects"), "g", schema=SCHEMA)
    try:
        receipt = runtime.submit(insert(1), timeout=5)
        assert runtime.read("SELECT * FROM items", receipt=receipt, timeout=1) == [(1,)]
        assert runtime.read("SELECT * FROM items", min_version=receipt.version, timeout=1) == [(1,)]
        with pytest.raises(ValueError, match="agree"):
            runtime.read("SELECT * FROM items", receipt=receipt, min_version=2)
    finally:
        runtime.close()


def test_min_version_waits_and_times_out_without_exposing_old_data(tmp_path):
    backend = PublicationBarrier(tmp_path / "objects")
    runtime = Runtime(backend, "g", schema=SCHEMA)
    backend.pause = True
    writing = threading.Thread(target=lambda: runtime.submit(insert(1), timeout=5))
    writing.start()
    try:
        assert backend.entered.wait(5)
        with pytest.raises(PublishedVersionUnavailable):
            runtime.read("SELECT * FROM items", min_version=1, timeout=0.02)
        backend.proceed.set()
        assert runtime.read("SELECT * FROM items", min_version=1, timeout=2) == [(1,)]
    finally:
        backend.proceed.set()
        writing.join(5)
        runtime.close()


def test_deleted_and_corrupt_local_cache_rebuild_from_remote(tmp_path):
    cache = tmp_path / "reads"
    runtime = Runtime(LocalBackend(tmp_path / "objects"), "g", schema=SCHEMA,
                      read_cache_root=cache)
    try:
        runtime.submit(insert(1), timeout=5)
        assert runtime.read("SELECT * FROM items") == [(1,)]
        shutil.rmtree(cache)
        assert runtime.read("SELECT * FROM items") == [(1,)]
        entry = next(path for path in cache.iterdir() if path.is_dir())
        (entry / "db.sqlite").write_bytes(b"corrupt")
        assert runtime.read("SELECT * FROM items") == [(1,)]
        metrics = runtime.read_metrics()
        assert metrics["cold_builds"] == 3
        assert metrics["local_corruptions"] == 1
    finally:
        runtime.close()


def test_corrupt_remote_frontier_is_distinct_and_fails_closed(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    cache = tmp_path / "reads"
    runtime = Runtime(backend, "g", schema=SCHEMA, read_cache_root=cache)
    try:
        runtime.submit(insert(1), timeout=5)
        manifest = runtime._owner.storage.manifest()[0]
        shutil.rmtree(cache, ignore_errors=True)
        body, etag = backend.get(manifest["image"])
        backend.put(manifest["image"], body[:-1], if_match=etag)
        with pytest.raises(RemoteFrontierCorrupt):
            runtime.read("SELECT * FROM items")
        assert runtime.read_metrics()["remote_corruptions"] == 1
    finally:
        runtime.close()


def test_pinned_readers_do_not_hold_the_cache_lock_while_querying(tmp_path):
    runtime = Runtime(LocalBackend(tmp_path / "objects"), "g", schema=SCHEMA)
    finished = threading.Event()
    try:
        with runtime._reader.open():
            reader = threading.Thread(
                target=lambda: (runtime.read("SELECT * FROM items"), finished.set())
            )
            reader.start()
            assert finished.wait(2)
            reader.join(2)
    finally:
        runtime.close()


def test_native_frontier_cache_replays_one_pinned_manifest(tmp_path):
    cache = tmp_path / "reads"
    runtime = Runtime(LocalBackend(tmp_path / "objects"), "g", schema=SCHEMA,
                      native=True, read_cache_root=cache)
    try:
        receipt = runtime.submit(insert(1), timeout=5)
        assert runtime.read("SELECT * FROM items", receipt=receipt) == [(1,)]
        shutil.rmtree(cache)
        assert runtime.read("SELECT * FROM items", min_version=1, timeout=1) == [(1,)]
    finally:
        runtime.close()


def test_native_reader_fetches_only_new_published_suffix(tmp_path, monkeypatch):
    from tessera.experimental.turso import BatchOwner, PublishedReader, Request
    from tessera.experimental.turso.native import NativeLogStore

    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        reader = PublishedReader(NativeLogStore(backend, "g"),
                                 cache_root=tmp_path / "reads", max_cache_bytes=16_777_216)
        try:
            owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
            assert reader.read("SELECT id FROM items") == [(1,)]
            first = owner.storage.manifest()[0]
            owner.commit([Request("two", [("INSERT INTO items VALUES(2)", [])])])
            second = owner.storage.manifest()[0]
            assert second["checkpoint"] == first["checkpoint"]
            fetched = []
            get = backend.get

            def counted_get(key):
                fetched.append(key)
                return get(key)

            monkeypatch.setattr(backend, "get", counted_get)
            assert reader.read("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
            assert second["segments"][-1]["key"] in fetched
            assert first["checkpoint"]["key"] not in fetched
            assert first["segments"][0]["key"] not in fetched
            assert reader.metrics()["incremental_builds"] == 1
        finally:
            reader.close()


def test_native_suffix_catchup_falls_back_from_corrupt_local_base(tmp_path):
    from tessera.experimental.turso import BatchOwner, PublishedReader, Request
    from tessera.experimental.turso.native import NativeLogStore

    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        reader = PublishedReader(NativeLogStore(backend, "g"),
                                 cache_root=tmp_path / "reads", max_cache_bytes=16_777_216)
        try:
            owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
            assert reader.read("SELECT id FROM items") == [(1,)]
            first = owner.storage.manifest()[0]
            log_path = reader._entry(first) / "db.db-log"
            log_path.write_bytes(b"x" * log_path.stat().st_size)
            owner.commit([Request("two", [("INSERT INTO items VALUES(2)", [])])])
            assert reader.read("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
            assert reader.metrics()["incremental_fallbacks"] == 1
            assert reader.metrics()["incremental_builds"] == 0
        finally:
            reader.close()


def test_native_suffix_catchup_uses_full_restore_after_rebase(tmp_path, monkeypatch):
    from tessera.experimental.turso import BatchOwner, PublishedReader, Request
    from tessera.experimental.turso.native import NativeLogStore

    monkeypatch.setattr(NativeLogStore, "max_segments", 1)
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        reader = PublishedReader(NativeLogStore(backend, "g"),
                                 cache_root=tmp_path / "reads", max_cache_bytes=16_777_216)
        try:
            owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
            assert reader.read("SELECT id FROM items") == [(1,)]
            first = owner.storage.manifest()[0]
            owner.commit([Request("two", [("INSERT INTO items VALUES(2)", [])])])
            assert owner.storage.manifest()[0]["checkpoint"] != first["checkpoint"]
            assert reader.read("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
            assert reader.metrics()["incremental_builds"] == 0
        finally:
            reader.close()


def test_native_suffix_catchup_rejects_corrupt_published_segment(tmp_path):
    from tessera.experimental.turso import BatchOwner, PublishedReader, Request
    from tessera.experimental.turso.native import NativeLogStore

    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        reader = PublishedReader(NativeLogStore(backend, "g"),
                                 cache_root=tmp_path / "reads", max_cache_bytes=16_777_216)
        try:
            owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
            assert reader.read("SELECT id FROM items") == [(1,)]
            owner.commit([Request("two", [("INSERT INTO items VALUES(2)", [])])])
            segment = owner.storage.manifest()[0]["segments"][-1]
            _, etag = backend.get(segment["key"])
            backend.put(segment["key"], b"x" * segment["bytes"], if_match=etag)
            with pytest.raises(RemoteFrontierCorrupt):
                reader.read("SELECT id FROM items")
        finally:
            reader.close()


def test_reader_retries_when_gc_retires_a_superseded_frontier(tmp_path, monkeypatch):
    from tessera.experimental.turso import BatchOwner, PublishedReader, Request

    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA) as owner:
        owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
        reader = PublishedReader(owner.storage, cache_root=tmp_path / "fresh-cache")
        original = owner.storage.restore_manifest
        retired = []

        def race(manifest, path):
            if not retired:
                retired.append(manifest["image"])
                owner.commit([Request("two", [("INSERT INTO items VALUES(2)", [])])])
                backend.delete(retired[0])
            return original(manifest, path)

        monkeypatch.setattr(owner.storage, "restore_manifest", race)
        try:
            assert reader.read("SELECT id FROM items ORDER BY id") == [(1,), (2,)]
            assert retired
        finally:
            reader.close()
