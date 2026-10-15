import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

pytest.importorskip("turso")

from tessera import LocalBackend
from tessera.experimental.turso.api import DatabaseQuotaExceeded
from tessera.experimental.turso.owner import BatchOwner, Request
from tessera.experimental.turso.snapshot import InvalidSnapshot

SCHEMA = [
    "CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB)",
    "INSERT INTO items VALUES(0, zeroblob(262144))",
]


def insert(operation_id, item_id, value="x"):
    return Request(operation_id, [("INSERT INTO items VALUES(?, ?)", [item_id, value])])


def test_native_log_transfers_changed_state_and_cold_recovers(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        checkpoint = owner.storage.manifest()[0]
        receipt = owner.commit([insert("one", 1)])[0]
        published = owner.storage.manifest()[0]

        assert published["format"] == "tessera-turso-native-log-1"
        assert published["checkpoint"] == checkpoint["checkpoint"]
        assert len(published["segments"]) == 1
        assert published["segments"][0]["bytes"] < checkpoint["checkpoint"]["bytes"] / 8
        assert published["receipt_count"] == 1

    with BatchOwner(backend, "g", native=True) as fresh:
        assert fresh.lookup("one") == receipt
        assert fresh.read("SELECT id,value FROM items WHERE id=1") == [(1, "x")]


@pytest.mark.parametrize("damage", ["truncate", "gap", "reorder", "engine", "receipt"])
def test_native_log_fails_closed_on_invalid_chain(tmp_path, damage):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        owner.commit([insert("one", 1)])
        owner.commit([insert("two", 2)])
        manifest, etag = owner.storage.manifest()

    if damage == "truncate":
        segment = manifest["segments"][-1]
        data, object_etag = backend.get(segment["key"])
        backend.put(segment["key"], data[:-1], if_match=object_etag)
    elif damage == "gap":
        manifest["segments"][1]["offset"] += 1
    elif damage == "reorder":
        manifest["segments"] = list(reversed(manifest["segments"]))
    elif damage == "engine":
        manifest["engine"] = "different"
    else:
        manifest["receipt_count"] += 1
    if damage != "truncate":
        from tessera.experimental.turso.snapshot import canonical
        backend.put("turso-native/g/manifest.json", canonical(manifest), if_match=etag)

    with pytest.raises(InvalidSnapshot):
        BatchOwner(backend, "g", native=True)


def test_native_log_rebases_at_a_quiesced_boundary(tmp_path, monkeypatch):
    from tessera.experimental.turso.native import NativeLogStore

    monkeypatch.setattr(NativeLogStore, "max_segments", 2)
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        owner.commit([insert("one", 1)])
        owner.commit([insert("two", 2)])
        before = owner.storage.manifest()[0]
        owner.commit([insert("three", 3)])
        after = owner.storage.manifest()[0]

        assert len(before["segments"]) == 2
        assert after["segments"] == []
        assert after["checkpoint"] != before["checkpoint"]
        assert owner.read("SELECT count(*) FROM items") == [(4,)]

    with BatchOwner(backend, "g", native=True) as fresh:
        assert fresh.read("SELECT count(*) FROM items") == [(4,)]
        assert all(fresh.lookup(name).version == int(number)
                   for name, number in (("one", 1), ("two", 2), ("three", 3)))


def test_native_rebase_quota_rejects_before_publishing(tmp_path, monkeypatch):
    from tessera.experimental.turso.native import NativeLogStore

    monkeypatch.setattr(NativeLogStore, "max_segments", 1)
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        accepted = owner.commit([insert("one", 1)])[0]
        before = owner.storage.manifest()[0]
        owner.configure_limits(max_frontier_bytes=before["checkpoint"]["bytes"])
        with pytest.raises(DatabaseQuotaExceeded):
            owner.commit([insert("two", 2, "x" * 50000)])
        after = owner.storage.manifest()[0]
        assert after == before
        assert owner.lookup("two") is None
        assert owner.lookup("one") == accepted

    with BatchOwner(backend, "g", native=True) as fresh:
        assert fresh.lookup("one") == accepted
        assert fresh.lookup("two") is None
        assert fresh.read("SELECT count(*) FROM items") == [(2,)]


def test_native_log_rewrites_fall_back_to_verified_checkpoint(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        first = owner.commit([insert("one", 1)])[0]
        before = owner.storage.manifest()[0]
        rewritten = tmp_path / "rewritten.db-log"
        rewritten.write_bytes(b"engine-replaced-local-log")
        monkeypatch.setattr(owner.storage, "_log_path", lambda source: rewritten)
        second = owner.commit([insert("two", 2)])[0]
        after = owner.storage.manifest()[0]
        assert after["segments"] == []
        assert after["checkpoint"] != before["checkpoint"]
        assert owner.read("SELECT count(*) FROM items") == [(3,)]
    with BatchOwner(backend, "g", native=True) as fresh:
        assert fresh.lookup("one") == first
        assert fresh.lookup("two") == second
        assert fresh.read("SELECT count(*) FROM items") == [(3,)]


def test_native_and_block_modes_are_mutually_exclusive(tmp_path):
    with pytest.raises(ValueError, match="mutually exclusive"):
        BatchOwner(LocalBackend(tmp_path / "objects"), "g", native=True, incremental=True)


def test_native_export_is_an_explicit_stock_sqlite_checkpoint(tmp_path):
    backend = LocalBackend(tmp_path / "objects")
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        owner.commit([insert("one", 1)])
        output = owner.storage.export(tmp_path / "export.sqlite")

    with closing(sqlite3.connect(output)) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert conn.execute("SELECT id,value FROM items WHERE id=1").fetchone() == (1, "x")
