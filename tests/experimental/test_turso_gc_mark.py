"""Fault checks for durable mark-only GC; no image sweep is enabled."""

import json
import time

import boto3
import pytest

pytest.importorskip("turso")
pytest.importorskip("moto")
from moto import mock_aws

from tessera.backends.base import CASFailed, LeaseLost, NotFound
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.turso.fencing import FencedBackend
from tessera.experimental.turso.lifecycle import RetentionPolicy, RetentionSession
from tessera.experimental.turso.owner import BatchOwner, Request
from tessera.experimental.turso.snapshot import InvalidSnapshot, canonical, digest

SCHEMA = ["CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)"]


@pytest.fixture
def remote():
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3, db = session.client("s3"), session.resource("dynamodb")
        s3.create_bucket(Bucket="tessera-mark-test")
        db.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        raw = S3DynamoBackend("tessera-mark-test", "leases", s3_client=s3,
                              dynamodb=db, backoff_max_attempts=1)
        yield FencedBackend(raw, session.client("dynamodb")), db.Table("leases")


def _publish(backend, count=4):
    with BatchOwner(backend, "g", schema=SCHEMA, native=True) as owner:
        for number in range(1, count + 1):
            owner.commit([Request(str(number), [
                ("INSERT INTO items VALUES(?, ?)", [number, str(number)])])])


def _state(backend):
    raw, etag = backend.get_gc_mark_state("g")
    return json.loads(raw), etag


def _complete_mark(lifecycle, *, page_size=2):
    for _ in range(30):
        result = lifecycle.incremental_mark_step(max_metadata=page_size)
        if result["phase"] == "marked_unverified":
            return result
    raise AssertionError("mark did not complete")


def test_mark_resumes_short_history_pages_and_records_head_and_pin(remote, monkeypatch):
    backend, _ = remote
    _publish(backend)
    with RetentionSession(backend, "g") as lifecycle:
        historical = lifecycle.manifest_at(0)
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(historical), if_none_match=True)
        all_images_before = backend.list(lifecycle.storage.prefix + "images/")
        original = backend.list_page

        def short_page(prefix, limit=1000, *, start_after=None):
            return original(prefix, min(limit, 1), start_after=start_after)

        monkeypatch.setattr(backend, "list_page", short_page)
        started = lifecycle.incremental_mark_step(max_metadata=3)
        assert started["started"] and started["marked"] == 0
        first = lifecycle.incremental_mark_step(max_metadata=3)
        assert first["marked"] == 1
    with RetentionSession(backend, "g") as lifecycle:
        phases = []
        for _ in range(20):
            result = lifecycle.incremental_mark_step(max_metadata=3)
            phases.append(result["phase"])
            if result["phase"] == "marked_unverified":
                break
        assert phases[-1] == "marked_unverified"
        state, _ = _state(backend)
        assert state["history_count"] == 4 and state["pin_count"] == 1
        assert state["chunk_count"] == 5
        assert state["head_artifacts"] == sorted(
            lifecycle.storage.artifact_keys(lifecycle.storage.manifest()[0]))
        assert backend.list(lifecycle.storage.prefix + "images/") == all_images_before
        chunks = backend.list(lifecycle.storage.prefix + "gc-mark/" + state["cycle_id"] + "/")
        assert len(chunks) == state["chunk_count"]
        refs = set(state["head_artifacts"])
        chain = digest(b"tessera-gc-chunk-chain-v1")
        for key in sorted(chunks):
            raw, _ = backend.get(key)
            refs.update(json.loads(raw)["artifact_keys"])
            chain = digest(bytes.fromhex(chain) + key.encode() + b"\0"
                           + bytes.fromhex(digest(raw)))
        assert chain == state["chunk_digest"]
        assert state["mark_bytes"] == sum(len(backend.get(key)[0]) for key in chunks)
        assert state["pin_digest"] == state["pin_digest_running"]
        assert lifecycle.storage.artifact_keys(historical) <= refs


def test_lost_chunk_reply_is_reconciled_and_cursor_commits_once(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        lifecycle.incremental_mark_step()
        original = backend.put

        def lost_reply(key, data, **kwargs):
            result = original(key, data, **kwargs)
            if "/gc-mark/" in key:
                raise OSError("lost S3 PUT reply")
            return result

        monkeypatch.setattr(backend, "put", lost_reply)
        result = lifecycle.incremental_mark_step()
        assert result["marked"] == 1
        state, _ = _state(backend)
        assert state["history_count"] == 1 and state["chunk_count"] == 1


def test_crash_after_chunk_before_cursor_cas_retries_same_page(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        lifecycle.incremental_mark_step()
        original = backend.put_gc_mark_state
        calls = 0

        def fail_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("crash before DynamoDB CAS")
            return original(*args, **kwargs)

        monkeypatch.setattr(backend, "put_gc_mark_state", fail_once)
        with pytest.raises(OSError, match="crash"):
            lifecycle.incremental_mark_step()
        state, _ = _state(backend)
        assert state["history_count"] == 0 and state["chunk_count"] == 0
        assert len(backend.list(lifecycle.storage.prefix + "gc-mark/")) == 1
        assert lifecycle.incremental_mark_step()["marked"] == 1
        state, _ = _state(backend)
        assert state["history_count"] == 1 and state["chunk_count"] == 1


def test_state_cas_reconciles_lost_reply_and_rejects_stale_lease(remote, monkeypatch):
    backend, table = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        head_etag = lifecycle.storage.manifest()[1]
        original = backend.client.transact_write_items

        def lose_reply(**kwargs):
            original(**kwargs)
            raise OSError("lost DynamoDB reply")

        monkeypatch.setattr(backend.client, "transact_write_items", lose_reply)
        assert lifecycle.incremental_mark_step()["started"] is True
        monkeypatch.setattr(backend.client, "transact_write_items", original)
        before, etag = _state(backend)
        assert before["head_etag"] == head_etag
        table.update_item(Key={"tenant_id": lifecycle.owner._lease.tenant},
                          UpdateExpression="SET expires_at=:v",
                          ExpressionAttributeValues={":v": 0})
        with pytest.raises(LeaseLost):
            backend.put_gc_mark_state("g", canonical({**before, "phase": "pins"}),
                                      previous_etag=etag, head_etag=head_etag,
                                      lease=lifecycle.owner._lease)
        assert _state(backend)[0] == before


def test_corrupt_history_and_changed_head_cannot_advance_old_cycle(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        lifecycle.incremental_mark_step()
        state, etag = _state(backend)
        bad_key = lifecycle.storage.prefix + "history/00000000000000000002.json"
        backend.put(bad_key, b"not-json", if_none_match=True)
        with pytest.raises(InvalidSnapshot):
            lifecycle.incremental_mark_step()
        assert _state(backend) == (state, etag)
        with pytest.raises(CASFailed):
            backend.put_gc_mark_state("g", canonical({**state, "phase": "pins"}),
                                      previous_etag=etag, head_etag="0" * 64,
                                      lease=lifecycle.owner._lease)
        assert _state(backend) == (state, etag)


def test_head_change_during_mark_leaves_cursor_unadvanced(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle()
        lifecycle.incremental_mark_step()
        before, before_etag = _state(backend)
        original = backend.list_page
        changed = False

        def publish_during_list(prefix, limit=1000, *, start_after=None):
            nonlocal changed
            page = original(prefix, limit, start_after=start_after)
            if not changed and prefix.endswith("/history/"):
                changed = True
                owner.commit([Request("late", [
                    ("INSERT INTO items VALUES(?, ?)", [2, "late"])])])
            return page

        monkeypatch.setattr(backend, "list_page", publish_during_list)
        with pytest.raises(CASFailed):
            lifecycle.incremental_mark_step()
        assert _state(backend) == (before, before_etag)
        restarted = lifecycle.incremental_mark_step()
        assert restarted["started"] and restarted["cycle_id"] != before["cycle_id"]


def test_mark_refuses_versioned_bucket(remote):
    backend, _ = remote
    _publish(backend, 1)
    backend.objects._s3.put_bucket_versioning(
        Bucket=backend.objects.bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    with RetentionSession(backend, "g") as lifecycle, pytest.raises(
            RuntimeError, match="unversioned"):
        lifecycle.incremental_mark_step()
    assert backend.get_gc_mark_state("g") == (None, None)


def test_mark_state_cleanup_requires_retired_pointer_and_reconciles_lost_reply(
        remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        lifecycle.incremental_mark_step()
        lease = lifecycle.owner._lease
        with pytest.raises(CASFailed, match="retired pointer"):
            backend.delete_gc_mark_state_after_retirement("g", lease)
        assert backend.get_gc_mark_state("g")[0] is not None
        _, head_etag = lifecycle.storage.manifest()
        backend.retire_pointer(lifecycle.storage.key, head_etag, lease)
        original = backend.client.transact_write_items

        def lose_reply(**kwargs):
            original(**kwargs)
            raise OSError("lost cleanup reply")

        monkeypatch.setattr(backend.client, "transact_write_items", lose_reply)
        backend.delete_gc_mark_state_after_retirement("g", lease)
        assert backend.get_gc_mark_state("g") == (None, None)


def test_corrupt_mark_state_does_not_advance(remote):
    backend, table = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        lifecycle.incremental_mark_step()
        state, _ = _state(backend)
        state["head_artifacts"] = []
        raw = canonical(state)
        table.put_item(Item={"tenant_id": "turso-gc-mark/g", "body": raw,
                             "etag": digest(raw)})
        with pytest.raises(InvalidSnapshot, match="GC mark state"):
            lifecycle.incremental_mark_step()
        assert backend.get_gc_mark_state("g")[0] == raw


def test_exact_index_contains_every_marked_artifact_and_never_sweeps(remote):
    backend, _ = remote
    _publish(backend, 5)
    with RetentionSession(backend, "g") as lifecycle:
        old = lifecycle.manifest_at(0)
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(old), if_none_match=True)
        images_before = backend.list(lifecycle.storage.prefix + "images/")
        _complete_mark(lifecycle)
        result = lifecycle.exact_index_step(max_chunks=16)
        assert result["phase"] == "indexed_unverified" and result["created"]
        state, _ = _state(backend)
        assert state["phase"] == "indexed_unverified"
        raw, _ = backend.get(state["index_root"]["key"])
        index = json.loads(raw)
        expected = set(lifecycle.storage.artifact_keys(lifecycle.storage.manifest()[0]))
        for key in backend.list(lifecycle.storage.prefix + "history/"):
            manifest, _ = lifecycle.storage.parse_manifest(*backend.get(key))
            expected.update(lifecycle.storage.artifact_keys(manifest))
        expected.update(lifecycle.storage.artifact_keys(old))
        assert index["keys"] == sorted(expected)
        assert state["index_root"]["key_count"] == len(expected)
        assert backend.list(lifecycle.storage.prefix + "images/") == images_before
        assert lifecycle.exact_index_step()["created"] is False
        assert lifecycle.incremental_mark_step()["phase"] == "indexed_unverified"
    with BatchOwner(backend, "g", native=True) as fresh:
        assert fresh.read("SELECT count(*) FROM items") == [(5,)]


@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_exact_index_refuses_missing_or_changed_mark_chunk(remote, damage):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        before = _state(backend)
        key = lifecycle.storage.prefix + "gc-mark/" + before[0]["cycle_id"]
        key += "/history/00000000000000000000.json"
        if damage == "missing":
            backend.objects.delete(key)
            error = NotFound
        else:
            backend.objects._s3.put_object(
                Bucket=backend.objects.bucket, Key=key, Body=b"corrupt")
            error = InvalidSnapshot
        with pytest.raises(error):
            lifecycle.exact_index_step()
        assert _state(backend) == before
        assert backend.list(lifecycle.storage.prefix + "gc-index/") == []


def test_exact_index_reconciles_lost_put_and_state_reply(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        original_put = backend.put
        original_transaction = backend.client.transact_write_items

        def lost_index_put(key, data, **kwargs):
            result = original_put(key, data, **kwargs)
            if "/gc-index/" in key:
                raise OSError("lost index PUT reply")
            return result

        def lost_state_reply(**kwargs):
            original_transaction(**kwargs)
            raise OSError("lost state CAS reply")

        monkeypatch.setattr(backend, "put", lost_index_put)
        monkeypatch.setattr(backend.client, "transact_write_items", lost_state_reply)
        assert lifecycle.exact_index_step()["created"] is True
        assert _state(backend)[0]["phase"] == "indexed_unverified"


def test_exact_index_retries_after_index_put_before_state_cas(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        before = _state(backend)
        original = backend.put_gc_mark_state
        failed = False

        def fail_once(*args, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError("crash before index CAS")
            return original(*args, **kwargs)

        monkeypatch.setattr(backend, "put_gc_mark_state", fail_once)
        with pytest.raises(OSError, match="crash"):
            lifecycle.exact_index_step()
        assert _state(backend) == before
        assert len(backend.list(lifecycle.storage.prefix + "gc-index/")) == 1
        assert lifecycle.exact_index_step()["created"] is True
        assert _state(backend)[0]["phase"] == "indexed_unverified"


def test_exact_index_budget_stops_without_publishing(remote):
    backend, _ = remote
    _publish(backend, 3)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        before = _state(backend)
        assert lifecycle.exact_index_step(max_chunks=1)["reason"] == "chunks"
        assert lifecycle.exact_index_step(max_index_bytes=64)["reason"] == "index_bytes"
        assert _state(backend) == before
        assert backend.list(lifecycle.storage.prefix + "gc-index/") == []


def test_exact_index_refuses_changed_head_between_put_and_cas(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle()
        _complete_mark(lifecycle)
        before = _state(backend)
        original = backend.put
        changed = False

        def publish_after_index_put(key, data, **kwargs):
            nonlocal changed
            result = original(key, data, **kwargs)
            if "/gc-index/" in key and not changed:
                changed = True
                owner.commit([Request("late-index", [
                    ("INSERT INTO items VALUES(?, ?)", [3, "late"])])])
            return result

        monkeypatch.setattr(backend, "put", publish_after_index_put)
        with pytest.raises(CASFailed):
            lifecycle.exact_index_step()
        assert _state(backend) == before
        assert owner.read("SELECT count(*) FROM items") == [(3,)]


def test_exact_index_refuses_wrong_pin_digest(remote):
    backend, table = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(lifecycle.manifest_at(0)), if_none_match=True)
        _complete_mark(lifecycle)
        state, _ = _state(backend)
        state["pin_digest"] = state["pin_digest_running"] = "0" * 64
        raw = canonical(state)
        table.put_item(Item={"tenant_id": "turso-gc-mark/g", "body": raw,
                             "etag": digest(raw)})
        with pytest.raises(InvalidSnapshot, match="chunk chain or inventory"):
            lifecycle.exact_index_step()
        assert backend.list(lifecycle.storage.prefix + "gc-index/") == []


def test_exact_index_state_cas_rejects_displaced_owner(remote):
    backend, table = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        before = _state(backend)
        table.update_item(Key={"tenant_id": lifecycle.owner._lease.tenant},
                          UpdateExpression="SET expires_at=:v",
                          ExpressionAttributeValues={":v": 0})
        with pytest.raises(LeaseLost):
            lifecycle.exact_index_step()
        assert _state(backend) == before


def test_exact_index_repeat_refuses_changed_index_bytes(remote):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        state, _ = _state(backend)
        backend.objects._s3.put_object(Bucket=backend.objects.bucket,
                                       Key=state["index_root"]["key"], Body=b"corrupt")
        with pytest.raises(InvalidSnapshot, match="length/hash"):
            lifecycle.exact_index_step()


def test_incremental_exact_index_resumes_and_sweeps_from_legacy_root(remote):
    backend, _ = remote
    _publish(backend, 7)
    policy = RetentionPolicy(artifact_grace_seconds=0)
    with RetentionSession(backend, "g", policy=policy) as lifecycle:
        orphan = _orphan(lifecycle)
        _complete_mark(lifecycle, page_size=1)
        first = lifecycle.incremental_exact_index_step(max_chunks=2)
        assert first["next_sequence"] == 2 and first["runs"] == 1
        with pytest.raises(ValueError, match="incremental exact reduction"):
            lifecycle.exact_index_step()
    with RetentionSession(backend, "g", policy=policy) as lifecycle:
        for _ in range(20):
            result = lifecycle.incremental_exact_index_step(max_chunks=2)
            if result["phase"] == "indexed_unverified":
                break
        else:
            raise AssertionError("incremental reduction did not finish")
        state, _ = _state(backend)
        assert result["runs"] >= 4
        assert "reduction" not in state
        raw, _ = backend.get(state["index_root"]["key"])
        assert len(json.loads(raw)["keys"]) == state["index_root"]["key_count"]
        assert lifecycle.incremental_exact_index_step()["created"] is False
        assert _finish_sweep(lifecycle)["phase"] == "complete"
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")
        assert lifecycle.inspect(1, "SELECT count(*) FROM items") == [(1,)]
    with BatchOwner(backend, "g", native=True) as owner:
        assert owner.read("SELECT count(*) FROM items") == [(7,)]


def test_incremental_index_reconciles_run_put_and_state_reply(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 3)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        original_put = backend.put
        original_transaction = backend.client.transact_write_items
        lost_put = lost_state = False

        def lose_run_reply(key, data, **kwargs):
            nonlocal lost_put
            result = original_put(key, data, **kwargs)
            if "/gc-index/" in key and "/runs/" in key and not lost_put:
                lost_put = True
                raise OSError("lost run PUT reply")
            return result

        def lose_state_reply(**kwargs):
            nonlocal lost_state
            result = original_transaction(**kwargs)
            if lost_put and not lost_state:
                lost_state = True
                raise OSError("lost run state reply")
            return result

        monkeypatch.setattr(backend, "put", lose_run_reply)
        monkeypatch.setattr(backend.client, "transact_write_items", lose_state_reply)
        result = lifecycle.incremental_exact_index_step(max_chunks=1)
        assert result["next_sequence"] == 1
        assert lost_put and lost_state
        assert _state(backend)[0]["reduction"]["next_sequence"] == 1


def test_incremental_index_retries_run_after_crash_before_cursor_cas(remote,
                                                                    monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        before = _state(backend)
        original = backend.put_gc_mark_state
        failed = False

        def fail_once(*args, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError("crash before run cursor CAS")
            return original(*args, **kwargs)

        monkeypatch.setattr(backend, "put_gc_mark_state", fail_once)
        with pytest.raises(OSError, match="crash before run cursor CAS"):
            lifecycle.incremental_exact_index_step(max_chunks=1)
        assert _state(backend) == before
        assert len(backend.list(lifecycle.storage.prefix + "gc-index/")) == 1
        assert lifecycle.incremental_exact_index_step(max_chunks=1)["next_sequence"] == 1


def test_incremental_index_refuses_corrupt_run_before_final_root(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        for _ in range(10):
            result = lifecycle.incremental_exact_index_step(max_chunks=1)
            if result["next_sequence"] == _state(backend)[0]["chunk_count"]:
                break
        state_before = _state(backend)
        first_run = lifecycle.storage.prefix + "gc-index/" + state_before[0]["cycle_id"]
        first_run += "/runs/00000000000000000000.json"
        backend.objects._s3.put_object(Bucket=backend.objects.bucket,
                                       Key=first_run, Body=b"corrupt")
        with pytest.raises(InvalidSnapshot, match="invalid GC index run"):
            lifecycle.incremental_exact_index_step(max_chunks=1)
        assert _state(backend) == state_before
        assert not any(key.endswith("/exact.json") for key in backend.list(
            lifecycle.storage.prefix + "gc-index/"))


@pytest.mark.parametrize("failure", ["lost-put-reply", "crash-before-cas"])
def test_incremental_index_retries_final_root_without_changing_runs(
        remote, monkeypatch, failure):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        while True:
            result = lifecycle.incremental_exact_index_step(max_chunks=1)
            if result["next_sequence"] == _state(backend)[0]["chunk_count"]:
                break
        before = _state(backend)
        runs = backend.list(lifecycle.storage.prefix + "gc-index/")
        if failure == "lost-put-reply":
            original = backend.put

            def lost_reply(key, data, **kwargs):
                result = original(key, data, **kwargs)
                if key.endswith("/exact.json"):
                    raise OSError("lost final PUT reply")
                return result

            monkeypatch.setattr(backend, "put", lost_reply)
            assert lifecycle.incremental_exact_index_step()["created"] is True
        else:
            original = backend.put_gc_mark_state
            failed = False

            def fail_once(*args, **kwargs):
                nonlocal failed
                if not failed:
                    failed = True
                    raise OSError("crash before final CAS")
                return original(*args, **kwargs)

            monkeypatch.setattr(backend, "put_gc_mark_state", fail_once)
            with pytest.raises(OSError, match="crash before final CAS"):
                lifecycle.incremental_exact_index_step()
            assert _state(backend) == before
            assert lifecycle.incremental_exact_index_step()["created"] is True
        assert _state(backend)[0]["phase"] == "indexed_unverified"
        assert set(backend.list(lifecycle.storage.prefix + "gc-index/")) == set(runs) | {
            lifecycle.storage.prefix + "gc-index/" + before[0]["cycle_id"]
            + "/exact.json"}


def test_incremental_index_fences_head_change_after_run_put(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle()
        _complete_mark(lifecycle, page_size=1)
        before = _state(backend)
        original = backend.put
        changed = False

        def publish_after_run_put(key, data, **kwargs):
            nonlocal changed
            result = original(key, data, **kwargs)
            if "/gc-index/" in key and "/runs/" in key and not changed:
                changed = True
                owner.commit([Request("late-run", [
                    ("INSERT INTO items VALUES(?, ?)", [3, "late"])])])
            return result

        monkeypatch.setattr(backend, "put", publish_after_run_put)
        with pytest.raises(CASFailed):
            lifecycle.incremental_exact_index_step(max_chunks=1)
        assert _state(backend) == before
        assert owner.read("SELECT count(*) FROM items") == [(3,)]


def test_stale_cleanup_removes_inactive_index_runs(remote):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle, page_size=1)
        lifecycle.incremental_exact_index_step(max_chunks=1)
        old_cycle = _state(backend)[0]["cycle_id"]
        old_run = lifecycle.storage.prefix + "gc-index/" + old_cycle
        old_run += "/runs/00000000000000000000.json"
        assert old_run in backend.list(lifecycle.storage.prefix + "gc-index/")
        assert lifecycle.incremental_mark_step(restart=True)["restarted"]
        cursor = None
        for _ in range(20):
            result = lifecycle.stale_gc_cleanup_step(cursor=cursor, max_scan=2)
            if result["phase"] == "complete":
                break
            cursor = result["next_cursor"]
        else:
            raise AssertionError("stale cleanup did not finish")
        assert old_run not in backend.list(lifecycle.storage.prefix + "gc-index/")


def _stale_cycle(owner):
    lifecycle = owner.lifecycle()
    _complete_mark(lifecycle, page_size=1)
    lifecycle.exact_index_step()
    old_cycle = _state(lifecycle.backend)[0]["cycle_id"]
    owner.commit([Request("after-mark", [
        ("INSERT INTO items VALUES(?, ?)", [99, "after-mark"])])])
    assert lifecycle.incremental_mark_step()["cycle_id"] != old_cycle
    lifecycle.incremental_mark_step(max_metadata=1)
    return lifecycle, old_cycle


def test_stale_cleanup_pages_only_old_temporary_objects(remote):
    backend, _ = remote
    _publish(backend, 3)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle, old_cycle = _stale_cycle(owner)
        _complete_mark(lifecycle, page_size=1)
        lifecycle.exact_index_step()
        mark_prefix = lifecycle.storage.prefix + "gc-mark/"
        index_prefix = lifecycle.storage.prefix + "gc-index/"
        old_keys = (backend.list(mark_prefix + old_cycle + "/")
                    + backend.list(index_prefix + old_cycle + "/"))
        assert len(old_keys) > 2
        active_cycle = _state(backend)[0]["cycle_id"]
        current_mark = backend.list(mark_prefix + active_cycle + "/")
        current_index = backend.list(index_prefix + active_cycle + "/")
        images_before = backend.list(lifecycle.storage.prefix + "images/")
        cursor = None
        for _ in range(30):
            result = lifecycle.stale_gc_cleanup_step(
                cursor=cursor, max_scan=2, max_objects=1)
            assert result["scanned"] <= 2 and result["deleted"] <= 1
            cursor = result["next_cursor"]
            if result["phase"] == "complete":
                break
        else:
            raise AssertionError("stale cleanup did not finish")
        assert all(key not in backend.list(mark_prefix) + backend.list(index_prefix)
                   for key in old_keys)
        assert backend.list(mark_prefix + active_cycle + "/") == current_mark
        assert backend.list(index_prefix + active_cycle + "/") == current_index
        assert backend.list(lifecycle.storage.prefix + "images/") == images_before
        assert owner.read("SELECT count(*) FROM items") == [(4,)]


def test_stale_cleanup_retries_after_unknown_delete_result(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle, old_cycle = _stale_cycle(owner)
        old_prefix = lifecycle.storage.prefix + "gc-mark/" + old_cycle + "/"
        before = backend.list(old_prefix)
        original = backend.objects.delete
        failed = False

        def lose_delete_reply(key):
            nonlocal failed
            result = original(key)
            if key.startswith(old_prefix) and not failed:
                failed = True
                raise OSError("lost DELETE reply")
            return result

        monkeypatch.setattr(backend.objects, "delete", lose_delete_reply)
        with pytest.raises(OSError, match="lost DELETE reply"):
            lifecycle.stale_gc_cleanup_step()
        assert len(backend.list(old_prefix)) == len(before) - 1
        cursor = None
        for _ in range(10):
            step = lifecycle.stale_gc_cleanup_step(cursor=cursor)
            cursor = step["next_cursor"]
            if step["phase"] == "complete":
                break
        assert backend.list(old_prefix) == []


def test_stale_cleanup_rejects_displaced_lease(remote):
    backend, table = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle, old_cycle = _stale_cycle(owner)
        old_prefix = lifecycle.storage.prefix + "gc-mark/" + old_cycle + "/"
        before = backend.list(old_prefix)
        table.update_item(Key={"tenant_id": owner._lease.tenant},
                          UpdateExpression="SET expires_at=:v",
                          ExpressionAttributeValues={":v": 0})
        with pytest.raises(LeaseLost):
            lifecycle.stale_gc_cleanup_step()
        assert backend.list(old_prefix) == before


def test_stale_cleanup_rejects_changed_cycle_before_delete(remote, monkeypatch):
    backend, table = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle, old_cycle = _stale_cycle(owner)
        old_prefix = lifecycle.storage.prefix + "gc-mark/" + old_cycle + "/"
        before = backend.list(old_prefix)
        original = backend.renew_lease
        changed = False

        def change_state_after_renew(*args, **kwargs):
            nonlocal changed
            lease = original(*args, **kwargs)
            if not changed:
                changed = True
                state, _ = _state(backend)
                state["cycle_id"] = "f" * 32
                raw = canonical(state)
                table.put_item(Item={"tenant_id": "turso-gc-mark/g", "body": raw,
                                     "etag": digest(raw)})
            return lease

        monkeypatch.setattr(backend, "renew_lease", change_state_after_renew)
        with pytest.raises(CASFailed, match="cycle changed"):
            lifecycle.stale_gc_cleanup_step()
        assert backend.list(old_prefix) == before


def test_stale_cleanup_refuses_versioned_bucket(remote):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle, old_cycle = _stale_cycle(owner)
        old_prefix = lifecycle.storage.prefix + "gc-mark/" + old_cycle + "/"
        before = backend.list(old_prefix)
        backend.objects._s3.put_bucket_versioning(
            Bucket=backend.objects.bucket,
            VersioningConfiguration={"Status": "Enabled"})
        with pytest.raises(RuntimeError, match="unversioned"):
            lifecycle.stale_gc_cleanup_step()
        assert backend.list(old_prefix) == before


def test_pin_verification_rechecks_complete_short_pages_without_state_change(
        remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        source = canonical(lifecycle.manifest_at(0))
        prefix = lifecycle.storage.prefix + "restore-pins/"
        for suffix in ("a", "b"):
            backend.put(prefix + suffix * 32 + ".json", source, if_none_match=True)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        before = _state(backend)
        original = backend.list_page
        pin_calls = 0

        def short_pin_pages(key_prefix, limit=1000, *, start_after=None):
            nonlocal pin_calls
            if key_prefix == prefix:
                pin_calls += 1
                limit = min(limit, 1)
            return original(key_prefix, limit, start_after=start_after)

        monkeypatch.setattr(backend, "list_page", short_pin_pages)
        assert lifecycle.verify_gc_pins_now()["pins"] == 2
        assert pin_calls == 3  # an empty follow-up page proves completion
        assert _state(backend) == before


def test_pin_verification_detects_same_count_replacement_and_restart(remote):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        prefix = lifecycle.storage.prefix + "restore-pins/"
        source = canonical(lifecycle.manifest_at(0))
        old_pin, new_pin = (prefix + suffix * 32 + ".json" for suffix in ("a", "b"))
        backend.put(old_pin, source, if_none_match=True)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        before = _state(backend)[0]
        backend.delete(old_pin)
        backend.put(new_pin, source, if_none_match=True)
        with pytest.raises(InvalidSnapshot, match="pin inventory changed"):
            lifecycle.verify_gc_pins_now()
        assert _state(backend)[0] == before
        restarted = lifecycle.incremental_mark_step(restart=True)
        assert restarted["restarted"] and restarted["cycle_id"] != before["cycle_id"]
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        assert lifecycle.verify_gc_pins_now()["pins"] == 1


def test_native_gc_cycle_restarts_mark_when_pin_changes_before_sweep(remote):
    backend, _ = remote
    _publish(backend, 2)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.incremental_exact_index_step()
        lifecycle.incremental_exact_index_step()
        before = _state(backend)[0]
        assert before["phase"] == "indexed_unverified"
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(lifecycle.manifest_at(0)), if_none_match=True)
        result = lifecycle.native_gc_cycle_step(max_steps=3)
        after = _state(backend)[0]
        assert result["stage"] in {"history", "pins"}
        assert after["cycle_id"] != before["cycle_id"]
        assert after["phase"] == result["stage"]
        assert backend.list(lifecycle.storage.prefix + "images/")


def test_pin_verification_fails_on_late_pin_and_corrupt_body(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(lifecycle.manifest_at(0)), if_none_match=True)
        with pytest.raises(InvalidSnapshot, match="pin inventory changed"):
            lifecycle.verify_gc_pins_now()
        backend.delete(pin)
        backend.put(pin, b"not a manifest", if_none_match=True)
        with pytest.raises(InvalidSnapshot):
            lifecycle.verify_gc_pins_now()


def test_pin_verification_bounds_large_inventory_without_advancing(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        source = canonical(lifecycle.manifest_at(0))
        prefix = lifecycle.storage.prefix + "restore-pins/"
        for suffix in ("a", "b"):
            backend.put(prefix + suffix * 32 + ".json", source, if_none_match=True)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        before = _state(backend)
        result = lifecycle.verify_gc_pins_now(max_pins=1, page_size=1)
        assert result["phase"] == "budget" and result["reason"] == "pins"
        assert _state(backend) == before


def test_pin_verification_rejects_head_change_during_listing(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle()
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        original = backend.list_page
        changed = False

        def publish_during_pin_list(prefix, limit=1000, *, start_after=None):
            nonlocal changed
            page = original(prefix, limit, start_after=start_after)
            if prefix.endswith("/restore-pins/") and not changed:
                changed = True
                owner.commit([Request("after-pin-list", [
                    ("INSERT INTO items VALUES(?, ?)", [99, "late"])])])
            return page

        monkeypatch.setattr(backend, "list_page", publish_during_pin_list)
        with pytest.raises(CASFailed, match="published head changed"):
            lifecycle.verify_gc_pins_now()


def test_pin_verification_rejects_displaced_owner(remote):
    backend, table = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        table.update_item(Key={"tenant_id": lifecycle.owner._lease.tenant},
                          UpdateExpression="SET expires_at=:v",
                          ExpressionAttributeValues={":v": 0})
        with pytest.raises(LeaseLost):
            lifecycle.verify_gc_pins_now()


def test_mark_restart_keeps_old_cycle_if_lease_is_lost(remote):
    backend, table = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g") as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        before = _state(backend)
        table.update_item(Key={"tenant_id": lifecycle.owner._lease.tenant},
                          UpdateExpression="SET expires_at=:v",
                          ExpressionAttributeValues={":v": 0})
        with pytest.raises(LeaseLost):
            lifecycle.incremental_mark_step(restart=True)
        assert _state(backend) == before


def _orphan(lifecycle, suffix="f", *, settle=True):
    key = lifecycle.storage.prefix + "images/" + suffix * 32 + ".db"
    lifecycle.backend.put(key, b"unpublished image", if_none_match=True)
    if settle:
        time.sleep(1.1)  # Cross S3's one-second LastModified precision boundary.
    return key


def _finish_sweep(lifecycle, *, max_scan=2):
    for _ in range(40):
        result = lifecycle.experimental_image_sweep_step(max_scan=max_scan)
        if result["phase"] == "complete":
            return result
    raise AssertionError("native image sweep did not complete")


def test_exact_native_sweep_deletes_old_orphan_and_recovers_acknowledged_rows(remote):
    backend, _ = remote
    _publish(backend, 3)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle)
        legitimate = backend.list(lifecycle.storage.prefix + "images/")
        _complete_mark(lifecycle, page_size=1)
        lifecycle.exact_index_step()
        started = lifecycle.experimental_image_sweep_step(max_scan=2)
        assert started["started"] and _state(backend)[0]["phase"] == "sweep"
        assert _finish_sweep(lifecycle)["phase"] == "complete"
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")
        assert set(backend.list(lifecycle.storage.prefix + "images/")) == set(legitimate) - {orphan}
        assert lifecycle.incremental_mark_step()["phase"] == "complete"
        assert lifecycle.inspect(1, "SELECT count(*) FROM items") == [(1,)]
    with BatchOwner(backend, "g", native=True) as owner:
        assert owner.read("SELECT count(*) FROM items") == [(3,)]


def test_native_sweep_rejects_late_pin_before_deleting(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        pin = lifecycle.storage.prefix + "restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(lifecycle.manifest_at(0)), if_none_match=True)
        with pytest.raises(InvalidSnapshot, match="pin inventory changed"):
            lifecycle.experimental_image_sweep_step()
        assert orphan in backend.list(lifecycle.storage.prefix + "images/")
        assert _state(backend)[0]["phase"] == "indexed_unverified"


def test_native_sweep_rechecks_pins_after_resume(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        assert lifecycle.experimental_image_sweep_step()["started"]
        pin = lifecycle.storage.prefix + "restore-pins/" + "b" * 32 + ".json"
        backend.put(pin, canonical(lifecycle.manifest_at(0)), if_none_match=True)
        with pytest.raises(InvalidSnapshot, match="pin inventory changed"):
            lifecycle.experimental_image_sweep_step()
        assert _state(backend)[0]["phase"] == "sweep"
        assert orphan in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_reconciles_lost_delete_reply(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle, "0")
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        original = backend.delete_retired_image

        def lose_reply(key, lease):
            original(key, lease)
            if key == orphan:
                raise OSError("lost S3 DELETE reply")

        monkeypatch.setattr(backend, "delete_retired_image", lose_reply)
        assert _finish_sweep(lifecycle, max_scan=1)["phase"] == "complete"
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_fences_head_change_before_delete(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle(policy=RetentionPolicy(artifact_grace_seconds=0))
        orphan = _orphan(lifecycle, "0")
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        before = _state(backend)
        original = backend.mtime
        changed = False

        def publish_after_image_head(key):
            nonlocal changed
            modified = original(key)
            if key == orphan and not changed:
                changed = True
                owner.commit([Request("during-sweep", [
                    ("INSERT INTO items VALUES(?, ?)", [99, "late"])])])
            return modified

        monkeypatch.setattr(backend, "mtime", publish_after_image_head)
        with pytest.raises(CASFailed, match="published head changed"):
            lifecycle.experimental_image_sweep_step(max_scan=1)
        assert _state(backend) == before
        assert orphan in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_refuses_corrupt_index(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        state, _ = _state(backend)
        backend.objects._s3.put_object(Bucket=backend.objects.bucket,
                                       Key=state["index_root"]["key"], Body=b"corrupt")
        with pytest.raises(InvalidSnapshot, match="length/hash"):
            lifecycle.experimental_image_sweep_step()
        assert orphan in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_keeps_image_uploaded_after_mark_started(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        late_orphan = _orphan(lifecycle, "e", settle=False)
        _finish_sweep(lifecycle)
        assert late_orphan in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_recovers_after_delete_before_cursor_cas(remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle, "0")
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        before = _state(backend)
        original = backend.put_gc_mark_state
        failed = False

        def fail_cursor_once(*args, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError("crash before cursor CAS")
            return original(*args, **kwargs)

        monkeypatch.setattr(backend, "put_gc_mark_state", fail_cursor_once)
        with pytest.raises(OSError, match="crash before cursor CAS"):
            lifecycle.experimental_image_sweep_step()
        assert _state(backend) == before
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")
        assert _finish_sweep(lifecycle)["phase"] == "complete"


def test_native_sweep_rejects_displaced_owner_before_delete(remote, monkeypatch):
    backend, table = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphan = _orphan(lifecycle, "0")
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        before = _state(backend)
        original = backend.mtime

        def expire_after_head(key):
            modified = original(key)
            if key == orphan:
                table.update_item(Key={"tenant_id": lifecycle.owner._lease.tenant},
                                  UpdateExpression="SET expires_at=:v",
                                  ExpressionAttributeValues={":v": 0})
            return modified

        monkeypatch.setattr(backend, "mtime", expire_after_head)
        with pytest.raises(LeaseLost):
            lifecycle.experimental_image_sweep_step()
        assert _state(backend) == before
        assert orphan in backend.list(lifecycle.storage.prefix + "images/")


def test_native_sweep_head_change_after_delete_keeps_new_ack_recoverable(
        remote, monkeypatch):
    backend, _ = remote
    _publish(backend, 1)
    with BatchOwner(backend, "g", native=True) as owner:
        lifecycle = owner.lifecycle(policy=RetentionPolicy(artifact_grace_seconds=0))
        orphan = _orphan(lifecycle, "0")
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        before = _state(backend)
        original = backend.put_gc_mark_state
        changed = False

        def publish_before_cursor_cas(*args, **kwargs):
            nonlocal changed
            if not changed:
                changed = True
                owner.commit([Request("after-delete", [
                    ("INSERT INTO items VALUES(?, ?)", [99, "ack-after-delete"])])])
            return original(*args, **kwargs)

        monkeypatch.setattr(backend, "put_gc_mark_state", publish_before_cursor_cas)
        with pytest.raises(CASFailed):
            lifecycle.experimental_image_sweep_step()
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")
        assert _state(backend) == before
        assert owner.read("SELECT count(*) FROM items") == [(2,)]
    with BatchOwner(backend, "g", native=True) as owner:
        assert owner.read("SELECT count(*) FROM items") == [(2,)]


def test_native_sweep_deletion_budget_resumes_next_page(remote):
    backend, _ = remote
    _publish(backend, 1)
    with RetentionSession(backend, "g",
                          policy=RetentionPolicy(artifact_grace_seconds=0)) as lifecycle:
        orphans = {_orphan(lifecycle, suffix, settle=False) for suffix in ("0", "1")}
        time.sleep(1.1)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        first = lifecycle.experimental_image_sweep_step(max_deletes=1)
        assert first["phase"] == "sweep" and first["deleted"] == 1
        assert len(orphans & set(backend.list(lifecycle.storage.prefix + "images/"))) == 1
        _finish_sweep(lifecycle)
        assert not orphans & set(backend.list(lifecycle.storage.prefix + "images/"))


def test_native_sweep_resumes_under_new_lease_from_durable_cursor(remote):
    backend, _ = remote
    _publish(backend, 2)
    policy = RetentionPolicy(artifact_grace_seconds=0)
    with RetentionSession(backend, "g", policy=policy) as lifecycle:
        orphan = _orphan(lifecycle)
        _complete_mark(lifecycle)
        lifecycle.exact_index_step()
        lifecycle.experimental_image_sweep_step()
        first = lifecycle.experimental_image_sweep_step(max_scan=1)
        assert first["phase"] == "sweep" and first["next_cursor"]
        cursor = _state(backend)[0]["image_cursor"]
    with RetentionSession(backend, "g", policy=policy) as lifecycle:
        assert _state(backend)[0]["image_cursor"] == cursor
        assert _finish_sweep(lifecycle, max_scan=1)["phase"] == "complete"
        assert orphan not in backend.list(lifecycle.storage.prefix + "images/")
    with BatchOwner(backend, "g", native=True) as owner:
        assert owner.read("SELECT count(*) FROM items") == [(2,)]
