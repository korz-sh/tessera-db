import pytest

pytest.importorskip("turso")
pytest.importorskip("moto")
import boto3
from moto import mock_aws

from tessera.backends.base import CASFailed, LeaseLost, NotFound
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.turso.fencing import FencedBackend


@pytest.fixture
def remote():
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-east-1")
        s3, db = session.client("s3"), session.resource("dynamodb")
        s3.create_bucket(Bucket="turso-test")
        db.create_table(TableName="leases", KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
                        AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("turso-test", "leases", s3_client=s3, dynamodb=db, backoff_max_attempts=1)
        yield FencedBackend(raw, session.client("dynamodb")), db.Table("leases")


def test_takeover_fences_old_owner_even_with_current_etag(remote):
    backend, table = remote
    key = "turso-blocks/g/manifest.json"
    first = backend.acquire_lease("turso-experiment-g", 60000)
    etag = backend.publish_pointer(key, b"first", None, first)
    table.update_item(Key={"tenant_id": first.tenant}, UpdateExpression="SET expires_at=:v", ExpressionAttributeValues={":v": 0})
    second = backend.acquire_lease(first.tenant, 60000, min_epoch=first.epoch)
    with pytest.raises(LeaseLost):
        backend.publish_pointer(key, b"stale", etag, first)
    assert backend.get(key) == (b"first", etag)
    new_etag = backend.publish_pointer(key, b"second", etag, second)
    with pytest.raises(CASFailed):
        backend.publish_pointer(key, b"wrong base", etag, second)
    assert backend.get(key) == (b"second", new_etag)
    assert "ttl_at" not in table.get_item(Key={"tenant_id": "turso-pointer/" + key})["Item"]


def test_pointer_requires_matching_group_and_live_lease(remote):
    backend, table = remote
    lease = backend.acquire_lease("turso-experiment-g", 60000)
    with pytest.raises(ValueError):
        backend.publish_pointer("turso-blocks/other/manifest.json", b"x", None, lease)
    table.update_item(Key={"tenant_id": lease.tenant}, UpdateExpression="SET expires_at=:v", ExpressionAttributeValues={":v": 0})
    with pytest.raises(LeaseLost):
        backend.publish_pointer("turso-blocks/g/manifest.json", b"x", None, lease)


def test_retired_pointer_rejects_old_and_new_publication(remote):
    backend, table = remote
    key = "turso-native/cloud-" + "a" * 32 + "/manifest.json"
    tenant = "turso-experiment-cloud-" + "a" * 32
    first = backend.acquire_lease(tenant, 60000)
    etag = backend.publish_pointer(key, b"published", None, first)
    backend.retire_pointer(key, etag, first)
    assert backend.pointer_retired(key)
    with pytest.raises(NotFound):
        backend.get(key)
    marker = table.get_item(Key={"tenant_id": "turso-pointer/" + key})["Item"]
    assert marker["deleted"] is True and "body" not in marker
    with pytest.raises(CASFailed):
        backend.publish_pointer(key, b"resurrect", etag, first)
    backend.release_lease(first)
    second = backend.acquire_lease(tenant, 60000, min_epoch=first.epoch)
    try:
        with pytest.raises(CASFailed):
            backend.publish_pointer(key, b"recreate", None, second)
    finally:
        backend.release_lease(second)


def test_retirement_requires_live_owner_and_exact_head(remote):
    backend, table = remote
    key = "turso-native/g/manifest.json"
    first = backend.acquire_lease("turso-experiment-g", 60000)
    etag = backend.publish_pointer(key, b"published", None, first)
    with pytest.raises(CASFailed):
        backend.retire_pointer(key, "0" * 64, first)
    assert backend.get(key) == (b"published", etag)
    table.update_item(Key={"tenant_id": first.tenant},
                      UpdateExpression="SET expires_at=:v",
                      ExpressionAttributeValues={":v": 0})
    second = backend.acquire_lease(first.tenant, 60000, min_epoch=first.epoch)
    with pytest.raises(LeaseLost):
        backend.retire_pointer(key, etag, first)
    assert backend.get(key) == (b"published", etag)
    backend.retire_pointer(key, etag, second)
    assert backend.pointer_retired(key)


def test_retirement_reconciles_lost_reply(remote, monkeypatch):
    backend, _ = remote
    key = "turso-native/g/manifest.json"
    lease = backend.acquire_lease("turso-experiment-g", 60000)
    etag = backend.publish_pointer(key, b"published", None, lease)
    original = backend.client.transact_write_items

    def lose_reply(**kwargs):
        original(**kwargs)
        raise OSError("retirement reply lost")

    monkeypatch.setattr(backend.client, "transact_write_items", lose_reply)
    backend.retire_pointer(key, etag, lease)
    assert backend.pointer_retired(key)
    backend.retire_pointer(key, etag, lease)


@pytest.mark.parametrize("mode", ["blocks", "native"])
def test_owner_takeover_between_seal_and_pointer_rejects_speculation(remote, monkeypatch, mode):
    from tessera.experimental.turso.owner import BatchOwner, Request
    backend, table = remote
    options = {"incremental": mode == "blocks", "native": mode == "native"}
    first = BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"],
                       **options)
    replacement = []
    original = backend.publish_pointer

    def takeover(key, raw, etag, lease):
        table.update_item(Key={"tenant_id": lease.tenant}, UpdateExpression="SET expires_at=:v", ExpressionAttributeValues={":v": 0})
        replacement.append(BatchOwner(backend, "g", **options))
        return original(key, raw, etag, lease)

    monkeypatch.setattr(backend, "publish_pointer", takeover)
    request = Request("one", [("INSERT INTO items VALUES(1)", [])])
    try:
        with pytest.raises(LeaseLost):
            first.commit([request])
        assert first.read("SELECT * FROM items") == []
        assert first.lookup("one") is None
        monkeypatch.setattr(backend, "publish_pointer", original)
        assert replacement[0].commit([request])[0].version == 1
        assert replacement[0].lookup("one").version == 1
        assert first.read("SELECT * FROM items") == [(1,)]
    finally:
        first.close()
        for owner in replacement:
            owner.close()


@pytest.mark.parametrize("mode", ["blocks", "native"])
def test_remote_lost_reply_reconciles_and_cold_owner_deduplicates(remote, monkeypatch, mode):
    from tessera.experimental.turso.owner import BatchOwner, Request
    backend, _ = remote
    options = {"incremental": mode == "blocks", "native": mode == "native"}
    original = backend.client.transact_write_items

    def lost(**kwargs):
        original(**kwargs)
        raise OSError("reply lost after remote commit")

    monkeypatch.setattr(backend.client, "transact_write_items", lost)
    request = Request("one", [("INSERT INTO items VALUES(1)", [])])
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"],
                    **options) as owner:
        result = owner.commit([request])[0]
        assert owner.lookup("one") == result
    with BatchOwner(backend, "g", **options) as fresh:
        assert fresh.commit([request]) == [result]
        assert fresh.read("SELECT * FROM items") == [(1,)]


def test_remote_bulk_scan_publishes_one_receipt_and_cold_recovers(remote):
    from tessera.experimental.turso import BulkOperation, Runtime, RuntimeLimits

    backend, _ = remote
    scan = BulkOperation("scan-1", [
        ("INSERT INTO items VALUES (?, ?)", [item, "finding" * 20])
        for item in range(5000)
    ])
    limits = RuntimeLimits(max_batch=1, max_pending=1,
                           max_pending_bytes=len(scan.payload) + 128)
    with Runtime(backend, "bulk", schema=[
        "CREATE TABLE items(id INTEGER PRIMARY KEY, message TEXT)"],
        native=True, limits=limits) as runtime:
        receipt = runtime.submit(scan, timeout=60)
        assert receipt.version == 1
    with Runtime(backend, "bulk", native=True, limits=limits) as fresh:
        assert fresh.lookup("scan-1").receipt == receipt
        assert fresh.read("SELECT COUNT(*) FROM items", receipt=receipt) == [(5000,)]


def test_remote_epoch_floor_survives_regressed_lease_metadata(remote):
    backend, table = remote
    table.put_item(Item={"tenant_id": "turso-experiment-g", "epoch": 1, "expires_at": 0})
    lease = backend.acquire_lease("turso-experiment-g", 60000, min_epoch=10)
    assert lease.epoch == 11
    backend.publish_pointer("turso-blocks/g/manifest.json", b"new", None, lease)
    assert backend.renew_lease(lease, 60000).epoch == 11


def test_remote_history_is_immutable_and_default_gc_retains_it(remote):
    from tessera.experimental.turso.owner import BatchOwner, Request

    backend, _ = remote
    with BatchOwner(
        backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]
    ) as owner:
        owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
        history = backend.list("turso-experiment/g/history/")
        assert history == ["turso-experiment/g/history/00000000000000000000.json"]
        assert owner.lifecycle().gc(dry_run=True)["retained_versions"] == [0, 1]
        report = owner.lifecycle().gc()
        assert report["artifacts_deleted"] == []
        assert report["history_pruned"] == []


def test_remote_gc_prunes_only_expired_history_and_images(remote):
    from tessera.experimental.turso import RetentionPolicy
    from tessera.experimental.turso.owner import BatchOwner, Request

    backend, _ = remote
    policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]) as owner:
        manifests = [owner.storage.manifest()[0]]
        for number in range(1, 4):
            owner.commit([Request(str(number), [("INSERT INTO items VALUES(?)", [number])])])
            manifests.append(owner.storage.manifest()[0])
        plan = owner.lifecycle(policy=policy).gc(dry_run=True)
        assert set(plan["history_candidates"]) == {
            "turso-experiment/g/history/00000000000000000000.json",
            "turso-experiment/g/history/00000000000000000001.json",
        }
        assert {manifests[0]["image"], manifests[1]["image"]} == set(
            plan["artifacts_candidates"])
        report = owner.lifecycle(policy=policy).gc()
        assert set(report["artifacts_deleted"]) == {
            manifests[0]["image"], manifests[1]["image"]}
        assert owner.read("SELECT COUNT(*) FROM items") == [(3,)]
        assert owner.lifecycle(policy=policy).inspect(2, "SELECT COUNT(*) FROM items") == [(2,)]
        with pytest.raises(NotFound, match="not retained"):
            owner.lifecycle(policy=policy).manifest_at(1)


def test_remote_gc_stops_before_deleting_when_owner_was_displaced(remote):
    from tessera.experimental.turso import RetentionPolicy
    from tessera.experimental.turso.owner import BatchOwner, Request

    backend, table = remote
    policy = RetentionPolicy(keep_last=0, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]) as old:
        old.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
        image = backend.list("turso-experiment/g/images/")[0]
        table.update_item(Key={"tenant_id": "turso-experiment-g"},
                          UpdateExpression="SET expires_at=:expired",
                          ExpressionAttributeValues={":expired": 0})
        with BatchOwner(backend, "g") as current:
            with pytest.raises(LeaseLost):
                old.lifecycle(policy=policy).gc()
            assert image in backend.list("turso-experiment/g/images/")
            assert current.read("SELECT COUNT(*) FROM items") == [(1,)]


def test_remote_gc_preserves_restore_pin_until_released(remote):
    from tessera.experimental.turso import RetentionPolicy
    from tessera.experimental.turso.owner import BatchOwner, Request
    from tessera.experimental.turso.snapshot import canonical

    backend, _ = remote
    policy = RetentionPolicy(keep_last=0, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]) as owner:
        old = owner.storage.manifest()[0]
        owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
        pin = "turso-experiment/g/restore-pins/" + "a" * 32 + ".json"
        backend.put(pin, canonical(old), if_none_match=True)
        report = owner.lifecycle(policy=policy).gc()
        assert old["image"] not in report["artifacts_deleted"]
        assert pin in report["restore_pins"]
        backend.delete(pin)
        report = owner.lifecycle(policy=policy).gc()
        assert old["image"] in report["artifacts_deleted"]


def test_remote_gc_interrupted_delete_can_resume(remote, monkeypatch):
    from tessera.experimental.turso import RetentionPolicy
    from tessera.experimental.turso.owner import BatchOwner, Request

    backend, _ = remote
    policy = RetentionPolicy(keep_last=0, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]) as owner:
        old_image = owner.storage.manifest()[0]["image"]
        owner.commit([Request("one", [("INSERT INTO items VALUES(1)", [])])])
        original = backend.objects.delete
        interrupted = False

        def fail_once(key):
            nonlocal interrupted
            if key == old_image and not interrupted:
                interrupted = True
                raise OSError("delete reply lost")
            return original(key)

        monkeypatch.setattr(backend.objects, "delete", fail_once)
        with pytest.raises(OSError, match="delete reply lost"):
            owner.lifecycle(policy=policy).gc()
        assert owner.read("SELECT COUNT(*) FROM items") == [(1,)]
        assert old_image in backend.list("turso-experiment/g/images/")
        monkeypatch.setattr(backend.objects, "delete", original)
        assert old_image in owner.lifecycle(policy=policy).gc()["artifacts_deleted"]
        assert owner.read("SELECT COUNT(*) FROM items") == [(1,)]


def test_remote_bounded_gc_pages_and_rechecks_fenced_head(remote):
    from tessera.experimental.turso import RetentionPolicy
    from tessera.experimental.turso.owner import BatchOwner, Request

    backend, _ = remote
    policy = RetentionPolicy(keep_last=1, retain_seconds=0,
                             artifact_grace_seconds=0)
    with BatchOwner(backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]) as owner:
        old_image = owner.storage.manifest()[0]["image"]
        for number in range(1, 4):
            owner.commit([Request(str(number), [("INSERT INTO items VALUES(?)", [number])])])
        lifecycle = owner.lifecycle(policy=policy)
        with pytest.raises(RuntimeError, match="metadata limit"):
            lifecycle.gc_step(max_metadata=1)
        assert old_image in backend.list("turso-experiment/g/images/")
        for _ in range(5):
            report = lifecycle.gc_step(max_objects=1)
            if report["phase"] == "images":
                break
        assert report["phase"] == "images"
        cursor = report["next_cursor"]
        for _ in range(10):
            report = lifecycle.gc_step(cursor=cursor, max_objects=1)
            cursor = report["next_cursor"]
            if report.get("wrapped"):
                break
        assert report["wrapped"] is True
        assert old_image not in backend.list("turso-experiment/g/images/")
        assert owner.read("SELECT COUNT(*) FROM items") == [(3,)]


def test_remote_additive_migration_uses_fenced_pointer(remote):
    from tessera.experimental.turso.compatibility import apply_additive_migration
    from tessera.experimental.turso.owner import BatchOwner

    backend, _ = remote
    with BatchOwner(
        backend, "g", schema=["CREATE TABLE items(id INTEGER PRIMARY KEY)"]
    ) as owner:
        manifest = apply_additive_migration(owner, [
            "ALTER TABLE items ADD COLUMN value TEXT",
            "CREATE INDEX IF NOT EXISTS idx_items_value ON items(value)",
        ])
        assert manifest["version"] == 1
    with BatchOwner(backend, "g") as reopened, reopened.storage.open() as (conn, _):
        assert [row[1] for row in conn.execute("PRAGMA table_info(items)")] == [
            "id", "value"
        ]
