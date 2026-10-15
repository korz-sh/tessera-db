"""Tenant admission must serialize final slots and fail closed on retries."""

import json
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256

import pytest

from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.service import (
    CatalogConflict,
    DatabaseUnavailable,
    dispatch_request,
    error_response,
)
from tessera.experimental.cloud.tenants import (
    TenantCatalog,
    TenantConflict,
    TenantQuotaExceeded,
)


def _limits(max_databases=1):
    return {"max_databases": max_databases, "max_frontier_bytes": 65_536,
            "max_operation_bytes": 1_024, "max_tokens": 1}


def test_tenant_creation_token_rotation_and_limit_ceiling(tmp_path):
    catalog = TenantCatalog(LocalBackend(tmp_path))
    tenant = catalog.create("app", request_id="stable", limits=_limits())
    tenant_id = tenant["id"]
    assert catalog.create("app", request_id="stable", limits=_limits()) == tenant
    with pytest.raises(TenantConflict):
        catalog.create("different", request_id="stable", limits=_limits())
    issued = catalog.issue_token(tenant_id)
    assert issued["token"] not in catalog.backend.get(catalog._key(tenant_id))[0].decode()
    assert catalog.authorize(issued["token"]) == tenant_id
    assert catalog.get(tenant_id)["token_ids"] == [issued["token_id"]]
    catalog.validate_database_limits(tenant_id, {"max_frontier_bytes": 65_536,
                                                 "max_operation_bytes": 1_024,
                                                 "max_tokens": 1})
    with pytest.raises(TenantQuotaExceeded):
        catalog.validate_database_limits(tenant_id, {"max_frontier_bytes": 65_537,
                                                     "max_operation_bytes": 1_024,
                                                     "max_tokens": 1})
    catalog.revoke_token(tenant_id, issued["token_id"])
    assert catalog.authorize(issued["token"]) is None
    assert catalog.authorize("invalid") is None


def test_final_database_slot_is_cas_serialized_and_replay_safe(tmp_path):
    catalog = TenantCatalog(LocalBackend(tmp_path))
    tenant_id = catalog.create("app", limits=_limits())["id"]
    first_id, second_id = "1" * 32, "2" * 32
    fingerprint = sha256(b"same content").hexdigest()

    def reserve(database_id):
        try:
            return catalog.reserve_database(tenant_id, database_id, fingerprint)
        except TenantQuotaExceeded:
            return "quota"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, (first_id, second_id)))
    assert sorted(results, key=str) == [True, "quota"]
    winner = first_id if results[0] is True else second_id
    assert catalog.get(tenant_id)["reserved_databases"] == 1
    assert catalog.reserve_database(tenant_id, winner, fingerprint) is False
    with pytest.raises(TenantConflict):
        catalog.reserve_database(tenant_id, winner, sha256(b"changed").hexdigest())


def test_tenant_limits_reject_bool_and_oversized_map(tmp_path):
    catalog = TenantCatalog(LocalBackend(tmp_path))
    with pytest.raises(ValueError):
        catalog.create("bad", limits=dict(_limits(), max_databases=True))
    with pytest.raises(ValueError):
        catalog.create("bad", limits=dict(_limits(), max_databases=257))
    with pytest.raises(ValueError):
        catalog.create("bad", limits=dict(_limits(), max_operation_bytes=0))


def _call(service, method, path, token, body=None):
    try:
        return dispatch_request(service, method, path, token, lambda: body)
    except Exception as exc:  # noqa: BLE001 - exercise transport error mapping
        return error_response(exc)


def test_tenant_management_isolation_and_database_quota(tmp_path):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path), admin_token=admin)
    tenant_limits = {"max_databases": 1, "max_frontier_bytes": 67_108_864,
                     "max_operation_bytes": 65_536, "max_tokens": 64}
    first = _call(service, "POST", "/v1/tenants", admin,
                  {"name": "one", "request_id": "tenant-one", "limits": tenant_limits})
    second = _call(service, "POST", "/v1/tenants", admin,
                   {"name": "two", "request_id": "tenant-two", "limits": tenant_limits})
    assert first[0] == second[0] == 201
    first_id, second_id = first[1]["id"], second[1]["id"]
    assert first_id != second_id
    assert _call(service, "POST", "/v1/tenants", admin, {"name": "no-id"})[0] == 400
    expected = [first[1], second[1]] if first_id < second_id else [second[1], first[1]]
    assert _call(service, "GET", "/v1/tenants", admin)[1]["tenants"] == expected
    token_one = _call(service, "POST", f"/v1/tenants/{first_id}/tokens", admin)[1]["token"]
    token_two = _call(service, "POST", f"/v1/tenants/{second_id}/tokens", admin)[1]["token"]
    assert _call(service, "GET", "/v1/tenants", token_one)[0] == 401
    body = {"name": "app", "request_id": "same", "schema": [
        "CREATE TABLE kv(v INTEGER)"]}
    created_one = _call(service, "POST", "/v1/databases", token_one, body)
    created_two = _call(service, "POST", "/v1/databases", token_two, body)
    assert created_one[0] == created_two[0] == 201
    one, two = created_one[1]["id"], created_two[1]["id"]
    assert one != two
    assert created_one[1]["tenant_id"] == first_id
    assert _call(service, "POST", "/v1/databases", token_one, body) == created_one
    assert _call(service, "POST", "/v1/databases", token_one,
                 dict(body, name="changed"))[0] == 409
    assert _call(service, "POST", "/v1/databases", token_one,
                 {"name": "no-id", "schema": []})[0] == 400
    assert _call(service, "POST", "/v1/databases", token_one,
                 {"name": "extra", "request_id": "extra", "schema": []})[0] == 413
    assert _call(service, "GET", "/v1/databases", token_one)[1]["databases"] == [
        created_one[1]]
    assert _call(service, "GET", "/v1/databases", token_two)[1]["databases"] == [
        created_two[1]]
    for suffix, method, request in (("", "GET", None), ("", "DELETE", None),
                                    ("/limits", "GET", None),
                                    ("/limits", "PUT", {"limits": created_two[1]["limits"]}),
                                    ("/tokens", "POST", {"scope": "read"}),
                                    ("/tokens/" + "1" * 16, "DELETE", None),
                                    ("/history", "GET", None),
                                    ("/usage", "GET", None),
                                    ("/deletion/step", "POST", None),
                                    ("/restore", "POST", {"source_version": 0,
                                                           "name": "copy",
                                                           "request_id": "cross"})):
        assert _call(service, method, f"/v1/databases/{two}{suffix}",
                     token_one, request)[0] == 404
    assert _call(service, "PUT", f"/v1/databases/{one}/limits", token_one,
                 {"limits": {"max_frontier_bytes": 67_108_865,
                             "max_operation_bytes": 65_536,
                             "max_tokens": 64}})[0] == 413
    sql_token = _call(service, "POST", f"/v1/databases/{one}/tokens", token_one,
                      {"scope": "write"})[1]["token"]
    assert _call(service, "POST", f"/v1/databases/{one}/query", sql_token,
                 {"sql": "SELECT COUNT(*) FROM kv"}) == (200, {"rows": [(0,)]})
    assert _call(service, "POST", f"/v1/databases/{two}/query", sql_token,
                 {"sql": "SELECT COUNT(*) FROM kv"})[0] == 401
    issued_one = service.tenants.get(first_id)["token_ids"][0]
    assert _call(service, "DELETE", f"/v1/tenants/{first_id}/tokens/{issued_one}",
                 admin)[0] == 200
    assert _call(service, "GET", "/v1/databases", token_one)[0] == 401


def test_tenant_restore_reserves_target_slot_and_keeps_owner(tmp_path):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path), admin_token=admin)
    limits = {"max_databases": 2, "max_frontier_bytes": 67_108_864,
              "max_operation_bytes": 65_536, "max_tokens": 64}
    tenant_id = _call(service, "POST", "/v1/tenants", admin,
                      {"name": "app", "request_id": "app-tenant",
                       "limits": limits})[1]["id"]
    token = _call(service, "POST", f"/v1/tenants/{tenant_id}/tokens", admin)[1]["token"]
    source = _call(service, "POST", "/v1/databases", token,
                   {"name": "source", "request_id": "source",
                    "schema": ["CREATE TABLE kv(v INTEGER)"]})[1]["id"]
    service.write(source, "insert", [["INSERT INTO kv VALUES (7)", []]])
    copied = _call(service, "POST", f"/v1/databases/{source}/restore", token,
                   {"source_version": 1, "name": "copy", "request_id": "copy"})
    assert copied[0] == 201 and copied[1]["tenant_id"] == tenant_id
    assert _call(service, "POST", f"/v1/databases/{source}/restore", token,
                 {"source_version": 1, "name": "copy", "request_id": "copy"}) == copied
    assert service.read(copied[1]["id"], "SELECT v FROM kv") == [(7,)]
    assert service.tenants.get(tenant_id)["reserved_databases"] == 2
    assert _call(service, "POST", "/v1/databases", token,
                 {"name": "third", "request_id": "third", "schema": []})[0] == 413


def test_create_recovers_after_tenant_slot_reserved_before_catalog_write(
        tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("app", limits={
        "max_databases": 1, "max_frontier_bytes": 67_108_864,
        "max_operation_bytes": 65_536, "max_tokens": 64})["id"]
    original_put = backend.put
    crashed = False

    def interrupted_put(key, data, **conditions):
        nonlocal crashed
        if key.startswith("_tessera_cloud/catalog/") and not crashed:
            crashed = True
            raise RuntimeError("process stopped after tenant reservation")
        return original_put(key, data, **conditions)

    monkeypatch.setattr(backend, "put", interrupted_put)
    with pytest.raises(RuntimeError, match="tenant reservation"):
        service.catalog.create("app", ["CREATE TABLE kv(v INTEGER)"],
                               request_id="stable", tenant_id=tenant_id)
    assert service.tenants.get(tenant_id)["reserved_databases"] == 1
    created = service.catalog.create("app", ["CREATE TABLE kv(v INTEGER)"],
                                     request_id="stable", tenant_id=tenant_id)
    assert created["tenant_id"] == tenant_id
    assert service.tenants.get(tenant_id)["reserved_databases"] == 1


def test_deleted_slot_requires_retired_pointer_before_release(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path)
    service = DatabaseService(backend, admin_token="a" * 48)
    tenant_id = service.tenants.create("app", limits={
        "max_databases": 1, "max_frontier_bytes": 67_108_864,
        "max_operation_bytes": 65_536, "max_tokens": 64})["id"]
    created = service.catalog.create("app", [], request_id="stable",
                                     tenant_id=tenant_id)
    database_id = created["id"]
    monkeypatch.setattr(backend, "pointer_retired", lambda _key: False, raising=False)
    with pytest.raises(CatalogConflict):
        service.catalog._release_tenant_slot_after_purge(database_id)
    record, etag = service.catalog._load(database_id)
    record["state"] = "deleted"
    backend.put(service.catalog._key(database_id),
                json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
                if_match=etag)
    with pytest.raises(CatalogConflict):
        service.catalog._release_tenant_slot_after_purge(database_id)
    assert service.tenants.get(tenant_id)["reserved_databases"] == 1
    monkeypatch.setattr(backend, "pointer_retired", lambda _key: True)
    with pytest.raises(CatalogConflict, match="deleted_nonempty"):
        service.catalog._release_tenant_slot_after_purge(database_id)
    for key in backend.list(f"turso-native/cloud-{database_id}/"):
        backend.delete(key)
    fingerprint = record["create_fingerprint"]

    def set_reservation(value):
        def edit(tenant):
            tenant["reservations"][database_id] = value

        service.tenants._change(tenant_id, edit)

    set_reservation(sha256(b"unexpected reservation").hexdigest())
    with pytest.raises(TenantConflict, match="fingerprint changed"):
        service.catalog._release_tenant_slot_after_purge(database_id)
    assert service.tenants.get(tenant_id)["reserved_databases"] == 1
    set_reservation(fingerprint)
    service.catalog._release_tenant_slot_after_purge(database_id)
    service.catalog._release_tenant_slot_after_purge(database_id)
    assert service.tenants.get(tenant_id)["reserved_databases"] == 0
    with pytest.raises(DatabaseUnavailable):
        service.catalog.create("app", [], request_id="stable", tenant_id=tenant_id)
    assert service.tenants.get(tenant_id)["reserved_databases"] == 0


def test_moto_tenant_slot_released_only_after_fenced_purge():
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend

    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        s3 = session.client("s3")
        dynamodb = session.resource("dynamodb")
        s3.create_bucket(Bucket="tessera-tenant-test")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        raw = S3DynamoBackend("tessera-tenant-test", "leases", s3_client=s3,
                              dynamodb=dynamodb, backoff_max_attempts=1)
        fenced = FencedBackend(raw, session.client("dynamodb"))
        service = DatabaseService(fenced, admin_token="a" * 48, catalog_backend=raw)
        tenant_id = service.tenants.create("app", limits={
            "max_databases": 1, "max_frontier_bytes": 67_108_864,
            "max_operation_bytes": 65_536, "max_tokens": 64})["id"]
        database_id = service.catalog.create("first", [], request_id="first",
                                             tenant_id=tenant_id)["id"]
        assert service.tenants.get(tenant_id)["reserved_databases"] == 1
        service.catalog.request_deletion(database_id)
        assert service.tenants.get(tenant_id)["reserved_databases"] == 1
        record, etag = service.catalog._load(database_id)
        record["delete_after"] = 0
        raw.put(service.catalog._key(database_id),
                json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
                if_match=etag)
        for _ in range(12):
            result = service.catalog.purge_step(database_id)
            if result["state"] == "deleted":
                break
        assert result["state"] == "deleted"
        assert fenced.pointer_retired(
            f"turso-native/cloud-{database_id}/manifest.json")
        assert service.tenants.get(tenant_id)["reserved_databases"] == 0
        service.catalog.purge_step(database_id)
        assert service.catalog.create("second", [], request_id="second",
                                      tenant_id=tenant_id)["tenant_id"] == tenant_id
