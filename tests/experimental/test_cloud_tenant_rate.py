"""Tenant admission is atomic and rejects requests before executing SQL."""

import json

import boto3
import pytest
from moto import mock_aws

from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.service import dispatch_request, error_response
from tessera.experimental.cloud.tenant_rate import (
    DynamoDBTenantRateMeter,
    TenantRateLimited,
)
from tessera.experimental.cloud.tenants import (
    DEFAULT_RATE_LIMITS, TenantCatalog, TenantConflict)


def test_dynamodb_meter_enforces_request_and_body_budgets_per_minute():
    tenant = "a" * 32
    limits = {"max_requests_per_minute": 2, "max_body_bytes_per_minute": 5}
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-2")
        client.create_table(
            TableName="coordination", BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
        )
        meter = DynamoDBTenantRateMeter(client, "coordination")
        meter.admit(tenant, 3, limits, now=120)
        meter.admit(tenant, 2, limits, now=121)
        with pytest.raises(TenantRateLimited) as exhausted:
            meter.admit(tenant, 0, limits, now=122)
        assert exhausted.value.retry_after_seconds == 58
        item = client.get_item(
            TableName="coordination",
            Key={"tenant_id": {"S": f"_tessera_cloud/rate/{tenant}/2"}},
            ConsistentRead=True)["Item"]
        assert item["requests"]["N"] == "2"
        assert item["body_bytes"]["N"] == "5"
        assert item["quota_expires_at"]["N"] == str(180 + 3_600)
        meter.admit(tenant, 5, limits, now=180)
        with pytest.raises(TenantRateLimited):
            meter.admit(tenant, 1, limits, now=181)
        with pytest.raises(TenantRateLimited):
            meter.admit("b" * 32, 6, limits, now=180)


def test_existing_tenant_replays_and_defaults_survive_rate_limit_addition(tmp_path):
    catalog = TenantCatalog(LocalBackend(tmp_path))
    tenant = catalog.create("existing", request_id="same")
    assert tenant["rate_limits"] == DEFAULT_RATE_LIMITS
    assert catalog.create("existing", request_id="same",
                          rate_limits=DEFAULT_RATE_LIMITS) == tenant
    record, etag = catalog._load(tenant["id"])
    del record["rate_limits"]
    catalog.backend.put(catalog._key(tenant["id"]),
                        json.dumps(record).encode(), if_match=etag)
    assert catalog.get(tenant["id"])["rate_limits"] == DEFAULT_RATE_LIMITS
    assert catalog.create("existing", request_id="same") == tenant
    with pytest.raises(ValueError, match="rate limits require both fields"):
        catalog.create("invalid", rate_limits={"max_requests_per_minute": 1})
    with pytest.raises(ValueError, match="max_requests_per_minute"):
        catalog.create("invalid", rate_limits={
            "max_requests_per_minute": True, "max_body_bytes_per_minute": 1024})
    with pytest.raises(TenantConflict, match="request_id was used"):
        catalog.create("existing", request_id="same", rate_limits={
            "max_requests_per_minute": 2, "max_body_bytes_per_minute": 1024})


def test_authenticated_tenant_is_metered_before_sql(tmp_path, monkeypatch):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path), admin_token=admin)
    tenant = service.tenants.create("app", request_id="tenant")
    manager = service.tenants.issue_token(tenant["id"])["token"]
    database = service.catalog.create(
        "db", ["CREATE TABLE kv(v INTEGER)"], request_id="db",
        tenant_id=tenant["id"])["id"]
    sql_token = service.catalog.issue_token(database, "read").secret

    class Meter:
        calls = []

        def admit(self, tenant_id, body_bytes, limits):
            self.calls.append((tenant_id, body_bytes, limits))
            if len(self.calls) > 1:
                raise TenantRateLimited(12)

    meter = Meter()
    service.tenants.rate_meter = meter

    def call(token):
        try:
            return dispatch_request(
                service, "POST", f"/v1/databases/{database}/query", token,
                lambda: {"sql": "SELECT count(*) FROM kv"}, request_body_bytes=73)
        except Exception as exc:  # noqa: BLE001 - exercise HTTP mapping
            return error_response(exc)

    assert call("wrong")[0] == 401
    assert meter.calls == []
    assert call(sql_token) == (200, {"rows": [(0,)]})
    monkeypatch.setattr(service, "read", lambda *_args, **_kwargs: pytest.fail(
        "rate-limited request executed SQL"))
    assert call(sql_token) == (429, {"error": "rate_limited",
                                    "retry_after_seconds": 12})
    assert [item[1] for item in meter.calls] == [73, 73]
    assert all(item[0] == tenant["id"] for item in meter.calls)
    assert all(item[2] == DEFAULT_RATE_LIMITS for item in meter.calls)
    assert service.tenants.authorize(manager) == tenant["id"]


def test_root_can_change_rate_budget_without_changing_tenant_identity(tmp_path):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path), admin_token=admin)
    tenant = service.tenants.create("app", request_id="same")
    path = f"/v1/tenants/{tenant['id']}/rate-limits"
    change = {"rate_limits": {"max_requests_per_minute": 2,
                              "max_body_bytes_per_minute": 4096}}
    assert dispatch_request(service, "PUT", path, "wrong", lambda: change)[0] == 401
    with pytest.raises(ValueError, match="rate_limits object required"):
        dispatch_request(service, "PUT", path, admin, lambda: {})
    assert dispatch_request(service, "PUT", path, admin, lambda: change) == (
        200, change)
    assert service.tenants.get(tenant["id"])["rate_limits"] == change["rate_limits"]
    assert service.tenants.create("app", request_id="same")["id"] == tenant["id"]
