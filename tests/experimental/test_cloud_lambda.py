"""API Gateway and local HTTP must share authorization and receipt behavior."""

import base64
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from urllib.parse import urlsplit

import boto3
from moto import mock_aws

from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud import lambda_handler as adapter
from tessera.experimental.cloud.cost_telemetry import (
    begin_request,
    end_request,
    register_session,
)
from tessera.experimental.cloud.tenant_rate import TenantRateLimited

ADMIN = "lambda-test-admin-" + "x" * 32


def _invoke(method, path, *, token=None, body=None, encoded=False):
    headers = {} if token is None else {"Authorization": "Bearer " + token}
    raw = None if body is None else json.dumps(body)
    parsed = urlsplit(path)
    event = {"requestContext": {"http": {"method": method}}, "rawPath": parsed.path,
             "rawQueryString": parsed.query,
             "headers": headers, "isBase64Encoded": encoded}
    if raw is not None:
        event["body"] = base64.b64encode(raw.encode()).decode() if encoded else raw
    response = adapter.lambda_handler(event, None)
    assert response["headers"]["cache-control"] == "no-store"
    return response["statusCode"], json.loads(response["body"])


def test_lambda_sql_auth_and_receipt_are_durable(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path / "objects")
    monkeypatch.setattr(adapter, "_get_service",
                        lambda: DatabaseService(backend, admin_token=ADMIN))
    assert _invoke("GET", "/v1/databases")[0] == 401
    status, created = _invoke("POST", "/v1/databases", token=ADMIN,
                              body={"name": "lambda-db", "schema": [
                                  "CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)"]})
    assert status == 201
    database_id = created["id"]
    issued = _invoke("POST", f"/v1/databases/{database_id}/tokens", token=ADMIN,
                     body={"scope": "write"})[1]
    transaction = {"operation_id": "lambda-op", "statements": [
        ["INSERT INTO kv VALUES(?, ?)", ["key", "value"]]]}
    assert _invoke("POST", f"/v1/databases/{database_id}/transactions",
                   body=transaction)[0] == 401
    status, receipt = _invoke("POST", f"/v1/databases/{database_id}/transactions",
                              token=issued["token"], body=transaction, encoded=True)
    assert status == 200 and receipt["version"] == 1
    assert _invoke("POST", f"/v1/databases/{database_id}/transactions",
                   token=issued["token"], body=transaction) == (status, receipt)
    assert _invoke("POST", f"/v1/databases/{database_id}/query",
                   token=issued["token"], body={"sql": "SELECT v FROM kv"}) == (
                       200, {"rows": [["value"]]})
    assert _invoke("GET", f"/v1/databases/{database_id}/operations/lambda-op",
                   token=issued["token"])[1]["state"] == "published"
    assert _invoke("POST", f"/v1/databases/{database_id}/query",
                   token=issued["token"], body={"sql": "SELECT 1", "params": []},
                   encoded=True)[0] == 200
    assert _invoke("GET", f"/v1/databases/{database_id}/usage?limit=1&limit=2",
                   token=ADMIN)[0] == 400


def test_lambda_routes_retention_event_without_http_request(monkeypatch):
    result = {"scanned": 2, "deleted": 1}
    catalog = SimpleNamespace(sweep_retention=lambda: result)
    monkeypatch.setattr(adapter, "_get_service", lambda: SimpleNamespace(catalog=catalog))
    assert adapter.lambda_handler({"tessera_maintenance": "retention"}, None) == result


def test_lambda_rate_limit_has_retry_header_and_accepts_null_get_body(tmp_path,
                                                                     monkeypatch):
    service = DatabaseService(LocalBackend(tmp_path), admin_token=ADMIN)
    tenant = service.tenants.create("app", request_id="tenant")
    manager = service.tenants.issue_token(tenant["id"])["token"]

    class Reject:
        def admit(self, *_args):
            raise TenantRateLimited(9)

    service.tenants.rate_meter = Reject()
    monkeypatch.setattr(adapter, "_get_service", lambda: service)
    event = {"requestContext": {"http": {"method": "GET"}},
             "rawPath": "/v1/databases", "headers": {"authorization": "Bearer " + manager},
             "body": None}
    response = adapter.lambda_handler(event, None)
    assert response["statusCode"] == 429
    assert response["headers"]["retry-after"] == "9"
    assert json.loads(response["body"]) == {"error": "rate_limited",
                                            "retry_after_seconds": 9}


def test_lambda_routes_pending_and_repair_deletions(monkeypatch):
    pending = {"attempted": 0, "completed": 0, "failed": 0}
    repair = {"scanned": 0, "attempted": 0, "failed": 0, "wrapped": True}
    catalog = SimpleNamespace(sweep_pending_deletions=lambda: pending,
                              sweep_deletions=lambda: repair)
    monkeypatch.setattr(adapter, "_get_service", lambda: SimpleNamespace(catalog=catalog))
    scheduled = {"source": "aws.events", "detail-type": "Scheduled Event"}
    assert adapter.lambda_handler(scheduled, None) == pending
    assert adapter.lambda_handler({"tessera_maintenance": "deletion_repair"}, None) == repair


def test_opt_in_cost_telemetry_counts_sdk_calls_without_request_data(tmp_path,
                                                                     monkeypatch, caplog):
    with mock_aws():
        session = boto3.Session(region_name="us-east-2")
        register_session(session)
        client = session.client("s3")
        table = session.resource("dynamodb").create_table(
            TableName="cost-test", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        client.list_buckets()  # Calls outside a request do not enter its count.
        counts, token = begin_request()
        client.list_buckets()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(table.get_item, Key={"id": "missing"}).result()
        session.events.emit("after-call.s3.ListBuckets",
                            model=client.meta.service_model.operation_model("ListBuckets"),
                            parsed={"ResponseMetadata": {"RetryAttempts": 2}})
        end_request(token)
        client.list_buckets()
    assert counts.logical == {"s3.ListBuckets": 1, "dynamodb.GetItem": 1}
    assert counts.http_attempts == {"s3.ListBuckets": 4, "dynamodb.GetItem": 1}

    backend = LocalBackend(tmp_path / "objects")
    monkeypatch.setattr(adapter, "_get_service",
                        lambda: DatabaseService(backend, admin_token=ADMIN))
    created = _invoke("POST", "/v1/databases", token=ADMIN,
                      body={"name": "metrics-db", "schema": [
                          "CREATE TABLE kv(k TEXT PRIMARY KEY)"]})[1]
    monkeypatch.setenv("TESSERA_AWS_CALL_METRICS", "1")
    with caplog.at_level(logging.INFO, logger=adapter.__name__):
        status, _ = _invoke("POST", f"/v1/databases/{created['id']}/transactions",
                            token=ADMIN[:-1], body={"operation_id": "private"})
    assert status == 401
    samples = [record.message.split("TesseraDB AWS call sample: ", 1)[1]
               for record in caplog.records
               if record.message.startswith("TesseraDB AWS call sample: ")]
    assert len(samples) == 1
    sample = json.loads(samples[0])
    assert sample["operation"] == "transactions"
    assert sample["status"] == 401
    assert sample["sdk_http_attempts"] == {}
    assert sample["logical_sdk_calls"] == {}
    assert ADMIN[:-1] not in samples[0]
    assert "private" not in samples[0]
    assert created["id"] not in samples[0]
