"""Interrupted AWS smokes must preserve a recoverable resource identifier."""

import json

import pytest

from experiments import cloud_api_staging_smoke as smoke_module
from experiments import cloud_deletion_deployed_drill as deployed_drill
from experiments import cloud_https_staging_probe as https_probe
from experiments import cloud_retention_deployed_drill as retention_drill
from examples.cloud_matched_scan_benchmark import (
    SYNTHETIC_SCHEMA,
    Tessera,
    statements_for_scan,
)
from tessera.backends.local import LocalBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.cloud.service import dispatch_request, error_response


class ExpiredClient:
    def create_bucket(self, **_):
        raise RuntimeError("SSO expired")

    def head_bucket(self, **_):
        raise RuntimeError("SSO expired")

    def describe_table(self, **_):
        raise RuntimeError("SSO expired")


class ExpiredSession:
    def __init__(self, **_):
        self.client_instance = ExpiredClient()

    def client(self, _name):
        return self.client_instance


def test_native_retention_drill_checks_account_before_resources(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="expected account ID"):
        retention_drill.drill("unused", "us-east-2", tmp_path / "report.json",
                              tmp_path / "lambda.zip", native_gc=True)

    class WrongSession:
        def client(self, name):
            assert name == "sts"
            return self

        def get_caller_identity(self):
            return {"Account": "111111111111"}

    monkeypatch.setattr(retention_drill.boto3, "Session", lambda **_: WrongSession())
    with pytest.raises(RuntimeError, match="account differs"):
        retention_drill.drill("unused", "us-east-2", tmp_path / "report.json",
                              tmp_path / "lambda.zip", native_gc=True,
                              expected_account_id="222222222222")


def test_expired_credentials_keep_failure_report_and_cleanup_suffix(tmp_path, monkeypatch):
    monkeypatch.setattr(smoke_module.boto3, "Session", ExpiredSession)
    output = tmp_path / "smoke.json"
    with pytest.raises(RuntimeError, match="SSO expired"):
        smoke_module.smoke("expired", "us-east-2", output)
    report = json.loads(output.read_text())
    state = json.loads(output.with_suffix(".state.json").read_text())
    assert report["failure_type"] == "RuntimeError"
    assert report["bucket_verification_error"] == "RuntimeError"
    assert report["table_verification_error"] == "RuntimeError"
    assert not report["bucket_absent"] and not report["table_absent"]
    assert state["bucket"] == state["table"]
    assert state["suffix"] in state["bucket"]


def test_tenant_deployed_drill_logic_locally(tmp_path):
    raw = LocalBackend(tmp_path)
    service = DatabaseService(raw, admin_token="a" * 48)
    report = smoke_module._tenant_drill(service, raw, raw)
    assert report["concurrent_final_slot"]
    assert report["crash_after_reservation_recovered"]
    assert report["cross_tenant_denied"]


def test_legacy_migration_drill_logic_locally(tmp_path):
    class PagedLocalBackend(LocalBackend):
        def list_page(self, prefix, limit=1000, *, start_after=None):
            keys = [key for key in self.list(prefix)
                    if start_after is None or key > start_after]
            return [(key, None) for key in keys[:limit]]

    raw = PagedLocalBackend(tmp_path)
    admin = "a" * 48
    service = DatabaseService(raw, admin_token=admin)
    report, database_id, version = smoke_module._legacy_migration_drill(
        service, raw, raw, admin)
    assert report["legacy_live_candidates"] == 1
    assert report["reservation_crash_recovered"]
    assert report["root_replay_preserved"]
    assert service.catalog.get(database_id)["tenant_id"]
    assert service.lookup(database_id, "staging-op")["version"] == version


def test_legacy_migration_drill_uses_s3_catalog_cas():
    import boto3
    from moto import mock_aws

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend

    with mock_aws():
        session = boto3.Session(region_name="us-east-1")
        s3 = session.client("s3")
        dynamodb = session.resource("dynamodb")
        s3.create_bucket(Bucket="tessera-legacy-migration-test")
        dynamodb.create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-legacy-migration-test", "leases",
                              s3_client=s3, dynamodb=dynamodb, backoff_max_attempts=1)
        fenced = FencedBackend(raw, session.client("dynamodb"))
        service = DatabaseService(fenced, admin_token="a" * 48, catalog_backend=raw)
        report, database_id, version = smoke_module._legacy_migration_drill(
            service, raw, fenced, "a" * 48)
        assert report["legacy_live_candidates"] == 1
        assert report["no_unowned_live_catalog_records"]
        assert service.lookup(database_id, "staging-op")["version"] == version


def test_https_probe_exercises_strict_tenant_route(tmp_path, monkeypatch):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path / "objects"), admin_token=admin,
                              allow_legacy_root_create=False)

    class Secrets:
        def get_secret_value(self, **_):
            return {"SecretString": admin}

    class Session:
        def client(self, name):
            assert name == "secretsmanager"
            return Secrets()

    def request(_base, method, path, token, body=None):
        try:
            status, payload = dispatch_request(service, method, path, token,
                                               lambda: body)
        except Exception as exc:  # noqa: BLE001 - model HTTP error translation
            status, payload = error_response(exc)
        return status, json.loads(json.dumps(payload)), 0

    monkeypatch.setattr(https_probe.boto3, "Session", lambda **_: Session())
    monkeypatch.setattr(https_probe, "_request", request)
    monkeypatch.setattr(https_probe.subprocess, "check_output", lambda *_, **__: "source")
    artifact = tmp_path / "lambda.zip"
    artifact.write_bytes(b"artifact")
    output = tmp_path / "probe.json"
    monkeypatch.setattr(https_probe.sys, "argv", ["probe", "--profile", "test",
                        "--region", "us-east-2", "--api-url", "https://example.test",
                        "--admin-secret-arn", "secret", "--lambda-zip", str(artifact),
                        "--output", str(output)])
    https_probe.main()
    report = json.loads(output.read_text())
    assert report["status"] == "passed"
    assert report["strict_root_admission"]


def test_matched_scan_client_uses_tenant_management_token(tmp_path, monkeypatch):
    admin = "a" * 48
    service = DatabaseService(LocalBackend(tmp_path), admin_token=admin,
                              allow_legacy_root_create=False)
    tenant_id = service.tenants.create("benchmark")["id"]
    manager = service.tenants.issue_token(tenant_id)["token"]

    def request(_self, method, path, token, body=None):
        try:
            status, payload = dispatch_request(service, method, path, token,
                                               lambda: body)
        except Exception as exc:  # noqa: BLE001 - model HTTP status mapping
            status, payload = error_response(exc)
        return status, json.loads(json.dumps(payload))

    monkeypatch.setattr(Tessera, "request", request)
    client = Tessera(SYNTHETIC_SCHEMA, "https://example.test", manager)
    assert service.catalog.get(client.database_id)["tenant_id"] == tenant_id
    assert client.write(1, statements_for_scan(1, 1, 1))[0] == 1
    assert client.count(1) == 1


def test_cost_sample_collection_counts_only_http_operations():
    def event(operation, calls):
        return {"message": "INFO TesseraDB AWS call sample: " + json.dumps({
            "kind": "tessera_aws_call_sample_v1", "operation": operation,
            "logical_sdk_calls": calls,
            "sdk_http_attempts": calls})}

    class Logs:
        def get_paginator(self, name):
            assert name == "filter_log_events"
            return self

        def paginate(self, **kwargs):
            assert kwargs["logGroupName"] == "/aws/lambda/test-sql"
            return iter([{"events": [
                event("transactions", {"s3.PutObject": 2, "dynamodb.GetItem": 3}),
                event("retention", {"s3.GetObject": 9}),
                event("query", {"s3.GetObject": 1, "dynamodb.GetItem": 1}),
            ]}])

    report = deployed_drill._cost_samples(Logs(), "/aws/lambda/test-sql", 1, 2, 2)
    assert report["complete"] and report["observed_samples"] == 2
    assert report["minimum_expected_http_requests"] == 2
    assert deployed_drill._cost_samples(Logs(), "/aws/lambda/test-sql", 1, 2, 1)["complete"]
    assert report["logical_sdk_calls"] == {
        "dynamodb.GetItem": 4, "s3.GetObject": 1, "s3.PutObject": 2}
    assert report["sdk_http_attempts_observed"] is True
    assert report["sdk_http_attempts"] == report["logical_sdk_calls"]


def test_matched_recovery_runs_in_new_process_and_aggregates_calls(monkeypatch):
    commands = []

    def completed(command, **options):
        commands.append(command)
        assert options["cwd"] == deployed_drill.ROOT
        assert options["timeout"] == 120
        return json.dumps({"published_version": 2, "rows": [[500]],
                           "logical_sdk_calls": {"s3.GetObject": 2},
                           "sdk_http_attempts": {"s3.GetObject": 3}})

    monkeypatch.setattr(deployed_drill.subprocess, "check_output", completed)
    report = deployed_drill._cold_recovery_samples(
        "stage", "us-east-2", "bucket", "table", "database", 2, samples=2)
    assert len(commands) == 2
    assert all("experiments.cloud_api_recovery_probe" in command
               and "matched-scan-2" in command for command in commands)
    assert report["process_wall_ms"]["count"] == 2
    assert report["logical_sdk_calls"] == {"s3.GetObject": 4}
    assert report["sdk_http_attempts"] == {"s3.GetObject": 6}
