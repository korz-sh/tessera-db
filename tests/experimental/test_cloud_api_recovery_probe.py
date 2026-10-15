"""Recover matched-workload rows and receipts from fresh remote clients."""

import boto3
import pytest

pytest.importorskip("turso")
pytest.importorskip("moto")
from moto import mock_aws

from experiments.cloud_api_recovery_probe import recover
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud import DatabaseService
from tessera.experimental.turso.fencing import FencedBackend


def test_recovery_probe_checks_receipt_rows_and_sdk_attempts():
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        session.client("s3").create_bucket(Bucket="tessera-matched-recovery-test")
        dynamodb = session.resource("dynamodb")
        dynamodb.create_table(
            TableName="coordination",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-matched-recovery-test", "coordination",
                              s3_client=session.client("s3"), dynamodb=dynamodb,
                              backoff_max_attempts=1)
        service = DatabaseService(FencedBackend(raw, session.client("dynamodb")),
                                  admin_token="a" * 48, catalog_backend=raw)
        database_id = service.catalog.create(
            "matched", ["CREATE TABLE scan_issue(scan_id INTEGER, value TEXT)"])["id"]
        receipt = service.write(database_id, "matched-scan-1", [
            ["INSERT INTO scan_issue VALUES(?, ?)", [1, "first"]],
            ["INSERT INTO scan_issue VALUES(?, ?)", [1, "second"]]])
        report = recover(session, raw.bucket, raw.table_name, database_id,
                         "matched-scan-1", "SELECT COUNT(*) FROM scan_issue WHERE scan_id=?",
                         [1], [[2]], region="us-east-1")
        assert report["published_version"] == receipt["version"]
        assert report["rows"] == [[2]]
        assert report["sdk_http_attempts"]
        with pytest.raises(AssertionError, match="cold recovery lost"):
            recover(session, raw.bucket, raw.table_name, database_id,
                    "matched-scan-1", "SELECT COUNT(*) FROM scan_issue WHERE scan_id=?",
                    [1], [[3]], region="us-east-1")
