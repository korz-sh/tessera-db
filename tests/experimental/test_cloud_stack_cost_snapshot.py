"""A stack meter must expose missing coverage and avoid false storage totals."""

import hashlib
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from moto import mock_aws

from experiments.cloud_stack_cost_snapshot import (
    _api_response_payload, _lambda_billing, _metric, _s3_inventory, collect,
)


def test_stack_snapshot_separates_gsi_capacity_and_bounds_s3_inventory():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)

    class CloudWatch:
        def get_metric_statistics(self, **arguments):
            unit = ("None" if arguments["Namespace"] == "AWS/ApiGateway"
                    and arguments["MetricName"] == "Count"
                    else "Bytes" if arguments["MetricName"] in {"DataProcessed", "IncomingBytes"}
                    else "Milliseconds" if arguments["MetricName"] == "Duration"
                    else "Count")
            return {"Datapoints": [{"Timestamp": start, "Sum": 2, "Unit": unit}]}

    class DynamoDB:
        def describe_table(self, **_kwargs):
            return {"Table": {"TableStatus": "ACTIVE", "TableSizeBytes": 0,
                              "ItemCount": 0,
                              "GlobalSecondaryIndexes": [{"IndexName": "pending-deletions",
                                                          "IndexSizeBytes": 0,
                                                          "ItemCount": 0}]}}

        def describe_continuous_backups(self, **_kwargs):
            return {"ContinuousBackupsDescription": {
                "PointInTimeRecoveryDescription": {"PointInTimeRecoveryStatus": "ENABLED"}}}

    class Logs:
        def filter_log_events(self, **arguments):
            if arguments["logGroupName"].startswith("/aws/apigateway/"):
                return {"events": [{
                    "eventId": "api-1", "timestamp": int(start.timestamp() * 1000),
                    "message": '{"request_id":"example","status":"200",'
                               '"response_bytes":"42"}',
                }]}
            return {"events": [{
                "eventId": "report-1", "timestamp": int(start.timestamp() * 1000),
                "message": "REPORT RequestId: sample-1  Duration: 88.10 ms  "
                           "Billed Duration: 100 ms  Memory Size: 512 MB",
            }]}

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(Bucket="tessera-snapshot-test",
                         CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        for number in range(3):
            s3.put_object(Bucket="tessera-snapshot-test", Key=str(number), Body=b"data")

        class Session:
            region_name = "us-east-2"

            def client(self, name):
                return {"s3": s3, "dynamodb": DynamoDB(),
                        "cloudwatch": CloudWatch(), "logs": Logs()}[name]

        partial = _s3_inventory(s3, "tessera-snapshot-test", 2)
        assert {key: partial[key] for key in ("current_objects", "current_bytes",
                                              "list_requests", "complete", "stop_reason")} == {
            "current_objects": 2, "current_bytes": 8, "list_requests": 1,
            "complete": False, "stop_reason": "object_limit"}
        assert partial["bucket_sha256"] == hashlib.sha256(
            b"tessera-snapshot-test").hexdigest()
        assert datetime.fromisoformat(partial["started_at_utc"]) <= datetime.fromisoformat(
            partial["completed_at_utc"])
        result = collect(Session(), bucket="tessera-snapshot-test", table="coordination",
                         api_url="https://abc123.execute-api.us-east-2.amazonaws.com",
                         lambda_function="tessera-sql", start=start,
                         end=start + timedelta(minutes=5))
        inventory = result["s3_inventory"]
        assert {key: inventory[key] for key in ("current_objects", "current_bytes",
                                                "list_requests", "complete", "stop_reason")} == {
            "current_objects": 3, "current_bytes": 12, "list_requests": 1,
            "complete": True, "stop_reason": None}
        assert result["dynamodb_pitr_status"] == "ENABLED"
        assert result["cloudwatch_metrics"]["dynamodb_gsi_pending-deletions_reads"]["sum"] == 2
        assert result["cloudwatch_metrics"]["api_data_processed_bytes"]["sum"] == 2
        assert result["cloudwatch_metrics"]["api_requests"]["unit"] == "None"
        assert result["cloudwatch_metrics"]["lambda_duration_ms"] == {
            "sum": 2, "observed_minutes": 1, "expected_minutes": 5,
            "missing_minutes": 4, "complete": False, "unit": "Milliseconds"}
        assert result["lambda_billing"]["report_count"] == 1
        assert result["lambda_billing"]["billed_duration_ms"] == 100
        assert result["lambda_billing"]["billed_gb_seconds"] == 0.05
        assert result["lambda_billing"]["matches_cloudwatch_invocations"] is None
        assert result["api_response_payload"]["response_payload_bytes"] == 42
        assert result["api_response_payload"]["parsed_response_lengths"] == 1
        assert result["api_response_payload"]["matches_cloudwatch_requests"] is None
        s3.put_bucket_versioning(Bucket="tessera-snapshot-test",
                                 VersioningConfiguration={"Status": "Enabled"})
        with pytest.raises(RuntimeError, match="billable versions"):
            _s3_inventory(s3, "tessera-snapshot-test", 10)


def test_metric_rejects_duplicate_minutes():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)

    class CloudWatch:
        def get_metric_statistics(self, **_kwargs):
            point = {"Timestamp": start, "Sum": 1, "Unit": "Count"}
            return {"Datapoints": [point, point]}

    with pytest.raises(ValueError, match="duplicate"):
        _metric(CloudWatch(), "AWS/ApiGateway", "Count", [],
                start, start + timedelta(minutes=5), "Count")


def test_s3_inventory_bounds_short_truncated_pages_without_objects():
    class ShortPages:
        requests = 0

        def get_bucket_versioning(self, **_kwargs):
            return {}

        def list_objects_v2(self, **_kwargs):
            self.requests += 1
            return {"Contents": [], "IsTruncated": True,
                    "NextContinuationToken": str(self.requests)}

    client = ShortPages()
    result = _s3_inventory(client, "bucket", 10, max_requests=3)
    assert {key: result[key] for key in ("current_objects", "current_bytes",
                                        "list_requests", "complete", "stop_reason")} == {
        "current_objects": 0, "current_bytes": 0, "list_requests": 3,
        "complete": False, "stop_reason": "request_limit"}
    assert client.requests == 3


def test_lambda_billing_follows_empty_pages_and_counts_retried_request_id():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)
    stamp = int(start.timestamp() * 1000)

    class Logs:
        calls = 0

        def filter_log_events(self, **arguments):
            self.calls += 1
            assert arguments["logGroupName"] == "/aws/lambda/tessera-sql"
            if self.calls == 1:
                assert "nextToken" not in arguments
                return {"events": [], "nextToken": "first"}
            assert arguments["nextToken"] == "first"
            return {"events": [
                {"eventId": "first", "timestamp": stamp,
                 "message": "REPORT RequestId: retried  Duration: 8.5 ms  "
                            "Billed Duration: 9 ms  Memory Size: 512 MB"},
                {"eventId": "second", "timestamp": stamp + 1,
                 "message": "REPORT RequestId: retried  Duration: 11.5 ms  "
                            "Billed Duration: 12 ms  Memory Size: 1024 MB"},
            ]}

    client = Logs()
    result = _lambda_billing(client, "tessera-sql", start,
                             start + timedelta(minutes=1))
    assert result["complete_scan"] is True
    assert result["report_count"] == 2
    assert result["billed_duration_ms"] == 21
    assert result["billed_gb_seconds"] == (9 * 512 + 12 * 1024) / (1024 * 1000)
    assert result["memory_mb"] == [512, 1024]
    assert result["filter_requests"] == 2


def test_lambda_billing_marks_truncated_scan():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)

    class Logs:
        def filter_log_events(self, **_kwargs):
            return {"events": [], "nextToken": "more"}

    result = _lambda_billing(Logs(), "tessera-sql", start,
                             start + timedelta(minutes=1), max_requests=1)
    assert result["complete_scan"] is False
    assert result["stop_reason"] == "request_limit"


def test_api_response_payload_handles_empty_pages_duplicates_and_old_records():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)
    stamp = int(start.timestamp() * 1000)

    class Logs:
        calls = 0

        def filter_log_events(self, **arguments):
            self.calls += 1
            assert arguments["logGroupName"] == "/aws/apigateway/tessera-sql"
            if self.calls == 1:
                return {"events": [], "nextToken": "page-2"}
            assert arguments["nextToken"] == "page-2"
            return {"events": [
                {"eventId": "a", "timestamp": stamp,
                 "message": '{"response_bytes":"123"}'},
                {"eventId": "a", "timestamp": stamp,
                 "message": '{"response_bytes":"123"}'},
                {"eventId": "b", "timestamp": stamp + 1,
                 "message": '{"response_bytes":"0"}'},
                {"eventId": "old", "timestamp": stamp + 2,
                 "message": '{"request_id":"legacy","status":"200"}'},
            ]}

    result = _api_response_payload(Logs(), "tessera-sql", start,
                                   start + timedelta(minutes=1))
    assert result["complete_scan"] is True
    assert result["logged_requests"] == 3
    assert result["parsed_response_lengths"] == 2
    assert result["unparsed_response_lengths"] == 1
    assert result["response_payload_bytes"] == 123
    assert result["filter_requests"] == 2


def test_api_response_payload_marks_truncated_scan():
    start = datetime(2026, 9, 29, 4, 0, tzinfo=timezone.utc)

    class Logs:
        def filter_log_events(self, **_kwargs):
            return {"events": [], "nextToken": "more"}

    result = _api_response_payload(Logs(), "tessera-sql", start,
                                   start + timedelta(minutes=1), max_requests=1)
    assert result["complete_scan"] is False
    assert result["stop_reason"] == "request_limit"
