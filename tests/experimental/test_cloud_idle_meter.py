"""Scheduled idle estimate must use billed Lambda time and matching request IDs."""

import json
from decimal import Decimal

import pytest

from experiments.cloud_idle_meter import summarize


def _events(request_id, operation, billed_ms, calls, attempts=None):
    sample = {"kind": "tessera_aws_call_sample_v1", "request_id": request_id,
              "operation": operation, "elapsed_ms": billed_ms - 2,
              "logical_sdk_calls": calls}
    if attempts is not None:
        sample["sdk_http_attempts"] = attempts
    return [
        {"message": "[INFO] TesseraDB AWS call sample: " + json.dumps(sample)},
        {"message": f"REPORT RequestId: {request_id}\tDuration: {billed_ms - 1}.0 ms\t"
                    f"Billed Duration: {billed_ms} ms\tMemory Size: 512 MB\t"},
    ]


def test_idle_projection_uses_matched_billed_reports():
    events = (_events("a", "scheduled_deletion", 100, {"dynamodb.Scan": 1})
              + _events("b", "retention", 200, {"dynamodb.Scan": 1}))
    result = summarize(events)["operations"]
    assert result["scheduled_deletion"]["observed_invocations"] == 1
    assert result["scheduled_deletion"]["logical_sdk_calls"] == {"dynamodb.Scan": 1}
    assert Decimal(result["scheduled_deletion"]["illustrative_monthly_lambda_requests_usd"]) == Decimal("0.00864")
    assert Decimal(result["scheduled_deletion"]["illustrative_monthly_lambda_duration_usd"]) == Decimal("0.036000072")
    assert Decimal(result["retention"]["illustrative_monthly_lambda_duration_usd"]) == Decimal("0.0144000288")
    assert result["scheduled_deletion"]["startup_secret_fetch_samples"] == 0
    assert "deletion_repair" not in result
    assert result["scheduled_deletion"]["sdk_http_attempts_observed"] is False


def test_idle_projection_includes_hourly_repair_when_observed():
    events = (_events("a", "scheduled_deletion", 100, {})
              + _events("b", "retention", 200, {})
              + _events("c", "deletion_repair", 50, {"s3.ListObjectsV2": 1}))
    repair = summarize(events)["operations"]["deletion_repair"]
    assert repair["monthly_invocations_at_configured_schedule"] == 720
    assert repair["logical_sdk_calls"] == {"s3.ListObjectsV2": 1}


def test_idle_projection_retains_reported_sdk_attempts():
    events = (_events("a", "scheduled_deletion", 100, {"s3.ListObjectsV2": 1},
                      {"s3.ListObjectsV2": 3})
              + _events("b", "retention", 200, {}, {}))
    deletion = summarize(events)["operations"]["scheduled_deletion"]
    assert deletion["sdk_http_attempts_observed"] is True
    assert deletion["sdk_http_attempts"] == {"s3.ListObjectsV2": 3}


def test_idle_projection_rejects_unmatched_or_duplicate_reports():
    events = (_events("a", "scheduled_deletion", 100, {})
              + _events("b", "retention", 200, {}))
    with pytest.raises(ValueError, match="missing Lambda REPORT"):
        summarize(events[:-1])
    with pytest.raises(ValueError, match="duplicate Lambda REPORT"):
        summarize(events + events[-1:])


def test_idle_projection_separates_startup_from_warm_invocations():
    events = (_events("a", "scheduled_deletion", 3500,
                      {"secretsmanager.GetSecretValue": 1})
              + _events("b", "scheduled_deletion", 80, {})
              + _events("c", "retention", 20, {}))
    deletion = summarize(events)["operations"]["scheduled_deletion"]
    assert deletion["startup_secret_fetch_samples"] == 1
    assert deletion["warm_billed_ms_samples"] == [80]
    assert Decimal(deletion["illustrative_warm_only_monthly_lambda_duration_usd"]) < Decimal(
        deletion["illustrative_monthly_lambda_duration_usd"])
