"""Price scheduled Lambda idle work from redacted CloudWatch log events.

Input is the JSON result of ``aws logs filter-log-events`` for one isolated
Lambda log group. Only the opt-in AWS call samples and Lambda REPORT lines are
used; SQL, tokens, database IDs and other log messages are not emitted.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import Counter
from decimal import Decimal
from pathlib import Path


REPORT = re.compile(
    r"^REPORT RequestId: ([a-zA-Z0-9-]+)\s+Duration: [0-9.]+ ms\s+"
    r"Billed Duration: ([0-9]+) ms\s+Memory Size: ([0-9]+) MB"
)
MARKER = "TesseraDB AWS call sample: "
MONTHLY = {"scheduled_deletion": 30 * 24 * 60,
           "deletion_repair": 30 * 24,
           "retention": 30 * 24 * 12}
LAMBDA_REQUEST_USD = Decimal("0.20") / 1_000_000
LAMBDA_GB_SECOND_USD = Decimal("0.0000166667")


# @spec experiments/summarize
def summarize(events):
    reports = {}
    samples = {}
    for event in events:
        message = event["message"]
        report = REPORT.search(message)
        if report:
            request_id, billed, memory = report.groups()
            if request_id in reports:
                raise ValueError(f"duplicate Lambda REPORT for {request_id}")
            reports[request_id] = (int(billed), int(memory))
        if MARKER in message:
            sample = json.loads(message.split(MARKER, 1)[1])
            if sample.get("kind") != "tessera_aws_call_sample_v1":
                raise ValueError("unexpected AWS call sample format")
            request_id = sample["request_id"]
            if request_id in samples:
                raise ValueError(f"duplicate AWS call sample for {request_id}")
            samples[request_id] = sample

    grouped = {operation: [] for operation in MONTHLY}
    for request_id, sample in samples.items():
        operation = sample["operation"]
        if operation not in grouped:
            continue
        if request_id not in reports:
            raise ValueError(f"missing Lambda REPORT for {request_id}")
        billed, memory = reports[request_id]
        if billed < 1 or memory < 1:
            raise ValueError("invalid billed duration or memory")
        grouped[operation].append({
            "request_id": request_id,
            "billed_ms": billed,
            "memory_mb": memory,
            "handler_ms": sample["elapsed_ms"],
            "logical_sdk_calls": sample["logical_sdk_calls"],
            "sdk_http_attempts": sample.get("sdk_http_attempts"),
            "startup_secret_fetch": bool(sample["logical_sdk_calls"].get(
                "secretsmanager.GetSecretValue", 0)),
        })

    result = {}
    for operation, observations in grouped.items():
        if not observations:
            if operation == "deletion_repair":
                # Historical samples predate the hourly repair rule.
                continue
            raise ValueError(f"no {operation} samples")
        memories = {item["memory_mb"] for item in observations}
        if len(memories) != 1:
            raise ValueError(f"mixed Lambda memory sizes for {operation}")
        billed = [item["billed_ms"] for item in observations]
        warm_billed = [item["billed_ms"] for item in observations
                       if not item["startup_secret_fetch"]]
        calls = Counter()
        attempts = Counter()
        for item in observations:
            calls.update(item["logical_sdk_calls"])
            if item["sdk_http_attempts"] is not None:
                attempts.update(item["sdk_http_attempts"])
        per_invocation_ms = Decimal(sum(billed)) / len(billed)
        monthly_invocations = MONTHLY[operation]
        monthly_billed_seconds = per_invocation_ms * monthly_invocations / 1_000
        memory_gb = Decimal(next(iter(memories))) / 1_024
        request_usd = LAMBDA_REQUEST_USD * monthly_invocations
        duration_usd = LAMBDA_GB_SECOND_USD * monthly_billed_seconds * memory_gb
        warm_duration_usd = None
        if warm_billed:
            warm_ms = Decimal(sum(warm_billed)) / len(warm_billed)
            warm_duration_usd = (LAMBDA_GB_SECOND_USD * warm_ms
                                 * monthly_invocations / 1_000 * memory_gb)
        result[operation] = {
            "observed_invocations": len(observations),
            "billed_ms_samples": billed,
            "billed_ms_mean": str(per_invocation_ms),
            "billed_ms_median": statistics.median(billed),
            "startup_secret_fetch_samples": len(billed) - len(warm_billed),
            "warm_billed_ms_samples": warm_billed,
            "memory_mb": next(iter(memories)),
            "logical_sdk_calls": dict(sorted(calls.items())),
            "sdk_http_attempts": dict(sorted(attempts.items())),
            "sdk_http_attempts_observed": all(
                item["sdk_http_attempts"] is not None for item in observations),
            "monthly_invocations_at_configured_schedule": monthly_invocations,
            "illustrative_monthly_lambda_requests_usd": str(request_usd),
            "illustrative_monthly_lambda_duration_usd": str(duration_usd),
            "illustrative_warm_only_monthly_lambda_duration_usd": (
                None if warm_duration_usd is None else str(warm_duration_usd)),
            "samples": observations,
        }
    return {
        "scope": "empty-stack scheduled Lambda invocations only",
        "prices": {"lambda_request_per_million_usd": "0.20",
                   "lambda_gb_second_usd": "0.0000166667"},
        "projection_caveat": "observed small-sample mean and warm-only scenario repeated for 30 days; cold-start frequency is unknown; excludes AWS free tier, SDK retries, DynamoDB capacity, S3, logs, secret, storage and operator time",
        "operations": result,
    }


# @spec experiments/cloud-idle-meter-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text())
    print(json.dumps(summarize(payload["events"]), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
