"""Capture a bounded, read-only AWS usage snapshot for one BYOC stack.

The output is raw usage evidence, not an invoice or a monthly price. Run while
the S3 bucket still exists; CloudWatch may deliver recent minutes later.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import boto3


LAMBDA_REPORT = re.compile(
    r"(?:^|\n)REPORT RequestId: [a-zA-Z0-9-]+\s+Duration: [0-9.]+ ms\s+"
    r"Billed Duration: ([0-9]+) ms\s+Memory Size: ([0-9]+) MB"
)


def _utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _metric(client, namespace, name, dimensions, start, end, unit):
    response = client.get_metric_statistics(
        Namespace=namespace, MetricName=name, Dimensions=dimensions,
        StartTime=start, EndTime=end, Period=60, Statistics=["Sum"])
    points = response.get("Datapoints", [])
    seen = set()
    total = 0
    for point in points:
        stamp = point["Timestamp"].astimezone(timezone.utc)
        if stamp in seen or not start <= stamp < end:
            raise ValueError(f"duplicate or out-of-window {namespace}/{name} datapoint")
        if point["Unit"] != unit:
            raise ValueError(f"unexpected unit for {namespace}/{name}")
        seen.add(stamp)
        total += point["Sum"]
    expected = int((end - start).total_seconds() // 60)
    return {"sum": total if points else None, "observed_minutes": len(points),
            "expected_minutes": expected, "missing_minutes": expected - len(points),
            "complete": len(points) == expected, "unit": unit}


def _s3_inventory(client, bucket, max_objects, *, max_requests=1000,
                  max_seconds=30):
    started_at = datetime.now(timezone.utc)
    if client.get_bucket_versioning(Bucket=bucket).get("Status") in {"Enabled", "Suspended"}:
        raise RuntimeError("versioned bucket inventory would hide billable versions")
    count = size = 0
    token = None
    seen_tokens = set()
    requests = 0
    deadline = time.monotonic() + max_seconds

    def result(complete, reason=None):
        return {"current_objects": count, "current_bytes": size,
                "list_requests": requests, "complete": complete,
                "stop_reason": reason,
                "bucket_sha256": hashlib.sha256(bucket.encode()).hexdigest(),
                "started_at_utc": started_at.isoformat(),
                "completed_at_utc": datetime.now(timezone.utc).isoformat()}

    while True:
        if requests >= max_requests:
            return result(False, "request_limit")
        if time.monotonic() >= deadline:
            return result(False, "deadline")
        arguments = {"Bucket": bucket, "MaxKeys": min(1000, max_objects - count)}
        if token is not None:
            arguments["ContinuationToken"] = token
        page = client.list_objects_v2(**arguments)
        requests += 1
        objects = page.get("Contents", [])
        count += len(objects)
        size += sum(item["Size"] for item in objects)
        if count > max_objects:
            raise RuntimeError("S3 returned more objects than MaxKeys")
        if not page.get("IsTruncated", False):
            return result(True)
        if count >= max_objects:
            return result(False, "object_limit")
        token = page.get("NextContinuationToken")
        if not token or token in seen_tokens:
            raise RuntimeError("truncated S3 listing lacked an advancing continuation token")
        seen_tokens.add(token)


def _lambda_billing(client, function, start, end, *, max_events=10000,
                    max_requests=100, max_seconds=30):
    """Sum Lambda REPORT billing units without retaining log messages or IDs."""
    reports = billed_ms = gb_seconds_numerator = 0
    requests = unparsed = 0
    token = None
    seen_tokens = set()
    seen_events = set()
    memory_sizes = set()
    deadline = time.monotonic() + max_seconds

    def result(complete, reason=None):
        return {
            "report_count": reports,
            "billed_duration_ms": billed_ms,
            "billed_mb_ms": gb_seconds_numerator,
            "billed_gb_seconds": gb_seconds_numerator / (1024 * 1000),
            "memory_mb": sorted(memory_sizes),
            "matching_events_unparsed": unparsed,
            "filter_requests": requests,
            "complete_scan": complete,
            "stop_reason": reason,
            "delivery_caveat": "CloudWatch Logs can deliver REPORT events after this scan",
        }

    while True:
        if len(seen_events) >= max_events:
            return result(False, "event_limit")
        if requests >= max_requests:
            return result(False, "request_limit")
        if time.monotonic() >= deadline:
            return result(False, "deadline")
        arguments = {
            "logGroupName": "/aws/lambda/" + function,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "filterPattern": "REPORT",
            "limit": min(10000, max_events - len(seen_events)),
        }
        if token is not None:
            arguments["nextToken"] = token
        page = client.filter_log_events(**arguments)
        requests += 1
        for event in page.get("events", []):
            event_id = event["eventId"]
            if event_id in seen_events:
                continue
            seen_events.add(event_id)
            if not arguments["startTime"] <= event["timestamp"] < arguments["endTime"]:
                raise ValueError("Lambda REPORT event is outside the requested window")
            match = LAMBDA_REPORT.search(event["message"])
            if match is None:
                unparsed += 1
                continue
            duration, memory = map(int, match.groups())
            if duration < 1 or memory < 1:
                raise ValueError("invalid Lambda REPORT billing units")
            reports += 1
            billed_ms += duration
            gb_seconds_numerator += duration * memory
            memory_sizes.add(memory)
        next_token = page.get("nextToken")
        if not next_token:
            return result(True)
        if next_token == token or next_token in seen_tokens:
            raise RuntimeError("Lambda log pagination token did not advance")
        seen_tokens.add(next_token)
        token = next_token


def _api_response_payload(client, function, start, end, *, max_events=10000,
                          max_requests=100, max_seconds=30):
    """Sum response payload lengths from API Gateway access logs."""
    bytes_out = parsed = unparsed = requests = 0
    token = None
    seen_tokens = set()
    seen_events = set()
    deadline = time.monotonic() + max_seconds

    def result(complete, reason=None):
        return {
            "logged_requests": len(seen_events),
            "parsed_response_lengths": parsed,
            "unparsed_response_lengths": unparsed,
            "response_payload_bytes": bytes_out,
            "filter_requests": requests,
            "complete_scan": complete,
            "stop_reason": reason,
            "bytes_scope": "API Gateway response payload length; excludes headers and is not billed internet egress",
            "delivery_caveat": "CloudWatch Logs can deliver access events after this scan",
        }

    while True:
        if len(seen_events) >= max_events:
            return result(False, "event_limit")
        if requests >= max_requests:
            return result(False, "request_limit")
        if time.monotonic() >= deadline:
            return result(False, "deadline")
        arguments = {
            "logGroupName": "/aws/apigateway/" + function,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "limit": min(10000, max_events - len(seen_events)),
        }
        if token is not None:
            arguments["nextToken"] = token
        page = client.filter_log_events(**arguments)
        requests += 1
        for event in page.get("events", []):
            event_id = event["eventId"]
            if event_id in seen_events:
                continue
            seen_events.add(event_id)
            if not arguments["startTime"] <= event["timestamp"] < arguments["endTime"]:
                raise ValueError("API access event is outside the requested window")
            try:
                payload = json.loads(event["message"])
            except json.JSONDecodeError:
                unparsed += 1
                continue
            value = payload.get("response_bytes") if isinstance(payload, dict) else None
            if type(value) is int and value >= 0:
                length = value
            elif isinstance(value, str) and value.isdecimal():
                length = int(value)
            else:
                unparsed += 1
                continue
            bytes_out += length
            parsed += 1
        next_token = page.get("nextToken")
        if not next_token:
            return result(True)
        if next_token == token or next_token in seen_tokens:
            raise RuntimeError("API log pagination token did not advance")
        seen_tokens.add(next_token)
        token = next_token


# @spec experiments/collect
def collect(session, *, bucket, table, api_url, lambda_function,
            start, end, max_objects=10000, max_list_requests=1000,
            max_list_seconds=30, max_report_events=10000,
            max_report_requests=100, max_report_seconds=30,
            max_api_events=10000, max_api_requests=100, max_api_seconds=30):
    """Collect meters with explicit scopes and freshness limitations."""
    if type(max_objects) is not int or not 1 <= max_objects <= 1_000_000:
        raise ValueError("max_objects must be 1..1000000")
    if type(max_list_requests) is not int or not 1 <= max_list_requests <= 10_000:
        raise ValueError("max_list_requests must be 1..10000")
    if (not isinstance(max_list_seconds, (int, float))
            or isinstance(max_list_seconds, bool)
            or not 0 < max_list_seconds <= 300):
        raise ValueError("max_list_seconds must be greater than 0 and at most 300")
    if type(max_report_events) is not int or not 1 <= max_report_events <= 1_000_000:
        raise ValueError("max_report_events must be 1..1000000")
    if type(max_report_requests) is not int or not 1 <= max_report_requests <= 10_000:
        raise ValueError("max_report_requests must be 1..10000")
    if (not isinstance(max_report_seconds, (int, float))
            or isinstance(max_report_seconds, bool)
            or not 0 < max_report_seconds <= 300):
        raise ValueError("max_report_seconds must be greater than 0 and at most 300")
    if type(max_api_events) is not int or not 1 <= max_api_events <= 1_000_000:
        raise ValueError("max_api_events must be 1..1000000")
    if type(max_api_requests) is not int or not 1 <= max_api_requests <= 10_000:
        raise ValueError("max_api_requests must be 1..10000")
    if (not isinstance(max_api_seconds, (int, float))
            or isinstance(max_api_seconds, bool)
            or not 0 < max_api_seconds <= 300):
        raise ValueError("max_api_seconds must be greater than 0 and at most 300")
    if (start.tzinfo is None or end.tzinfo is None
            or start.second or start.microsecond or end.second or end.microsecond
            or not start < end or (end - start).total_seconds() > 86400):
        raise ValueError("use minute-aligned timestamps within a 24-hour window")
    hostname = urlsplit(api_url).hostname or ""
    api_id = hostname.split(".", 1)[0]
    if not api_id or not hostname.endswith(".execute-api." + session.region_name
                                     + ".amazonaws.com"):
        raise ValueError("api_url must be an API Gateway URL in the selected region")

    s3 = session.client("s3")
    dynamodb = session.client("dynamodb")
    cloudwatch = session.client("cloudwatch")
    logs = session.client("logs")
    description = dynamodb.describe_table(TableName=table)["Table"]
    backups = dynamodb.describe_continuous_backups(TableName=table)
    if description["TableStatus"] != "ACTIVE":
        raise RuntimeError("coordination table is not ACTIVE")
    indexes = description.get("GlobalSecondaryIndexes", [])
    table_dimensions = [{"Name": "TableName", "Value": table}]
    metrics = {}
    for label, namespace, name, dimensions, unit in (
        ("dynamodb_table_reads", "AWS/DynamoDB", "ConsumedReadCapacityUnits",
         table_dimensions, "Count"),
        ("dynamodb_table_writes", "AWS/DynamoDB", "ConsumedWriteCapacityUnits",
         table_dimensions, "Count"),
        # HTTP API Count is returned by CloudWatch with Unit="None".
        ("api_requests", "AWS/ApiGateway", "Count",
         [{"Name": "ApiId", "Value": api_id}], "None"),
        ("api_data_processed_bytes", "AWS/ApiGateway", "DataProcessed",
         [{"Name": "ApiId", "Value": api_id}], "Bytes"),
        ("lambda_invocations", "AWS/Lambda", "Invocations",
         [{"Name": "FunctionName", "Value": lambda_function}], "Count"),
        ("lambda_duration_ms", "AWS/Lambda", "Duration",
         [{"Name": "FunctionName", "Value": lambda_function}], "Milliseconds"),
        ("lambda_log_ingested_bytes", "AWS/Logs", "IncomingBytes",
         [{"Name": "LogGroupName", "Value": "/aws/lambda/" + lambda_function}],
         "Bytes"),
        ("api_log_ingested_bytes", "AWS/Logs", "IncomingBytes",
         [{"Name": "LogGroupName", "Value": "/aws/apigateway/" + lambda_function}],
         "Bytes"),
    ):
        metrics[label] = _metric(cloudwatch, namespace, name, dimensions, start, end,
                                 unit)
    for index in indexes:
        dimensions = table_dimensions + [{"Name": "GlobalSecondaryIndexName",
                                          "Value": index["IndexName"]}]
        for label, name in (("reads", "ConsumedReadCapacityUnits"),
                            ("writes", "ConsumedWriteCapacityUnits")):
            metrics[f"dynamodb_gsi_{index['IndexName']}_{label}"] = _metric(
                cloudwatch, "AWS/DynamoDB", name, dimensions, start, end, "Count")

    lambda_billing = _lambda_billing(
        logs, lambda_function, start, end, max_events=max_report_events,
        max_requests=max_report_requests, max_seconds=max_report_seconds)
    invocation_metric = metrics["lambda_invocations"]
    lambda_billing["cloudwatch_invocations"] = invocation_metric["sum"]
    lambda_billing["matches_cloudwatch_invocations"] = (
        lambda_billing["report_count"] == invocation_metric["sum"]
        if lambda_billing["complete_scan"] and invocation_metric["complete"]
        else None)
    api_response = _api_response_payload(
        logs, lambda_function, start, end, max_events=max_api_events,
        max_requests=max_api_requests, max_seconds=max_api_seconds)
    request_metric = metrics["api_requests"]
    api_response["cloudwatch_requests"] = request_metric["sum"]
    api_response["matches_cloudwatch_requests"] = (
        api_response["logged_requests"] == request_metric["sum"]
        if api_response["complete_scan"] and request_metric["complete"]
        else None)

    return {
        "window_utc": [start.isoformat(), end.isoformat()],
        "s3_inventory": _s3_inventory(
            s3, bucket, max_objects, max_requests=max_list_requests,
            max_seconds=max_list_seconds),
        "dynamodb_reported_size": {
            "table_bytes": description.get("TableSizeBytes"),
            "table_items": description.get("ItemCount"),
            "indexes": {index["IndexName"]: {
                "bytes": index.get("IndexSizeBytes"),
                "items": index.get("ItemCount")}
                        for index in indexes},
            "freshness": "DescribeTable size and item counts update about every six hours"},
        "dynamodb_pitr_status": backups["ContinuousBackupsDescription"].get(
            "PointInTimeRecoveryDescription", {}).get("PointInTimeRecoveryStatus"),
        "cloudwatch_metrics": metrics,
        "lambda_billing": lambda_billing,
        "api_response_payload": api_response,
        "limits": [
            "CloudWatch minutes without datapoints may be idle or delayed; sums may underrepresent short spikes",
            "S3 bytes are a point-in-time inventory, not GB-months or backups",
            "API DataProcessed combines traffic processed and is not outbound egress",
            "API response payload bytes exclude protocol overhead and are not billed internet egress",
            "Lambda Duration is measured execution time; REPORT billing units can be late or absent and are not an invoice",
            "DynamoDB table and GSI capacity are distinct metrics, not invoice line items",
        ],
    }


# @spec experiments/cloud-stack-cost-snapshot-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--lambda-function", required=True)
    parser.add_argument("--start", required=True, type=_utc)
    parser.add_argument("--end", required=True, type=_utc)
    parser.add_argument("--max-objects", type=int, default=10000)
    parser.add_argument("--max-list-requests", type=int, default=1000)
    parser.add_argument("--max-list-seconds", type=float, default=30)
    parser.add_argument("--max-report-events", type=int, default=10000)
    parser.add_argument("--max-report-requests", type=int, default=100)
    parser.add_argument("--max-report-seconds", type=float, default=30)
    parser.add_argument("--max-api-events", type=int, default=10000)
    parser.add_argument("--max-api-requests", type=int, default=100)
    parser.add_argument("--max-api-seconds", type=float, default=30)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    report = collect(session, bucket=args.bucket, table=args.table,
                     api_url=args.api_url, lambda_function=args.lambda_function,
                     start=args.start, end=args.end, max_objects=args.max_objects,
                     max_list_requests=args.max_list_requests,
                     max_list_seconds=args.max_list_seconds,
                     max_report_events=args.max_report_events,
                     max_report_requests=args.max_report_requests,
                     max_report_seconds=args.max_report_seconds,
                     max_api_events=args.max_api_events,
                     max_api_requests=args.max_api_requests,
                     max_api_seconds=args.max_api_seconds)
    with open(args.output, "w", encoding="utf-8") as destination:
        json.dump(report, destination, indent=2, sort_keys=True)
        destination.write("\n")


if __name__ == "__main__":
    main()
