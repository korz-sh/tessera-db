"""API Gateway HTTP API v2 adapter for the experimental SQL service."""

from __future__ import annotations

import base64
import json
import logging
import os
import time

from .service import (
    DatabaseService,
    dispatch_request,
    error_response,
    parse_json_body,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_cached_service = None


def _get_service():
    global _cached_service
    if _cached_service is not None:
        return _cached_service

    bucket = os.environ["TESSERA_BUCKET"]
    table = os.environ["TESSERA_TABLE"]
    region = os.environ["AWS_REGION"]
    import boto3

    from tessera.backends.s3dynamo import S3DynamoBackend
    from tessera.experimental.turso.fencing import FencedBackend
    from tessera.experimental.turso.lifecycle import RetentionPolicy

    from .tenant_rate import DynamoDBTenantRateMeter

    session = boto3.Session(region_name=region)
    if os.environ.get("TESSERA_AWS_CALL_METRICS") == "1":
        from .cost_telemetry import register_session

        register_session(session)
    secret_arn = os.environ.get("TESSERA_ADMIN_SECRET_ARN")
    if secret_arn:
        admin_token = session.client("secretsmanager").get_secret_value(
            SecretId=secret_arn)["SecretString"]
    else:
        admin_token = os.environ["TESSERA_ADMIN_TOKEN"]
    dynamodb = session.client("dynamodb")
    raw = S3DynamoBackend(bucket, table, region,
                          s3_client=session.client("s3"),
                          dynamodb=session.resource("dynamodb"))
    keep_last = int(os.environ.get("TESSERA_RETENTION_KEEP_LAST", "8"))
    retain_seconds = int(os.environ.get("TESSERA_RETENTION_SECONDS", "604800"))
    grace_seconds = int(os.environ.get("TESSERA_GC_GRACE_SECONDS", "3600"))
    if keep_last < 1 or retain_seconds < 0 or grace_seconds < 60:
        raise ValueError("unsafe retention policy")
    retention_policy = RetentionPolicy(keep_last=keep_last,
                                        retain_seconds=retain_seconds,
                                        artifact_grace_seconds=grace_seconds)
    _cached_service = DatabaseService(FencedBackend(raw, dynamodb),
                                      admin_token=admin_token, catalog_backend=raw,
                                      retention_policy=retention_policy,
                                      read_cache_bytes=16_777_216,
                                      rate_meter=DynamoDBTenantRateMeter(dynamodb, table),
                                      native_gc_enabled=(
                                          os.environ.get("TESSERA_NATIVE_GC_ENABLED") == "1"),
                                      retention_metadata_limit=int(os.environ.get(
                                          "TESSERA_RETENTION_METADATA_LIMIT", "999")),
                                      allow_legacy_root_create=(
                                          os.environ.get("TESSERA_ALLOW_LEGACY_ROOT_CREATE")
                                          == "1"))
    return _cached_service


def _dispatch(event):
    if event.get("tessera_maintenance") == "deletion_repair":
        result = _get_service().catalog.sweep_deletions()
        logger.info("TesseraDB deletion repair: %s", result)
        return result
    if event.get("tessera_maintenance") == "retention":
        result = _get_service().catalog.sweep_retention()
        logger.info("TesseraDB retention sweep: %s", result)
        return result
    if event.get("source") == "aws.events" and event.get("detail-type") == "Scheduled Event":
        result = _get_service().catalog.sweep_pending_deletions()
        logger.info("TesseraDB pending deletions: %s", result)
        return result
    try:
        method = event["requestContext"]["http"]["method"]
        path = event["rawPath"]
        if event.get("rawQueryString"):
            path += "?" + event["rawQueryString"]
        headers = {str(key).lower(): value for key, value in event.get("headers", {}).items()}
        authorization = headers.get("authorization", "")
        token = authorization[7:] if authorization.startswith("Bearer ") else None

        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(body, validate=True)
        else:
            raw = body.encode("utf-8")

        def read_body():
            return parse_json_body(raw)

        status, payload = dispatch_request(_get_service(), method, path, token, read_body,
                                           request_body_bytes=len(raw))
    except Exception as exc:
        status, payload = error_response(exc)
        if status >= 500:
            logger.exception("TesseraDB Lambda request failed")
    headers = {"content-type": "application/json", "cache-control": "no-store"}
    if status == 429:
        headers["retry-after"] = str(payload["retry_after_seconds"])
    return {"statusCode": status,
            "headers": headers,
            "body": json.dumps(payload, separators=(",", ":"), allow_nan=False),
            "isBase64Encoded": False}


def _operation_class(event):
    if event.get("tessera_maintenance") == "deletion_repair":
        return "deletion_repair"
    if event.get("tessera_maintenance") == "retention":
        return "retention"
    if event.get("source") == "aws.events":
        return "scheduled_deletion"
    parts = event.get("rawPath", "").strip("/").split("/")
    if parts[:2] == ["v1", "tenants"]:
        return "tenants"
    if parts[:2] != ["v1", "databases"]:
        return "other"
    if len(parts) <= 3:
        return "databases"
    return parts[3] if parts[3] in {"transactions", "query", "tokens", "operations",
                                    "usage", "history", "restore", "deletion"} else "other"


# @spec cloud/lambda-handler
def lambda_handler(event, context):
    """Translate one API Gateway v2 event; optionally log SDK call counts."""
    if os.environ.get("TESSERA_AWS_CALL_METRICS") != "1":
        return _dispatch(event)
    from .cost_telemetry import begin_request, end_request

    counts, token = begin_request()
    started = time.monotonic()
    response = None
    try:
        response = _dispatch(event)
        return response
    finally:
        end_request(token)
        logger.info("TesseraDB AWS call sample: %s", json.dumps({
            "kind": "tessera_aws_call_sample_v1",
            "request_id": getattr(context, "aws_request_id", None),
            "operation": _operation_class(event),
            "status": response.get("statusCode") if isinstance(response, dict) else None,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "logical_sdk_calls": dict(sorted(counts.logical.items())),
            "sdk_http_attempts": dict(sorted(counts.http_attempts.items())),
        }, separators=(",", ":"), sort_keys=True))
