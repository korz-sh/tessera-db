# SPDX-License-Identifier: Apache-2.0
"""Atomic, fixed-minute admission for the deployed tenant SQL API."""

from __future__ import annotations

import re
import time

from botocore.exceptions import ClientError

_TENANT_ID = re.compile(r"[0-9a-f]{32}\Z")
_PREFIX = "_tessera_cloud/rate/"


# @spec cloud/tenant-rate-limited
class TenantRateLimited(RuntimeError):
    """The tenant's current fixed-minute budget is exhausted."""

    def __init__(self, retry_after_seconds):
        super().__init__("tenant request budget exceeded")
        self.retry_after_seconds = retry_after_seconds


# @spec cloud/dynamo-dbtenant-rate-meter
class DynamoDBTenantRateMeter:
    """One conditional UpdateItem per admitted request; no read on the hot path."""

    def __init__(self, client, table_name):
        self.client = client
        self.table_name = table_name

    def admit(self, tenant_id, body_bytes, limits, *, now=None):
        if not isinstance(tenant_id, str) or not _TENANT_ID.fullmatch(tenant_id):
            raise ValueError("invalid tenant ID")
        if type(body_bytes) is not int or body_bytes < 0:
            raise ValueError("body_bytes must be a nonnegative integer")
        instant = time.time() if now is None else now
        minute = int(instant // 60)
        retry_after = max(1, (minute + 1) * 60 - int(instant))
        max_bytes = limits["max_body_bytes_per_minute"]
        if body_bytes > max_bytes:
            raise TenantRateLimited(retry_after)
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key={"tenant_id": {"S": f"{_PREFIX}{tenant_id}/{minute}"}},
                UpdateExpression=("SET #ttl = if_not_exists(#ttl, :ttl) "
                                  "ADD #requests :one, #body_bytes :body_bytes"),
                ConditionExpression=("attribute_not_exists(#requests) OR "
                                     "(#requests < :request_limit AND "
                                     "#body_bytes <= :remaining_bytes)"),
                ExpressionAttributeNames={
                    "#ttl": "quota_expires_at", "#requests": "requests",
                    "#body_bytes": "body_bytes"},
                ExpressionAttributeValues={
                    ":ttl": {"N": str((minute + 1) * 60 + 3_600)},
                    ":one": {"N": "1"},
                    ":body_bytes": {"N": str(body_bytes)},
                    ":request_limit": {"N": str(limits["max_requests_per_minute"])},
                    ":remaining_bytes": {"N": str(max_bytes - body_bytes)},
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise TenantRateLimited(retry_after) from None
            raise
