# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""S3 + DynamoDB object-storage backend for tessera-db.

Objects live in S3 (one key per blob; the manifest is the only mutable object
and is written with CAS conditions).  Per-tenant leases live in a DynamoDB
table keyed by ``tenant_id``; DynamoDB's conditional UpdateItem provides the
atomic test-and-set needed for crash-safe fencing.

Fencing epochs: every successful :meth:`S3DynamoBackend.acquire_lease` bumps
the per-tenant ``epoch`` counter by exactly one
(``if_not_exists(epoch, 0) + 1``), so epochs are strictly monotonically
increasing per tenant.

Note: :meth:`S3DynamoBackend.release_lease` does NOT delete the item — a
``DeleteItem`` would erase the epoch counter and break fencing-token
monotonicity across release/acquire cycles.  Instead it clears ``owner`` and
zeroes ``expires_at`` while preserving ``epoch``.  A released lease is
logically gone: ``acquire_lease`` treats ``expires_at = 0`` as free (it is
always in the past) and ``renew_lease`` cannot succeed without a live owner.
"""

from __future__ import annotations

import random
import time
import uuid
from decimal import Decimal
from typing import Any

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError as exc:  # pragma: no cover - exercised only without boto3
    raise ImportError(
        "S3DynamoBackend requires the 'aws' extra: "
        "pip install 'tessera-db[aws]'"
    ) from exc

from tessera.backends.base import (
    Backend,
    CASFailed,
    Lease,
    LeaseLost,
    NotFound,
    TenantBusy,
)

__all__ = ["S3DynamoBackend"]

# Shared client config: keep TCP connections alive across calls (a warm
# Lambda sandbox reuses the same clients for every request; without
# keep-alive each burst can pay a fresh TLS handshake) and cap botocore's
# own retries low — the backend has its own backoff loop.
_BOTO_CONFIG = Config(
    tcp_keepalive=True,
    retries={"max_attempts": 2, "mode": "standard"},
    max_pool_connections=32,
)


def _strip_quotes(etag: str | None) -> str:
    """Remove the surrounding double quotes S3 wraps etags in."""
    if etag and len(etag) >= 2 and etag.startswith('"') and etag.endswith('"'):
        return etag[1:-1]
    return etag or ""


def _error_code(exc: ClientError) -> str:
    """Extract the AWS error code (e.g. ``NoSuchKey``) from a ClientError."""
    error = (exc.response or {}).get("Error") or {}
    return str(error.get("Code", "") or "")


def _error_status(exc: ClientError) -> int:
    """Extract the HTTP status code from a ClientError (0 when unknown)."""
    response = exc.response or {}
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status is None:
        status = response.get("status")
    try:
        return int(status or 0)
    except (TypeError, ValueError):
        return 0


def _is_missing_object(exc: ClientError) -> bool:
    """True when the error means "object/key not found" (S3 GET/HEAD/PUT)."""
    return _error_status(exc) == 404 or _error_code(exc) in {
        "NoSuchKey",
        "NotFound",
        "404",
    }


# @spec backends/s3-dynamo-backend
class S3DynamoBackend(Backend):
    """Blobs in S3, per-tenant leases in a DynamoDB table.

    Lease item schema (PK ``tenant_id`` string): ``owner`` string,
    ``epoch`` number, ``expires_at`` number (unix seconds), plus the
    optional ``endpoint`` string (absent when acquired without one).
    """

    def __init__(
        self,
        bucket: str,
        table_name: str,
        region: str = "us-east-1",
        s3_client: Any = None,
        dynamodb: Any = None,
        backoff_base_ms: int = 50,
        backoff_factor: float = 2,
        backoff_max_attempts: int = 5,
        express_bucket: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.table_name = table_name
        self.region = region
        # Optional S3 Express One Zone directory bucket for the write-once
        # deltas (<tenant>/wal/...). Express PUTs are single-digit ms vs
        # 20-60 ms on standard, and the delta PUT sits on the hot write path.
        # Only deltas route there: they are short-lived (folded into
        # snapshots at compaction, then GC'd), so one-AZ durability is an
        # acceptable, bounded window. The manifest (CAS via If-Match),
        # snapshots and chunks stay on standard S3 — Express does not support
        # If-Match on PUT, and everything long-lived should be multi-AZ.
        self.express_bucket = express_bucket or None
        self._s3 = (
            s3_client
            if s3_client is not None
            else boto3.client("s3", region_name=region, config=_BOTO_CONFIG)
        )
        self._dynamodb = (
            dynamodb
            if dynamodb is not None
            else boto3.resource("dynamodb", region_name=region, config=_BOTO_CONFIG)
        )
        self._table = self._dynamodb.Table(table_name)
        self._backoff_base_ms = backoff_base_ms
        self._backoff_factor = backoff_factor
        self._backoff_max_attempts = backoff_max_attempts

    def _bucket_for(self, key: str) -> str:
        """Route write-once deltas to the Express bucket when configured."""
        if self.express_bucket is not None and "/wal/" in key:
            return self.express_bucket
        return self.bucket

    # ------------------------------------------------------------------
    # Object operations (S3)
    # ------------------------------------------------------------------

    def get(self, key: str) -> tuple[bytes, str]:
        try:
            response = self._s3.get_object(Bucket=self._bucket_for(key), Key=key)
        except ClientError as exc:
            if _is_missing_object(exc):
                raise NotFound(
                    f"key {key!r} not found in bucket {self.bucket!r}"
                ) from exc
            raise
        return response["Body"].read(), _strip_quotes(response["ETag"])

    def head(self, key: str) -> str | None:
        try:
            response = self._s3.head_object(Bucket=self._bucket_for(key), Key=key)
        except ClientError as exc:
            if _is_missing_object(exc):
                return None
            raise
        return _strip_quotes(response["ETag"])

    def put(
        self,
        key: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> str:
        kwargs: dict[str, Any] = {
            "Bucket": self._bucket_for(key),
            "Key": key,
            "Body": data,
        }
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        try:
            response = self._s3.put_object(**kwargs)
        except ClientError as exc:
            if _error_status(exc) == 412 or _error_code(exc) in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                raise CASFailed(
                    f"conditional put failed for key {key!r}"
                ) from exc
            # A conditional put against a missing object never matches
            # (Backend contract) — S3 answers NoSuchKey/404.
            if (if_match is not None or if_none_match) and _is_missing_object(exc):
                raise CASFailed(
                    f"conditional put failed for key {key!r}: object is missing"
                ) from exc
            raise
        return _strip_quotes(response["ETag"])

    def delete(self, key: str) -> None:
        # S3 delete_object is a no-op for missing keys: idempotent by design.
        self._s3.delete_object(Bucket=self._bucket_for(key), Key=key)

    # S3 cannot atomically exclude lease takeover during a delete; the
    # fencing bound comes from the mark phase instead (see the gc_delete
    # docstring) — skew-bounded deletes on a complete mark.
    supports_fenced_gc = True

    # Matches tessera.lease._SKEW_S: every GC delete must be issued at
    # least this many seconds before the lease could be taken over, so it
    # always lands before the earliest possible successor publication.
    _GC_SKEW_S = 1.0

    def _check_gc_lease(self, key: str, lease: Lease) -> None:
        if not key.startswith(f"{lease.tenant}/"):
            raise ValueError("GC key must belong to the leased tenant")
        if time.time() >= lease.expires_at - self._GC_SKEW_S:
            raise LeaseLost(
                f"GC lease for {lease.tenant!r} is expired or inside the "
                f"skew margin; refusing to delete under a takeover-able lease"
            )

    def gc_delete(self, key: str, lease: Lease) -> None:
        """Delete under the S3 GC fencing contract.

        S3 offers no "delete only if I still hold the lease" conditional,
        so correctness cannot come from atomic exclusion. It comes from
        the mark phase: Store.gc computes candidates from the complete
        closure of committed manifests (current + every archived history)
        while holding the lease, and manifests only evolve forward through
        CAS — a successor can reference objects in that closure or objects
        it writes itself under fresh keys (content-addressed re-PUTs
        recreate, they never reference a deleted object). The expiry check
        replicates ensure_alive's skew margin so every delete is issued
        before the earliest possible takeover.

        Documented residual: a delete issued just inside the margin whose
        delivery exceeds it could land after a successor re-created the
        same content-addressed key — that needs >1s post-check latency,
        the same order of assumption the lease skew itself makes.
        """
        self._check_gc_lease(key, lease)
        self.delete(key)

    def gc_delete_batch(self, keys: list[str], lease: Lease) -> None:
        """Bulk delete under the same contract; delete_objects per bucket."""
        if not keys:
            return
        for key in keys:
            self._check_gc_lease(key, lease)
        standard = [k for k in keys if self._bucket_for(k) == self.bucket]
        express = [k for k in keys if self._bucket_for(k) != self.bucket]
        for i in range(0, len(standard), 1000):
            self.delete_batch(standard[i : i + 1000])
        for key in express:
            self.delete(key)

    def delete_batch(self, keys: list[str]) -> None:
        """Idempotently delete one bounded batch from the standard bucket."""
        if not 1 <= len(keys) <= 1000:
            raise ValueError("batch must contain 1..1000 keys")
        if any(self._bucket_for(key) != self.bucket for key in keys):
            raise ValueError("batch cannot span buckets")
        result = self._s3.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
        )
        if result.get("Errors"):
            raise RuntimeError("S3 batch deletion was incomplete")

    def require_unversioned_bucket(self) -> None:
        """Refuse physical purge when hidden object versions could remain."""
        versioning = self._s3.get_bucket_versioning(Bucket=self.bucket)
        if versioning.get("Status") is not None:
            raise RuntimeError("physical purge requires an unversioned S3 bucket")

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        # Deltas live in the Express bucket when configured; GC and
        # delete_tenant discover objects via list, so both buckets answer.
        if self.express_bucket is not None:
            for page in paginator.paginate(
                Bucket=self.express_bucket, Prefix=prefix
            ):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def list_page(self, prefix: str, limit: int = 1000, *,
                  start_after: str | None = None) -> list[tuple[str, int]]:
        """Return one bounded first page from the standard bucket.

        Callers deleting keys must restart at the first page after each batch;
        a continuation token over a changing key set can skip objects.
        """
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        if self.express_bucket is not None and "/wal/" in prefix:
            raise ValueError("list_page does not support Express WAL prefixes")
        arguments = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": limit}
        if start_after is not None:
            arguments["StartAfter"] = start_after
        seen_tokens = set()
        for _ in range(10):
            response = self._s3.list_objects_v2(**arguments)
            contents = response.get("Contents", [])
            if contents or not response.get("IsTruncated", False):
                return [(item["Key"], item["Size"]) for item in contents]
            # An empty truncated response is not proof that the prefix is
            # empty. Continue by token until S3 returns keys or a true end.
            token = response.get("NextContinuationToken")
            if not token or token in seen_tokens:
                raise RuntimeError("S3 listing stalled on an empty truncated page")
            seen_tokens.add(token)
            arguments.pop("StartAfter", None)
            arguments["ContinuationToken"] = token
        raise RuntimeError("S3 listing exceeded the empty-page request budget")

    def mtime(self, key: str) -> float | None:
        """Object ``Last-Modified`` as unix seconds, or ``None`` if missing."""
        try:
            response = self._s3.head_object(Bucket=self._bucket_for(key), Key=key)
        except ClientError as exc:
            if _is_missing_object(exc):
                return None
            raise
        last_modified = response.get("LastModified")
        if last_modified is None:
            return None
        try:
            return float(last_modified.timestamp())  # boto3: tz-aware datetime
        except (AttributeError, OSError, OverflowError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Lease operations (DynamoDB)
    # ------------------------------------------------------------------

    # How long the lease item lives after the last acquire. The TTL sweeper
    # should only collect TRULY abandoned tenants, not one that went 48 h
    # without writing: the item weighs ~100 bytes, so retaining it for a year
    # is cost noise (500 tenants ~= 50 KB ~= $0.0000125/mo) next to losing
    # the epoch. See `_ttl_at`.
    LEASE_ITEM_TTL_S = 365 * 24 * 3600

    def _ttl_at(self, now: float) -> int:
        """Instant (epoch in SECONDS) at which the sweeper may delete the item.

        DynamoDB requires whole seconds: a float with decimals would be
        truncated anyway, but we round explicitly so as not to depend on it.
        """
        return int(now) + self.LEASE_ITEM_TTL_S


    def acquire_lease(
        self,
        tenant: str,
        ttl_ms: int,
        endpoint: str = "",
        session_token: str = "",
        min_epoch: int = 0,
    ) -> Lease:
        """Acquire the lease; the resulting epoch always exceeds *min_epoch*.

        ``min_epoch`` is a FLOOR, not a value: the caller passes the
        ``last_epoch`` it read from the manifest and the issued epoch lands
        above it even if the lease item no longer exists. It is the net that
        covers the item swept by the TTL sweeper (see ``_ttl_at``).
        """
        last_error: ClientError | None = None
        names = {
            "#epoch": "epoch",
            "#owner": "owner",
            "#expires": "expires_at",
            "#endpoint": "endpoint",
            "#token": "session_token",
            "#ttl": "ttl_at",
        }
        # Endpoint and token are stored ONLY when provided: empty values
        # REMOVE whatever a previous lease left instead of writing "". Both
        # travel in the SAME conditional UpdateItem of the acquire, so a
        # client that sees the endpoint also sees its token: there is never
        # a window where the owner is reachable but its credential is not
        # yet published.
        # `#epoch`: take the MAXIMUM between the stored epoch and the
        # caller's floor, and only then add 1. With the item present the
        # floor changes nothing (the stored value always wins); with the
        # item swept by TTL it is what keeps the epoch from returning to 1.
        #
        # `#ttl_at`: a TTL attribute SEPARATE from `expires_at`.
        # `expires_at` lapses in seconds and after a release stays at 0, so
        # using it as the TTL would make EVERY item eligible; `ttl_at` is
        # set far ahead (see `_ttl_at`) so the sweeper only collects truly
        # abandoned tenants — and even then the floor above covers that case.
        set_expr = (
            "SET #epoch = if_not_exists(#epoch, :zero) + :inc, "
            "#owner = :owner, #expires = :exp, #ttl = :ttl"
        )
        sets, removes = [], []
        (sets if endpoint else removes).append(("#endpoint", ":ep"))
        (sets if session_token else removes).append(("#token", ":tok"))
        update_expr = set_expr
        for name, placeholder in sets:
            update_expr += f", {name} = {placeholder}"
        if removes:
            update_expr += " REMOVE " + ", ".join(name for name, _ in removes)
        for attempt in range(self._backoff_max_attempts):
            now = time.time()
            values: dict[str, Any] = {
                # The floor travels as :zero, the if_not_exists default:
                # with the item absent it starts at min_epoch instead of 0.
                ":zero": Decimal(max(0, int(min_epoch))),
                ":inc": 1,
                ":ttl": Decimal(str(self._ttl_at(now))),
                ":owner": uuid.uuid4().hex,
                # boto3's TypeSerializer rejects floats: use Decimal.
                ":exp": Decimal(str(now + ttl_ms / 1000)),
                ":now": Decimal(str(now)),
            }
            if endpoint:
                values[":ep"] = endpoint
            if session_token:
                values[":tok"] = session_token
            try:
                response = self._table.update_item(
                    Key={"tenant_id": tenant},
                    UpdateExpression=update_expr,
                    ConditionExpression=(
                        "attribute_not_exists(tenant_id) OR #expires < :now"
                    ),
                    ExpressionAttributeNames=names,
                    ExpressionAttributeValues=values,
                    ReturnValues="ALL_NEW",
                )
            except ClientError as exc:
                if _error_code(exc) != "ConditionalCheckFailedException":
                    raise
                last_error = exc
                if attempt + 1 < self._backoff_max_attempts:
                    # Full-jitter exponential backoff.
                    delay_ms = self._backoff_base_ms * (
                        self._backoff_factor**attempt
                    )
                    time.sleep(random.uniform(0, delay_ms / 1000))
                continue
            attrs = response["Attributes"]
            return Lease(
                tenant=tenant,
                owner=str(attrs["owner"]),
                epoch=int(attrs["epoch"]),
                expires_at=float(attrs["expires_at"]),
                endpoint=str(attrs.get("endpoint", "")),
                session_token=str(attrs.get("session_token", "")),
            )
        raise TenantBusy(
            f"lease for tenant {tenant!r} is busy after "
            f"{self._backoff_max_attempts} attempts"
        ) from last_error

    def renew_lease(self, lease: Lease, ttl_ms: int) -> Lease:
        now = time.time()
        try:
            response = self._table.update_item(
                Key={"tenant_id": lease.tenant},
                UpdateExpression="SET #expires = :exp",
                ConditionExpression=(
                    "attribute_exists(tenant_id) AND #owner = :o "
                    "AND #epoch = :e AND #expires > :now"
                ),
                ExpressionAttributeNames={
                    "#owner": "owner",
                    "#epoch": "epoch",
                    "#expires": "expires_at",
                },
                ExpressionAttributeValues={
                    ":exp": Decimal(str(now + ttl_ms / 1000)),
                    ":o": lease.owner,
                    ":e": lease.epoch,
                    ":now": Decimal(str(now)),
                },
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if _error_code(exc) == "ConditionalCheckFailedException":
                raise LeaseLost(
                    f"lease for tenant {lease.tenant!r} no longer owned or expired"
                ) from exc
            raise
        attrs = response["Attributes"]
        return Lease(
            tenant=lease.tenant,
            owner=str(attrs["owner"]),
            epoch=int(attrs["epoch"]),
            expires_at=float(attrs["expires_at"]),
            endpoint=str(attrs.get("endpoint", "")),
            session_token=str(attrs.get("session_token", "")),
        )

    def release_lease(self, lease: Lease) -> None:
        # ONE single call. There used to be a prior get_item(ConsistentRead)
        # whose only purpose was to distinguish "already released / item
        # absent" (idempotent no-op) from "stolen by another owner"
        # (LeaseLost). That distinction now comes from the SAME UpdateItem
        # with ReturnValuesOnConditionCheckFailure=ALL_OLD: when the
        # condition fails, DynamoDB returns the item in the exception and
        # the decision is made from that. Besides saving the call, it
        # removes a TOCTOU: the state evaluated is the one the condition
        # saw, not an earlier read's.
        try:
            self._table.update_item(
                Key={"tenant_id": lease.tenant},
                # Preserve `epoch` (fencing tokens must never go backwards);
                # expires_at = 0 marks the lease as free for future acquires.
                # `endpoint` and `session_token` are REMOVED: the owner is no
                # longer serving, and leaving its credential published would
                # turn it into an orphan secret nobody will rotate. The token
                # dies with the lease — that is why there is no revocation to
                # implement.
                UpdateExpression=(
                    "SET #expires = :zero REMOVE #owner, #endpoint, #token"
                ),
                ConditionExpression="#owner = :o AND #epoch = :e",
                ExpressionAttributeNames={
                    "#owner": "owner",
                    "#epoch": "epoch",
                    "#expires": "expires_at",
                    "#endpoint": "endpoint",
                    "#token": "session_token",
                },
                ExpressionAttributeValues={
                    ":zero": 0,
                    ":o": lease.owner,
                    ":e": lease.epoch,
                },
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        except ClientError as exc:
            if _error_code(exc) != "ConditionalCheckFailedException":
                raise
            # The condition failed: the item it evaluated comes in the
            # exception. No item, or no `owner`, means the lease was already
            # released (or never existed) and the release is an idempotent
            # no-op. With a different `owner`, someone else took it: that IS
            # a LeaseLost.
            previous = exc.response.get("Item") or {}
            if not previous or "owner" not in previous:
                return
            raise LeaseLost(
                f"lease for tenant {lease.tenant!r} no longer owned"
            ) from exc
