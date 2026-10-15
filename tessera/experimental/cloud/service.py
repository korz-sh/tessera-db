"""Small durable control plane and HTTP transport for isolated v2 groups.

This is a single-region alpha. The catalog is stored through the same Backend
CAS interface as the data plane; tokens are only stored as SHA-256 digests.
"""

from __future__ import annotations

import hmac
import json
import logging
import math
import re
import secrets
import threading
import time
from dataclasses import dataclass
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import uuid4

from tessera.backends.base import CASFailed, NotFound, TenantBusy
from tessera.experimental.turso.api import (
    BulkOperation,
    DatabaseQuotaExceeded,
    OperationOutcomeUnknown,
    check_sql,
)
from tessera.experimental.turso.lifecycle import (
    MetadataLimitExceeded,
    RetentionPolicy,
    RetentionSession,
)
from tessera.experimental.turso.native import NativeLogStore
from tessera.experimental.turso.owner import BatchOwner
from tessera.experimental.turso.reads import PublishedReader

from .tenant_rate import TenantRateLimited
from .tenants import TenantCatalog, TenantConflict, TenantQuotaExceeded

_DB_ID = re.compile(r"^[0-9a-f]{32}$")
_TOKEN_ID = re.compile(r"^[0-9a-f]{16}$")
_CATALOG_PREFIX = "_tessera_cloud/catalog/"
_SWEEP_KEY = "_tessera_cloud/deletion-sweep"
_DELETION_QUEUE_PREFIX = "_tessera_cloud/deletion/"
_DELETION_INDEX = "pending-deletions"
_DELETION_SHARDS = 4
_RETENTION_SWEEP_KEY = "_tessera_cloud/retention-sweep"
_RETENTION_CURSOR_PREFIX = "_tessera_cloud/retention/"
_NATIVE_GC_CLEANUP_PREFIX = "_tessera_cloud/native-gc-cleanup/"
MAX_HTTP_BODY_BYTES = 8_388_608
MAX_OPERATION_BYTES = 8_388_608
DEFAULT_LIMITS = {"max_frontier_bytes": 67_108_864,
                  "max_operation_bytes": 65_536, "max_tokens": 64}
logger = logging.getLogger(__name__)


# @spec cloud/catalog-conflict
class CatalogConflict(RuntimeError):
    pass


# @spec cloud/database-unavailable
class DatabaseUnavailable(RuntimeError):
    pass


# @spec cloud/sql-execution-failed
class SqlExecutionFailed(RuntimeError):
    """The SQL operation failed before acquiring a durable receipt."""


# @spec cloud/query-limit-exceeded
class QueryLimitExceeded(RuntimeError):
    pass


# @spec cloud/catalog-quota-exceeded
class CatalogQuotaExceeded(RuntimeError):
    pass


# @spec cloud/restore-outcome-unknown
class RestoreOutcomeUnknown(OperationOutcomeUnknown):
    """A restore copy may have published; retry the same request ID."""

    def __init__(self, database_id):
        super().__init__("restore copy outcome is unknown")
        self.database_id = database_id


def _document(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _group(database_id):
    if not _DB_ID.fullmatch(database_id):
        raise ValueError("invalid database ID")
    return "cloud-" + database_id


def _limits(value):
    if value is None:
        return dict(DEFAULT_LIMITS)
    if not isinstance(value, dict) or set(value) != set(DEFAULT_LIMITS):
        raise ValueError("limits require max_frontier_bytes, max_operation_bytes and max_tokens")
    bounds = {"max_frontier_bytes": (65_536, 1_073_741_824),
              "max_operation_bytes": (1_024, MAX_OPERATION_BYTES), "max_tokens": (1, 64)}
    for name, (minimum, maximum) in bounds.items():
        if type(value[name]) is not int or not minimum <= value[name] <= maximum:
            raise ValueError(f"{name} must be an integer from {minimum} to {maximum}")
    return dict(value)


@dataclass(frozen=True)
# @spec cloud/token
class Token:
    token_id: str
    secret: str
    scope: str


# @spec cloud/database-catalog
class DatabaseCatalog:
    """CAS-backed database registry; only active records accept traffic."""

    def __init__(self, backend, *, data_backend=None, retention_policy=None,
                 tenant_catalog=None, native_gc_enabled=False,
                 retention_metadata_limit=999):
        self.backend = backend
        self.data_backend = backend if data_backend is None else data_backend
        self.retention_policy = (RetentionPolicy() if retention_policy is None
                                 else retention_policy)
        self.tenants = tenant_catalog
        if not isinstance(self.retention_policy, RetentionPolicy):
            raise TypeError("retention_policy must be RetentionPolicy")
        if type(native_gc_enabled) is not bool:
            raise TypeError("native_gc_enabled must be a boolean")
        if (type(retention_metadata_limit) is not int
                or not 1 <= retention_metadata_limit <= 999):
            raise ValueError("retention_metadata_limit must be 1..999")
        if retention_metadata_limit < 999 and not native_gc_enabled:
            raise ValueError("lower retention metadata limit requires native GC")
        if native_gc_enabled:
            from tessera.experimental.turso.fencing import FencedBackend

            if not isinstance(self.data_backend, FencedBackend):
                raise TypeError("native_gc_enabled requires the fenced AWS backend")
        self.native_gc_enabled = native_gc_enabled
        self.retention_metadata_limit = retention_metadata_limit

    @staticmethod
    def _key(database_id):
        _group(database_id)
        return _CATALOG_PREFIX + database_id + ".json"

    def _load(self, database_id):
        try:
            body, etag = self.backend.get(self._key(database_id))
        except NotFound as exc:
            raise NotFound("database not found") from exc
        return json.loads(body), etag

    def get(self, database_id):
        record, _ = self._load(database_id)
        if record["state"] != "active":
            raise DatabaseUnavailable("database is not active")
        result = {"id": record["id"], "name": record["name"], "state": record["state"],
                  "limits": _limits(record.get("limits"))}
        if "tenant_id" in record:
            result["tenant_id"] = record["tenant_id"]
        if "restored_from" in record:
            result["restored_from"] = record["restored_from"]
        return result

    def list(self):
        records = []
        for key in self.backend.list(_CATALOG_PREFIX):
            if not key.endswith(".json"):
                continue
            database_id = key[len(_CATALOG_PREFIX):-5]
            if _DB_ID.fullmatch(database_id):
                try:
                    records.append(self.get(database_id))
                except (NotFound, DatabaseUnavailable):
                    pass
        return sorted(records, key=lambda record: record["id"])

    def list_page(self, *, cursor=None, limit=100):
        """Return one bounded catalog page; inactive entries still advance the cursor."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        if cursor is not None and (not isinstance(cursor, str)
                                   or not cursor.startswith(_CATALOG_PREFIX)
                                   or len(cursor) > 1024):
            raise ValueError("cursor must belong to the catalog prefix")
        paged = hasattr(self.backend, "list_page")
        if paged:
            keys = [key for key, _ in self.backend.list_page(
                _CATALOG_PREFIX, limit, start_after=cursor)]
        else:
            keys = [key for key in self.backend.list(_CATALOG_PREFIX)
                    if cursor is None or key > cursor][:limit + 1]
        selected = keys[:limit]
        records = []
        for key in selected:
            if not key.endswith(".json"):
                continue
            database_id = key[len(_CATALOG_PREFIX):-5]
            if _DB_ID.fullmatch(database_id):
                try:
                    records.append(self.get(database_id))
                except (NotFound, DatabaseUnavailable):
                    pass
        return {"databases": records,
                # S3 can return fewer than MaxKeys despite more objects. Only
                # an empty follow-up page proves the inventory is complete.
                "next_cursor": (selected[-1] if selected and
                                (paged or len(keys) > limit) else None)}

    def tenant_list_page(self, tenant_id, *, cursor=None, limit=100):
        """List only a tenant's reserved IDs; incomplete creates are hidden."""
        if self.tenants is None:
            raise RuntimeError("tenant admission is unavailable")
        ids, next_cursor = self.tenants.reserved_page(
            tenant_id, cursor=cursor, limit=limit)
        records = []
        for database_id in ids:
            try:
                record = self.get(database_id)
            except (NotFound, DatabaseUnavailable):
                continue
            if record.get("tenant_id") != tenant_id:
                raise CatalogConflict("tenant reservation ownership mismatch")
            records.append(record)
        return {"databases": records, "next_cursor": next_cursor}

    def create(self, name, schema=(), *, request_id=None, limits=None,
               tenant_id=None, legacy_root_ids=False):
        if legacy_root_ids and (tenant_id is None or request_id is None):
            raise ValueError("legacy root replay requires tenant and request_id")
        if not isinstance(name, str) or not 1 <= len(name) <= 128:
            raise ValueError("name must be 1..128 characters")
        if not isinstance(schema, list) or not all(isinstance(s, str) for s in schema):
            raise ValueError("schema must be a list of SQL strings")
        if len(schema) > 64 or len(_document(schema)) > 65536:
            raise ValueError("schema exceeds the creation limit")
        if request_id is not None and (not isinstance(request_id, str)
                                       or not 1 <= len(request_id) <= 128):
            raise ValueError("request_id must be 1..128 characters")
        parsed_limits = _limits(limits)
        if tenant_id is not None:
            if self.tenants is None:
                raise RuntimeError("tenant admission is unavailable")
            self.tenants.validate_database_limits(tenant_id, parsed_limits)
        fingerprint_input = {"name": name, "schema": schema}
        if tenant_id is not None and not legacy_root_ids:
            fingerprint_input["tenant_id"] = tenant_id
        if limits is not None:
            fingerprint_input["limits"] = parsed_limits
        fingerprint = sha256(_document(fingerprint_input)).hexdigest()
        if request_id is None:
            database_id = uuid4().hex
        elif tenant_id is None or legacy_root_ids:
            database_id = sha256(("tessera-create-v1:" + request_id).encode()).hexdigest()[:32]
        else:
            database_id = sha256(("tessera-create-tenant-v1:" + tenant_id + ":" +
                                  request_id).encode()).hexdigest()[:32]
        record = {"id": database_id, "name": name, "state": "creating", "tokens": {},
                  "create_fingerprint": fingerprint, "limits": parsed_limits}
        if tenant_id is not None:
            # A consumed deterministic ID must never reserve a fresh slot
            # after deletion or under a changed payload.
            try:
                existing, _ = self._load(database_id)
            except NotFound:
                self.tenants.reserve_database(tenant_id, database_id, fingerprint)
            else:
                if (existing.get("create_fingerprint") != fingerprint
                        or existing.get("tenant_id") != tenant_id):
                    raise CatalogConflict("request_id was used with different database content")
                if existing["state"] not in ("creating", "active"):
                    raise DatabaseUnavailable("database is not active")
                self.tenants.require_reservation(tenant_id, database_id, fingerprint)
            record["tenant_id"] = tenant_id
        try:
            etag = self.backend.put(self._key(database_id), _document(record),
                                    if_none_match=True)
        except CASFailed:
            if request_id is None:
                raise CatalogConflict("generated database ID collision") from None
            record, etag = self._load(database_id)
            if (record.get("create_fingerprint") != fingerprint
                    or record.get("tenant_id") != tenant_id):
                raise CatalogConflict("request_id was used with different database content")
            if record["state"] == "active":
                return self.get(database_id)
            if record["state"] != "creating":
                if record["state"] == "deleted" and tenant_id is not None:
                    self._release_tenant_slot_after_purge(database_id)
                raise DatabaseUnavailable("database is not active")
        # A retry with the same request_id can finish a reservation left by a
        # crash, or activate a manifest that was already published.
        with BatchOwner(self.data_backend, _group(database_id), schema=schema, native=True,
                        max_frontier_bytes=parsed_limits["max_frontier_bytes"]) as owner:
            if not owner.created and request_id is None:
                raise CatalogConflict("generated database group already exists")
        record["state"] = "active"
        try:
            self.backend.put(self._key(database_id), _document(record), if_match=etag)
        except CASFailed:
            current, _ = self._load(database_id)
            if (current.get("create_fingerprint") != fingerprint
                    or current.get("tenant_id") != tenant_id
                    or current["state"] != "active"):
                raise CatalogConflict("database activation conflicted") from None
        return self.get(database_id)

    def restore_copy(self, source_id, version, name, *, request_id, limits=None):
        """Publish a retained source frontier under a fresh database ID.

        The source and its receipts remain intact. A deterministic request ID
        lets a retry finish catalog activation after an uncertain response.
        """
        _group(source_id)
        if type(version) is not int or version < 0:
            raise ValueError("source version must be a nonnegative integer")
        if not isinstance(name, str) or not 1 <= len(name) <= 128:
            raise ValueError("name must be 1..128 characters")
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
            raise ValueError("request_id must be 1..128 characters")
        parsed_explicit = None if limits is None else _limits(limits)
        fingerprint_input = {"source_id": source_id, "version": version, "name": name}
        if parsed_explicit is not None:
            fingerprint_input["limits"] = parsed_explicit
        fingerprint = sha256(_document(fingerprint_input)).hexdigest()
        database_id = sha256(("tessera-restore-copy-v1:" + source_id + ":" + request_id
                              ).encode()).hexdigest()[:32]
        restored_from = {"database_id": source_id, "version": version}
        reserved_new = False
        try:
            record, etag = self._load(database_id)
        except NotFound:
            source = self.get(source_id)
            parsed = source["limits"] if parsed_explicit is None else parsed_explicit
            tenant_id = source.get("tenant_id")
            if tenant_id is not None:
                self.tenants.validate_database_limits(tenant_id, parsed)
                self.tenants.reserve_database(tenant_id, database_id, fingerprint)
                reserved_new = True
            record = {"id": database_id, "name": name, "state": "creating", "tokens": {},
                      "create_fingerprint": fingerprint, "limits": parsed,
                      "restored_from": restored_from}
            if tenant_id is not None:
                record["tenant_id"] = tenant_id
            try:
                etag = self.backend.put(self._key(database_id), _document(record),
                                        if_none_match=True)
            except CASFailed:
                record, etag = self._load(database_id)
        if record.get("create_fingerprint") != fingerprint:
            raise CatalogConflict("request_id was used with different restore content")
        if record["state"] not in ("creating", "active"):
            if record["state"] == "deleted" and reserved_new:
                self._release_tenant_slot_after_purge(database_id)
            raise DatabaseUnavailable("database is not available for restore retry")
        if "tenant_id" in record and not reserved_new:
            self.tenants.require_reservation(record["tenant_id"], database_id,
                                             fingerprint)
        if record["state"] == "active":
            return self.get(database_id)
        if record.get("restored_from") != restored_from:
            raise DatabaseUnavailable("database is not available for restore retry")

        target = NativeLogStore(self.data_backend, _group(database_id))
        try:
            target.manifest()
        except NotFound:
            # Do not let BatchOwner implicitly initialize a missing source.
            self.get(source_id)
            NativeLogStore(self.data_backend, _group(source_id)).manifest()
            with BatchOwner(self.data_backend, _group(source_id), native=True) as owner:
                self.get(source_id)
                try:
                    owner.lifecycle().clone(
                        version, _group(database_id), reset_receipts=True,
                        max_frontier_bytes=_limits(record["limits"])["max_frontier_bytes"])
                except OperationOutcomeUnknown as exc:
                    raise RestoreOutcomeUnknown(database_id) from exc
        reader = PublishedReader(target)
        try:
            with reader.open() as (_connection, manifest):
                if manifest["version"] != 0 or manifest["receipt_count"] != 0:
                    raise CatalogConflict("restored database has an unexpected frontier")
        finally:
            reader.close()
        record["state"] = "active"
        try:
            self.backend.put(self._key(database_id), _document(record), if_match=etag)
        except CASFailed:
            current, _ = self._load(database_id)
            if (current.get("create_fingerprint") != fingerprint
                    or current.get("tenant_id") != record.get("tenant_id")
                    or current["state"] != "active"):
                raise CatalogConflict("restore activation conflicted") from None
        return self.get(database_id)

    def history_page(self, database_id, *, cursor=None, limit=100):
        """List retained version numbers without opening historical SQLite images."""
        self.get(database_id)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        prefix = NativeLogStore(self.data_backend, _group(database_id)).prefix + "history/"
        if cursor is not None and (not isinstance(cursor, str)
                                   or not cursor.startswith(prefix)
                                   or len(cursor) > 1024):
            raise ValueError("cursor must belong to the exact history prefix")
        current = NativeLogStore(self.data_backend, _group(database_id)).manifest()[0]
        if hasattr(self.backend, "list_page"):
            page = self.backend.list_page(prefix, limit + 1, start_after=cursor)
            keys = [key for key, _ in page]
        else:
            keys = [key for key in self.backend.list(prefix)
                    if cursor is None or key > cursor][:limit + 1]
        selected = keys[:limit]
        versions = []
        for key in selected:
            suffix = key[len(prefix):]
            if not re.fullmatch(r"[0-9]{20}\.json", suffix):
                raise CatalogConflict("invalid retained history key")
            versions.append(int(suffix[:-5]))
        return {"id": database_id, "current_version": current["version"],
                "retained_versions": versions,
                # The S3 backend exposes objects but not IsTruncated. A short
                # nonempty page cannot prove completion; the next empty page can.
                "next_cursor": (selected[-1] if selected and
                                (hasattr(self.backend, "list_page") or len(keys) > limit)
                                else None)}

    def _change(self, database_id, edit):
        for _ in range(8):
            record, etag = self._load(database_id)
            if record["state"] != "active":
                raise DatabaseUnavailable("database is not active")
            result = edit(record)
            try:
                self.backend.put(self._key(database_id), _document(record), if_match=etag)
                return result
            except CASFailed:
                continue
        raise CatalogConflict("catalog update conflicted repeatedly")

    def update_limits(self, database_id, limits):
        """Serialize a full quota replacement with the database writer lease."""
        if limits is None:
            raise ValueError("limits object required")
        parsed = _limits(limits)
        current = self.get(database_id)
        if "tenant_id" in current:
            self.tenants.validate_database_limits(current["tenant_id"], parsed)

        def replace(record):
            record["limits"] = parsed

        with RetentionSession(self.data_backend, _group(database_id)):
            self._change(database_id, replace)
        return parsed

    def request_deletion(self, database_id):
        """Stop new API admission; physical retirement is a separate worker step."""
        for _ in range(8):
            record, etag = self._load(database_id)
            if record["state"] in ("deleting", "deleted"):
                if record["state"] == "deleting":
                    self._enqueue_deletion(database_id, record["delete_after"])
                return {"id": database_id, "state": record["state"],
                        "delete_after": record.get("delete_after")}
            if record["state"] != "active":
                raise DatabaseUnavailable("database is not active")
            # Publish intent first. A crash before the catalog CAS leaves an
            # active record, which the worker can safely transition itself.
            self._enqueue_deletion(database_id, time.time())
            record["state"] = "deleting"
            record["tokens"] = {}
            record["delete_after"] = time.time() + 60
            try:
                self.backend.put(self._key(database_id), _document(record), if_match=etag)
            except CASFailed:
                continue
            return {"id": database_id, "state": "deleting",
                    "delete_after": record["delete_after"]}
        raise CatalogConflict("catalog update conflicted repeatedly")

    def _deletion_queue(self):
        if not hasattr(self.data_backend, "client"):
            return None
        return self.data_backend.client, self.data_backend.table_name

    @staticmethod
    def _deletion_due(database_id, when):
        return f"{int(when * 1000):013d}#{database_id}"

    @staticmethod
    def _valid_deletion_queue_item(database_id, item):
        return (item is not None
                and item.get("tenant_id", {}).get("S") == _DELETION_QUEUE_PREFIX + database_id
                and item.get("deletion_shard", {}).get("S")
                == str(int(database_id[:2], 16) % _DELETION_SHARDS)
                and re.fullmatch(r"[0-9]{13}#" + database_id,
                                 item.get("deletion_due", {}).get("S", "")) is not None)

    def _enqueue_deletion(self, database_id, when):
        queue = self._deletion_queue()
        if queue is None:
            return
        from botocore.exceptions import ClientError

        client, table = queue
        try:
            client.put_item(
                TableName=table,
                Item={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id},
                      "deletion_shard": {"S": str(int(database_id[:2], 16) % _DELETION_SHARDS)},
                      "deletion_due": {"S": self._deletion_due(database_id, when)}},
                ConditionExpression="attribute_not_exists(tenant_id)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            item = client.get_item(
                TableName=table,
                Key={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id}},
                ConsistentRead=True).get("Item")
            if not self._valid_deletion_queue_item(database_id, item):
                raise CatalogConflict("invalid pending deletion queue item") from exc

    def sweep_pending_deletions(self, *, max_seconds=20, page_size=25):
        """Process due queue items; every failure remains retryable."""
        if not 0 < max_seconds <= 20 or not 1 <= page_size <= 100:
            raise ValueError("invalid sweep budget")
        queue = self._deletion_queue()
        if queue is None:
            raise RuntimeError("pending sweep requires DynamoDB")
        client, table = queue
        deadline = time.monotonic() + max_seconds
        now = time.time()
        attempted = failed = completed = sampled_due = 0
        queried_shards = 0
        truncated_shards = []
        oldest_due_ms = None
        # A busy first shard can consume the whole Lambda budget. Rotate the
        # starting shard so other queues still receive service under backlog.
        first_shard = int(now // 60) % _DELETION_SHARDS
        for offset in range(_DELETION_SHARDS):
            if time.monotonic() >= deadline:
                break
            shard = (first_shard + offset) % _DELETION_SHARDS
            response = client.query(
                TableName=table, IndexName=_DELETION_INDEX,
                KeyConditionExpression="deletion_shard = :shard AND deletion_due <= :due",
                ExpressionAttributeValues={":shard": {"S": str(shard)},
                                           ":due": {"S": f"{int(now * 1000):013d}~"}},
                Limit=page_size)
            queried_shards += 1
            sampled_due += len(response["Items"])
            if response.get("LastEvaluatedKey"):
                truncated_shards.append(shard)
            if response["Items"]:
                # The GSI sort key orders due time first. This is the oldest
                # visible due item in this shard, including entries that this
                # invocation may not have enough time to process.
                first_due = response["Items"][0].get("deletion_due", {}).get("S", "")
                if re.fullmatch(r"[0-9]{13}#.*", first_due):
                    first_due_ms = int(first_due[:13])
                    oldest_due_ms = (first_due_ms if oldest_due_ms is None
                                     else min(oldest_due_ms, first_due_ms))
            for item in response["Items"]:
                if time.monotonic() >= deadline:
                    break
                database_id = item["tenant_id"]["S"][len(_DELETION_QUEUE_PREFIX):]
                due = item["deletion_due"]["S"]
                attempted += 1
                try:
                    status = self.deletion_status(database_id)
                    if status["state"] == "active":
                        status = self.request_deletion(database_id)
                    if status["state"] == "deleting":
                        remaining = max(0.1, min(5.0, deadline - time.monotonic()))
                        status = self.purge_step(database_id, max_seconds=remaining)
                    if status["state"] == "deleted":
                        if self.tenants is not None:
                            self._release_tenant_slot_after_purge(database_id)
                        client.delete_item(
                            TableName=table,
                            Key={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id}},
                            ConditionExpression="deletion_due = :due",
                            ExpressionAttributeValues={":due": {"S": due}})
                        completed += 1
                    else:
                        retry_at = max(time.time() + 60, status["delete_after"])
                        client.update_item(
                            TableName=table,
                            Key={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id}},
                            UpdateExpression="SET deletion_due = :next",
                            ConditionExpression="deletion_due = :due",
                            ExpressionAttributeValues={":due": {"S": due},
                                                       ":next": {"S": self._deletion_due(
                                                           database_id, retry_at)}})
                except Exception as exc:  # noqa: BLE001 - retain item for retry
                    failed += 1
                    logger.warning("pending deletion failed for %s: %s", database_id,
                                   type(exc).__name__)
                    try:
                        client.update_item(
                            TableName=table,
                            Key={"tenant_id": {"S": _DELETION_QUEUE_PREFIX + database_id}},
                            UpdateExpression="SET deletion_due = :next",
                            ConditionExpression="deletion_due = :due",
                            ExpressionAttributeValues={":due": {"S": due},
                                                       ":next": {"S": self._deletion_due(
                                                           database_id, time.time() + 60)}})
                    except Exception:  # noqa: BLE001, S110 - original due remains retryable
                        pass
        return {"attempted": attempted, "completed": completed, "failed": failed,
                "queried_shards": queried_shards, "sampled_due": sampled_due,
                "truncated_shards": truncated_shards,
                "oldest_visible_due_age_s": (None if oldest_due_ms is None else
                                             round(max(0, now - oldest_due_ms / 1000), 3))}

    def deletion_status(self, database_id):
        """Expose deletion state to an administrator without leaking tokens."""
        record, _ = self._load(database_id)
        return {"id": database_id, "state": record["state"],
                "delete_after": record.get("delete_after")}

    def _tenant_slot_release_status(self, database_id, record):
        """Recheck the durable deletion proof before releasing capacity."""
        if record["state"] != "deleted":
            return "not_deleted"
        if not hasattr(self.data_backend, "pointer_retired"):
            return "pointer_unverifiable"
        prefix = f"turso-native/{_group(database_id)}/"
        if not self.data_backend.pointer_retired(prefix + "manifest.json"):
            return "deleted_unretired"
        if hasattr(self.backend, "require_unversioned_bucket"):
            self.backend.require_unversioned_bucket()
        if hasattr(self.backend, "list_page"):
            remaining = self.backend.list_page(prefix, 1)
        else:
            remaining = self.backend.list(prefix)[:1]
        return "deleted_nonempty" if remaining else "release_ready"

    def _release_tenant_slot_after_purge(self, database_id, *, expected_tenant_id=None,
                                         expected_fingerprint=None):
        record, _ = self._load(database_id)
        tenant_id = record.get("tenant_id")
        if expected_tenant_id is not None and tenant_id != expected_tenant_id:
            raise CatalogConflict("tenant ownership changed during slot reconciliation")
        if (expected_fingerprint is not None
                and record.get("create_fingerprint") != expected_fingerprint):
            raise CatalogConflict("database fingerprint changed during slot reconciliation")
        if tenant_id is None:
            return
        status = self._tenant_slot_release_status(database_id, record)
        if status != "release_ready":
            raise CatalogConflict(f"tenant slot cannot be released: {status}")
        self.tenants._release_database_after_purge(
            tenant_id, database_id, record.get("create_fingerprint"))

    def purge_step(self, database_id, *, max_objects=100, max_bytes=16_777_216,
                   max_seconds=15):
        """Retire the head and remove one bounded batch; safe to invoke again."""
        if not (1 <= max_objects <= 1000 and max_bytes > 0 and 0 < max_seconds <= 20):
            raise ValueError("invalid purge budget")
        status = self.deletion_status(database_id)
        if status["state"] == "deleted":
            self._delete_retention_side_cursors(database_id)
            self._release_tenant_slot_after_purge(database_id)
            return {**status, "purged_objects": 0, "purged_bytes": 0}
        if status["state"] != "deleting":
            raise DatabaseUnavailable("database is not deleting")
        if time.time() < status["delete_after"]:
            return {**status, "purged_objects": 0, "purged_bytes": 0}
        if not (hasattr(self.data_backend, "retire_pointer")
                and hasattr(self.data_backend, "pointer_retired")
                and hasattr(self.backend, "list_page")
                and hasattr(self.backend, "require_unversioned_bucket")):
            raise RuntimeError("purge requires the fenced S3 backend")

        self.backend.require_unversioned_bucket()

        group = _group(database_id)
        pointer = f"turso-native/{group}/manifest.json"
        prefix = f"turso-native/{group}/"
        deadline = time.monotonic() + max_seconds
        lease = self.data_backend.acquire_lease("turso-experiment-" + group, 25_000)
        removed, removed_bytes = 0, 0
        try:
            # The permanent marker must precede every S3 deletion. Recheck it
            # on retries instead of trusting a previous worker's reply.
            if not self.data_backend.pointer_retired(pointer):
                _, etag = self.data_backend.get(pointer)
                self.data_backend.retire_pointer(pointer, etag, lease)
            if not self.data_backend.pointer_retired(pointer):
                raise RuntimeError("pointer retirement was not confirmed")
            # Mark state is optional. A retired pointer prevents any new mark
            # transition; remove its DynamoDB side item before catalog deletion.
            self.data_backend.delete_gc_mark_state_after_retirement(group, lease)
            if time.monotonic() < deadline:
                page = self.backend.list_page(prefix, max_objects)
                if not page:
                    # A fresh empty list and a strong marker read establish
                    # physical completion; the catalog CAS makes it visible.
                    if not self.data_backend.pointer_retired(pointer):
                        raise RuntimeError("pointer retirement was lost")
                    self._delete_retention_side_cursors(database_id)
                    for _ in range(8):
                        record, etag = self._load(database_id)
                        if record["state"] == "deleted":
                            self._release_tenant_slot_after_purge(database_id)
                            return {"id": database_id, "state": "deleted",
                                    "delete_after": record.get("delete_after"),
                                    "purged_objects": removed, "purged_bytes": removed_bytes}
                        if record["state"] != "deleting":
                            raise DatabaseUnavailable("database is not deleting")
                        record["state"] = "deleted"
                        try:
                            self.backend.put(self._key(database_id), _document(record),
                                             if_match=etag)
                            self._release_tenant_slot_after_purge(database_id)
                            return {"id": database_id, "state": "deleted",
                                    "delete_after": record.get("delete_after"),
                                    "purged_objects": removed, "purged_bytes": removed_bytes}
                        except CASFailed:
                            continue
                    raise CatalogConflict("catalog update conflicted repeatedly")
                batch = []
                for key, size in page:
                    if time.monotonic() >= deadline or (batch and removed_bytes + size > max_bytes):
                        break
                    # Allow one oversized object so repeated calls progress.
                    batch.append(key)
                    removed_bytes += size
                    if removed_bytes >= max_bytes:
                        break
                if batch:
                    self.backend.delete_batch(batch)
                    removed = len(batch)
            return {**status, "purged_objects": removed, "purged_bytes": removed_bytes}
        finally:
            self.data_backend.release_lease(lease)

    def sweep_deletions(self, *, max_seconds=20, page_size=50):
        """Scan one bounded catalog page and advance a durable sweep cursor.

        Catalog keys are retained permanently. A full cycle therefore revisits
        every deletion even if DELETE crashes before any queue publication.
        """
        if not 0 < max_seconds <= 20 or not 1 <= page_size <= 100:
            raise ValueError("invalid sweep budget")
        if not (hasattr(self.backend, "list_page")
                and hasattr(self.data_backend, "client")):
            raise RuntimeError("sweep requires the fenced S3 backend")
        client = self.data_backend.client
        table = self.data_backend.table_name
        item = client.get_item(TableName=table, Key={"tenant_id": {"S": _SWEEP_KEY}},
                               ConsistentRead=True).get("Item", {})
        cursor = item.get("cursor", {}).get("S", "")
        page = self.backend.list_page(_CATALOG_PREFIX, page_size,
                                      start_after=cursor or None)
        if not page:
            if cursor:
                client.put_item(TableName=table, Item={"tenant_id": {"S": _SWEEP_KEY},
                                                       "cursor": {"S": ""}})
            return {"scanned": 0, "attempted": 0, "failed": 0, "wrapped": True}
        deadline = time.monotonic() + max_seconds
        scanned = attempted = failed = 0
        for key, _ in page:
            if time.monotonic() >= deadline:
                break
            scanned += 1
            cursor = key
            if key.endswith(".json"):
                database_id = key[len(_CATALOG_PREFIX):-5]
                if _DB_ID.fullmatch(database_id):
                    try:
                        status = self.deletion_status(database_id)
                        if status["state"] == "deleting":
                            self._enqueue_deletion(database_id, status["delete_after"])
                            if time.time() >= status["delete_after"]:
                                attempted += 1
                                remaining = max(0.1, min(5.0, deadline - time.monotonic()))
                                self.purge_step(database_id, max_objects=100,
                                                max_bytes=16_777_216, max_seconds=remaining)
                    except Exception as exc:  # noqa: BLE001 - retry on the next catalog sweep
                        failed += 1
                        logger.warning("deletion sweep failed for %s: %s", database_id,
                                       type(exc).__name__)
        if scanned:
            client.put_item(TableName=table, Item={"tenant_id": {"S": _SWEEP_KEY},
                                                   "cursor": {"S": cursor}})
        return {"scanned": scanned, "attempted": attempted,
                "failed": failed, "wrapped": False}

    def _retention_cursor(self, key):
        client = self.data_backend.client
        item = client.get_item(TableName=self.data_backend.table_name,
                               Key={"tenant_id": {"S": key}},
                               ConsistentRead=True).get("Item")
        return (item["cursor"]["S"], True) if item else ("", False)

    def _save_retention_cursor(self, key, previous, existed, cursor):
        arguments = {
            "TableName": self.data_backend.table_name,
            "Item": {"tenant_id": {"S": key}, "cursor": {"S": cursor}},
            "ConditionExpression": ("#cursor = :previous" if existed
                                    else "attribute_not_exists(tenant_id)"),
        }
        if existed:
            arguments["ExpressionAttributeNames"] = {"#cursor": "cursor"}
            arguments["ExpressionAttributeValues"] = {":previous": {"S": previous}}
        self.data_backend.client.put_item(**arguments)

    def _delete_retention_side_cursors(self, database_id):
        if not hasattr(self.data_backend, "client"):
            return
        for prefix in (_RETENTION_CURSOR_PREFIX, _NATIVE_GC_CLEANUP_PREFIX):
            self.data_backend.client.delete_item(
                TableName=self.data_backend.table_name,
                Key={"tenant_id": {"S": prefix + database_id}})

    def _native_gc_cleanup_step(self, lifecycle, database_id, *, max_seconds):
        """Resume inactive-cycle cleanup, with a daily pass for delayed objects."""
        from tessera.experimental.turso.gc_mark import _read_state

        state, _ = _read_state(self.data_backend, _group(database_id))
        if state is None or state["phase"] != "complete":
            return None
        key = _NATIVE_GC_CLEANUP_PREFIX + database_id
        previous, existed = self._retention_cursor(key)
        try:
            checkpoint = json.loads(previous) if existed else None
            if checkpoint is not None and (
                    not isinstance(checkpoint, dict)
                    or set(checkpoint) != {"cycle_id", "cursor", "next_due"}
                    or not isinstance(checkpoint["cycle_id"], str)
                    or (checkpoint["cursor"] is not None
                        and not isinstance(checkpoint["cursor"], str))
                    or type(checkpoint["next_due"]) not in (int, float)
                    or not math.isfinite(checkpoint["next_due"])
                    or checkpoint["next_due"] < 0):
                raise ValueError("invalid native GC cleanup checkpoint")
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid native GC cleanup checkpoint") from exc
        if checkpoint is None or checkpoint["cycle_id"] != state["cycle_id"]:
            checkpoint = {"cycle_id": state["cycle_id"], "cursor": None,
                          "next_due": 0}
        now = time.time()
        if now < checkpoint["next_due"]:
            return {"phase": "waiting", "deleted": 0, "bytes": 0}
        result = lifecycle.stale_gc_cleanup_step(
            cursor=checkpoint["cursor"], max_scan=100, max_objects=100,
            max_seconds=max_seconds)
        if result["phase"] == "complete":
            checkpoint = {"cycle_id": state["cycle_id"], "cursor": None,
                          "next_due": now + 86_400}
        else:
            checkpoint = {"cycle_id": state["cycle_id"],
                          "cursor": result["next_cursor"], "next_due": 0}
        encoded = json.dumps(checkpoint, sort_keys=True, separators=(",", ":"))
        if encoded != previous:
            self._save_retention_cursor(key, previous, existed, encoded)
        return result

    def retention_step(self, database_id, *, max_seconds=12):
        """Advance one active database's GC cursor under its fenced group lease."""
        if not 0 < max_seconds <= 20:
            raise ValueError("invalid retention budget")
        record, _ = self._load(database_id)
        if record["state"] != "active":
            raise DatabaseUnavailable("database is not active")
        self.backend.require_unversioned_bucket()
        key = _RETENTION_CURSOR_PREFIX + database_id
        cursor, existed = self._retention_cursor(key)
        started = time.monotonic()
        with RetentionSession(self.data_backend, _group(database_id),
                              policy=self.retention_policy) as lifecycle:
            active_cycle = False
            state = None
            if self.native_gc_enabled:
                from tessera.experimental.turso.gc_mark import _read_state

                state, _ = _read_state(self.data_backend, _group(database_id))
                active_cycle = state is not None and state["phase"] != "complete"
            if active_cycle:
                result = lifecycle.native_gc_cycle_step(max_seconds=max_seconds)
            else:
                try:
                    result = lifecycle.gc_step(cursor=cursor or None,
                                               max_seconds=max_seconds,
                                               max_metadata=self.retention_metadata_limit)
                except MetadataLimitExceeded:
                    remaining = max_seconds - (time.monotonic() - started)
                    if remaining <= 0:
                        result = {"phase": "deadline", "deleted": 0, "bytes": 0,
                                  "next_cursor": cursor or None}
                    else:
                        result = lifecycle.prune_overflow_history_step(
                            max_seconds=min(remaining, 20))
                        if (self.native_gc_enabled
                                and result["phase"] == "history_overflow"
                                and result["deleted"] == 0):
                            remaining = max_seconds - (time.monotonic() - started)
                            if remaining > 0:
                                result = lifecycle.native_gc_cycle_step(
                                    max_seconds=min(remaining, 20))
            remaining = max_seconds - (time.monotonic() - started)
            if (self.native_gc_enabled and remaining > 1
                    and (state is not None or result["phase"] == "native_gc")):
                cleanup = self._native_gc_cleanup_step(
                    lifecycle, database_id, max_seconds=min(remaining, 5))
                if cleanup is not None:
                    result = {**result, "cleanup_phase": cleanup["phase"],
                              "cleanup_deleted": cleanup["deleted"],
                              "cleanup_bytes": cleanup["bytes"]}
            new_cursor = result["next_cursor"] or ""
            if new_cursor != cursor:
                self._save_retention_cursor(key, cursor, existed, new_cursor)
        return result

    def sweep_retention(self, *, max_seconds=20, page_size=5):
        """Visit one catalog page and persist progress across Lambda restarts."""
        if not 0 < max_seconds <= 20 or not 1 <= page_size <= 20:
            raise ValueError("invalid retention sweep budget")
        if not (hasattr(self.backend, "list_page")
                and hasattr(self.data_backend, "client")):
            raise RuntimeError("retention sweep requires the fenced S3 backend")
        cursor, existed = self._retention_cursor(_RETENTION_SWEEP_KEY)
        previous_cursor = cursor
        page = self.backend.list_page(_CATALOG_PREFIX, page_size,
                                      start_after=cursor or None)
        if not page:
            if cursor:
                self._save_retention_cursor(_RETENTION_SWEEP_KEY, cursor, existed, "")
            return {"scanned": 0, "attempted": 0, "failed": 0,
                    "busy": 0, "deleted": 0, "bytes": 0,
                    "history_overflow": 0, "native_gc": 0,
                    "cleanup_deleted": 0, "cleanup_bytes": 0,
                    "wrapped": True}
        deadline = time.monotonic() + max_seconds
        scanned = attempted = failed = busy = deleted = removed_bytes = overflow = native_gc = 0
        cleanup_deleted = cleanup_bytes = 0
        for key, _ in page:
            if time.monotonic() >= deadline:
                break
            scanned += 1
            cursor = key
            if not key.endswith(".json"):
                continue
            database_id = key[len(_CATALOG_PREFIX):-5]
            if not _DB_ID.fullmatch(database_id):
                continue
            try:
                record, _ = self._load(database_id)
                if record["state"] != "active":
                    continue
                attempted += 1
                remaining = max(0.1, min(12.0, deadline - time.monotonic()))
                result = self.retention_step(database_id, max_seconds=remaining)
                deleted += result["deleted"]
                removed_bytes += result["bytes"]
                overflow += result.get("phase") == "history_overflow"
                native_gc += result.get("phase") == "native_gc"
                cleanup_deleted += result.get("cleanup_deleted", 0)
                cleanup_bytes += result.get("cleanup_bytes", 0)
            except TenantBusy:
                busy += 1
            except Exception as exc:  # noqa: BLE001 - revisit on the next cycle
                failed += 1
                logger.warning("retention sweep failed for %s: %s", database_id,
                               type(exc).__name__)
        if scanned:
            self._save_retention_cursor(_RETENTION_SWEEP_KEY, previous_cursor,
                                        existed, cursor)
        return {"scanned": scanned, "attempted": attempted, "failed": failed,
                "busy": busy, "deleted": deleted, "bytes": removed_bytes,
                "history_overflow": overflow,
                "native_gc": native_gc,
                "cleanup_deleted": cleanup_deleted,
                "cleanup_bytes": cleanup_bytes,
                "wrapped": False}

    def issue_token(self, database_id, scope="write"):
        if scope not in ("read", "write"):
            raise ValueError("scope must be read or write")
        token = Token(secrets.token_hex(8), secrets.token_urlsafe(32), scope)

        def add(record):
            if len(record["tokens"]) >= _limits(record.get("limits"))["max_tokens"]:
                raise CatalogQuotaExceeded("database token limit reached")
            if token.token_id in record["tokens"]:
                raise CatalogConflict("token ID collision")
            record["tokens"][token.token_id] = {
                "digest": sha256(token.secret.encode()).hexdigest(), "scope": scope
            }
            return token

        return self._change(database_id, add)

    def revoke_token(self, database_id, token_id):
        if not _TOKEN_ID.fullmatch(token_id):
            raise ValueError("invalid token ID")

        def remove(record):
            if token_id not in record["tokens"]:
                raise NotFound("token not found")
            del record["tokens"][token_id]

        self._change(database_id, remove)

    def authorized_record(self, database_id, secret, *, write=False):
        if not isinstance(secret, str):
            return None
        record, _ = self._load(database_id)
        if record["state"] != "active":
            return None
        digest = sha256(secret.encode()).hexdigest()
        if any(
            hmac.compare_digest(digest, item["digest"])
            and (not write or item["scope"] == "write")
            for item in record["tokens"].values()
        ):
            return record
        return None

    def authorize(self, database_id, secret, *, write=False):
        return self.authorized_record(database_id, secret, write=write) is not None


# @spec cloud/database-service
class DatabaseService:
    """Transport-independent operations; caller must authorize first."""

    def __init__(self, backend, *, admin_token, catalog_backend=None,
                 retention_policy=None, read_cache_bytes=0,
                 allow_legacy_root_create=True, rate_meter=None,
                 native_gc_enabled=False, retention_metadata_limit=999):
        if not isinstance(admin_token, str) or len(admin_token) < 32:
            raise ValueError("admin_token must have at least 32 characters")
        if any(character.isspace() for character in admin_token):
            raise ValueError("admin_token must not contain whitespace")
        if type(read_cache_bytes) is not int or not 0 <= read_cache_bytes <= 67_108_864:
            raise ValueError("read_cache_bytes must be between 0 and 64 MiB")
        if type(allow_legacy_root_create) is not bool:
            raise TypeError("allow_legacy_root_create must be a boolean")
        self.backend = backend
        # FencedBackend intentionally refuses mutable S3 objects. The catalog
        # uses the raw Backend CAS interface; v2 SQL only sees the fenced one.
        raw_catalog = backend if catalog_backend is None else catalog_backend
        self.tenants = TenantCatalog(raw_catalog, rate_meter=rate_meter)
        self.catalog = DatabaseCatalog(raw_catalog,
                                       data_backend=backend,
                                       retention_policy=retention_policy,
                                       tenant_catalog=self.tenants,
                                       native_gc_enabled=native_gc_enabled,
                                       retention_metadata_limit=retention_metadata_limit)
        self._admin_digest = sha256(admin_token.encode()).hexdigest()
        self.allow_legacy_root_create = allow_legacy_root_create
        self._read_cache_bytes = read_cache_bytes
        self._read_lock = threading.RLock()
        self._cached_reader_database = None
        self._cached_reader = None

    def close(self):
        with self._read_lock:
            if self._cached_reader is not None:
                self._cached_reader.close()
                self._cached_reader = None
                self._cached_reader_database = None

    def discard_read_cache(self, database_id):
        with self._read_lock:
            if self._cached_reader_database == database_id:
                self.close()

    def is_admin(self, token):
        return isinstance(token, str) and hmac.compare_digest(
            sha256(token.encode()).hexdigest(), self._admin_digest
        )

    def write(self, database_id, operation_id, statements):
        self.catalog.get(database_id)
        if not isinstance(statements, (list, tuple)) or not 1 <= len(statements) <= 8192:
            raise ValueError("transaction needs 1..8192 statements")
        operation = BulkOperation(operation_id, statements)
        if len(operation.payload) > MAX_OPERATION_BYTES:
            raise DatabaseQuotaExceeded("managed operation exceeds 8 MiB")
        with BatchOwner(self.backend, _group(database_id), native=True,
                        max_result_bytes=1_048_576, max_sql_seconds=15) as owner:
            limits = self.catalog.get(database_id)["limits"]
            owner.configure_limits(max_frontier_bytes=limits["max_frontier_bytes"],
                                   max_operation_bytes=limits["max_operation_bytes"])
            outcome = owner.commit([operation])[0]
        if isinstance(outcome, DatabaseQuotaExceeded):
            raise outcome
        if isinstance(outcome, Exception):
            raise SqlExecutionFailed("SQL transaction failed") from outcome
        return {"operation_id": outcome.operation_id, "fingerprint": outcome.fingerprint,
                "version": outcome.version, "rows": outcome.rows}

    def read(self, database_id, sql, params=(), *, min_version=None):
        self.catalog.get(database_id)
        check_sql(sql, read=True)
        def query(reader):
            rows = []
            encoded_bytes = 2  # JSON list delimiters
            with reader.open(min_version=min_version) as (connection, _):
                cursor = connection.execute(sql, params)
                while (row := cursor.fetchone()) is not None:
                    try:
                        encoded_bytes += len(_document(tuple(row))) + 1
                    except (TypeError, ValueError) as exc:
                        raise ValueError("query returned a non-JSON value") from exc
                    if encoded_bytes > 1_048_576 or len(rows) >= 10_000:
                        raise QueryLimitExceeded("query result limit exceeded")
                    rows.append(tuple(row))
            return rows

        if self._read_cache_bytes == 0:
            reader = PublishedReader(NativeLogStore(self.backend, _group(database_id)))
            try:
                return query(reader)
            finally:
                reader.close()
        # One Lambda execution environment handles one invocation at a time.
        # The lock also protects local threaded callers from closing a reader
        # while its isolated pinned read is still open.
        with self._read_lock:
            if self._cached_reader_database != database_id:
                self.close()
                self._cached_reader = PublishedReader(
                    NativeLogStore(self.backend, _group(database_id)),
                    max_cache_bytes=self._read_cache_bytes)
                self._cached_reader_database = database_id
            return query(self._cached_reader)

    def lookup(self, database_id, operation_id):
        self.catalog.get(database_id)
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 128:
            raise ValueError("invalid operation ID")
        reader = PublishedReader(NativeLogStore(self.backend, _group(database_id)))
        try:
            receipt = reader.lookup(operation_id)
        finally:
            reader.close()
        if receipt is None:
            return {"operation_id": operation_id, "state": "absent"}
        return {"operation_id": operation_id, "state": "published",
                "fingerprint": receipt.fingerprint, "version": receipt.version,
                "rows": receipt.rows}

    def usage(self, database_id, *, cursor=None, limit=100, include_logical=False):
        """One bounded S3 inventory page plus an authoritative published frontier."""
        if type(limit) is not int or not 1 <= limit <= 999:
            raise ValueError("limit must be 1..999")
        if type(include_logical) is not bool:
            raise ValueError("include_logical must be a boolean")
        raw = self.catalog.backend
        if not hasattr(raw, "list_page"):
            raise RuntimeError("usage inventory requires the S3 backend")
        state = self.catalog.deletion_status(database_id)["state"]
        prefix = f"turso-native/{_group(database_id)}/"
        if cursor is not None and (not isinstance(cursor, str)
                                   or not cursor.startswith(prefix)
                                   or len(cursor) > 1024):
            raise ValueError("cursor must belong to the exact database group")
        if cursor is not None and include_logical:
            raise ValueError("logical size is available on the first page only")

        published = None
        logical_bytes = None
        if state == "active" and cursor is None:
            storage = NativeLogStore(self.backend, _group(database_id))
            if include_logical:
                reader = PublishedReader(storage)
                try:
                    with reader.open() as (connection, manifest):
                        pages = connection.execute("PRAGMA page_count").fetchone()[0]
                        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
                        logical_bytes = pages * page_size
                finally:
                    reader.close()
            else:
                manifest, _ = storage.manifest()
            published = {
                "version": manifest["version"],
                "artifact_bytes": manifest["checkpoint"]["bytes"] + manifest["log_bytes"],
                "logical_database_bytes": logical_bytes,
            }

        page = raw.list_page(prefix, limit + 1, start_after=cursor)
        selected = page[:limit]
        return {
            "id": database_id, "state": state, "published": published,
            "s3_objects_page": len(selected),
            "s3_retained_bytes_page": sum(size for _, size in selected),
            "next_cursor": selected[-1][0] if selected else None,
            "sampled_at": time.time(),
        }


# @spec cloud/parse-json-body
def parse_json_body(raw):
    if not 1 <= len(raw) <= MAX_HTTP_BODY_BYTES:
        raise ValueError(f"request body must be 1..{MAX_HTTP_BODY_BYTES} bytes")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("JSON object required")
    return value


# @spec cloud/dispatch-request
def dispatch_request(service, method, path, token, read_body, *, request_body_bytes=0):
    """Shared lazy route dispatcher for HTTP servers and API Gateway v2."""
    if type(request_body_bytes) is not int or not 0 <= request_body_bytes <= MAX_HTTP_BODY_BYTES:
        raise ValueError("invalid request body length")
    parsed = urlsplit(path)
    parts = [unquote(part) for part in parsed.path.strip("/").split("/")]
    if parts[:2] == ["v1", "tenants"]:
        if not service.is_admin(token):
            return 401, {"error": "unauthorized"}
        if len(parts) == 2 and method == "GET":
            query = parse_qs(parsed.query, keep_blank_values=True)
            if any(len(values) != 1 for values in query.values()) or set(query) - {"cursor", "limit"}:
                raise ValueError("invalid tenant list query parameters")
            return 200, service.tenants.list_page(
                cursor=query.get("cursor", [None])[0],
                limit=int(query.get("limit", ["100"])[0]))
        if len(parts) == 2 and method == "POST":
            body = read_body()
            if body.get("request_id") is None:
                raise ValueError("tenant creation requires request_id")
            return 201, service.tenants.create(body.get("name"),
                                               request_id=body.get("request_id"),
                                               limits=body.get("limits"),
                                               rate_limits=body.get("rate_limits"))
        if len(parts) >= 3 and _DB_ID.fullmatch(parts[2]):
            tenant_id = parts[2]
            if len(parts) == 3 and method == "GET":
                return 200, service.tenants.get(tenant_id)
            if len(parts) == 4 and parts[3] == "rate-limits" and method == "PUT":
                return 200, {"rate_limits": service.tenants.update_rate_limits(
                    tenant_id, read_body().get("rate_limits"))}
            if len(parts) == 4 and parts[3] == "tokens" and method == "POST":
                return 201, service.tenants.issue_token(tenant_id)
            if (len(parts) == 5 and parts[3] == "tokens" and method == "DELETE"):
                service.tenants.revoke_token(tenant_id, parts[4])
                return 200, {"revoked": parts[4]}
        return 404, {"error": "not_found"}
    if parts[:2] != ["v1", "databases"]:
        return 404, {"error": "not_found"}
    if len(parts) == 2:
        root = service.is_admin(token)
        tenant_record = None if root else service.tenants.authorized_record(token)
        tenant_id = None if tenant_record is None else tenant_record["id"]
        if not root and tenant_id is None:
            return 401, {"error": "unauthorized"}
        if tenant_id is not None:
            service.tenants.admit_request(
                tenant_id, request_body_bytes, authorized_record=tenant_record)
        if method == "GET":
            query = parse_qs(parsed.query, keep_blank_values=True)
            if any(len(values) != 1 for values in query.values()) or set(query) - {"cursor", "limit"}:
                raise ValueError("invalid database list query parameters")
            cursor = query.get("cursor", [None])[0]
            limit = int(query.get("limit", ["100"])[0])
            if root:
                return 200, service.catalog.list_page(cursor=cursor, limit=limit)
            return 200, service.catalog.tenant_list_page(
                tenant_id, cursor=cursor, limit=limit)
        if method == "POST":
            body = read_body()
            if not root and body.get("request_id") is None:
                raise ValueError("tenant database creation requires request_id")
            if root and body.get("request_id") is not None:
                request_id = body.get("request_id")
                if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
                    raise ValueError("root replay requires request_id")
                database_id = sha256(("tessera-create-v1:" + request_id
                                      ).encode()).hexdigest()[:32]
                try:
                    existing, _ = service.catalog._load(database_id)
                except NotFound:
                    if not service.allow_legacy_root_create:
                        raise CatalogConflict(
                            "root creation is disabled; create with a tenant token") from None
                else:
                    owner = existing.get("tenant_id")
                    if owner is not None or not service.allow_legacy_root_create:
                        return 201, service.catalog.create(
                            body.get("name"), body.get("schema", []),
                            request_id=request_id, limits=body.get("limits"),
                            tenant_id=owner, legacy_root_ids=owner is not None)
            if root and not service.allow_legacy_root_create:
                raise CatalogConflict(
                    "root creation is disabled; create with a tenant token")
            return 201, service.catalog.create(body.get("name"), body.get("schema", []),
                                               request_id=body.get("request_id"),
                                               limits=body.get("limits"),
                                               tenant_id=tenant_id)
    if len(parts) < 3 or not _DB_ID.fullmatch(parts[2]):
        return 404, {"error": "not_found"}
    database_id = parts[2]

    def management_error():
        if service.is_admin(token):
            return None
        tenant_record = service.tenants.authorized_record(token)
        if tenant_record is None:
            return 401, {"error": "unauthorized"}
        tenant_id = tenant_record["id"]
        record, _ = service.catalog._load(database_id)
        if record.get("tenant_id") != tenant_id:
            return 404, {"error": "not_found"}
        service.tenants.admit_request(
            tenant_id, request_body_bytes, authorized_record=tenant_record)
        return None

    if len(parts) == 3 and method in ("GET", "DELETE"):
        if denial := management_error():
            return denial
        if method == "DELETE":
            if not hasattr(service.backend, "retire_pointer"):
                return 503, {"error": "deletion_requires_fenced_backend"}
            result = service.catalog.request_deletion(database_id)
            service.discard_read_cache(database_id)
            return 202, result
        return 200, service.catalog.deletion_status(database_id)
    if len(parts) == 5 and parts[3:] == ["deletion", "step"] and method == "POST":
        if denial := management_error():
            return denial
        return 200, service.catalog.purge_step(database_id)
    if len(parts) == 4 and parts[3] == "limits" and method in ("GET", "PUT"):
        if denial := management_error():
            return denial
        if method == "PUT":
            return 200, {"limits": service.catalog.update_limits(
                database_id, read_body().get("limits"))}
        return 200, {"limits": service.catalog.get(database_id)["limits"]}
    if len(parts) == 4 and parts[3] == "history" and method == "GET":
        if denial := management_error():
            return denial
        query = parse_qs(parsed.query, keep_blank_values=True)
        if any(len(values) != 1 for values in query.values()) or set(query) - {"cursor", "limit"}:
            raise ValueError("invalid history query parameters")
        return 200, service.catalog.history_page(
            database_id, cursor=query.get("cursor", [None])[0],
            limit=int(query.get("limit", ["100"])[0]))
    if len(parts) == 4 and parts[3] == "restore" and method == "POST":
        if denial := management_error():
            return denial
        body = read_body()
        if service.is_admin(token) and not service.allow_legacy_root_create:
            source, _ = service.catalog._load(database_id)
            if source.get("tenant_id") is None:
                request_id = body.get("request_id")
                if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
                    raise ValueError("root restore replay requires request_id")
                target_id = sha256(("tessera-restore-copy-v1:" + database_id + ":" +
                                    request_id).encode()).hexdigest()[:32]
                try:
                    service.catalog._load(target_id)
                except NotFound:
                    raise CatalogConflict(
                        "legacy source requires migration before restore") from None
        return 201, service.catalog.restore_copy(
            database_id, body.get("source_version"), body.get("name"),
            request_id=body.get("request_id"), limits=body.get("limits"))
    if len(parts) == 4 and parts[3] == "usage" and method == "GET":
        if denial := management_error():
            return denial
        query = parse_qs(parsed.query, keep_blank_values=True)
        if any(len(values) != 1 for values in query.values()):
            raise ValueError("query parameters must appear once")
        if set(query) - {"cursor", "limit", "logical"}:
            raise ValueError("unknown usage query parameter")
        logical = query.get("logical", ["false"])[0]
        if logical not in ("true", "false"):
            raise ValueError("logical must be true or false")
        return 200, service.usage(database_id, cursor=query.get("cursor", [None])[0],
                                  limit=int(query.get("limit", ["100"])[0]),
                                  include_logical=logical == "true")
    if len(parts) == 4 and parts[3] == "tokens" and method == "POST":
        if denial := management_error():
            return denial
        item = service.catalog.issue_token(database_id, read_body().get("scope", "write"))
        return 201, {"token_id": item.token_id, "token": item.secret, "scope": item.scope}
    if len(parts) == 5 and parts[3] == "tokens" and method == "DELETE":
        if denial := management_error():
            return denial
        service.catalog.revoke_token(database_id, parts[4])
        return 200, {"revoked": parts[4]}
    write = method == "POST" and len(parts) == 4 and parts[3] == "transactions"
    record = service.catalog.authorized_record(database_id, token, write=write)
    if record is None:
        return 401, {"error": "unauthorized"}
    if record.get("tenant_id") is not None:
        service.tenants.admit_request(record["tenant_id"], request_body_bytes)
    if write:
        body = read_body()
        return 200, service.write(database_id, body.get("operation_id"), body.get("statements"))
    if method == "POST" and len(parts) == 4 and parts[3] == "query":
        body = read_body()
        return 200, {"rows": service.read(database_id, body.get("sql"),
                                          body.get("params", []),
                                          min_version=body.get("min_version"))}
    if method == "GET" and len(parts) == 5 and parts[3] == "operations":
        return 200, service.lookup(database_id, parts[4])
    return 404, {"error": "not_found"}


# @spec cloud/error-response
def error_response(exc):
    if isinstance(exc, TenantRateLimited):
        return 429, {"error": "rate_limited",
                     "retry_after_seconds": exc.retry_after_seconds}
    if isinstance(exc, (ValueError, TypeError, json.JSONDecodeError)):
        return 400, {"error": "invalid_request", "detail": str(exc)}
    if isinstance(exc, (NotFound, DatabaseUnavailable)):
        return 404, {"error": "not_found"}
    if isinstance(exc, TenantBusy):
        return 409, {"error": "writer_busy"}
    if isinstance(exc, (CatalogConflict, TenantConflict)):
        return 409, {"error": "catalog_conflict"}
    if isinstance(exc, (QueryLimitExceeded, CatalogQuotaExceeded, DatabaseQuotaExceeded,
                        TenantQuotaExceeded)):
        return 413, {"error": "limit_exceeded"}
    if isinstance(exc, SqlExecutionFailed):
        return 422, {"error": "sql_failed"}
    if isinstance(exc, RestoreOutcomeUnknown):
        return 202, {"state": "unknown", "database_id": exc.database_id,
                     "hint": "retry the same source_version, name and request_id"}
    if isinstance(exc, OperationOutcomeUnknown):
        return 202, {"state": "unknown", "hint": "lookup or retry the same operation_id and payload"}
    return 503, {"error": "operation_failed_or_unknown",
                 "hint": "lookup before retrying the same operation_id and payload"}


def _handler(service):
    class Handler(BaseHTTPRequestHandler):
        server_version = "TesseraDB/2-alpha"

        def _send(self, status, value):
            body = _document(value)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if status == 429:
                self.send_header("Retry-After", str(value["retry_after_seconds"]))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            length = self._body_length()
            if length < 1:
                raise ValueError("request body must be 1..1048576 bytes")
            return parse_json_body(self.rfile.read(length))

        def _body_length(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValueError("invalid Content-Length") from exc
            if not 0 <= length <= 1_048_576:
                raise ValueError("request body must be 1..1048576 bytes")
            return length

        def _token(self):
            value = self.headers.get("Authorization", "")
            return value[7:] if value.startswith("Bearer ") else None

        def _dispatch(self):
            return dispatch_request(service, self.command, self.path,
                                    self._token(), self._body,
                                    request_body_bytes=self._body_length())

        def _run(self):
            try:
                status, value = self._dispatch()
            except Exception as exc:  # noqa: BLE001 - HTTP boundary maps unexpected errors
                status, value = error_response(exc)
            self._send(status, value)

        do_GET = _run
        do_POST = _run
        do_PUT = _run
        do_DELETE = _run

    return Handler


# @spec cloud/serve
def serve(service, host="127.0.0.1", port=8080):
    """Return an HTTP server; the caller controls TLS, lifecycle and shutdown."""
    return ThreadingHTTPServer((host, port), _handler(service))
