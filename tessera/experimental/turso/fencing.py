"""Experimental regional S3 images + transactionally fenced DynamoDB pointers."""

import hashlib
import json
import re
import time
import uuid
from dataclasses import replace

from botocore.exceptions import ClientError

from tessera.backends.base import CASFailed, LeaseLost, NotFound
from tessera.backends.s3dynamo import S3DynamoBackend

POINTER = re.compile(r"(?:turso-native|turso-blocks|turso-experiment)/([A-Za-z0-9_-]{1,64})/manifest\.json")
IMAGE = re.compile(r"(?:turso-native|turso-blocks|turso-experiment)/[A-Za-z0-9_-]{1,64}/images/[0-9a-f]{32}\.db")
METADATA = re.compile(r"(?:turso-native|turso-blocks|turso-experiment)/[A-Za-z0-9_-]{1,64}/(?:history/[0-9]{20}\.json|restore-pins/[0-9a-f]{32}\.json)")
GC_MARK = re.compile(r"turso-native/([A-Za-z0-9_-]{1,64})/gc-mark/[0-9a-f]{32}/(?:history|pins)/[0-9]{20}\.json")
GC_INDEX = re.compile(r"turso-native/([A-Za-z0-9_-]{1,64})/gc-index/[0-9a-f]{32}/(?:exact\.json|runs/[0-9]{20}\.json)")


# @spec turso/fenced-backend
class FencedBackend:
    """Use a dedicated handle; client is a low-level DynamoDB client in the same region/table."""

    def __init__(self, backend, client):
        if not isinstance(backend, S3DynamoBackend):
            raise TypeError("S3DynamoBackend required")
        self.objects, self.client = backend, client
        self.table_name = backend.table_name

    def get(self, key):
        if POINTER.fullmatch(key):
            item = self.client.get_item(TableName=self.table_name,
                                       Key={"tenant_id": {"S": "turso-pointer/" + key}},
                                       ConsistentRead=True).get("Item")
            if item is None or item.get("deleted", {}).get("BOOL") is True:
                raise NotFound(key)
            raw, etag = bytes(item["body"]["B"]), item["etag"]["S"]
            if hashlib.sha256(raw).hexdigest() != etag:
                raise ValueError("pointer body hash mismatch")
            return raw, etag
        if not (IMAGE.fullmatch(key) or METADATA.fullmatch(key)
                or GC_MARK.fullmatch(key) or GC_INDEX.fullmatch(key)):
            raise ValueError("experimental image, metadata, or mark key required")
        return self.objects.get(key)

    def put(self, key, data, *, if_match=None, if_none_match=False):
        if (not (IMAGE.fullmatch(key) or METADATA.fullmatch(key)
                 or GC_MARK.fullmatch(key) or GC_INDEX.fullmatch(key))
                or if_match is not None or not if_none_match):
            raise ValueError("only immutable experimental objects use S3 PUT")
        return self.objects.put(key, data, if_none_match=True)

    def get_gc_mark_state(self, group):
        """Strongly read the optional durable mark cursor for one native group."""
        pointer = f"turso-native/{group}/manifest.json"
        if POINTER.fullmatch(pointer) is None:
            raise ValueError("invalid native group")
        item = self.client.get_item(
            TableName=self.table_name,
            Key={"tenant_id": {"S": "turso-gc-mark/" + group}},
            ConsistentRead=True,
        ).get("Item")
        if item is None:
            return None, None
        raw, etag = bytes(item["body"]["B"]), item["etag"]["S"]
        if hashlib.sha256(raw).hexdigest() != etag:
            raise ValueError("GC mark state body hash mismatch")
        return raw, etag

    def put_gc_mark_state(self, group, raw, *, previous_etag, head_etag, lease):
        """CAS the mark cursor with a live lease and the unchanged published head.

        A lost transaction reply is reconciled by a strongly consistent read.
        Equal bytes mean this exact state transition committed, even if the
        worker lost the lease immediately afterward; the next step rechecks it.
        """
        pointer = f"turso-native/{group}/manifest.json"
        if (POINTER.fullmatch(pointer) is None or lease is None
                or lease.tenant != "turso-experiment-" + group
                or not isinstance(raw, bytes) or len(raw) > 16000
                or not isinstance(head_etag, str) or len(head_etag) != 64):
            raise ValueError("invalid fenced GC mark transition")
        new_etag = hashlib.sha256(raw).hexdigest()
        lease_check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": lease.tenant}},
            "ConditionExpression": "#owner = :owner AND #epoch = :epoch AND #expires > :now",
            "ExpressionAttributeNames": {"#owner": "owner", "#epoch": "epoch", "#expires": "expires_at"},
            "ExpressionAttributeValues": {":owner": {"S": lease.owner},
                                          ":epoch": {"N": str(lease.epoch)},
                                          ":now": {"N": str(time.time())}},
        }
        head_check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": "turso-pointer/" + pointer}},
            "ConditionExpression": "#etag = :etag AND attribute_not_exists(#deleted)",
            "ExpressionAttributeNames": {"#etag": "etag", "#deleted": "deleted"},
            "ExpressionAttributeValues": {":etag": {"S": head_etag}},
        }
        state_put = {
            "TableName": self.table_name,
            "Item": {"tenant_id": {"S": "turso-gc-mark/" + group},
                     "body": {"B": raw}, "etag": {"S": new_etag}},
            "ConditionExpression": ("attribute_not_exists(tenant_id)" if previous_etag is None
                                    else "#etag = :previous"),
        }
        if previous_etag is not None:
            state_put.update(ExpressionAttributeNames={"#etag": "etag"},
                             ExpressionAttributeValues={":previous": {"S": previous_etag}})
        try:
            self.client.transact_write_items(
                TransactItems=[{"ConditionCheck": lease_check},
                               {"ConditionCheck": head_check}, {"Put": state_put}],
                ClientRequestToken=uuid.uuid4().hex,
            )
        except Exception as exc:
            try:
                actual, _ = self.get_gc_mark_state(group)
            except Exception:  # noqa: BLE001 - preserve the original remote failure
                actual = None
            if actual == raw:
                return new_etag
            if isinstance(exc, ClientError):
                reasons = exc.response.get("CancellationReasons", [])
                if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                    raise LeaseLost("GC mark lease no longer current") from exc
                if any(reason.get("Code") == "ConditionalCheckFailed" for reason in reasons[1:]):
                    raise CASFailed("GC mark head or cursor changed") from exc
            raise
        return new_etag

    def delete_gc_mark_state_after_retirement(self, group, lease):
        """Remove side state only after the group pointer is permanently retired."""
        pointer = f"turso-native/{group}/manifest.json"
        if (POINTER.fullmatch(pointer) is None or lease is None
                or lease.tenant != "turso-experiment-" + group):
            raise ValueError("invalid fenced GC mark cleanup")
        lease_check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": lease.tenant}},
            "ConditionExpression": "#owner = :owner AND #epoch = :epoch AND #expires > :now",
            "ExpressionAttributeNames": {"#owner": "owner", "#epoch": "epoch", "#expires": "expires_at"},
            "ExpressionAttributeValues": {":owner": {"S": lease.owner},
                                          ":epoch": {"N": str(lease.epoch)},
                                          ":now": {"N": str(time.time())}},
        }
        retired_check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": "turso-pointer/" + pointer}},
            "ConditionExpression": "#deleted = :true",
            "ExpressionAttributeNames": {"#deleted": "deleted"},
            "ExpressionAttributeValues": {":true": {"BOOL": True}},
        }
        try:
            self.client.transact_write_items(
                TransactItems=[{"ConditionCheck": lease_check},
                               {"ConditionCheck": retired_check},
                               {"Delete": {"TableName": self.table_name,
                                           "Key": {"tenant_id": {"S": "turso-gc-mark/" + group}}}}],
                ClientRequestToken=uuid.uuid4().hex,
            )
        except Exception as exc:
            if isinstance(exc, ClientError):
                reasons = exc.response.get("CancellationReasons", [])
                if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                    raise LeaseLost("GC mark cleanup lease no longer current") from exc
                if len(reasons) > 1 and reasons[1].get("Code") == "ConditionalCheckFailed":
                    raise CASFailed("GC mark cleanup requires a retired pointer") from exc
            try:
                actual, _ = self.get_gc_mark_state(group)
                retired = self.pointer_retired(pointer)
            except Exception:  # noqa: BLE001 - preserve the original remote failure
                actual, retired = b"unknown", False
            if actual is None and retired:
                return
            raise

    def delete(self, key):
        if not METADATA.fullmatch(key):
            raise ValueError("only experimental metadata can be deleted directly")
        return self.objects.delete(key)

    def delete_retired_image(self, key, lease):
        """Delete an image selected by lifecycle GC after owner/head checks."""
        if (IMAGE.fullmatch(key) is None or lease is None
                or lease.tenant != "turso-experiment-" + key.split("/")[1]):
            raise ValueError("retirement requires an image and its group lease")
        return self.objects.delete(key)

    def delete_inactive_gc_object(self, key, active_cycle, state_etag, lease):
        """Delete only a validated temporary key outside the current GC cycle.

        The caller renews the group lease; this method strongly checks the
        current cycle before the S3 DELETE.
        """
        match = GC_MARK.fullmatch(key) or GC_INDEX.fullmatch(key)
        if (match is None or lease is None
                or lease.tenant != "turso-experiment-" + match[1]
                or key.split("/")[3] == active_cycle):
            raise ValueError("inactive GC object and its group lease required")
        state, etag = self.get_gc_mark_state(match[1])
        if etag != state_etag or state is None or json.loads(state).get("cycle_id") != active_cycle:
            raise CASFailed("GC cycle changed during temporary cleanup")
        return self.objects.delete(key)

    def list(self, prefix):
        return self.objects.list(prefix)

    def list_page(self, prefix, limit=1000, *, start_after=None):
        return self.objects.list_page(prefix, limit, start_after=start_after)

    def mtime(self, key):
        return self.objects.mtime(key)

    def publish_pointer(self, key, raw, etag, lease):
        match = POINTER.fullmatch(key)
        if match is None or lease is None or lease.tenant != "turso-experiment-" + match[1]:
            raise ValueError("publication requires the matching group lease")
        if len(raw) > 16000:
            raise ValueError("experimental pointer exceeds 16 KiB")
        new_etag = hashlib.sha256(raw).hexdigest()
        put = {
            "TableName": self.table_name,
            "Item": {"tenant_id": {"S": "turso-pointer/" + key},
                     "body": {"B": raw}, "etag": {"S": new_etag}},
            "ConditionExpression": ("attribute_not_exists(tenant_id)" if etag is None
                                    else "#etag = :etag AND attribute_not_exists(#deleted)"),
        }
        if etag is not None:
            put.update(ExpressionAttributeNames={"#etag": "etag", "#deleted": "deleted"},
                       ExpressionAttributeValues={":etag": {"S": etag}})
        check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": lease.tenant}},
            "ConditionExpression": "#owner = :owner AND #epoch = :epoch AND #expires > :now",
            "ExpressionAttributeNames": {"#owner": "owner", "#epoch": "epoch", "#expires": "expires_at"},
            "ExpressionAttributeValues": {":owner": {"S": lease.owner}, ":epoch": {"N": str(lease.epoch)},
                                          ":now": {"N": str(time.time())}},
        }
        try:
            self.client.transact_write_items(TransactItems=[{"ConditionCheck": check}, {"Put": put}],
                                             ClientRequestToken=uuid.uuid4().hex)
        except ClientError as exc:
            reasons = exc.response.get("CancellationReasons", [])
            if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                raise LeaseLost("publication lease no longer current") from exc
            if len(reasons) > 1 and reasons[1].get("Code") == "ConditionalCheckFailed":
                raise CASFailed("published pointer changed") from exc
            raise
        return new_etag

    def pointer_retired(self, key):
        """Strongly read the permanent retirement marker for one group head."""
        if POINTER.fullmatch(key) is None:
            raise ValueError("experimental pointer key required")
        item = self.client.get_item(
            TableName=self.table_name,
            Key={"tenant_id": {"S": "turso-pointer/" + key}},
            ConsistentRead=True,
        ).get("Item")
        return item is not None and item.get("deleted", {}).get("BOOL") is True

    def retire_pointer(self, key, etag, lease):
        """Permanently fence a head under its live lease and exact old etag.

        A missing transaction reply is success only after a strong read proves
        the marker exists. This does not delete any S3 artifacts.
        """
        match = POINTER.fullmatch(key)
        if (match is None or not isinstance(etag, str) or lease is None
                or lease.tenant != "turso-experiment-" + match[1]):
            raise ValueError("retirement requires the matching pointer, etag and group lease")
        tombstone = hashlib.sha256(
            b"tessera-pointer-tombstone-v1\0" + key.encode() + b"\0" + etag.encode()
        ).hexdigest()
        check = {
            "TableName": self.table_name,
            "Key": {"tenant_id": {"S": lease.tenant}},
            "ConditionExpression": "#owner = :owner AND #epoch = :epoch AND #expires > :now",
            "ExpressionAttributeNames": {"#owner": "owner", "#epoch": "epoch", "#expires": "expires_at"},
            "ExpressionAttributeValues": {
                ":owner": {"S": lease.owner}, ":epoch": {"N": str(lease.epoch)},
                ":now": {"N": str(time.time())},
            },
        }
        put = {
            "TableName": self.table_name,
            "Item": {"tenant_id": {"S": "turso-pointer/" + key},
                     "deleted": {"BOOL": True}, "etag": {"S": tombstone}},
            "ConditionExpression": "#etag = :etag AND attribute_not_exists(#deleted)",
            "ExpressionAttributeNames": {"#etag": "etag", "#deleted": "deleted"},
            "ExpressionAttributeValues": {":etag": {"S": etag}},
        }
        try:
            self.client.transact_write_items(
                TransactItems=[{"ConditionCheck": check}, {"Put": put}],
                ClientRequestToken=uuid.uuid4().hex,
            )
        except Exception as exc:
            try:
                retired = self.pointer_retired(key)
            except Exception:  # noqa: BLE001 - preserve the original remote failure
                retired = False
            if retired:
                return
            if isinstance(exc, ClientError):
                reasons = exc.response.get("CancellationReasons", [])
                if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                    raise LeaseLost("retirement lease no longer current") from exc
                if len(reasons) > 1 and reasons[1].get("Code") == "ConditionalCheckFailed":
                    raise CASFailed("published pointer changed before retirement") from exc
            raise

    def acquire_lease(self, tenant, ttl_ms, **kwargs):
        lease = self.objects.acquire_lease(tenant, ttl_ms, **kwargs)
        floor = kwargs.get("min_epoch", 0)
        if lease.epoch > floor:
            return lease
        # The legacy backend applies its floor only when the epoch attribute
        # is absent. Repair regressed metadata while still fencing by owner.
        # On an ambiguous update, propagate the error without returning a
        # possibly stale token; the unreturned lease expires without publication.
        self.client.update_item(
            TableName=self.table_name, Key={"tenant_id": {"S": tenant}},
            UpdateExpression="SET #epoch = :next",
            ConditionExpression="#owner = :owner AND #epoch = :old AND #expires > :now",
            ExpressionAttributeNames={"#owner": "owner", "#epoch": "epoch", "#expires": "expires_at"},
            ExpressionAttributeValues={":owner": {"S": lease.owner}, ":old": {"N": str(lease.epoch)},
                                       ":next": {"N": str(floor + 1)}, ":now": {"N": str(time.time())}})
        return replace(lease, epoch=floor + 1)

    def renew_lease(self, lease, ttl_ms):
        return self.objects.renew_lease(lease, ttl_ms)

    def release_lease(self, lease):
        return self.objects.release_lease(lease)
