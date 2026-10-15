"""Explicit, bounded native image sweep after exact marking and live pin checks.

This path is not scheduled. Each invocation verifies the indexed mark, current
pin inventory, published head and group lease before visiting one image page.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict

from tessera.backends.base import CASFailed

from .fencing import FencedBackend, IMAGE
from .gc_mark import _read_state
from .gc_reduce import INDEX_FORMAT, _verified_index
from .native import NativeLogStore
from .snapshot import InvalidSnapshot, canonical, digest


def _index_keys(storage, backend, state):
    raw = _verified_index(backend, state["index_root"])
    try:
        index = json.loads(raw)
        keys = index["keys"]
        if (raw != canonical(index)
                or set(index) != {"format", "cycle_id", "head_etag", "chunk_digest",
                                  "pin_digest", "keys"}
                or index["format"] != INDEX_FORMAT
                or index["cycle_id"] != state["cycle_id"]
                or index["head_etag"] != state["head_etag"]
                or index["chunk_digest"] != state["chunk_digest"]
                or index["pin_digest"] != state["pin_digest"]
                or not isinstance(keys, list)
                or len(keys) != state["index_root"]["key_count"]
                or keys != sorted(set(keys))
                or not set(state["head_artifacts"]) <= set(keys)
                or any(not isinstance(key, str) or not key.startswith(storage.prefix)
                       or IMAGE.fullmatch(key) is None for key in keys)):
            raise ValueError("invalid exact-index payload")
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise InvalidSnapshot("invalid GC exact-index payload") from exc
    return set(keys)


# @spec turso/image-sweep-step
def image_sweep_step(lifecycle, *, max_scan=100, max_deletes=100,
                     max_bytes=16_777_216, max_seconds=20, max_pins=1000):
    """Delete one bounded page of old, unreferenced native images under a lease."""
    if (type(max_scan) is not int or not 1 <= max_scan <= 1000
            or type(max_deletes) is not int or not 1 <= max_deletes <= 1000
            or type(max_bytes) is not int or not 1 <= max_bytes <= 16_777_216
            or type(max_pins) is not int or not 1 <= max_pins <= 10_000
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= 20):
        raise ValueError("invalid native image sweep budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("image sweep requires native S3/DynamoDB storage")
    deadline = time.monotonic() + max_seconds
    with owner._lock:
        owner._require_live_heartbeat()
        head, head_etag = storage.manifest()
        state, state_etag = _read_state(backend, storage.group)
        if state is None or state["phase"] not in {"indexed_unverified", "sweep", "complete"}:
            raise ValueError("an indexed native GC cycle is required")
        if (state["head_etag"] != head_etag or state["head_version"] != head["version"]
                or state["policy_hash"] != digest(canonical(asdict(lifecycle.policy)))
                or set(state["head_artifacts"]) != storage.artifact_keys(head)):
            raise InvalidSnapshot("GC sweep head or policy changed; restart marking")
        backend.objects.require_unversioned_bucket()
        referenced = _index_keys(storage, backend, state)
        if state["phase"] == "complete":
            return {"phase": "complete", "cycle_id": state["cycle_id"],
                    "scanned": 0, "deleted": 0, "bytes": 0}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        pin_result = lifecycle.verify_gc_pins_now(
            max_pins=max_pins, max_seconds=min(20, remaining))
        if pin_result["phase"] != "verified":
            return {"phase": pin_result["phase"], "reason": pin_result.get("reason"),
                    "cycle_id": state["cycle_id"]}
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        if state["phase"] == "indexed_unverified":
            next_state = {**state, "phase": "sweep", "image_cursor": None}
            backend.put_gc_mark_state(
                storage.group, canonical(next_state), previous_etag=state_etag,
                head_etag=head_etag, lease=owner._lease)
            return {"phase": "sweep", "cycle_id": state["cycle_id"],
                    "started": True, "scanned": 0, "deleted": 0, "bytes": 0}
        cursor = state["image_cursor"]
        page = backend.list_page(storage.prefix + "images/", max_scan,
                                 start_after=cursor)
        if not page:
            next_state = {**state, "phase": "complete"}
            backend.put_gc_mark_state(
                storage.group, canonical(next_state), previous_etag=state_etag,
                head_etag=head_etag, lease=owner._lease)
            return {"phase": "complete", "cycle_id": state["cycle_id"],
                    "scanned": 0, "deleted": 0, "bytes": 0}
        scanned = deleted = removed_bytes = 0
        last_key = cursor
        cutoff = state["started_at"] - lifecycle.policy.artifact_grace_seconds
        for key, size in page:
            if (not isinstance(key, str) or not key.startswith(storage.prefix + "images/")
                    or IMAGE.fullmatch(key) is None
                    or (last_key is not None and key <= last_key)
                    or type(size) is not int or size < 0):
                raise InvalidSnapshot("invalid GC image listing")
            if time.monotonic() >= deadline:
                break
            if key not in referenced:
                modified = backend.mtime(key)
                # S3 LastModified may have only second precision. Keep the whole
                # boundary second so a later upload cannot look older.
                if modified is not None and modified + 1 <= cutoff:
                    if deleted >= max_deletes or (deleted and removed_bytes + size > max_bytes):
                        break
                    with owner._lease_guard:
                        owner._require_live_heartbeat()
                        owner._lease = backend.renew_lease(owner._lease, owner._lease_ttl_ms)
                        storage.lease = owner._lease
                        _, current_etag = _read_state(backend, storage.group)
                        if current_etag != state_etag or storage.manifest()[1] != head_etag:
                            raise CASFailed("GC cycle or published head changed before image DELETE")
                        try:
                            backend.delete_retired_image(key, owner._lease)
                        except Exception as exc:
                            try:
                                still_exists = backend.mtime(key) is not None
                            except Exception:  # noqa: BLE001 - keep the original unknown outcome
                                raise exc
                            if still_exists:
                                raise
                    deleted += 1
                    removed_bytes += size
            scanned += 1
            last_key = key
            if deleted >= max_deletes or removed_bytes >= max_bytes:
                break
        if last_key == cursor:
            return {"phase": "deadline" if time.monotonic() >= deadline else "budget",
                    "cycle_id": state["cycle_id"], "scanned": 0,
                    "deleted": 0, "bytes": 0}
        next_state = {**state, "image_cursor": last_key}
        backend.put_gc_mark_state(
            storage.group, canonical(next_state), previous_etag=state_etag,
            head_etag=head_etag, lease=owner._lease)
        return {"phase": "sweep", "cycle_id": state["cycle_id"],
                "next_cursor": last_key, "scanned": scanned,
                "deleted": deleted, "bytes": removed_bytes}
