"""Bounded cleanup of superseded native GC mark and index objects.

This only removes temporary objects from cycles other than the one in the
strongly read, fenced state. It never deletes database images or metadata.
"""

from __future__ import annotations

import math
import time

from .fencing import FencedBackend, GC_INDEX, GC_MARK
from .gc_mark import _read_state
from .native import NativeLogStore
from .snapshot import InvalidSnapshot


# @spec turso/stale-gc-cleanup-step
def stale_gc_cleanup_step(lifecycle, *, cursor=None, max_scan=100,
                          max_objects=100, max_bytes=16_777_216, max_seconds=15):
    """Visit one S3 page, deleting only temporary objects of inactive cycles.

    An unknown DELETE outcome leaves the cursor at the last known processed
    key. Repeating the step is safe because an inactive temporary DELETE is
    idempotent. Run a new pass from ``None`` later to find delayed old writes.
    """
    if (type(max_scan) is not int or not 1 <= max_scan <= 1000
            or type(max_objects) is not int or not 1 <= max_objects <= 1000
            or type(max_bytes) is not int or not 1 <= max_bytes <= 16_777_216
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= 20):
        raise ValueError("invalid inactive GC cleanup budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("inactive GC cleanup requires native S3/DynamoDB storage")
    if cursor is not None and not isinstance(cursor, str):
        raise ValueError("invalid inactive GC cleanup cursor")
    mark_prefix, index_prefix = storage.prefix + "gc-mark/", storage.prefix + "gc-index/"
    if cursor is None:
        prefix = mark_prefix
    elif cursor == index_prefix or GC_INDEX.fullmatch(cursor):
        if not cursor.startswith(index_prefix):
            raise ValueError("GC cleanup cursor belongs to another group")
        prefix = index_prefix
    elif GC_MARK.fullmatch(cursor) and cursor.startswith(mark_prefix):
        prefix = mark_prefix
    else:
        raise ValueError("invalid inactive GC cleanup cursor")
    deadline = time.monotonic() + max_seconds
    with owner._lock:
        owner._require_live_heartbeat()
        state, etag = _read_state(backend, storage.group)
        if state is None:
            raise ValueError("a current GC cycle is required before stale cleanup")
        backend.objects.require_unversioned_bucket()
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "next_cursor": cursor,
                    "scanned": 0, "deleted": 0, "bytes": 0}
        page = backend.list_page(prefix, max_scan, start_after=cursor)
        if not page:
            return {"phase": "index" if prefix == mark_prefix else "complete",
                    "next_cursor": index_prefix if prefix == mark_prefix else None,
                    "scanned": 0, "deleted": 0, "bytes": 0}
        scanned = deleted = removed_bytes = 0
        last_key = cursor
        for key, size in page:
            if (not isinstance(key, str) or not key.startswith(prefix)
                    or (last_key is not None and key <= last_key)
                    or type(size) is not int or size < 0):
                raise InvalidSnapshot("invalid inactive GC listing")
            match = (GC_MARK if prefix == mark_prefix else GC_INDEX).fullmatch(key)
            if match is None:
                raise InvalidSnapshot("unexpected key in GC temporary prefix")
            if time.monotonic() >= deadline:
                break
            if key.split("/")[3] != state["cycle_id"]:
                if (deleted >= max_objects or (deleted and removed_bytes + size > max_bytes)):
                    break
                with owner._lease_guard:
                    owner._require_live_heartbeat()
                    owner._lease = backend.renew_lease(owner._lease, owner._lease_ttl_ms)
                    storage.lease = owner._lease
                    backend.delete_inactive_gc_object(
                        key, state["cycle_id"], etag, owner._lease)
                deleted += 1
                removed_bytes += size
            scanned += 1
            last_key = key
            if deleted >= max_objects or removed_bytes >= max_bytes:
                break
        return {"phase": "mark" if prefix == mark_prefix else "index",
                "next_cursor": last_key, "scanned": scanned,
                "deleted": deleted, "bytes": removed_bytes}
