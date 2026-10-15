"""Bounded live pin-inventory check for an indexed native GC cycle.

The result is deliberately ephemeral. Every future image sweep invocation must
recheck pins under its own lease before deleting anything.
"""

from __future__ import annotations

import math
import time
from dataclasses import asdict

from tessera.backends.base import CASFailed

from .fencing import FencedBackend
from .gc_mark import PIN_KEY, _read_state
from .native import NativeLogStore
from .snapshot import InvalidSnapshot, canonical, digest


# @spec turso/pin-inventory-changed
class PinInventoryChanged(InvalidSnapshot):
    """The indexed pin set changed; a fresh fenced mark is required."""


# @spec turso/verify-gc-pins-now
def verify_gc_pins_now(lifecycle, *, max_pins=1000, page_size=100,
                       max_input_bytes=16_777_216, max_seconds=20):
    """Compare a complete, strongly read pin set to the indexed mark digest."""
    if (type(max_pins) is not int or not 1 <= max_pins <= 10_000
            or type(page_size) is not int or not 1 <= page_size <= 1000
            or type(max_input_bytes) is not int or not 1 <= max_input_bytes <= 16_777_216
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= 20):
        raise ValueError("invalid GC pin verification budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("pin verification requires native S3/DynamoDB storage")
    deadline = time.monotonic() + max_seconds
    prefix = storage.prefix + "restore-pins/"
    with owner._lock:
        owner._require_live_heartbeat()
        head, head_etag = storage.manifest()
        state, state_etag = _read_state(backend, storage.group)
        if state is None or state["phase"] not in {"indexed_unverified", "sweep"}:
            raise ValueError("an indexed native GC mark is required")
        if (state["head_etag"] != head_etag or state["head_version"] != head["version"]
                or state["policy_hash"] != digest(canonical(asdict(lifecycle.policy)))):
            raise InvalidSnapshot("GC mark head or policy changed before pin check")
        backend.objects.require_unversioned_bucket()
        count = total_bytes = 0
        running = digest(b"tessera-gc-pin-set-v1")
        cursor = None
        while True:
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"]}
            page = backend.list_page(
                prefix, min(page_size, max_pins - count + 1), start_after=cursor)
            if not page:
                break
            for key, listed_size in page:
                if (not isinstance(key, str) or not key.startswith(prefix)
                        or (cursor is not None and key <= cursor)
                        or PIN_KEY.fullmatch(key[len(storage.prefix):]) is None
                        or type(listed_size) is not int or listed_size < 0):
                    raise InvalidSnapshot("invalid GC pin listing")
                if count >= max_pins:
                    return {"phase": "budget", "reason": "pins",
                            "cycle_id": state["cycle_id"]}
                if listed_size > 16_000:
                    raise InvalidSnapshot("GC pin manifest exceeds format bound")
                if total_bytes + listed_size > max_input_bytes:
                    return {"phase": "budget", "reason": "input_bytes",
                            "cycle_id": state["cycle_id"]}
                if time.monotonic() >= deadline:
                    return {"phase": "deadline", "cycle_id": state["cycle_id"]}
                raw, etag = backend.get(key)
                if len(raw) != listed_size:
                    raise InvalidSnapshot("GC pin size changed during verification")
                storage.parse_manifest(raw, etag)
                running = digest(bytes.fromhex(running) + key.encode()
                                 + b"\0" + bytes.fromhex(digest(raw)))
                total_bytes += len(raw)
                count += 1
                cursor = key
        with owner._lease_guard:
            owner._require_live_heartbeat()
            owner._lease = backend.renew_lease(owner._lease, owner._lease_ttl_ms)
            storage.lease = owner._lease
            _, current_etag = _read_state(backend, storage.group)
            if current_etag != state_etag or storage.manifest()[1] != head_etag:
                raise CASFailed("GC cycle or published head changed during pin check")
        if count != state["pin_count"] or running != state["pin_digest"]:
            raise PinInventoryChanged("GC pin inventory changed; restart the mark cycle")
        return {"phase": "verified", "cycle_id": state["cycle_id"],
                "pins": count, "bytes": total_bytes}
