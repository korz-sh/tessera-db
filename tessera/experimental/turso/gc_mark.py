"""Durable, bounded native image marking; this module never deletes images.

The mark is evidence for the exact reducer. It never authorizes an image
deletion. State transitions require the current group lease and exact
published head in one DynamoDB transaction; page chunks are immutable S3 data.
"""

from __future__ import annotations

import json
import math
import re
import time
import uuid
from dataclasses import asdict

from .fencing import FencedBackend
from .native import NativeLogStore
from .snapshot import InvalidSnapshot, canonical, digest

FORMAT = "tessera-native-gc-mark-1"
HISTORY_KEY = re.compile(r"history/([0-9]{20})\.json")
PIN_KEY = re.compile(r"restore-pins/[0-9a-f]{32}\.json")
PHASES = {"history", "pins", "marked_unverified", "indexed_unverified",
          "sweep", "complete"}
INDEXED_PHASES = {"indexed_unverified", "sweep", "complete"}
CLOSED_PHASES = {"marked_unverified", *INDEXED_PHASES}


def _valid_reduction(reduction, state, group):
    return not (not isinstance(reduction, dict)
            or set(reduction) != {"next_sequence", "run_count", "chunk_digest",
                                  "pin_digest", "run_digest", "history_cursor",
                                  "pin_cursor", "history_count", "pin_count",
                                  "input_bytes"}
            or any(type(reduction[name]) is not int or reduction[name] < 0
                   for name in ("next_sequence", "run_count", "history_count",
                                "pin_count", "input_bytes"))
            or not 0 <= reduction["next_sequence"] <= state["chunk_count"]
            or not 0 <= reduction["run_count"] <= min(32, reduction["next_sequence"])
            or (reduction["run_count"] == 0) != (reduction["next_sequence"] == 0)
            or reduction["history_count"] > state["history_count"]
            or reduction["pin_count"] > state["pin_count"]
            or reduction["input_bytes"] > state["mark_bytes"]
            or any(not isinstance(reduction[name], str)
                   or re.fullmatch(r"[0-9a-f]{64}", reduction[name]) is None
                   for name in ("chunk_digest", "pin_digest", "run_digest"))
            or any(reduction[name] is not None and
                   (not isinstance(reduction[name], str) or
                    re.fullmatch(r"turso-native/" + re.escape(group) + suffix,
                                 reduction[name]) is None)
                    for name, suffix in (("history_cursor", r"/history/[0-9]{20}\.json"),
                                        ("pin_cursor", r"/restore-pins/[0-9a-f]{32}\.json"))))


def _read_state(backend, group):
    raw, etag = backend.get_gc_mark_state(group)
    if raw is None:
        return None, None
    try:
        state = json.loads(raw)
        if (state["format"] != FORMAT or state["group"] != group
                or state["phase"] not in PHASES
                or re.fullmatch(r"[0-9a-f]{32}", state["cycle_id"]) is None
                or re.fullmatch(r"[0-9a-f]{64}", state["head_etag"]) is None
                or re.fullmatch(r"[0-9a-f]{64}", state["policy_hash"]) is None
                or type(state["head_version"]) is not int or state["head_version"] < 0
                or type(state["started_at"]) not in (int, float)
                or not math.isfinite(state["started_at"]) or state["started_at"] < 0
                or any(type(state[name]) is not int or state[name] < 0
                       for name in ("chunk_count", "history_chunks", "pin_chunks",
                                    "history_count", "pin_count", "mark_bytes"))
                or state["history_chunks"] + state["pin_chunks"] != state["chunk_count"]
                or any(re.fullmatch(r"[0-9a-f]{64}", state[name]) is None
                       for name in ("chunk_digest", "pin_digest_running"))
                or (state["pin_digest"] is not None
                    and re.fullmatch(r"[0-9a-f]{64}", state["pin_digest"]) is None)
                or (state["phase"] in CLOSED_PHASES
                    and state["pin_digest"] != state["pin_digest_running"])
                or (state["phase"] not in CLOSED_PHASES
                    and state["pin_digest"] is not None)
                or (state["phase"] in INDEXED_PHASES
                    and (not isinstance(state.get("index_root"), dict)
                         or set(state["index_root"]) != {"key", "sha256", "bytes", "key_count"}
                         or re.fullmatch(r"turso-native/" + re.escape(group)
                                         + r"/gc-index/" + state["cycle_id"]
                                         + r"/exact\.json", state["index_root"]["key"]) is None
                         or re.fullmatch(r"[0-9a-f]{64}", state["index_root"]["sha256"]) is None
                         or type(state["index_root"]["bytes"]) is not int
                         or not 1 <= state["index_root"]["bytes"] <= 8_388_608
                         or type(state["index_root"]["key_count"]) is not int
                         or state["index_root"]["key_count"] < 1))
                or (state["phase"] not in INDEXED_PHASES and "index_root" in state)
                or (state["phase"] in {"sweep", "complete"}
                    and ("image_cursor" not in state
                         or (state["image_cursor"] is not None
                             and re.fullmatch(r"turso-native/" + re.escape(group)
                                              + r"/images/[0-9a-f]{32}\.db",
                                              state["image_cursor"]) is None)))
                or (state["phase"] not in {"sweep", "complete"}
                    and "image_cursor" in state)
                or (state["phase"] == "history"
                    and (state["pin_cursor"] is not None or state["pin_count"] != 0
                         or state["pin_chunks"] != 0))
                or any(state[name] is not None and
                       re.fullmatch(r"turso-native/" + re.escape(group) + suffix,
                                    state[name]) is None
                       for name, suffix in (("history_cursor", r"/history/[0-9]{20}\.json"),
                                            ("pin_cursor", r"/restore-pins/[0-9a-f]{32}\.json")))
                or (state["history_chunks"] > 0 and state["history_cursor"] is None)
                or (state["pin_chunks"] > 0 and state["pin_cursor"] is None)
                or not isinstance(state["head_artifacts"], list)
                or not state["head_artifacts"]
                or state["head_artifacts"] != sorted(set(state["head_artifacts"]))
                or any(re.fullmatch(r"turso-native/" + re.escape(group)
                                    + r"/images/[0-9a-f]{32}\.db", key) is None
                       for key in state["head_artifacts"])):
            raise ValueError("invalid fields")
        if state["phase"] == "marked_unverified":
            if "reduction" in state and not _valid_reduction(state["reduction"], state, group):
                raise ValueError("invalid reduction cursor")
        elif "reduction" in state:
            raise ValueError("reduction cursor in wrong phase")
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidSnapshot("invalid GC mark state") from exc
    return state, etag


# @spec turso/mark-step
def mark_step(lifecycle, *, max_metadata=32, max_chunk_bytes=1_048_576,
              max_seconds=15, restart=False):
    """Advance one page of a native mark cycle under the caller's group lease.

    Short pages are not completion; only a later empty page closes a phase.
    The returned ``marked_unverified`` state is never a sweep authorization.
    """
    if (type(max_metadata) is not int or not 1 <= max_metadata <= 100
            or type(max_chunk_bytes) is not int or not 4096 <= max_chunk_bytes <= 4_194_304
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not 0 < max_seconds <= 20 or type(restart) is not bool):
        raise ValueError("invalid GC mark budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("durable GC marking requires native S3/DynamoDB storage")
    deadline = time.monotonic() + max_seconds
    with owner._lock:
        owner._require_live_heartbeat()
        head, head_etag = storage.manifest()
        policy_hash = digest(canonical(asdict(lifecycle.policy)))
        state, previous_etag = _read_state(backend, storage.group)
        if (restart or state is None or state["head_etag"] != head_etag
                or state["policy_hash"] != policy_hash):
            backend.objects.require_unversioned_bucket()
            next_state = {
                "format": FORMAT, "group": storage.group,
                "cycle_id": uuid.uuid4().hex, "phase": "history",
                "head_etag": head_etag, "head_version": head["version"],
                "head_artifacts": sorted(storage.artifact_keys(head)),
                "policy_hash": policy_hash, "started_at": time.time(),
                "history_cursor": None, "pin_cursor": None,
                "history_count": 0, "pin_count": 0, "chunk_count": 0,
                "history_chunks": 0, "pin_chunks": 0,
                "mark_bytes": 0,
                "chunk_digest": digest(b"tessera-gc-chunk-chain-v1"),
                "pin_digest_running": digest(b"tessera-gc-pin-set-v1"),
                "pin_digest": None,
            }
            backend.put_gc_mark_state(
                storage.group, canonical(next_state), previous_etag=previous_etag,
                head_etag=head_etag, lease=owner._lease)
            return {"phase": "history", "started": True, "restarted": state is not None,
                    "cycle_id": next_state["cycle_id"], "marked": 0}
        if state["phase"] in CLOSED_PHASES:
            return {"phase": state["phase"], "cycle_id": state["cycle_id"],
                    "marked": 0, "chunks": state["chunk_count"]}
        phase = state["phase"]
        prefix = storage.prefix + ("history/" if phase == "history" else "restore-pins/")
        cursor_field = "history_cursor" if phase == "history" else "pin_cursor"
        cursor = state[cursor_field]
        if cursor is not None and (not isinstance(cursor, str) or not cursor.startswith(prefix)):
            raise InvalidSnapshot("invalid GC mark cursor")
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"], "marked": 0}
        page = backend.list_page(prefix, max_metadata, start_after=cursor)
        if not page:
            next_state = dict(state)
            next_state["phase"] = ("pins" if phase == "history" else "marked_unverified")
            if phase == "pins":
                next_state["pin_digest"] = state["pin_digest_running"]
            backend.put_gc_mark_state(
                storage.group, canonical(next_state), previous_etag=previous_etag,
                head_etag=head_etag, lease=owner._lease)
            return {"phase": next_state["phase"], "cycle_id": state["cycle_id"],
                    "marked": 0, "chunks": state["chunk_count"]}

        artifacts = set()
        metadata = []
        pin_digest = state["pin_digest_running"]
        last_key = cursor
        total_bytes = 0
        for key, listed_size in page:
            if (not isinstance(key, str) or not key.startswith(prefix)
                    or (last_key is not None and key <= last_key)):
                raise InvalidSnapshot("GC mark listing did not advance")
            relative = key[len(storage.prefix):]
            match = (HISTORY_KEY if phase == "history" else PIN_KEY).fullmatch(relative)
            if match is None:
                raise InvalidSnapshot("invalid GC mark metadata key")
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"], "marked": 0}
            raw, etag = backend.get(key)
            if (len(raw) != listed_size or len(raw) > 16_000
                    or total_bytes + len(raw) > max_chunk_bytes):
                raise InvalidSnapshot("GC mark metadata exceeds page byte budget")
            manifest, _ = storage.parse_manifest(raw, etag)
            if phase == "history" and (manifest["version"] != int(match[1])
                                        or manifest["version"] > state["head_version"]):
                raise InvalidSnapshot("GC mark history version mismatch")
            metadata.append({"key": key, "sha256": digest(raw)})
            artifacts.update(storage.artifact_keys(manifest))
            total_bytes += len(raw)
            last_key = key
            if phase == "pins":
                pin_digest = digest(bytes.fromhex(pin_digest) + key.encode()
                                    + b"\0" + bytes.fromhex(digest(raw)))
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"], "marked": 0}
        chunk = {
            "format": FORMAT, "cycle_id": state["cycle_id"], "phase": phase,
            "sequence": state["chunk_count"],
            "cursor_start": cursor, "cursor_end": last_key,
            "metadata": metadata, "artifact_keys": sorted(artifacts),
        }
        chunk_raw = canonical(chunk)
        if len(chunk_raw) > max_chunk_bytes:
            raise InvalidSnapshot("GC mark chunk exceeds page byte budget")
        chunk_key = (storage.prefix + "gc-mark/" + state["cycle_id"] + "/"
                     + phase + "/" + f"{state['chunk_count']:020d}.json")
        try:
            backend.put(chunk_key, chunk_raw, if_none_match=True)
        except Exception as exc:
            try:
                actual, _ = backend.get(chunk_key)
            except Exception:  # noqa: BLE001 - preserve the original PUT failure
                raise exc
            if actual != chunk_raw:
                raise InvalidSnapshot("GC mark chunk key has conflicting content") from exc
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"], "marked": 0}
        next_state = dict(state)
        next_state[cursor_field] = last_key
        next_state["chunk_count"] += 1
        next_state["history_chunks" if phase == "history" else "pin_chunks"] += 1
        next_state["history_count" if phase == "history" else "pin_count"] += len(metadata)
        next_state["pin_digest_running"] = pin_digest
        next_state["chunk_digest"] = digest(
            bytes.fromhex(state["chunk_digest"]) + chunk_key.encode()
            + b"\0" + bytes.fromhex(digest(chunk_raw)))
        next_state["mark_bytes"] += len(chunk_raw)
        backend.put_gc_mark_state(
            storage.group, canonical(next_state), previous_etag=previous_etag,
            head_etag=head_etag, lease=owner._lease)
        return {"phase": phase, "cycle_id": state["cycle_id"],
                "marked": len(metadata), "chunks": next_state["chunk_count"],
                "next_cursor": last_key}
