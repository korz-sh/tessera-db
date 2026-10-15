"""Bounded exact-index reduction for a completed native GC mark.

Both reducers stop at explicit input/output budgets. The incremental reducer
stores immutable, fenced runs and publishes the same exact index format as the
single-call reducer. Neither verifies the live pin inventory.
"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict

from .fencing import FencedBackend
from .gc_mark import FORMAT as MARK_FORMAT
from .gc_mark import _read_state
from .native import NativeLogStore
from .snapshot import InvalidSnapshot, canonical, digest

INDEX_FORMAT = "tessera-native-gc-exact-index-1"
RUN_FORMAT = "tessera-native-gc-index-run-1"
_RUN_SEED = digest(b"tessera-gc-index-run-chain-v1")


def _chunk_key(storage, state, sequence):
    phase = "history" if sequence < state["history_chunks"] else "pins"
    return (storage.prefix + "gc-mark/" + state["cycle_id"] + "/" + phase
            + "/" + f"{sequence:020d}.json"), phase


def _validated_chunk(raw, *, key, phase, sequence, cycle_id, group, prior_cursor):
    try:
        chunk = json.loads(raw)
        if (raw != canonical(chunk)
                or set(chunk) != {"format", "cycle_id", "phase", "sequence",
                                  "cursor_start", "cursor_end", "metadata", "artifact_keys"}
                or chunk["format"] != MARK_FORMAT or chunk["cycle_id"] != cycle_id
                or chunk["phase"] != phase or type(chunk["sequence"]) is not int
                or chunk["sequence"] != sequence
                or chunk["cursor_start"] != prior_cursor
                or not isinstance(chunk["metadata"], list)
                or not 1 <= len(chunk["metadata"]) <= 100
                or not isinstance(chunk["artifact_keys"], list)
                or not chunk["artifact_keys"]
                or chunk["artifact_keys"] != sorted(set(chunk["artifact_keys"]))):
            raise ValueError("invalid chunk envelope")
        prefix = f"turso-native/{group}/" + ("history/" if phase == "history"
                                               else "restore-pins/")
        suffix = r"[0-9]{20}\.json" if phase == "history" else r"[0-9a-f]{32}\.json"
        previous = prior_cursor
        for item in chunk["metadata"]:
            if (not isinstance(item, dict) or set(item) != {"key", "sha256"}
                    or re.fullmatch(re.escape(prefix) + suffix, item["key"]) is None
                    or (previous is not None and item["key"] <= previous)
                    or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None):
                raise ValueError("invalid chunk metadata")
            previous = item["key"]
        if chunk["cursor_end"] != previous:
            raise ValueError("chunk cursor mismatch")
        image_pattern = re.escape(f"turso-native/{group}/images/") + r"[0-9a-f]{32}\.db"
        if any(re.fullmatch(image_pattern, image) is None
               for image in chunk["artifact_keys"]):
            raise ValueError("invalid chunk image key")
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise InvalidSnapshot(f"invalid GC mark chunk {key!r}") from exc
    return chunk


def _verified_index(backend, root):
    raw, _ = backend.get(root["key"])
    if len(raw) != root["bytes"] or digest(raw) != root["sha256"]:
        raise InvalidSnapshot("GC exact index length/hash mismatch")
    return raw


def _put_immutable(backend, key, raw):
    try:
        backend.put(key, raw, if_none_match=True)
    except Exception as exc:
        try:
            actual, _ = backend.get(key)
        except Exception:  # noqa: BLE001 - preserve original S3 failure
            raise exc
        if actual != raw:
            raise InvalidSnapshot("GC index key has conflicting content") from exc


def _run_key(storage, state, number):
    return (storage.prefix + "gc-index/" + state["cycle_id"] + "/runs/"
            + f"{number:020d}.json")


def _seed_reduction():
    return {"next_sequence": 0, "run_count": 0,
            "chunk_digest": digest(b"tessera-gc-chunk-chain-v1"),
            "pin_digest": digest(b"tessera-gc-pin-set-v1"),
            "run_digest": _RUN_SEED, "history_cursor": None,
            "pin_cursor": None, "history_count": 0, "pin_count": 0,
            "input_bytes": 0}


def _verified_runs(storage, backend, state, reduction, *, deadline,
                   max_input_bytes):
    keys = set(state["head_artifacts"])
    run_digest = _RUN_SEED
    sequence = input_bytes = 0
    chain = digest(b"tessera-gc-chunk-chain-v1")
    pins = digest(b"tessera-gc-pin-set-v1")
    for number in range(reduction["run_count"]):
        if time.monotonic() >= deadline:
            return None, "deadline"
        key = _run_key(storage, state, number)
        raw, _ = backend.get(key)
        input_bytes += len(raw)
        if input_bytes > max_input_bytes:
            return None, "run_input_bytes"
        try:
            run = json.loads(raw)
            images = run["keys"]
            if (raw != canonical(run)
                    or set(run) != {"format", "cycle_id", "head_etag",
                                        "first_sequence", "next_sequence",
                                        "prior_chunk_digest", "chunk_digest",
                                        "prior_pin_digest", "pin_digest", "keys"}
                    or run["format"] != RUN_FORMAT
                    or run["cycle_id"] != state["cycle_id"]
                    or run["head_etag"] != state["head_etag"]
                    or type(run["first_sequence"]) is not int
                    or run["first_sequence"] != sequence
                    or type(run["next_sequence"]) is not int
                    or not sequence < run["next_sequence"] <= state["chunk_count"]
                    or run["prior_chunk_digest"] != chain
                    or run["prior_pin_digest"] != pins
                    or not isinstance(images, list)
                    or not images
                    or images != sorted(set(images))
                    or any(not isinstance(image, str) or re.fullmatch(
                        re.escape(storage.prefix + "images/") + r"[0-9a-f]{32}\.db",
                        image) is None for image in images)
                    or any(re.fullmatch(r"[0-9a-f]{64}", run[name]) is None
                           for name in ("chunk_digest", "pin_digest"))):
                raise ValueError("invalid index run")
        except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
            raise InvalidSnapshot(f"invalid GC index run {key!r}") from exc
        keys.update(images)
        sequence = run["next_sequence"]
        chain, pins = run["chunk_digest"], run["pin_digest"]
        run_digest = digest(bytes.fromhex(run_digest) + key.encode()
                            + b"\0" + bytes.fromhex(digest(raw)))
    if (sequence != reduction["next_sequence"]
            or chain != reduction["chunk_digest"]
            or pins != reduction["pin_digest"]
            or run_digest != reduction["run_digest"]):
        raise InvalidSnapshot("GC index run chain mismatch")
    return keys, None


# @spec turso/exact-index-step
def exact_index_step(lifecycle, *, max_chunks=1024, max_input_bytes=16_777_216,
                     max_index_bytes=8_388_608, max_seconds=20):
    """Verify all marked chunks and publish one exact S3 index if budgets fit."""
    if (type(max_chunks) is not int or not 1 <= max_chunks <= 1024
            or type(max_input_bytes) is not int or not 4096 <= max_input_bytes <= 16_777_216
            or type(max_index_bytes) is not int or not 64 <= max_index_bytes <= 8_388_608
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= 20):
        raise ValueError("invalid exact-index budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("exact reduction requires native S3/DynamoDB storage")
    deadline = time.monotonic() + max_seconds
    with owner._lock:
        owner._require_live_heartbeat()
        head, head_etag = storage.manifest()
        state, previous_etag = _read_state(backend, storage.group)
        if state is None or state["phase"] not in {"marked_unverified", "indexed_unverified"}:
            raise ValueError("a completed native mark is required")
        if (state["head_etag"] != head_etag or state["head_version"] != head["version"]
                or state["policy_hash"] != digest(canonical(asdict(lifecycle.policy)))):
            raise InvalidSnapshot("GC mark head or policy changed before reduction")
        if set(state["head_artifacts"]) != storage.artifact_keys(head):
            raise InvalidSnapshot("GC mark omitted a published head artifact")
        if state["phase"] == "indexed_unverified":
            _verified_index(backend, state["index_root"])
            return {"phase": "indexed_unverified", "cycle_id": state["cycle_id"],
                    "image_keys": state["index_root"]["key_count"], "created": False}
        if "reduction" in state:
            raise ValueError("incremental exact reduction already active")
        if state["chunk_count"] > max_chunks:
            return {"phase": "budget", "reason": "chunks", "cycle_id": state["cycle_id"]}
        backend.objects.require_unversioned_bucket()
        keys = set(state["head_artifacts"])
        chain = digest(b"tessera-gc-chunk-chain-v1")
        pins = digest(b"tessera-gc-pin-set-v1")
        cursors = {"history": None, "pins": None}
        counts = {"history": 0, "pins": 0}
        input_bytes = 0
        for sequence in range(state["chunk_count"]):
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"]}
            chunk_key, phase = _chunk_key(storage, state, sequence)
            raw, _ = backend.get(chunk_key)
            input_bytes += len(raw)
            if input_bytes > max_input_bytes:
                return {"phase": "budget", "reason": "input_bytes",
                        "cycle_id": state["cycle_id"]}
            chunk = _validated_chunk(raw, key=chunk_key, phase=phase, sequence=sequence,
                                     cycle_id=state["cycle_id"], group=storage.group,
                                     prior_cursor=cursors[phase])
            cursors[phase] = chunk["cursor_end"]
            counts[phase] += len(chunk["metadata"])
            keys.update(chunk["artifact_keys"])
            chain = digest(bytes.fromhex(chain) + chunk_key.encode()
                           + b"\0" + bytes.fromhex(digest(raw)))
            if phase == "pins":
                for item in chunk["metadata"]:
                    pins = digest(bytes.fromhex(pins) + item["key"].encode()
                                  + b"\0" + bytes.fromhex(item["sha256"]))
        if (chain != state["chunk_digest"] or input_bytes != state["mark_bytes"]
                or pins != state["pin_digest"]
                or cursors["history"] != state["history_cursor"]
                or cursors["pins"] != state["pin_cursor"]
                or counts["history"] != state["history_count"]
                or counts["pins"] != state["pin_count"]):
            raise InvalidSnapshot("GC mark chunk chain or inventory mismatch")
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        index = {"format": INDEX_FORMAT, "cycle_id": state["cycle_id"],
                 "head_etag": head_etag, "chunk_digest": chain,
                 "pin_digest": pins, "keys": sorted(keys)}
        index_raw = canonical(index)
        if len(index_raw) > max_index_bytes:
            return {"phase": "budget", "reason": "index_bytes",
                    "cycle_id": state["cycle_id"], "required_bytes": len(index_raw)}
        index_key = storage.prefix + "gc-index/" + state["cycle_id"] + "/exact.json"
        try:
            backend.put(index_key, index_raw, if_none_match=True)
        except Exception as exc:
            try:
                actual, _ = backend.get(index_key)
            except Exception:  # noqa: BLE001 - preserve original S3 failure
                raise exc
            if actual != index_raw:
                raise InvalidSnapshot("GC exact index key has conflicting content") from exc
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        next_state = dict(state)
        next_state["phase"] = "indexed_unverified"
        next_state["index_root"] = {"key": index_key, "sha256": digest(index_raw),
                                    "bytes": len(index_raw), "key_count": len(keys)}
        backend.put_gc_mark_state(
            storage.group, canonical(next_state), previous_etag=previous_etag,
            head_etag=head_etag, lease=owner._lease)
        return {"phase": "indexed_unverified", "cycle_id": state["cycle_id"],
                "image_keys": len(keys), "index_bytes": len(index_raw), "created": True}


# @spec turso/incremental-exact-index-step
def incremental_exact_index_step(lifecycle, *, max_chunks=32,
                                 max_input_bytes=4_194_304,
                                 max_index_bytes=8_388_608, max_seconds=20):
    """Verify one batch of mark chunks, then publish an exact index from runs.

    Each committed run is immutable. A fenced state CAS advances the chunk
    sequence only after its run PUT is durable or exactly reconciled. The final
    call verifies every run and publishes the legacy exact index format, so
    existing sweep readers retain their validation contract.
    """
    if (type(max_chunks) is not int or not 1 <= max_chunks <= 100
            or type(max_input_bytes) is not int or not 4096 <= max_input_bytes <= 16_777_216
            or type(max_index_bytes) is not int or not 64 <= max_index_bytes <= 8_388_608
            or not isinstance(max_seconds, (int, float)) or isinstance(max_seconds, bool)
            or not math.isfinite(max_seconds) or not 0 < max_seconds <= 20):
        raise ValueError("invalid incremental exact-index budget")
    storage, backend, owner = lifecycle.storage, lifecycle.backend, lifecycle.owner
    if not isinstance(storage, NativeLogStore) or not isinstance(backend, FencedBackend):
        raise TypeError("incremental exact reduction requires native S3/DynamoDB storage")
    deadline = time.monotonic() + max_seconds
    with owner._lock:
        owner._require_live_heartbeat()
        head, head_etag = storage.manifest()
        state, previous_etag = _read_state(backend, storage.group)
        if state is None or state["phase"] not in {"marked_unverified", "indexed_unverified"}:
            raise ValueError("a completed native mark is required")
        if (state["head_etag"] != head_etag or state["head_version"] != head["version"]
                or state["policy_hash"] != digest(canonical(asdict(lifecycle.policy)))
                or set(state["head_artifacts"]) != storage.artifact_keys(head)):
            raise InvalidSnapshot("GC mark head or policy changed before reduction")
        if state["phase"] == "indexed_unverified":
            _verified_index(backend, state["index_root"])
            return {"phase": "indexed_unverified", "cycle_id": state["cycle_id"],
                    "image_keys": state["index_root"]["key_count"], "created": False}
        backend.objects.require_unversioned_bucket()
        reduction = state.get("reduction", _seed_reduction())
        if reduction["next_sequence"] == state["chunk_count"]:
            if (reduction["chunk_digest"] != state["chunk_digest"]
                    or reduction["pin_digest"] != state["pin_digest"]
                    or reduction["history_cursor"] != state["history_cursor"]
                    or reduction["pin_cursor"] != state["pin_cursor"]
                    or reduction["history_count"] != state["history_count"]
                    or reduction["pin_count"] != state["pin_count"]
                    or reduction["input_bytes"] != state["mark_bytes"]):
                raise InvalidSnapshot("GC mark chunk chain or inventory mismatch")
            keys, stop_reason = _verified_runs(
                storage, backend, state, reduction, deadline=deadline,
                max_input_bytes=max_input_bytes)
            if stop_reason:
                return {"phase": "deadline" if stop_reason == "deadline" else "budget",
                        "reason": stop_reason, "cycle_id": state["cycle_id"]}
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"]}
            index_raw = canonical({"format": INDEX_FORMAT, "cycle_id": state["cycle_id"],
                                   "head_etag": head_etag,
                                   "chunk_digest": state["chunk_digest"],
                                   "pin_digest": state["pin_digest"],
                                   "keys": sorted(keys)})
            if len(index_raw) > max_index_bytes:
                return {"phase": "budget", "reason": "index_bytes",
                        "cycle_id": state["cycle_id"], "required_bytes": len(index_raw)}
            index_key = storage.prefix + "gc-index/" + state["cycle_id"] + "/exact.json"
            _put_immutable(backend, index_key, index_raw)
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"]}
            next_state = {key: value for key, value in state.items() if key != "reduction"}
            next_state["phase"] = "indexed_unverified"
            next_state["index_root"] = {"key": index_key, "sha256": digest(index_raw),
                                        "bytes": len(index_raw), "key_count": len(keys)}
            backend.put_gc_mark_state(
                storage.group, canonical(next_state), previous_etag=previous_etag,
                head_etag=head_etag, lease=owner._lease)
            return {"phase": "indexed_unverified", "cycle_id": state["cycle_id"],
                    "image_keys": len(keys), "index_bytes": len(index_raw),
                    "runs": reduction["run_count"], "created": True}
        if reduction["run_count"] >= 32:
            return {"phase": "budget", "reason": "runs", "cycle_id": state["cycle_id"]}
        current = dict(reduction)
        first_sequence = current["next_sequence"]
        first_chain, first_pins = current["chunk_digest"], current["pin_digest"]
        keys = set()
        call_bytes = 0
        while (current["next_sequence"] < state["chunk_count"]
               and current["next_sequence"] - first_sequence < max_chunks):
            if time.monotonic() >= deadline:
                return {"phase": "deadline", "cycle_id": state["cycle_id"]}
            sequence = current["next_sequence"]
            chunk_key, phase = _chunk_key(storage, state, sequence)
            cursor_field = "history_cursor" if phase == "history" else "pin_cursor"
            count_field = "history_count" if phase == "history" else "pin_count"
            raw, _ = backend.get(chunk_key)
            call_bytes += len(raw)
            if call_bytes > max_input_bytes:
                return {"phase": "budget", "reason": "input_bytes",
                        "cycle_id": state["cycle_id"]}
            chunk = _validated_chunk(
                raw, key=chunk_key, phase=phase, sequence=sequence,
                cycle_id=state["cycle_id"], group=storage.group,
                prior_cursor=current[cursor_field])
            current[cursor_field] = chunk["cursor_end"]
            current[count_field] += len(chunk["metadata"])
            keys.update(chunk["artifact_keys"])
            current["chunk_digest"] = digest(
                bytes.fromhex(current["chunk_digest"]) + chunk_key.encode()
                + b"\0" + bytes.fromhex(digest(raw)))
            if phase == "pins":
                for item in chunk["metadata"]:
                    current["pin_digest"] = digest(
                        bytes.fromhex(current["pin_digest"]) + item["key"].encode()
                        + b"\0" + bytes.fromhex(item["sha256"]))
            current["next_sequence"] += 1
            current["input_bytes"] += len(raw)
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        run_key = _run_key(storage, state, current["run_count"])
        run_raw = canonical({"format": RUN_FORMAT, "cycle_id": state["cycle_id"],
                             "head_etag": head_etag, "first_sequence": first_sequence,
                             "next_sequence": current["next_sequence"],
                             "prior_chunk_digest": first_chain,
                             "chunk_digest": current["chunk_digest"],
                             "prior_pin_digest": first_pins,
                             "pin_digest": current["pin_digest"],
                             "keys": sorted(keys)})
        if len(run_raw) > max_index_bytes:
            return {"phase": "budget", "reason": "run_bytes",
                    "cycle_id": state["cycle_id"], "required_bytes": len(run_raw)}
        _put_immutable(backend, run_key, run_raw)
        if time.monotonic() >= deadline:
            return {"phase": "deadline", "cycle_id": state["cycle_id"]}
        current["run_digest"] = digest(
            bytes.fromhex(current["run_digest"]) + run_key.encode()
            + b"\0" + bytes.fromhex(digest(run_raw)))
        current["run_count"] += 1
        next_state = {**state, "reduction": current}
        backend.put_gc_mark_state(
            storage.group, canonical(next_state), previous_etag=previous_etag,
            head_etag=head_etag, lease=owner._lease)
        return {"phase": "marked_unverified", "cycle_id": state["cycle_id"],
                "reduced_chunks": current["next_sequence"] - first_sequence,
                "next_sequence": current["next_sequence"],
                "runs": current["run_count"]}
