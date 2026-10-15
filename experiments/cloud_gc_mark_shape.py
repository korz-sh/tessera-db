"""Measure native-manifest reference shape for an exact incremental GC mark.

LocalBackend only: these are local artifact counts and bytes, not AWS request,
latency or cost measurements. Use the matched scan schema/workload for the scan
shape; use a single-row transaction for a high-publication-count shape.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import subprocess
import sys
import tempfile
import time
import zlib
from collections import Counter
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from examples.cloud_matched_scan_benchmark import schema, statements_for_scan
from tessera import LocalBackend
from tessera.experimental.turso.api import BulkOperation, Operation
from tessera.experimental.turso.lifecycle import RetentionSession
from tessera.experimental.turso.native import NativeLogStore
from tessera.experimental.turso.owner import BatchOwner


def _source_identity():
    identity = {"probe_sha256": sha256(Path(__file__).read_bytes()).hexdigest()}
    manifest = ROOT / "PUBLIC_SOURCE_MANIFEST.json"
    if manifest.is_file():
        identity["public_source_manifest_sha256"] = sha256(manifest.read_bytes()).hexdigest()
        return identity
    if (ROOT / ".git").exists():
        identity["source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        identity["working_tree_clean"] = not bool(subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    return identity


def _mark_shape(backend, storage, *, page_size):
    prefix = storage.prefix
    history_keys = backend.list(prefix + "history/")
    current, _ = storage.manifest()
    references = Counter()
    reference_sizes = {}
    checkpoints = set()
    max_manifest_bytes = 0
    chunk_bytes = []
    chunk_compressed_bytes = []
    for offset in range(0, len(history_keys), page_size):
        chunk_refs = set()
        for key in history_keys[offset:offset + page_size]:
            raw, etag = backend.get(key)
            max_manifest_bytes = max(max_manifest_bytes, len(raw))
            manifest, _ = storage.parse_manifest(raw, etag)
            checkpoints.add(manifest["checkpoint"]["key"])
            for item in [manifest["checkpoint"], *manifest["segments"]]:
                references[item["key"]] += 1
                if (item["key"] in reference_sizes
                        and reference_sizes[item["key"]] != item["bytes"]):
                    raise ValueError("one image key has inconsistent published sizes")
                reference_sizes[item["key"]] = item["bytes"]
                chunk_refs.add(item["key"])
        encoded = ("\n".join(sorted(chunk_refs)) + "\n").encode() if chunk_refs else b""
        chunk_bytes.append(len(encoded))
        chunk_compressed_bytes.append(len(zlib.compress(encoded, level=6)))
    current_raw, _ = storage.backend.get(storage.key)
    max_manifest_bytes = max(max_manifest_bytes, len(current_raw))
    checkpoints.add(current["checkpoint"]["key"])
    for item in [current["checkpoint"], *current["segments"]]:
        references[item["key"]] += 1
        if (item["key"] in reference_sizes
                and reference_sizes[item["key"]] != item["bytes"]):
            raise ValueError("one image key has inconsistent published sizes")
        reference_sizes[item["key"]] = item["bytes"]
    image_keys = backend.list(prefix + "images/")
    exact_index = ("\n".join(sorted(references)) + "\n").encode()
    partitions = Counter(key.rsplit("/", 1)[-1][:2] for key in references)
    history_bytes = sum(backend._path(key).stat().st_size for key in history_keys)
    return {
        "history_manifests": len(history_keys),
        "current_version": current["version"],
        "current_artifact_references": len(storage.artifact_keys(current)),
        "total_artifact_references": sum(references.values()),
        "unique_referenced_images": len(references),
        "actual_image_objects": len(image_keys),
        "distinct_checkpoints": len(checkpoints),
        "max_native_manifest_bytes": max_manifest_bytes,
        "fits_16kb_fenced_pointer_limit": max_manifest_bytes <= 16_000,
        "local_history_manifest_bytes": history_bytes,
        "local_referenced_image_bytes": sum(reference_sizes.values()),
        "local_all_image_bytes": sum(backend._path(key).stat().st_size
                                     for key in image_keys),
        "exact_key_index_bytes": len(exact_index),
        "exact_key_index_zlib_bytes": len(zlib.compress(exact_index, level=6)),
        "mark_page_manifests": page_size,
        "mark_chunks": len(chunk_bytes),
        "mark_chunk_bytes": sum(chunk_bytes),
        "mark_chunk_zlib_bytes": sum(chunk_compressed_bytes),
        "mark_chunk_max_bytes": max(chunk_bytes, default=0),
        "nonempty_two_hex_partitions": len(partitions),
        "largest_two_hex_partition_images": max(partitions.values(), default=0),
    }


# @spec experiments/run
def run(shape, commits, *, findings=500, rows_per_statement=40,
        ddl_dir=None, mark_page_size=100, max_segments=8):
    if shape not in {"small", "scan"}:
        raise ValueError("shape must be small or scan")
    if not 1 <= commits <= (10_000 if shape == "small" else 100):
        raise ValueError("commit count exceeds local measurement limit")
    if (not 1 <= findings <= 500 or not 1 <= rows_per_statement <= 500
            or math.ceil(findings / rows_per_statement) > 64):
        raise ValueError("scan shape exceeds bounded transaction limits")
    if not 1 <= mark_page_size <= 999:
        raise ValueError("mark page size must be 1..999")
    if type(max_segments) is not int or not 1 <= max_segments <= 64:
        raise ValueError("experimental max_segments must be 1..64")
    if shape == "small" and ddl_dir is not None:
        raise ValueError("external DDL only applies to the scan shape")
    if shape == "scan":
        ddl, schema_hashes = schema(ddl_dir)
    else:
        ddl = ["CREATE TABLE kv(k INTEGER PRIMARY KEY, v TEXT)"]
        schema_hashes = {"small_schema": sha256(json.dumps(
            ddl, separators=(",", ":")).encode()).hexdigest()}
    default_max_segments = NativeLogStore.max_segments
    NativeLogStore.max_segments = max_segments
    try:
        with tempfile.TemporaryDirectory(prefix="tessera-mark-shape-") as scratch:
            backend = LocalBackend(Path(scratch) / "objects")
            with BatchOwner(backend, "mark-shape", schema=ddl, native=True,
                            workers=1) as owner:
                started = time.perf_counter()
                for number in range(1, commits + 1):
                    if shape == "scan":
                        operation = BulkOperation(
                            f"scan-{number}", statements_for_scan(
                                number, findings, rows_per_statement))
                    else:
                        operation = Operation(
                            f"small-{number}", [["INSERT INTO kv VALUES(?, ?)",
                                                 [number, "value"]]])
                    outcome = owner.commit([operation])[0]
                    if isinstance(outcome, Exception):
                        raise RuntimeError(f"publication {number} failed") from outcome
                publish_seconds = time.perf_counter() - started
                table = "scan_issue" if shape == "scan" else "kv"
                expected_rows = commits * findings if shape == "scan" else commits
                actual_rows = owner.read(f"SELECT count(*) FROM {table}")[0][0]
                if actual_rows != expected_rows:
                    raise AssertionError(f"expected {expected_rows} rows, got {actual_rows}")
                storage = owner.storage
            measured = _mark_shape(backend, storage, page_size=mark_page_size)
            cold_started = time.perf_counter()
            with BatchOwner(backend, "mark-shape", native=True, workers=1) as recovered:
                cold_open_seconds = time.perf_counter() - cold_started
                cold_read_started = time.perf_counter()
                cold_rows = recovered.read(f"SELECT count(*) FROM {table}")[0][0]
                first = recovered.lookup(f"{shape}-1")
                last = recovered.lookup(f"{shape}-{commits}")
                cold_read_seconds = time.perf_counter() - cold_read_started
                if (cold_rows != expected_rows or first is None or first.version != 1
                        or last is None or last.version != commits):
                    raise AssertionError("cold recovery lost SQL rows or receipts")
            history_started = time.perf_counter()
            with RetentionSession(backend, "mark-shape") as lifecycle:
                version_zero_rows = lifecycle.inspect(
                    0, f"SELECT count(*) FROM {table}")[0][0]
            historical_read_seconds = time.perf_counter() - history_started
            if version_zero_rows != 0:
                raise AssertionError("version-zero historical recovery changed")
    finally:
        NativeLogStore.max_segments = default_max_segments
    return {
        "kind": "tessera_local_gc_mark_shape_v2",
        "measurement": "local NativeLogStore artifact and manifest shape",
        "aws_requests_measured": False,
        "monthly_cost_measured": False,
        "shape": shape,
        "commits": commits,
        "findings_per_scan": findings if shape == "scan" else None,
        "rows_per_statement": rows_per_statement if shape == "scan" else None,
        "experimental_max_segments": max_segments,
        "schema_sha256": schema_hashes,
        "sqlite_rows_verified": actual_rows,
        "cold_sql_rows_verified": cold_rows,
        "cold_first_receipt_version": first.version,
        "cold_last_receipt_version": last.version,
        "historical_version_zero_rows_verified": version_zero_rows,
        "cold_open_seconds_local": round(cold_open_seconds, 6),
        "cold_read_seconds_local": round(cold_read_seconds, 6),
        "historical_read_seconds_local": round(historical_read_seconds, 6),
        "commit_loop_seconds_local": round(publish_seconds, 6),
        "python": platform.python_version(),
        "pyturso": version("pyturso"),
        **_source_identity(),
        **measured,
    }


# @spec experiments/cloud-gc-mark-shape-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shape", choices=("small", "scan"), required=True)
    parser.add_argument("--commits", type=int, required=True)
    parser.add_argument("--findings", type=int, default=500)
    parser.add_argument("--rows-per-statement", type=int, default=40)
    parser.add_argument("--ddl-dir", type=Path)
    parser.add_argument("--mark-page-size", type=int, default=100)
    parser.add_argument("--max-segments", type=int, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.shape, args.commits, findings=args.findings,
                 rows_per_statement=args.rows_per_statement,
                 ddl_dir=args.ddl_dir, mark_page_size=args.mark_page_size,
                 max_segments=args.max_segments)
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
