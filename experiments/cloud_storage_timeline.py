"""Estimate S3 byte-hours from repeated complete BYOC stack inventories.

Linear interpolation between samples is an estimate, not AWS billed storage.
The result covers only the interval between the first and last sample.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_HOUR_US = 3_600_000_000
_MONTH_US = 30 * 24 * _HOUR_US
_DECIMAL_GB = 1_000_000_000
_BINARY_GB = 1 << 30


def _utc(value):
    if not isinstance(value, str):
        raise ValueError("inventory timestamp must be an ISO-8601 string")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("inventory timestamp must include a UTC offset")
    return stamp.astimezone(timezone.utc)


def _microseconds(delta):
    return ((delta.days * 86400 + delta.seconds) * 1_000_000
            + delta.microseconds)


def _next_month(moment):
    if moment.month == 12:
        return datetime(moment.year + 1, 1, 1, tzinfo=timezone.utc)
    return datetime(moment.year, moment.month + 1, 1, tzinfo=timezone.utc)


def _calendar_months(samples):
    months = {}
    for left, right in zip(samples, samples[1:]):
        start, end = left["midpoint"], right["midpoint"]
        total_us = _microseconds(end - start)
        span = Decimal(right["bytes"] - left["bytes"])
        cursor = start
        while cursor < end:
            stop = min(end, _next_month(cursor))
            first_fraction = Decimal(_microseconds(cursor - start)) / total_us
            last_fraction = Decimal(_microseconds(stop - start)) / total_us
            first_bytes = Decimal(left["bytes"]) + span * first_fraction
            last_bytes = Decimal(left["bytes"]) + span * last_fraction
            byte_us = (first_bytes + last_bytes) * _microseconds(stop - cursor) / 2
            label = cursor.strftime("%Y-%m")
            entry = months.setdefault(label, {"start": cursor, "end": stop,
                                               "byte_us": Decimal(0)})
            entry["end"] = stop
            entry["byte_us"] += byte_us
            cursor = stop
    result = []
    for label, entry in sorted(months.items()):
        month_start = datetime.fromisoformat(label + "-01").replace(tzinfo=timezone.utc)
        month_end = _next_month(month_start)
        month_us = _microseconds(month_end - month_start)
        result.append({
            "month_utc": label,
            "observed_start_utc": entry["start"].isoformat(),
            "observed_end_utc": entry["end"].isoformat(),
            "sample_span_covers_month": (entry["start"] == month_start
                                         and entry["end"] == month_end),
            "month_hours": month_us // _HOUR_US,
            "observed_seconds": str(Decimal(_microseconds(entry["end"] - entry["start"]))
                                    / 1_000_000),
            "estimated_byte_hours": str(entry["byte_us"] / _HOUR_US),
            "estimated_aws_gb_months_in_observed_span": str(
                entry["byte_us"] / (_BINARY_GB * month_us)),
        })
    return result


def _sample(snapshot, *, max_scan_seconds):
    inventory = snapshot["s3_inventory"]
    if inventory.get("complete") is not True or inventory.get("stop_reason") is not None:
        raise ValueError("storage timeline requires complete S3 inventories")
    size = inventory.get("current_bytes")
    objects = inventory.get("current_objects")
    requests = inventory.get("list_requests")
    bucket_hash = inventory.get("bucket_sha256")
    if (type(size) is not int or size < 0 or type(objects) is not int or objects < 0
            or type(requests) is not int or requests < 1
            or not isinstance(bucket_hash, str) or not _DIGEST.fullmatch(bucket_hash)):
        raise ValueError("invalid S3 inventory size, count, requests or bucket hash")
    started = _utc(inventory.get("started_at_utc"))
    completed = _utc(inventory.get("completed_at_utc"))
    duration_us = _microseconds(completed - started)
    if duration_us < 0 or duration_us > max_scan_seconds * 1_000_000:
        raise ValueError("S3 inventory scan exceeded its allowed time span")
    midpoint = started + (completed - started) / 2
    return {"started": started, "completed": completed, "midpoint": midpoint,
            "bytes": size, "objects": objects, "list_requests": requests,
            "bucket_sha256": bucket_hash, "scan_us": duration_us}


# @spec experiments/cloud-storage-timeline-summarize
def summarize(snapshots, *, max_gap_seconds=3900, max_scan_seconds=300):
    """Report a bounded interpolation over one bucket's observed interval."""
    if type(max_gap_seconds) is not int or not 1 <= max_gap_seconds <= 86400:
        raise ValueError("max_gap_seconds must be 1..86400")
    if type(max_scan_seconds) is not int or not 1 <= max_scan_seconds <= 300:
        raise ValueError("max_scan_seconds must be 1..300")
    samples = [_sample(item, max_scan_seconds=max_scan_seconds)
               for item in snapshots]
    if len(samples) < 2:
        raise ValueError("at least two complete S3 inventories are required")
    hashes = {item["bucket_sha256"] for item in samples}
    if len(hashes) != 1:
        raise ValueError("inventories belong to different buckets")
    samples.sort(key=lambda item: item["midpoint"])
    byte_microseconds = 0
    gaps_us = []
    for left, right in zip(samples, samples[1:]):
        if left["completed"] > right["started"]:
            raise ValueError("S3 inventory scans overlap")
        gap_us = _microseconds(right["midpoint"] - left["midpoint"])
        if gap_us <= 0:
            raise ValueError("inventory midpoint timestamps must advance")
        gaps_us.append(gap_us)
        byte_microseconds += Decimal(left["bytes"] + right["bytes"]) * gap_us / 2
    covered_us = sum(gaps_us)
    return {
        "scope": "current objects in one unversioned S3 bucket; excludes backups and other AWS storage",
        "method": "trapezoidal linear interpolation between inventory midpoints; estimate, not a bill or error bound",
        "bucket_sha256": samples[0]["bucket_sha256"],
        "sample_count": len(samples),
        "observed_start_utc": samples[0]["midpoint"].isoformat(),
        "observed_end_utc": samples[-1]["midpoint"].isoformat(),
        "covered_seconds": str(Decimal(covered_us) / 1_000_000),
        "largest_gap_seconds": str(Decimal(max(gaps_us)) / 1_000_000),
        "target_gap_seconds": max_gap_seconds,
        "target_cadence_met": max(gaps_us) <= max_gap_seconds * 1_000_000,
        "largest_scan_seconds": str(Decimal(max(x["scan_us"] for x in samples))
                                    / 1_000_000),
        "total_inventory_list_requests": sum(x["list_requests"] for x in samples),
        "minimum_sampled_bytes": min(x["bytes"] for x in samples),
        "maximum_sampled_bytes": max(x["bytes"] for x in samples),
        "estimated_byte_hours": str(byte_microseconds / _HOUR_US),
        "estimated_decimal_gb_months_30d_in_observed_span": str(
            byte_microseconds / (_DECIMAL_GB * _MONTH_US)),
        "estimated_aws_gb_months_30d_in_observed_span": str(
            byte_microseconds / (_BINARY_GB * _MONTH_US)),
        "calendar_months": _calendar_months(samples),
        "samples": [{"midpoint_utc": item["midpoint"].isoformat(),
                     "current_bytes": item["bytes"],
                     "current_objects": item["objects"]}
                    for item in samples],
    }


# @spec experiments/cloud-storage-timeline-main
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshots", nargs="+", type=Path)
    parser.add_argument("--max-gap-seconds", type=int, default=3900)
    parser.add_argument("--max-scan-seconds", type=int, default=300)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = summarize([json.loads(path.read_text()) for path in args.snapshots],
                       max_gap_seconds=args.max_gap_seconds,
                       max_scan_seconds=args.max_scan_seconds)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"sample_count": report["sample_count"],
                      "target_cadence_met": report["target_cadence_met"],
                      "estimated_aws_gb_months_30d_in_observed_span": report[
                          "estimated_aws_gb_months_30d_in_observed_span"],
                      "estimated_decimal_gb_months_30d_in_observed_span": report[
                          "estimated_decimal_gb_months_30d_in_observed_span"]}))


if __name__ == "__main__":
    main()
