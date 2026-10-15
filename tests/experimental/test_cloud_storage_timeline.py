"""Storage rent estimates require complete, ordered samples of one bucket."""

import hashlib
from datetime import datetime, timedelta, timezone

import pytest

from experiments.cloud_storage_timeline import summarize


def sample(moment, size, *, bucket="bucket", complete=True, duration_ms=200):
    return {"s3_inventory": {
        "bucket_sha256": hashlib.sha256(bucket.encode()).hexdigest(),
        "current_bytes": size,
        "current_objects": 1,
        "list_requests": 1,
        "complete": complete,
        "stop_reason": None if complete else "object_limit",
        "started_at_utc": (moment - timedelta(milliseconds=duration_ms // 2)).isoformat(),
        "completed_at_utc": (moment + timedelta(milliseconds=duration_ms // 2)).isoformat(),
    }}


def test_timeline_integrates_only_the_observed_hour():
    start = datetime(2026, 9, 29, 0, 0, tzinfo=timezone.utc)
    report = summarize([sample(start + timedelta(hours=1), 2_000_000_000),
                        sample(start, 0)])
    assert report["target_cadence_met"] is True
    assert report["covered_seconds"] == "3600"
    assert report["estimated_byte_hours"] == "1000000000"
    assert float(report["estimated_decimal_gb_months_30d_in_observed_span"]) == pytest.approx(
        1 / 720)
    assert float(report["estimated_aws_gb_months_30d_in_observed_span"]) == pytest.approx(
        1_000_000_000 / (1_073_741_824 * 720))
    assert [row["current_bytes"] for row in report["samples"]] == [0, 2_000_000_000]


def test_timeline_flags_a_large_gap_without_extrapolating():
    start = datetime(2026, 9, 29, 0, 0, tzinfo=timezone.utc)
    report = summarize([sample(start, 1_000_000),
                        sample(start + timedelta(hours=3), 1_000_000)])
    assert report["target_cadence_met"] is False
    assert report["largest_gap_seconds"] == "10800"
    assert report["covered_seconds"] == "10800"


def test_timeline_rejects_different_buckets():
    start = datetime(2026, 9, 29, 0, 0, tzinfo=timezone.utc)
    items = [sample(start, 1), sample(start + timedelta(hours=1), 2)]
    items[1]["s3_inventory"]["bucket_sha256"] = hashlib.sha256(b"other").hexdigest()
    with pytest.raises(ValueError, match="different buckets"):
        summarize(items)


def test_timeline_rejects_incomplete_or_overlapping_scans():
    start = datetime(2026, 9, 29, 0, 0, tzinfo=timezone.utc)
    complete = sample(start, 1)
    incomplete = sample(start + timedelta(hours=1), 2, complete=False)
    with pytest.raises(ValueError, match="complete S3 inventories"):
        summarize([complete, incomplete])
    overlapping = sample(start + timedelta(milliseconds=100), 2)
    with pytest.raises(ValueError, match="overlap"):
        summarize([complete, overlapping])


def test_timeline_splits_linear_growth_at_calendar_month_boundary():
    first = datetime(2026, 9, 30, 23, 30, tzinfo=timezone.utc)
    second = datetime(2026, 10, 1, 0, 30, tzinfo=timezone.utc)
    report = summarize([sample(first, 0), sample(second, 2 * 1_073_741_824)])
    september, october = report["calendar_months"]
    assert [september["month_utc"], october["month_utc"]] == ["2026-09", "2026-10"]
    assert [september["month_hours"], october["month_hours"]] == [720, 744]
    assert september["sample_span_covers_month"] is False
    assert october["sample_span_covers_month"] is False
    assert september["observed_seconds"] == october["observed_seconds"] == "1800"
    assert float(september["estimated_byte_hours"]) == pytest.approx(
        0.25 * 1_073_741_824)
    assert float(october["estimated_byte_hours"]) == pytest.approx(
        0.75 * 1_073_741_824)
    assert float(september["estimated_aws_gb_months_in_observed_span"]) == pytest.approx(
        0.25 / 720)
    assert float(october["estimated_aws_gb_months_in_observed_span"]) == pytest.approx(
        0.75 / 744)


def test_timeline_marks_full_leap_february_coverage():
    start = datetime(2028, 2, 1, tzinfo=timezone.utc)
    end = datetime(2028, 3, 1, tzinfo=timezone.utc)
    report = summarize([sample(start, 1_073_741_824),
                        sample(end, 1_073_741_824)])
    assert len(report["calendar_months"]) == 1
    month = report["calendar_months"][0]
    assert month["month_utc"] == "2028-02"
    assert month["month_hours"] == 696
    assert month["sample_span_covers_month"] is True
    assert month["estimated_aws_gb_months_in_observed_span"] == "1"
