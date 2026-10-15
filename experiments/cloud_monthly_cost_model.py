"""Estimate a TesseraDB BYOC monthly bill from explicit workload meters.

This is a list-price model, not an AWS invoice.  Values are labelled as
measured, derived, or hypothetical in the output.  Change the inputs instead
of changing the formulas when a deployment supplies better measurements.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

MILLION = 1_000_000.0
GIB = 1024**3


@dataclass(frozen=True)
# @spec experiments/prices
class Prices:
    """US East list-price inputs refreshed 2026-09-29."""

    api_request_per_million: float = 1.00
    lambda_request_per_million: float = 0.20
    lambda_gb_second: float = 0.0000166667
    ddb_read_per_million: float = 0.125
    ddb_write_per_million: float = 0.625
    ddb_storage_gb_month: float = 0.25
    ddb_pitr_gb_month: float = 0.20
    s3_put_per_million: float = 5.00
    s3_get_per_million: float = 0.40
    s3_list_per_million: float = 5.00
    s3_storage_gb_month: float = 0.023
    cloudwatch_ingest_gb: float = 0.50
    cloudwatch_archive_gb_month: float = 0.03
    secret_month: float = 0.40
    secret_api_per_10k: float = 0.05
    egress_gb: float = 0.09


@dataclass(frozen=True)
# @spec experiments/meters
class Meters:
    """Monthly billable quantities.  Bytes are decimal bytes unless stated."""

    api_requests: float = 0
    lambda_invocations: float = 0
    lambda_gb_seconds: float = 0
    ddb_read_units: float = 0
    ddb_write_units: float = 0
    ddb_storage_gb: float = 0
    ddb_pitr_gb: float = 0
    s3_put_requests: float = 0
    s3_get_requests: float = 0
    s3_list_requests: float = 0
    s3_storage_gb: float = 0
    cloudwatch_ingest_gb: float = 0
    cloudwatch_archive_gb: float = 0
    secret_count: float = 1
    secret_api_calls: float = 0
    egress_gb: float = 0
    operator_hours: float = 0


def _usd(meters: Meters, prices: Prices, operator_hour: float) -> dict[str, float]:
    values = {
        "api_gateway": meters.api_requests / MILLION * prices.api_request_per_million,
        "lambda_requests": meters.lambda_invocations / MILLION * prices.lambda_request_per_million,
        "lambda_duration": meters.lambda_gb_seconds * prices.lambda_gb_second,
        "dynamodb_reads": meters.ddb_read_units / MILLION * prices.ddb_read_per_million,
        "dynamodb_writes": meters.ddb_write_units / MILLION * prices.ddb_write_per_million,
        "dynamodb_storage": meters.ddb_storage_gb * prices.ddb_storage_gb_month,
        "dynamodb_pitr": meters.ddb_pitr_gb * prices.ddb_pitr_gb_month,
        "s3_put": meters.s3_put_requests / MILLION * prices.s3_put_per_million,
        "s3_get": meters.s3_get_requests / MILLION * prices.s3_get_per_million,
        "s3_list": meters.s3_list_requests / MILLION * prices.s3_list_per_million,
        "s3_storage": meters.s3_storage_gb * prices.s3_storage_gb_month,
        "cloudwatch_ingest": meters.cloudwatch_ingest_gb * prices.cloudwatch_ingest_gb,
        "cloudwatch_archive": meters.cloudwatch_archive_gb * prices.cloudwatch_archive_gb_month,
        "secrets": meters.secret_count * prices.secret_month,
        "secret_api": meters.secret_api_calls / 10_000 * prices.secret_api_per_10k,
        "network_egress": meters.egress_gb * prices.egress_gb,
        "operator": meters.operator_hours * operator_hour,
    }
    values["total"] = sum(values.values())
    values["aws_subtotal"] = values["total"] - values["operator"]
    return {key: round(value, 8) for key, value in values.items()}


def _idle() -> Meters:
    """Measured/derived one-stack idle schedule for a 30-day month."""
    deletion = 43_200
    retention = 8_640
    # Six warm deletion reports averaged 75.6ms at 512MiB; retention was 21ms.
    gb_seconds = deletion * 0.0756 * 0.5 + retention * 0.021 * 0.5
    return Meters(
        lambda_invocations=deletion + retention,
        lambda_gb_seconds=gb_seconds,
        ddb_read_units=deletion + retention,
        ddb_write_units=deletion,
        s3_list_requests=deletion + retention,
        cloudwatch_ingest_gb=7383 / 1_000_000_000 * (deletion + retention) / 7,
        secret_count=1,
        secret_api_calls=1,
    )


def _campaign_per_scan() -> Meters:
    """Derived from the 30-scan consumer campaign report, including setup scope."""
    scans = 30
    return Meters(
        api_requests=102 / scans,
        lambda_invocations=106 / scans,
        lambda_gb_seconds=55.974 / scans,
        ddb_read_units=(203 + 8) / scans,
        ddb_write_units=435 / scans,
        s3_put_requests=65 / scans,
        s3_get_requests=504 / scans,
        cloudwatch_ingest_gb=(87_450 + 9_830) / 1_000_000_000 / scans,
        egress_gb=6_586 / 1_000_000_000 / scans,
        secret_count=0,
    )


def _scenario(name: str, meters: Meters, prices: Prices, operator_hour: float,
              evidence: str, notes: list[str]) -> dict:
    costs = _usd(meters, prices, operator_hour)
    return {"name": name, "evidence": evidence, "meters": asdict(meters),
            "cost_usd": costs, "notes": notes}


# @spec experiments/report
def report(operator_hour: float = 100.0, prices: Prices = Prices()) -> dict:
    idle = _idle()
    per_scan = _campaign_per_scan()
    small = replace(idle, s3_storage_gb=0.5, ddb_storage_gb=0.01,
                    ddb_pitr_gb=0.01, secret_count=1, operator_hours=0.25)
    fleet = replace(idle, secret_count=1, s3_storage_gb=5,
                    ddb_storage_gb=0.2, ddb_pitr_gb=0.2, operator_hours=2)
    hot = replace(idle, secret_count=1, s3_storage_gb=8, ddb_storage_gb=0.05,
                  ddb_pitr_gb=0.05, operator_hours=4)
    hot = replace(hot, **{field: getattr(hot, field) + getattr(per_scan, field) * 10_000
                          for field in asdict(per_scan) if field != "secret_count"})
    return {
        "model": "tessera-byoc-monthly-cost-v1",
        "date": "2026-09-29",
        "region": "us-east-2 list-price assumptions",
        "prices": asdict(prices),
        "operator_hour_usd": operator_hour,
        "scenarios": [
            _scenario("one_idle_stack", idle, prices, operator_hour, "derived from measured idle sample", [
                "One admin secret and the current 1-minute deletion/5-minute retention schedule.",
                "No databases or SQL traffic; Lambda warm duration is measured from the idle drill.",
            ]),
            _scenario("small_tenant", small, prices, operator_hour, "hypothetical storage plus measured idle schedule", [
                "One tenant with 0.5 decimal GB in S3 and 10 MiB in DynamoDB/PITR.",
                "Operator time is an explicit hypothetical input, not an AWS charge.",
            ]),
            _scenario("one_hundred_mostly_idle_tenants", fleet, prices, operator_hour, "hypothetical fleet plus measured idle schedule", [
                "One stack with 100 tenant token sets and 5 decimal GB of S3 data; tenant tokens are not Secrets Manager secrets.",
                "This does not multiply the whole stack 100 times.",
            ]),
            _scenario("hot_consumer_tenant_10000_scans", hot, prices, operator_hour, "derived from measured 30-scan campaign plus hypothetical retained storage and egress", [
                "10,000 scans/month at 500 findings and one write statement per scan.",
                "Campaign counters include setup and rollback; scaling them linearly is a scenario, not a capacity claim.",
                "The 6,586-byte API payload was used as a proxy for egress only to make the assumption visible; replace it with a network meter.",
            ]),
        ],
        "excluded": [
            "AWS account free tiers, credits, taxes, discounts and regional price differences",
            "S3 versioning, backup exports, NAT/VPC endpoints, KMS, CloudTrail and support plans",
            "Concurrency, retries not present in the source report, and capacity limits",
        ],
    }


# @spec experiments/cloud-monthly-cost-model-main
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operator-hour", type=float, default=100.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    raw = json.dumps(report(args.operator_hour), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(raw)
    print(raw)


if __name__ == "__main__":
    main()
