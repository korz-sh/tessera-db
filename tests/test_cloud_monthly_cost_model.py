from experiments.cloud_monthly_cost_model import Meters, Prices, _usd, report


def test_cost_model_prices_every_meter_and_operator_time():
    costs = _usd(Meters(api_requests=1_000_000, lambda_invocations=1_000_000,
                        lambda_gb_seconds=1, ddb_read_units=1_000_000,
                        ddb_write_units=1_000_000, ddb_storage_gb=1,
                        ddb_pitr_gb=1, s3_put_requests=1_000_000,
                        s3_get_requests=1_000_000, s3_list_requests=1_000_000,
                        s3_storage_gb=1, cloudwatch_ingest_gb=1,
                        cloudwatch_archive_gb=1, secret_count=1,
                        secret_api_calls=10_000, egress_gb=1,
                        operator_hours=1), Prices(), 100)
    assert costs["api_gateway"] == 1
    assert costs["lambda_requests"] == 0.2
    assert costs["dynamodb_pitr"] == 0.2
    assert costs["secrets"] == 0.4
    assert costs["operator"] == 100
    assert costs["aws_subtotal"] == round(costs["total"] - costs["operator"], 8)
    assert costs["total"] == round(sum(value for key, value in costs.items()
                                       if key not in {"total", "aws_subtotal"}), 8)


def test_report_keeps_idle_and_hot_scenarios_distinct():
    scenarios = {item["name"]: item for item in report()["scenarios"]}
    assert scenarios["one_idle_stack"]["meters"]["s3_list_requests"] == 51_840
    assert scenarios["hot_consumer_tenant_10000_scans"]["meters"]["api_requests"] > 1000
    assert scenarios["hot_consumer_tenant_10000_scans"]["cost_usd"]["total"] > scenarios["small_tenant"]["cost_usd"]["total"]
