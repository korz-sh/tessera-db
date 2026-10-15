"""Exercise the isolated native drill and verify teardown in Moto."""

import json

import boto3
import pytest

pytest.importorskip("turso")
pytest.importorskip("moto")
from moto import mock_aws

import experiments.cloud_gc_native_deployed_drill as drill
from experiments.cloud_gc_native_deployed_drill import run


@pytest.mark.parametrize("incremental_index_chunks", [None, 1])
def test_native_drill_passes_and_destroys_resources(tmp_path, incremental_index_chunks):
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-2")
        account = session.client("sts").get_caller_identity()["Account"]
        output = tmp_path / "drill.json"
        report = run(session, expected_account_id=account, region="us-east-2",
                     commits=3, output=output, identity={"test": True},
                     incremental_index_chunks=incremental_index_chunks,
                     mark_metadata_per_step=1 if incremental_index_chunks else 32)
        assert report["status"] == "passed"
        assert report["resources_destroyed"] is True
        assert report["sweep_deleted_images"] == 1
        assert report["recovered_rows"] == 3
        assert any(key.startswith("s3.") for key in report["publish_sdk_calls"])
        assert any(key.startswith("dynamodb.") for key in report["publish_sdk_calls"])
        assert report["publish_commits_per_s"] > 0
        assert report["index_step_ms"]["count"] == (4 if incremental_index_chunks else 1)
        assert report["index_result"].get("runs") == (3 if incremental_index_chunks else None)
        assert json.loads(output.read_text()) == report
        assert not session.client("s3").list_buckets()["Buckets"]
        assert not session.client("dynamodb").list_tables()["TableNames"]


def test_native_drill_takeover_fences_old_mark_and_recovers(tmp_path):
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-2")
        account = session.client("sts").get_caller_identity()["Account"]
        report = run(session, expected_account_id=account, region="us-east-2",
                     commits=3, output=tmp_path / "takeover.json", identity={"test": True},
                     mark_metadata_per_step=1, incremental_index_chunks=1,
                     takeover=True)
        assert report["status"] == "passed"
        assert report["resources_destroyed"] is True
        assert report["takeover"]["stale_owner_rejected"] is True
        assert report["takeover"]["new_owner_resumed"] is True
        assert report["takeover"]["cycle_id_unchanged"] is True
        assert report["takeover"]["new_epoch"] > report["takeover"]["old_epoch"]
        assert report["recovered_rows"] == 3
        assert report["sweep_deleted_images"] == 1
        assert not session.client("s3").list_buckets()["Buckets"]
        assert not session.client("dynamodb").list_tables()["TableNames"]


def test_account_mismatch_creates_nothing(tmp_path):
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-2")
        with pytest.raises(RuntimeError, match="account differs"):
            run(session, expected_account_id="111111111111", region="us-east-2",
                commits=1, output=tmp_path / "drill.json", identity={"test": True})
        assert not session.client("s3").list_buckets()["Buckets"]
        assert not session.client("dynamodb").list_tables()["TableNames"]


def test_process_kill_requires_profile_before_creating_resources(tmp_path):
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-2")
        account = session.client("sts").get_caller_identity()["Account"]
        with pytest.raises(ValueError, match="process kill requires a profile"):
            run(session, expected_account_id=account, region="us-east-2", commits=3,
                output=tmp_path / "unused.json", identity={"test": True},
                process_kill=True)
        assert not session.client("s3").list_buckets()["Buckets"]
        assert not session.client("dynamodb").list_tables()["TableNames"]


def test_failure_after_resource_creation_still_destroys_resources(tmp_path, monkeypatch):
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-2")
        account = session.client("sts").get_caller_identity()["Account"]
        output = tmp_path / "failed.json"

        def broken_owner(*_args, **_kwargs):
            raise RuntimeError("injected publication failure")

        monkeypatch.setattr(drill, "BatchOwner", broken_owner)
        with pytest.raises(RuntimeError, match="injected publication failure"):
            run(session, expected_account_id=account, region="us-east-2",
                commits=1, output=output, identity={"test": True})
        report = json.loads(output.read_text())
        assert report["status"] == "failed"
        assert report["resources_destroyed"] is True
        assert not session.client("s3").list_buckets()["Buckets"]
        assert not session.client("dynamodb").list_tables()["TableNames"]
