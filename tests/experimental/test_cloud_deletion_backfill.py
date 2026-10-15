"""Legacy deletion migration must resume without skipping catalog records."""

import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from experiments.cloud_deletion_queue_backfill import backfill
from tessera.backends.base import NotFound
from tessera.backends.s3dynamo import S3DynamoBackend
from tessera.experimental.cloud.service import CatalogConflict, DatabaseCatalog
from tessera.experimental.turso.fencing import FencedBackend


def test_backfill_is_replayable_and_only_queues_deleting_records():
    with mock_aws():
        session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing",
                                region_name="us-east-1")
        session.client("s3").create_bucket(Bucket="tessera-backfill-test")
        session.resource("dynamodb").create_table(
            TableName="leases",
            KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"},
                                  {"AttributeName": "deletion_shard", "AttributeType": "S"},
                                  {"AttributeName": "deletion_due", "AttributeType": "S"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "pending-deletions",
                "KeySchema": [{"AttributeName": "deletion_shard", "KeyType": "HASH"},
                              {"AttributeName": "deletion_due", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}}],
            BillingMode="PAY_PER_REQUEST")
        raw = S3DynamoBackend("tessera-backfill-test", "leases",
                              s3_client=session.client("s3"),
                              dynamodb=session.resource("dynamodb"))
        client = session.client("dynamodb")
        catalog = DatabaseCatalog(raw, data_backend=FencedBackend(raw, client))
        ids = [f"{number:032x}" for number in range(4)]
        for number, database_id in enumerate(ids):
            raw.put(catalog._key(database_id), json.dumps({
                "id": database_id, "state": "deleting" if number % 2 else "active",
                "delete_after": 1234.0,
            }).encode(), if_none_match=True)

        preview = list(backfill(catalog, page_size=2, max_pages=1))
        assert preview[0]["scanned"] == 2
        assert preview[0]["deleting"] == 1
        assert preview[0]["ensured"] == 0
        cursor = preview[0]["next_cursor"]
        assert cursor == catalog._key(ids[1])
        assert sum(page["missing_queue"] for page in backfill(
            catalog, verify=True, page_size=2)) == 2
        assert list(backfill(catalog, apply=True, page_size=2, max_pages=1,
                             start_after=cursor))[0]["ensured"] == 1
        assert sum(page["ensured"] for page in backfill(catalog, apply=True,
                                                        page_size=2)) == 2
        assert sum(page["missing_queue"] for page in backfill(
            catalog, verify=True, page_size=2)) == 0
        corrupt_key = {"tenant_id": {"S": "_tessera_cloud/deletion/" + ids[1]}}
        client.put_item(TableName="leases", Item={**corrupt_key,
                        "deletion_shard": {"S": "wrong"},
                        "deletion_due": {"S": "invalid"}})
        assert sum(page["invalid_queue"] for page in backfill(
            catalog, verify=True, page_size=2)) == 1
        with pytest.raises(CatalogConflict, match="invalid pending"):
            list(backfill(catalog, apply=True, page_size=2))
        client.delete_item(TableName="leases", Key=corrupt_key)
        list(backfill(catalog, apply=True, page_size=2))
        assert sum(page["invalid_queue"] for page in backfill(
            catalog, verify=True, page_size=2)) == 0
        for database_id in ids:
            item = client.get_item(TableName="leases", Key={
                "tenant_id": {"S": "_tessera_cloud/deletion/" + database_id}})
            assert ("Item" in item) == (int(database_id, 16) % 2 == 1)
        assert list(backfill(catalog, apply=True, page_size=2))[-1]["complete"]
        with pytest.raises(ValueError, match="exact catalog"):
            list(backfill(catalog, start_after="untrusted-key"))
        with (patch.object(catalog, "deletion_status", side_effect=NotFound("lost catalog")),
              pytest.raises(NotFound, match="lost catalog")):
            list(backfill(catalog, page_size=2))
