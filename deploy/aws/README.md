# Experimental single-region AWS stack

The opt-in native GC scheduler passed an isolated AWS drill with three SQL publications and a forced two-object metadata cap. The drill invoked the maintenance Lambda directly while its EventBridge rule was enabled; it did not measure rule delivery timing or large-history recovery.

This Terraform module creates a private S3 bucket, an on-demand DynamoDB table with PITR, API Gateway HTTP API, Python 3.12 Lambda, EventBridge deletion and retention sweeps, and seven-day CloudWatch log groups. Retention runs every five minutes and scans at most five catalog entries per invocation. Its default complete-listing collector cannot sweep images while retained history exceeds 999 manifests or restore pins; an experimental native cycle can be enabled with `enable_native_gc=true`. It allocates no continuously running compute, but requests, storage, Secrets Manager and logs can incur charges. It is an alpha, not a production availability or retention commitment.

Prerequisites: Terraform >=1.6, `uv`, AWS credentials in the target account, and a Secrets Manager secret whose `SecretString` is a random, single-line administrator token of at least 32 characters with no whitespace. The exact string is sent as an HTTP Bearer token; a trailing newline in the secret makes authentication fail. Never place that token in Terraform variables, source control or shell history. Use a secured Terraform state backend for non-disposable installations.

From the source root, build a Linux x86_64 Lambda artifact and review the plan:

The build installs third-party packages from `requirements-lambda.txt` with
exact versions and SHA-256 hashes. Refresh that file deliberately when changing
dependencies, then rebuild and review the bundled license inventory.

```bash
python3 scripts/build_lambda_artifact.py --output /tmp/tessera-lambda.zip
terraform -chdir=deploy/aws/terraform init
terraform -chdir=deploy/aws/terraform plan \
  -var='name=tessera-test' \
  -var='region=us-east-2' \
  -var='lambda_zip_path=/tmp/tessera-lambda.zip' \
  -var='admin_secret_arn=YOUR_SECRET_ARN'
```

Apply with the same variables after review. The `api_url` output is the HTTPS endpoint. For a **disposable empty test only**, set `force_destroy_bucket=true` and run `terraform destroy` with the same variables afterward; verify the bucket, table, Lambda and rule are absent. The default is `false` so an ordinary destroy will not remove customer objects.

Retention defaults to the newest eight historical versions and seven days, with a one-hour grace before deleting unreferenced artifacts. Terraform variables `retention_keep_last`, `retention_seconds`, and `gc_grace_seconds` configure those bounds. Keep the grace longer than the maximum in-flight reader and publication reconciliation window; the module refuses values below 60 seconds. A value of zero for `retention_seconds` is only appropriate for an isolated disposable test. Keep the data bucket unversioned: collection fails closed when S3 versioning is enabled or suspended because delete markers would leave hidden billable versions. Monitor Lambda logs for `retention sweep failed`; by default, a group above the 999-manifest metadata cap stops image collection without deleting data. The disabled-by-default `enable_native_gc` option advances bounded mark, reduction, image sweep and inactive-cycle cleanup; it still needs a 10,000-version recovery and takeover drill. `retention_metadata_limit` defaults to 999 and can be lowered only with native GC enabled, which may increase metadata requests. Before rolling back to a ZIP that lacks retention event handling, disable the `retention_sweep` rule.

The one-minute deletion rule queries a sparse DynamoDB index. A separate hourly rule scans catalog pages to repair older `deleting` records without queue items. On an existing large fleet, backfill or drain those records before relying on prompt deletion; the hourly scan covers only 50 catalog keys per invocation. After deploying the queue-first writer everywhere and confirming the index is ACTIVE, run `python3 experiments/cloud_deletion_queue_backfill.py --profile PROFILE --region REGION --bucket BUCKET --table TABLE` to count migration candidates, then repeat with `--apply` to queue them and `--verify` to count missing or malformed queue items. Each JSON line is a completed page checkpoint; after interruption pass its `next_cursor` via `--start-after`. Require an empty final page and zero `missing_queue` and `invalid_queue` across the verification scan; repeat verification if a concurrent deletion changed state during it. Keep the output private. Monitor `pending deletion failed` and `deletion sweep failed` logs. An interrupted DELETE can have an unknown outcome because durable queue intent is written first; retry DELETE or GET its status. Before rolling back to a ZIP without pending-deletion and repair event handling, disable both deletion rules. The queue protocol is documented in the internal design notes.

The default IAM role is scoped to the module's bucket, table, admin secret and log group. DynamoDB PITR and TTL for tenant-minute counters are enabled. The API Gateway stage has a global throttle and Lambda concurrency cap. Each database has published-frontier, operation-size and token-count limits; tenant requests also have a fixed-minute count and incoming-body budget. These do not cap total billed S3 storage or AWS bytes. One writer can publish to each database at a time. A `writer_busy` result should be retried with the same operation ID and payload. See [the API contract](../../docs/CLOUD-API.md) for uncertain outcomes and limits.

The Lambda keeps one verified published read frontier in its local temporary storage when it is at most 16 MiB. Each query still checks the remote published pointer. An append-only native frontier can be reconstructed from the cached checkpoint and log plus verified new segments; a checkpoint change or invalid cached base triggers a full restore. A query for another database replaces the cached one; larger frontiers are read without retention. This cache is best effort per warm Lambda environment and is never a durability source.

The BYOC default disables new root-created databases. Use root to create a
tenant and issue its management token, then use that token to create databases
and issue SQL tokens. Existing root-created databases can be migrated with the
checkpointed tenant procedure documented in the internal protocol notes.
`allow_legacy_root_create=true` is an upgrade escape hatch that reopens an
unmetered root creation path. Do not roll a migrated catalog back to a binary
without legacy-ID replay support.

For a bounded cost study, `enable_aws_call_metrics=true` logs one structured summary per Lambda invocation with the route class, status, elapsed handler time, logical S3/DynamoDB/Secrets Manager SDK calls and observed HTTP attempts including SDK retries. It logs no SQL, parameters, bearer tokens or database IDs. The default is off. These counters are not DynamoDB read/write capacity units or AWS billed request counts. The default API access log records response payload length as well as status and request ID; it does not record SQL or bearer tokens. The read-only `examples/cloud_stack_cost_snapshot.py` collects Lambda REPORT billed duration and API response payload length through `logs:FilterLogEvents`, alongside CloudWatch metrics and current S3 inventory. Its log and S3 scans are bounded; check coverage and delayed metrics before projecting a monthly cost. Response payload length excludes protocol overhead and is not billed network egress. Account billing data is still needed for a complete cost. The opt-in call records also add CloudWatch ingestion.
