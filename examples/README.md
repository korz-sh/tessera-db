# Examples

Canonical, user-facing examples. One-off measurement probes and drills
live in [`experiments/`](../experiments/) instead.

| File | What it shows |
|---|---|
| `handler.py` + `template.yaml` | Reference AWS deployment (this README) |
| `benchmark.py` + `BENCHMARK.md` | Reproducible latency/cost benchmarks |
| `soak.py` | Sustained-workload harness for the stable gate |
| `smoke_test.py` | Multi-tenant HTTP smoke test |
| `orm_example.py`, `use_cases_orm.py` | SQLAlchemy ORM usage |
| `cache_benchmark.py`, `benchmark_contention.py` | Cache and contention benchmarks |
| `cloud_matched_scan_benchmark.py` | Tessera-vs-Turso matched scan benchmark |
| `owner.Dockerfile`, `owner-taskdef.json` | Hot-owner Fargate reference IaC |

# Reference deployment on AWS (SAM)

**Example stack to try tessera on AWS in 5 minutes**: blobs in S3, leases
in DynamoDB, one Lambda behind HTTP API (`POST /{tenant}/create|write|read`)
and a compactor/GC every 15 minutes. The files in this folder are
self-contained.

> [!IMPORTANT]
> **This is a reference, not the way to consume tessera.** tessera is a
> client library: your own service embeds the package and talks directly
> to S3 and DynamoDB, with no API in between (see the root
> [README](../README.md)). This stack exists to *validate* the project and
> to measure. After each validation it is **decommissioned**: it is not
> left running.

## Index

| Section | What for |
|---|---|
| [Prerequisites](#prerequisites) · [Deploy](#deploy) | Stand it up |
| [Optional parameters](#optional-template-parameters) | Edge, S3 Express, auto-promotion |
| [curl examples](#curl-examples) · [Smoke test](#smoke-test) | Prove it works |
| [Cleanup](#cleanup) · [Costs](#costs) | **Tear it down when done** |
| [Hot owner](#hot-owner-level-2) | Level 2, step by step |
| [BENCHMARK.md](BENCHMARK.md) | All the measured numbers |

## Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) installed.
- AWS credentials configured (`aws configure` or environment variables).
- Local Python 3.12 (the Lambda runtime already bundles boto3).

## Deploy

`sam build` runs from **this folder** (`examples/`): the `CodeUri` is `.`.
The handler imports the `tessera` package, so copy it next to the handler
before building:

```bash
cd examples/
cp -r ../tessera .        # the package travels inside the Lambda bundle
sam build
sam deploy --guided       # accept the defaults; bucket/table name themselves
```

When it finishes, the stack outputs give you what you need:

- `ApiUrl` — base endpoint (https://…execute-api.<region>.amazonaws.com/)
- `BucketName` — S3 bucket (snapshots, deltas, manifests)
- `TableName` — DynamoDB table (per-tenant leases)

### Optional template parameters

All off by default; enabled with `--parameter-overrides`:

| Parameter | What it enables | Measured |
|---|---|---|
| `EnableEdge=true` | CloudFront in front of the API (pure transport, nothing cached) | reads −53%, writes −20% p50 from South America |
| `EnableExpress=true` (+ `ExpressAZ`, default `use2-az1`) | Deltas in an S3 Express One Zone directory bucket | warm write 70→55 ms p50 (PUT 28→11 ms) |
| `EnableAutoPromote=true` (+ `OwnerImageUri`, `OwnerSubnets`, `OwnerSecurityGroup`) | Ephemeral owner on Fargate Spot under write contention | ~7x steady-state ops/s, ~25 s cold start |
| `ApiKeySecretArn=arn:...` | Requires `X-Tessera-Key` on every request (401 without it) | < 1 ms warm overhead (the key is cached per sandbox) |

> [!WARNING]
> **Without `ApiKeySecretArn` the API is open**: anyone with the URL can
> write to any tenant. Tolerable only for an ephemeral stack in a private
> account. The key never goes in the template — it goes in Secrets Manager.

The details of each measurement are in [`BENCHMARK.md`](BENCHMARK.md).
Express trade-off: deltas live in ONE AZ only between publish and
compaction. The owner's capacity provider is controlled with
`OWNER_CAPACITY_PROVIDER` (default `FARGATE_SPOT`; `FARGATE` forces
on-demand).

## curl examples

```bash
API=https://xxxx.execute-api.us-east-1.amazonaws.com   # your ApiUrl output (no trailing /)

# 1) Create the tenant with a schema (201; 409 if it already exists)
curl -sS -X POST "$API/demo/create" -H 'Content-Type: application/json' \
  -H "X-Tessera-Key: $TESSERA_KEY" \
  -d '{"schema": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"}'
# -> {"tenant": "demo", "version": 1}        (X-Tessera-Version: 1 header)

# 2) Write (one SQLite transaction; params = positional '?' binds)
curl -sS -X POST "$API/demo/write" -H 'Content-Type: application/json' \
  -d '{"sql": "INSERT INTO users (name) VALUES (?)", "params": ["ada"]}'
# -> {"result": {"lastrowid": 1, "rowcount": 1}, "version": 2}

# 3) Read, demanding to see at least version 2 (read-your-writes)
curl -sS -X POST "$API/demo/read" -H 'Content-Type: application/json' \
  -H 'X-Tessera-Version: 2' \
  -d '{"sql": "SELECT id, name FROM users ORDER BY id"}'
# -> {"rows": [[1, "ada"]], "columns": ["id", "name"], "version": 2}
```

Notes:

- `create` accepts `{"schema": null}` (empty DB) or a SQL script
  (`executescript`).
- `read` accepts the optional header `X-Tessera-Version: N`; if version N
  is not visible in time it responds `409 {"error": "StaleRead"}`.
  Without the header, the body's `version` is a guaranteed floor of the
  state read.
- `BLOB` values are returned as UTF-8 text (invalid bytes → U+FFFD).
- Errors: body `{"error": <Exception>, "detail": "…"}` mapped as
  `Conflict/LeaseLost/TenantExists/StaleRead/CASFailed → 409`,
  `TenantNotFound → 404`, `TenantBusy → 503`, invalid SQL → 400, the
  unexpected → 500.

## Smoke test

```bash
python3 smoke_test.py --url "$API"                      # defaults: 20 tenants, 1000 writes, 10000 reads, 8 threads
python3 smoke_test.py --url "$API" --tenants 4 --writes 200 --reads 1000   # quick check
```

Phases: creates the tenants (tolerates 409), spreads writes round-robin
with a single writer per tenant (shard by thread), and fires random
reads — 10% with `X-Tessera-Version` of the last confirmed version
(read-your-writes).

Expected output (summary): totals and p50/p95 write/read latencies,
per-HTTP-code counts, `conflicts` and `violations` at 0, and
`RESULT: PASS`. Exit code is 1 if there were Conflicts or
read-your-writes violations. Only connection errors are retried;
4xx/5xx count as results.

## Cleanup

```bash
sam delete    # asks for confirmation and empties/deletes bucket + table + functions
```

## Costs

With test-level usage everything runs under the free tier or for cents:
S3 + on-demand DynamoDB + arm64 Lambda (the API is only billed per
invocation; the compactor runs 96 times a day and lists the whole bucket
each pass — delete it or disable it with `sam delete` when not in use).

## Hot owner (level 2)

Optional deployment level: a **hot owner** per tenant running on Fargate.
It is a long-lived process that acquires the lease for ONE tenant at
startup, serves `POST /write` and `POST /read` over HTTP (port 8080) from
a warm hydrated local SQLite, and publishes writes in batches (one
write-once delta + one manifest CAS every `TESSERA_PUBLISH_EVERY_S`).
With no writes for `TESSERA_IDLE_S` the owner exits cleanly and releases
the lease.

Artifacts (reference IaC — **nothing deploys itself**):

- `owner.Dockerfile` — arm64 owner image (`python -m tessera.owner`,
  configured 100% via environment variables).
- `owner-taskdef.json` — Fargate task definition (0.25 vCPU / 512 MB,
  `awsvpc`) with `${IMAGE_URI}`, `${EXECUTION_ROLE_ARN}`, and
  `${TASK_ROLE_ARN}` placeholders you substitute before registering (JSON
  has no comments: the editable defaults live in the `environment`
  field).

### Build and push to ECR

The build context is the **repo root** (the Dockerfile does `COPY tessera/`):

```bash
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
ECR=$AWS_ACCOUNT.dkr.ecr.$REGION.amazonaws.com

aws ecr create-repository --repository-name tessera-owner --region "$REGION"   # once
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR"

docker buildx build --platform linux/arm64 \
  -f examples/owner.Dockerfile -t "$ECR/tessera-owner:latest" .
docker push "$ECR/tessera-owner:latest"
```

`--platform linux/arm64` is the natural pairing with Fargate Graviton
(cheaper); building from an ARM Mac or ARM64 Linux makes the flag
redundant but harmless.

### Task IAM roles

Two roles, both with a trust policy for `ecs-tasks.amazonaws.com`:

- **`EXECUTION_ROLE_ARN`** — the standard ECS execution role (the managed
  `ecsTaskExecutionRole` works): ECR pull + `awslogs`. The managed policy
  `AmazonECSTaskExecutionRolePolicy` covers both.
- **`TASK_ROLE_ARN`** — the same permission logic as the level-1 Lambda
  (S3 CRUD on the store + DynamoDB CRUD on the lease table):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::tessera-prod-store-ACCOUNT",
        "arn:aws:s3:::tessera-prod-store-ACCOUNT/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
        "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/tessera-prod-leases"
    }
  ]
}
```

Trust policy (for both roles):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ecs-tasks.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Register the task definition

Substitute the three placeholders (`envsubst` ships in `gettext`; or
`sed`) and **edit the JSON defaults** for your stack: `TESSERA_TENANT`
(one owner = ONE tenant), `TESSERA_BUCKET` (`tessera-prod-store-ACCOUNT`,
the level-1 `BucketName` output), `TESSERA_TABLE` (`tessera-prod-leases`),
`TESSERA_REGION`, and `awslogs-region`:

```bash
export IMAGE_URI=$ECR/tessera-owner:latest
export EXECUTION_ROLE_ARN=arn:aws:iam::$AWS_ACCOUNT:role/ecsTaskExecutionRole
export TASK_ROLE_ARN=arn:aws:iam::$AWS_ACCOUNT:role/tessera-owner-task-role

envsubst < examples/owner-taskdef.json > /tmp/owner-taskdef.json
aws ecs register-task-definition --cli-input-json file:///tmp/owner-taskdef.json
```

### Run the service

```bash
aws ecs create-cluster --cluster-name tessera

aws ecs create-service \
  --cluster tessera \
  --service-name tessera-owner-demo \
  --task-definition tessera-owner \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration 'awsvpcConfiguration={subnets=[subnet-xxxx],securityGroups=[sg-xxxx],assignPublicIp=ENABLED}'
```

- `awsvpc` gives the task its own private ENI: the security group must
  allow outbound HTTPS to S3/DynamoDB/ECR and let the Lambda reach the
  task's port 8080. On a private subnet without egress, `assignPublicIp`
  requires NAT (see the cost below) or VPC endpoints for
  S3/DynamoDB/ECR.
- **One task per tenant**: each container pins `TESSERA_TENANT`, so one
  hot tenant = one service (or a one-off task with `--overrides`) with
  its own `TESSERA_TENANT`.
- `POST /write` responses report the latest **published** version; the
  write is visible to object-store readers at the latest by the next
  publish window.

### How forwarding works

The bridge to level 1 is the lease item in DynamoDB:

1. On acquiring the lease, the owner registers `endpoint` =
   `http://<IP>:8080`.
2. When a `POST /{tenant}/write` lands on the Lambda and the lease is
   live with an `endpoint`, `_lookup_owner_endpoint` reads it
   (consistent read) and `_forward_to_owner` resends the SAME
   `{"sql", "params"}` body to the owner, with an
   `OWNER_FORWARD_TIMEOUT_MS` budget (2000 ms default).
3. The owner's response (200/400/409/500...) is returned verbatim to the
   client — including the 409 `LeaseLost` fencing errors.
4. If the owner does not answer in time, the Lambda **fails open** to the
   normal path (acquire + hydrate + local write): it never stays in the
   middle. Reads are never forwarded (the Lambda already serves fresh
   state from the store).
5. If the owner dies, `release` removes the `endpoint` from the lease and
   the Lambda stops forwarding automatically.

### COST — leave it undeployed if you don't need it

Warning: a 0.25 vCPU / 512 MB Fargate service running 24/7 costs ~US$9/mo
per direct task; on a private subnet the NAT Gateway alone is around
US$33/mo. Total: **~US$35/mo if you forget a running service**. And mind
the dirty detail: `TESSERA_IDLE_S` shuts down the CONTAINER when there
are no writes, but an ECS service with `desired-count 1` relaunches it in
a loop (still billing). To "really turn it off": `desired-count 0`. This
is reference IaC — nothing deploys unless you run the commands above.

### Hot owner teardown

```bash
aws ecs update-service --cluster tessera --service tessera-owner-demo --desired-count 0
aws ecs delete-service  --cluster tessera --service tessera-owner-demo
aws ecs delete-cluster  --cluster tessera
aws ecr delete-repository --repository-name tessera-owner --force
aws logs delete-log-group --log-group-name /aws/ecs/tessera-owner   # optional
```

The level-1 bucket and table are deleted with `sam delete` as in
[Cleanup](#cleanup).
