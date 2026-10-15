# tessera-db benchmark

**Historical measurements.** The in-development `reliable-tenant-lifecycle` change makes the owner wait for durable publication and makes history precede the CAS. The local/deferred ACK numbers and the two-calls-on-the-critical-path numbers in this document do not describe that new contract. The methodology is preserved; new measurements under the same guarantees must be run and published before comparing latencies.

Results from a real run of the [`benchmark.py`](benchmark.py) harness (2026-08-30): same Linux dev box for all engines, a single sequential client, PostgreSQL 16 and MySQL 8 in Docker on localhost, tessera's local modes on `LocalBackend` (filesystem), and `tessera-serverless` against the real SAM stack (Lambda + API Gateway + S3 + DynamoDB in us-east-2). 1000 ops per workload were measured on the local engines and 150 on serverless.

## How to run

Containers for the compared databases (postgres:16-alpine and mysql:8, with the credentials and ports the harness uses by default):

```bash
docker run -d --name bench-pg \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16-alpine

docker run -d --name bench-mysql \
  -e MYSQL_ROOT_PASSWORD=bench -p 3306:3306 mysql:8
# wait ~30 s for MySQL to finish initializing
```

Benchmark dependencies (psycopg2 + pymysql):

```bash
pip install -e ".[bench]"
```

Full run (engines that cannot connect are skipped with a note):

```bash
python examples/benchmark.py --rows 1000 --batch 100 --json-out benchmark-results.json
```

To include `tessera-serverless` you must point at the real stack, via `--api-url` or the `TESSERA_API_URL` environment variable; without it that engine is skipped:

```bash
TESSERA_API_URL="https://xxxx.execute-api.us-east-2.amazonaws.com/Prod" \
python examples/benchmark.py --rows 1000 --batch 100 --json-out benchmark-results.json
```

Owner publish mode for the `tessera-hot` engines is selectable:
`--tessera-publish-mode per_commit` (default — each `/write` waits for the
publish round-trip) or `windowed` (`/write` ACKs at local commit; commits
inside `--tessera-publish-every-s`, default 1 s, fold into ONE publish —
bounded durability window). In `windowed` mode the JSON artifact's
`meta.tessera_backend_ops` reports measured PUTs/publishes per commit via
a `_CountingBackend` proxy.

## Methodology

- **Engines**: `sqlite` (stdlib embedded sqlite3, WAL, `synchronous=NORMAL`, commit per op), `postgres` (psycopg2, commit per op), `mysql` (pymysql, commit per op), `tessera-hot` (Owner over LocalBackend: HTTP `/write` per op, publish every 1 s — the ACK arrives BEFORE the publish), `tessera-store` (full Store.write per row over LocalBackend: lease + WAL + delta PUT + manifest CAS), and `tessera-serverless` (API Gateway -> Lambda -> real S3 + DynamoDB).
- **Workloads**: `insert_row` (N single-row INSERTs, commit each), `insert_batch` (N rows in batches of `--batch` = 100, one transaction per batch), `point_select` (SELECT by seeded id), and `update_row` (UPDATE by id, commit each).
- **Measurement**: 20 warmup ops per workload (never measured), p50/p95 over per-op latencies in ms, ops/s = ops / sum of latencies. Deterministic ~256 B payloads. A single sequential client, same machine for all engines.
- **Serverless**: the harness caps measured ops (`min(N, 300)`, see `benchmark.py`); this run measured 150 ops per workload. Rows per request are capped by the API Gateway payload limit.
- **Fairness**: tessera's local modes run over LocalBackend (local filesystem), NOT over S3 — they measure tessera's coordination overhead (lease + manifest CAS + delta publication), not object-storage latency. `tessera-serverless` is the only one exercising the real AWS stack, and it is the production-faithful number.

`insert_batch` semantics (same as the harness report): p50/p95 are **per batch** of 100 rows; ops/s is **per row** (for serverless, batches/s). Where this run has no p95 data it is marked `—`.

## Results

### sqlite (embedded, WAL, synchronous=NORMAL)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 0.02 | 0.03 | 885 |
| insert_batch | 0.22 (per 100-row batch) | — | 441k (rows/s) |
| point_select | 0.01 | — | 186k |
| update_row | 0.01 | — | 2716 |

### postgresql 16 (Docker, commit per op)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 0.48 | — | 1917 |
| insert_batch | 12.3 (per 100-row batch) | — | 7700 (rows/s) |
| point_select | 0.15 | — | 6548 |
| update_row | 0.51 | — | 1853 |

### mysql 8 (Docker, commit per op)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 3.3 | — | 214 |
| insert_batch | 337.6 (per 100-row batch) | — | 289 (rows/s) |
| point_select | 0.16 | — | 6458 |
| update_row | 3.7 | — | 240 |

NOTE: dominated by the container volume's fsync on every commit; indicative number, not representative of dedicated hardware.

### tessera-hot (Owner over LocalBackend, publish every 1 s)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 0.57 | — | 1735 |
| insert_batch | 57.7 (per 100-row batch) | 2410 (per batch) | 342 (rows/s) |
| point_select | 0.53 | — | 1841 |
| update_row | 0.58 | — | 739 |

The publish window blocks: every ~1 s a batch eats the publication window (p95 of 2410 ms per batch) and mean throughput drops to 342 rows/s. Honest and documented: the write commits to the warm local DB and ACKs instantly, but object-store durability arrives with the next publish window (~1 s cadence, not per row).

### tessera-store (full Store.write per row over LocalBackend: lease + WAL + delta PUT + manifest CAS)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 52.9 | 91.3 | 17.9 |
| insert_batch | 76.5 (per 100-row batch) | — | 1335 (rows/s; 1 CAS per batch) |
| point_select | 11.1 | — | 79 |
| update_row | 52.6 | — | 17.6 |

### tessera-serverless (PRODUCTION: real Lambda + API Gateway + S3 + DynamoDB)

| Workload | p50 (ms) | p95 (ms) | ops/s |
|---|---|---|---|
| insert_row | 706.0 | 778.4 | 1.4 |
| insert_batch | 7039.7 (per 100-row batch, ≈70 ms/row) | — | 1.4 (batches/s) |
| point_select | 576.6 | — | 1.7 |
| update_row | 699.0 | — | 1.4 |

### Tessera-exclusive metric

`cold_first_query_vfs` = **220.3 ms**: first query of a 2000-row tenant in format 2 with an empty cache (manifest HEAD + VFS session registration + chunk download + first query), single-shot measurement.

## Honest reading

- Embedded SQLite wins by design: local process, no network, no coordination layer. It is the physical floor of any comparison, not a rival.
- `tessera-hot` competes in latency with embedded PostgreSQL (0.57 ms vs 0.48 ms p50 on insert_row) because it serves from local memory/disk; its cost is that durability is per publish window (~1 s), not per row.
- tessera's real cost is per-object durability (delta PUT + manifest CAS): ~53 ms/row local, ~700 ms/row serverless.
- The serverless mode pays Lambda + API Gateway + S3 + DynamoDB on every transaction — that is the price of DB-per-tenant over object storage with fencing.
- Batching amortizes the CAS: 1335 rows/s local in batch (one CAS per 100 rows) vs 17.9 ops/s row by row. One and a half orders of magnitude from doing less coordination work.

## API-style benchmark (concurrency)

Second run on 2026-08-30 on the same box, this time with API-style concurrent traffic (the micro above is a single sequential client): 3 duration-bound scenarios with 8 vusers and 30 s per scenario (`--type api --vusers 8 --duration 30`), each vuser on its own thread with its OWN connection. `api_write_single`: all 8 writers INSERT to the SAME tenant; `api_write_multi`: vuser i writes to tenant i (pre-provisioned tenants); `api_mixed_multi`: same multi-tenant layout with 80% point SELECTs and 20% INSERTs. Engines: sqlite, postgres, mysql, tessera-hot, and tessera-serverless (tessera-store is left out: its Store path is a single-client durability measurement). ops/s is the scenario's aggregate total; p50/p95 are percentiles over all ops from all workers; serverless latencies include the retry-backoff sleeps — honest queueing, not invisible errors.

Documented and deliberate asymmetry: the SQL engines share ONE `bench_api` table with an indexed `tenant_id` column (shared-schema pattern), while tessera is DB-per-tenant — each vuser writes to its own sqlite database behind its own lease/endpoint and its statements carry no `tenant_id`. Both serve the same logical workload; the difference in the isolation model is exactly what is being measured. Errors are counted honestly: SQLITE_BUSY surviving `busy_timeout=5000` in sqlite goes to `busy_errors`; a 503 TenantBusy surviving the retry policy (up to 4 full-jitter retries, counted in `retries`) goes to `http_503`. Where this run has no latency data, `—`.

### api_write_single (8 writers → one tenant)

| Engine | ops/s | p50 (ms) | p95 (ms) | errors |
|---|---|---|---|---|
| sqlite | 2715 | 0.02 | 0.04 | 25 busy (WAL single-writer; busy_timeout=5000 absorbs) |
| postgres | 2963 | 2.13 | 5.99 | — |
| mysql | 842.6 | 8.39 | 17.66 | — |
| tessera-hot (1 owner) | 296.4 | 4.75 | 5.73 | — (the owner lock serializes) |
| tessera-serverless | 3.1 | 1895.6 | 5966.8 | 3× 503 (single lease + backoff) |

### api_write_multi (8 workers → 8 tenants)

| Engine | ops/s | p50 (ms) | p95 (ms) | errors |
|---|---|---|---|---|
| sqlite | 1998.3 | — | — | 30 busy |
| postgres | 2873.3 | 1.26 | — | — |
| mysql | 751.7 | 9.44 | — | — |
| tessera-hot (8 owners) | 449.5 | 4.57 | 6.85 | — |
| tessera-serverless | 9.4 | 799.0 | 1139.4 | 0× 503 |

### api_mixed_multi (80% reads / 20% writes, 8 tenants)

| Engine | ops/s | p50 (ms) | p95 (ms) | errors |
|---|---|---|---|---|
| sqlite | 11005.3 | — | — | — |
| postgres | 6781.5 | — | — | — |
| mysql | 3015.7 | 0.65 | — | — |
| tessera-hot | 1144.0 | 4.85 | — | — |
| tessera-serverless | 11.4 | 651.4 | — | — |

### Honest reading

- One tenant = one writer is tessera's central guarantee, and serverless mode shows the raw ceiling: 3.1 ops/s with second-scale latencies under 8 concurrent writers to the SAME tenant. Mitigations: client retry (already implemented in the harness), natural sharding of traffic by tenant, or a hot owner.
- With 8 tenants tessera scales: serverless mode goes from 3.1 to 9.4 ops/s (~3×, with 0× 503 when each worker writes sequentially to its own tenant) — lease isolation is the model.
- tessera-hot keeps p50 < 5 ms even with 8 concurrent writers on the same tenant (the owner lock serializes); in multi-tenant it is ~170× better p50 than serverless (4.57 vs 799 ms). The 449 ops/s aggregate is underestimated by the harness's GIL (client and owners run in the same Python process) — in production the client and the owner are separate processes.
- Mixed 80/20: lease-free reads carry the volume (tessera-hot 1144 ops/s).
- PG/MySQL have no writer ceiling: 2900+ ops/s writing concurrently to the same tenant (PG) — that is the fundamental trade-off of DB-per-tenant over object storage (isolation + zero operational cost vs per-tenant writer ceiling).

## Field findings: 503 TenantBusy

A `503 TenantBusy` was observed live when API Gateway routes to a sandbox that does not hold the tenant's lease (LEASE_TTL_MS of 60 s + 2 s retention after releasing it). Mitigations:

- **Client retry with backoff** (implemented in `benchmark.py`): up to 4 retries with full-jitter backoff (0.5 s base, doubling) only for 503/TenantBusy; any other status passes untouched.
- **Hot owner (level 2)**: retains the lease and removes the per-request acquire, so the condition disappears at the source.

## What this benchmark does NOT compare

- Multi-client concurrency: a single sequential client.
- Dedicated hardware: dev box; MySQL additionally suffers the container volume's fsync per commit.
- Real networks for local modes: LocalBackend is filesystem, not S3.
- Tenants of tens of GB: ~256 B payloads and one 2000-row cold query.

Indicative numbers, not marketing.

## Contention + auto-promotion benchmark (N writers, 2026-08-31)

Harness: [`benchmark_contention.py`](benchmark_contention.py) — 6 threads writing to the SAME tenant against the real serverless stack (us-east-2) with `EnableAutoPromote=true`. The timeline tells the full story:

| Phase | Window | What happens |
|---|---|---|
| Contention | t=0–25 s | 503 `TenantBusy` + direct writes in sticky-lease turns (~1.5 ops/s) |
| Transition | t≈25 s | The ephemeral owner (Fargate) acquires the lease and publishes its endpoint |
| Steady state | t=25–150 s | **100% forwarding** (`X-Tessera-Owner: 1`): 1338 writes, 0 errors, 10.7 ops/s |
| Post-burst | t+60 s idle | The owner shuts itself down and the task dies (0 residual tasks) |

- forwarded p50 = **554 ms** / p95 = 601 ms (includes ~550 ms South America→Ohio RTT: the harness saturates the client's network, not the system — the owner serves at ~0.6 ms).
- Promotion cold start: ~25 s with a cached image, ~40 s cold (image pull + boot + acquire with sticky-lease yield).
- Measured improvement: ~7x steady-state ops/s vs pure contention; for in-region clients the ceiling is the hot owner's (~300 writes/s).
- Burst cost: 0.25 vCPU Fargate task × burst duration + 60 s idle ≈ **fractions of a cent**.

Honest reading: promotion amortizes on sustained bursts (>1 min). On short spikes (<30 s) the burst ends before the owner is up and the system behaves as if the feature did not exist (sticky lease + retries): fail-open by design.

## Edge benchmark (CloudFront in front of the API, 2026-08-31)

Question: how much does CloudFront (`EnableEdge=true`) shave off for a client
far from the region? Measured from Argentina (POP `EZE50`, Buenos Aires)
against the real stack in us-east-2, 30 requests per cell, same tenant and
same Lambda — the only variable is the entry door. `CachingDisabled`:
nothing is cached here; the gain is pure transport (TLS to the local POP +
the AWS backbone with warm connections to the origin).

| Cell | Direct (API GW) | Edge (CloudFront) | Saving |
|---|---|---|---|
| write, fresh connection | p50 618 ms | p50 492 ms | **−126 ms (−20%)** |
| read, fresh connection | p50 587 ms | p50 274 ms | **−313 ms (−53%)** |
| write, keep-alive | p50 620 ms | p50 498 ms | −122 ms |
| read, keep-alive | p50 588 ms | p50 271 ms | −317 ms |

Reading:

- **Reads take the big prize**: −53%. The serverless read cost is almost
  all network, and the edge removes the full handshake RTT to the local
  POP (~10 ms to EZE vs ~190 ms to Ohio).
- **Writes gain less** (−20%): the real write (delta PUT + manifest CAS,
  both against S3, inside the Lambda) still costs the same; the edge only
  trims client transport.
- keep-alive ≈ fresh connection on both doors: API GW closes the TLS
  session frequently, so in practice you almost always pay a handshake —
  another reason the edge (which DOES keep its origin connections warm)
  wins so much on reads.
- For clients INSIDE the region (an internal Lambda in us-east-2) the
  edge adds nothing: it would be an extra hop. It is a feature for
  remote clients.

Cost: CloudFront has no fixed charge; with `CachingDisabled` you pay
transfer + requests (~$0.01/10k). For low internal traffic: cents.

## Serverless write breakdown (Phase 0 of the latency plan, 2026-08-31)

150 consecutive warm writes to one tenant, real us-east-2 stack,
`_TimingBackend` timings extracted from CloudWatch (`tessera.request`):

| Segment | p50 | p90 |
|---|---|---|
| **total server-side** | **70 ms** | 79 ms |
| lease (DynamoDB) | 0 ms | 0 ms |
| manifest CAS (S3 `PutObject` + `If-Match`) | 35 ms | 41 ms |
| delta PUT (S3) | 28 ms | 31 ms |
| rest (SQLite + runtime) | ~7 ms | — |

`aws_calls = 2` per write: the sticky lease (Phase 2) removed
acquire/release from the hot path and the warm write reached its
structural minimum — **two S3 PUTs and zero DynamoDB calls**: the
write-once delta and the CAS manifest (`If-Match`, S3's conditional —
DynamoDB only holds leases, and the sticky lease took it off the path).
**The plan's goal (p50 ≤ 250 ms) is beaten by 3.5x and we are AT the
estimated theoretical floor (70–120 ms)**; August baseline: 706 ms.

Implication for the plan's remaining phases:

- **S3 Express One Zone (A3.5)**: would trim only the PUT (28 → ~5 ms),
  i.e. ~23 ms of the 70 server-side — and ~4% of the end-to-end from
  South America (~490 ms via edge), where 85% of the cost is client
  transport. **Not justified today**; re-evaluate if a latency-sensitive
  in-region client appears (there, 70 → ~50 ms is indeed 30%).
- **Inline delta in DDB (A3.1–A3.4)**: would collapse PUT+CAS to ~35 ms
  total. Same verdict: a format change to gain ~30 ms that no consumer
  asks for. Frozen.
- For remote clients the lever order is: **edge (CloudFront, −126 ms
  measured on writes)** > moving the client to the region > any
  remaining server-side optimization.
- For sustained bursts the hot owner remains the answer (~0.6 ms/write
  once up).

## S3 Express One Zone for deltas (A3.5, 2026-08-31)

Same harness as Phase 0 (150 warm writes, real us-east-2 stack), with
`EnableExpress=true`: write-once deltas (`<tenant>/wal/`) go to an S3
Express One Zone directory bucket; manifest/snapshots/chunks stay in
standard multi-AZ S3. Verified in the bucket: all 150 deltas in Express,
zero in standard.

| Segment | standard | Express | delta |
|---|---|---|---|
| **total server-side p50** | **70 ms** | **55 ms** | **−21%** |
| delta PUT | 28 ms | 11 ms | −61% |
| manifest CAS (S3 `If-Match`) | 35 ms | 36 ms | = |
| lease | 0 ms | 0 ms | = |

- The write's residual is now the manifest CAS — a conditional
  `PutObject` to *standard* S3 (36 of 55 ms). The manifest cannot go to
  Express (it does not support `If-Match` on PUT), so the next real cut
  would be moving the CAS to DynamoDB or inline-delta (A3.1), both
  format/semantics changes that remain frozen.
- Accepted and bounded trade-off: deltas live in ONE AZ only between
  publish and compaction (afterwards they are folded into a multi-AZ
  snapshot and GC deletes them). Losing the AZ in that window loses the
  tenant's uncompacted writes.
- Cost: Express charges ~7x per GB-month vs standard, but deltas are KB
  and ephemeral; with compaction every 15 min resident storage is ~0.
  PUTs cost half of standard.
- Config: `express_bucket=` on `S3DynamoBackend`, `EXPRESS_BUCKET` on the
  handler/compactor, `TESSERA_EXPRESS_BUCKET` on the owner. Empty = off.

## Owner throughput spike (2026-09-01)

**Question**: the hot owner measured 296 ops/s with 8 concurrent writers
and p50 of 4.75 ms, when a local write costs ~0.6 ms. Where does the rest
go?

The August run's note attributed it to the owner's lock ("the owner lock
serializes"). **That was wrong.** Measuring the same work with and
without the HTTP layer:

| Path | ops/s |
|---|---|
| Real work (lock + INSERT + commit, no HTTP) | **58 472** |
| The same work over HTTP | **1 285** |

In other words: **98% of the cost was the HTTP layer**, not the lock or
SQLite. The lock is not the bottleneck; at 58k ops/s it has headroom to
spare.

### The cause: HTTP/1.0

`BaseHTTPRequestHandler` uses `protocol_version = "HTTP/1.0"` by default,
i.e. **no keep-alive**: the server closes the connection after each
response, and the client pays a fresh TCP handshake per write.

Fixed with two lines in the handler: `protocol_version = "HTTP/1.1"` and
`disable_nagle_algorithm = True` (the bodies are small and the response
is sent in two writes; waiting for the first one's ACK adds latency).

### Measured, same client with a persistent connection

| vusers | before (HTTP/1.0) | after (HTTP/1.1) | improvement |
|---|---|---|---|
| 1 | 802 ops/s | **1 949 ops/s** | **2.4x** |
| 2 | 1 271 | 1 665 | 1.3x |
| 4 | 860 | 1 411 | 1.6x |
| 8 | — | 1 145 | — |

p50 with 1 vuser: 0.53 → **0.28 ms**.

### Honest reading

- **The gain needs both ends.** Server keep-alive is useless if the
  client opens a fresh connection per request. The reference handler used
  `urllib.request` without a session, so its forwarding did not improve
  with this change: solved below.
- **Throughput drops as vusers rise** (1949 → 1145 with 8). That IS
  serialization, but the aggregate ceiling is already above what a single
  tenant needs; useful work per second does not grow because the lock
  serializes writes to the SAME tenant, which is the design guarantee,
  not a defect to fix.
- August's 296 ops/s were measured with a keep-alive-less client against
  an HTTP/1.0 server: **both ends paying handshakes**.

### Second half: the client (connection pool in forwarding)

With the server already on HTTP/1.1, the other end was missing. `urlopen`
opens a fresh TCP connection per request, so the reference handler kept
paying the full handshake on every forwarded write.

Measured against a real owner, isolating `_forward_to_owner`:

| Path | ms/write | ops/s |
|---|---|---|
| `urlopen` (fresh connection) | 0.595 | 1 681 |
| Persistent connection pool | **0.299** | **3 344** |

**2.0x**, and the saving (0.295 ms) is **half the forwarding cost** — on
loopback. Over a VPC network the handshake weighs more, not less, so this
is the lower bound of the improvement.

Design details:

- One dict per sandbox is enough: a Lambda sandbox serves one request at
  a time.
- A **dead** cached connection (the owner shut down idle, Spot took the
  task) is detected on use and retried ONCE with a fresh connection.
  Without that retry, every owner handover would cause a needless
  fallback to the slow path.
- If the owner is really gone, it falls back to the normal path
  (fail-open) **and the broken connection leaves the pool**, so it does
  not poison the next writes.

### What remains (and why it stops here)

With both ends fixed, the write-over-HTTP profile shows what dominates is
the **stdlib header parsing** (`email.parser`/`http.client`), not tessera
code. Getting below that would require replacing
`BaseHTTPRequestHandler` with a custom server: much more risk than the
throughput a single tenant needs justifies.

The useful-work ceiling is still 58 472 ops/s; HTTP is no longer the
limiting factor it was.

## Protocol spike: do gRPC / binary RPC help? (2026-09-02)

**Question**: with HTTP no longer being 98% of the cost, is switching
protocols worth it? Transport was measured ISOLATED (no-op handler, same
payloads), because what matters is the round-trip cost, not the work.

### One client

| Protocol | ms/round-trip | ops/s | vs HTTP/1.1 |
|---|---|---|---|
| **gRPC** (raw bytes, no protobuf) | 0.318 | 3 148 | **0.68x — slower** |
| HTTP/1.1 keep-alive (what exists today) | 0.215 | 4 641 | 1.00x |
| TCP + framing + JSON | 0.035 | 28 945 | **6.2x** |
| UNIX socket + framing + JSON | 0.021 | 46 899 | **10.1x** |

### 8 concurrent clients (the owner case)

| Protocol | ops/s |
|---|---|
| HTTP/1.1 keep-alive | 3 445 |
| gRPC | 3 820 |
| **TCP + framing** | **23 222** |

### gRPC is SLOWER than what we already have

Counterintuitive, so it was verified with three configurations
(`optimization_target=latency`, local subchannel pool, keepalive): all
give 0.30–0.33 ms. It is not a setup error — it is the cost of the
HTTP/2 + C-core stack crossing the Python binding on every call. gRPC
wins at other things (streaming, deadlines, multi-language, balancing),
but **not at the latency of a local unary RPC**, which is exactly our
case.

It would also bring: a heavy binary dependency (~50 MB, relevant in a
Lambda layer), `.proto` files to compile and version, and a rigid schema
for a payload that is today arbitrary SQL + params.

### The conclusion that matters: the protocol is the wrong lever

Binary framing over TCP would give 6x, and over a UNIX socket 10x. But
before writing a custom server it is worth looking at the other lever —
**not speeding up the round-trip, but making fewer round-trips**:

| Rows per request | ms/row | rows/s |
|---|---|---|
| 1 (today) | 0.296 | 3 381 |
| 10 | 0.0297 | 33 636 |
| 100 | 0.0036 | **277 643** |
| 500 | 0.0014 | **737 920** |

**Grouping 100 rows in one request gives 82x**, versus the 6-10x of the
best possible protocol, and **requires changing nothing in the
transport**: it already works today with the deployed owner.

### What if the consumer uses an ORM?

Batching is not a raw-SQL trick: **the ORM benefits the same way**, and
the idiomatic way of writing it is already the fast one. What costs in
tessera is the **write** (write-once delta + manifest CAS), not the
statement — so the only thing that matters is how many rows go into ONE
transaction.

Measured with `TesseraORM` (60 rows, minimum of 3 runs):

| How it is written | ms/row | rows/s | |
|---|---|---|---|
| One `db.write()` per row | 43.3 | 23 | ← the mistake |
| `session.add()` in a loop, **one** `db.write()` | 0.765 | 1 307 | **57x** |

**The 57x comes purely from putting the loop inside the `write`.** No
bulk `insert()`, no new API, no touching the transport: it is idiomatic
ORM.

With SQLAlchemy 2.0's bulk `insert()` the ORM collapses the N rows into
ONE statement (verified by capturing the emitted SQL: 200 `INSERT`s → 1),
but that moves the needle little, because the statement was never the
bottleneck: the cost is in the delta + CAS per write.

The practical consequence for whoever integrates: **batch the unit of
work, not the rows**. One `db.write()` per HTTP request of your API is
fine; a `db.write()` per row inside a loop is the anti-pattern.

### Verdict

- **gRPC: no.** Slower in our use case, and adds weight and rigidity.
- **Custom binary protocol: not yet.** The 6-10x is real, but it implies
  maintaining a custom server and client, with their bug surface, for a
  gain that batching beats by an order of magnitude.
- **Batching: yes, and it already works.** Documentation should push
  consumers to group writes rather than change the transport.

### And at the code level? (profiling the write, 2026-09-02)

With transport ruled out, the `write` path was profiled internally. Two
findings:

**1. `asdict()` in manifest serialization.** The profile showed 31 500
calls to `dataclasses.asdict` for 150 writes — 210 per write.
`DeltaEntry` and `ChunkEntry` are **flat** dataclasses (only `str`/`int`,
no nesting), so `asdict`'s recursive copy is pure overhead; `vars()`
returns the same. And the manifest is serialized on **every** write,
inside the CAS:

| Manifest | before (`asdict`) | after (`vars`) | |
|---|---|---|---|
| v1, 5 deltas | 0.0123 ms | 0.0062 ms | 2.0x |
| v1, 50 deltas | 0.0927 ms | 0.0312 ms | 3.0x |
| v2, 20 deltas + 200 chunks | 0.4174 ms | 0.1452 ms | **2.9x** |

The gain **grows with manifest size**: a large tenant saves 0.27 ms per
write. Safe because `json.dumps` does not mutate what it receives
(verified: two consecutive `to_json()` produce the same string and the
manifest stays intact).

**2. The fsyncs are the local backend's, not production's.** The profile
showed 1 500 `fsync`s for 150 writes dominating the time, but they come
from `LocalBackend` (atomic write: temp + fsync + rename), which is the
dev-and-tests backend. In production the backend is S3, which does no
local fsync. **Optimizing it would move nothing real**, so it was left
as is.

After the fix, `dataclasses` disappears from the profile: what remains is
backend I/O, not tessera code.

If batching ever stops being enough (a case with thousands of
independent writes/s that cannot be grouped), the option is a **UNIX
socket with framing** for same-host clients, not gRPC.

## Real cost: the estimated table, replaced by measurement (2026-09-02)

The README cost table had been an **order-of-magnitude estimate** since
it was written, and it was the last unmeasured thing in the project. It
was replaced with data from two sources, neither of them "per the AWS
price list":

### 1. Unit prices, from the real account's Cost Explorer

Derived from billed usage 08/28–09/01 (account REDACTED, us-east-2),
dividing cost by quantity:

| Resource | Derived price | AWS list |
|---|---|---|
| S3 PUT/POST/LIST (Tier1) | $0.005001 / 1k | $0.005 / 1k |
| S3 GET (Tier2) | $0.000400 / 1k | $0.0004 / 1k |
| DynamoDB WRU | $0.6249 / M | $1.25 / M |
| DynamoDB RRU | $0.1242 / M | $0.25 / M |

S3 matches the list. DynamoDB comes out at half because part of the usage
fell in the free tier — the table uses the derived price, which is what
the account actually pays.

### 2. Operations per write, counted in the code

Instrumenting the backend with a call-counting proxy, over 10 writes
with the sticky lease active:

| Operation | Per write |
|---|---|
| `put` | **3.0** |
| `renew_lease` | 0.3 |
| `get` | 0.2 |
| `acquire_lease` | 0.1 |

**This is where the old table's error appeared**: it said "2 PUTs per
write" and it is **three** — the write-once delta, the manifest CAS, and
the **history archive** added with the backup/restore plan. The estimate
predated that capability and nobody reviewed it: exactly how an estimated
number silently goes stale.

### The result

| Scenario | Measured total/month |
|---|---|
| 500 sleeping tenants (100 writes/month each) | **$1.40** |
| 50 active tenants (10k writes/month each) | **$13.96** |
| 1 intensive tenant (1M writes/month) | **$27.92** |

The old estimate said "$3–5/mo" for light use and "$30–50/mo" for medium
use. The order of magnitude was right; the composition was not (it blamed
DynamoDB, which the sticky lease had already removed from the hot path).

### Batching also makes it cheaper

Since the bill is 97% requests (S3 PUTs + Lambda invocations) and not
bytes, grouping N rows into one write divides those requests by N:

| Scenario | 1 row/write | 100 rows/write |
|---|---|---|
| 500 sleeping tenants | $1.40 | **$0.54** |
| 50 active tenants | $13.96 | **$5.43** |
| 1 intensive tenant | $27.92 | **$10.86** |

**2.6x cheaper with the same rows written.** Batching is not just a
latency optimization: it is the largest cost lever a consumer has, and
it requires changing nothing in the infrastructure.

## Lambda memory: 1769 → 512 MB (2026-09-02)

Phase 1 of the latency plan raised the function to **1769 MB** (exactly 1
vCPU) when the write took ~700 ms and real CPU was at play. After Phases
0-2 the warm write landed at 70 ms, and the breakdown showed almost all
of it is **network wait**, not compute:

| | ms |
|---|---|
| Real CPU per write (measured with `process_time`) | **3.5** |
| Waiting on S3 (`manifest_ms` + `s3_put_ms`) | ~62 |

In other words: **95% of billed time is waiting on S3**, and waiting does
not go faster with more MB. We were paying GB-seconds of vCPU to block
on the network.

### The experiment

Real stack in us-east-2, same code and same load (120 warm writes per
configuration, new tenant), varying only `MemorySize`. Billed duration
from CloudWatch Logs, which is what is actually paid:

| MemorySize | p50 billed | p95 billed | Peak used | $/1M writes (duration) |
|---|---|---|---|---|
| 1769 MB | 73 ms | 87 ms | 111 MB | $2.10 |
| **512 MB** | **76 ms** | **101 ms** | 111 MB | **$0.63** |
| 256 MB | 98 ms | 168 ms | 111 MB | $0.41 |

1769 was re-run at the end to rule out drift (73 → 73 ms).

**Decision: 512 MB.** Costs **71% less** for **+3 ms of p50**. 256 MB was
discarded for two concrete reasons: p95 rises 81 ms (+81%), and the peak
memory used is 111 MB — headroom for a tenant with a large working set
would be too tight.

Methodological note: measured end-to-end from Argentina, the three
configurations all give ~610 ms and look identical, because the RTT hides
the difference. The number that matters is server-side **billed
duration**; end-to-end is useless for this decision.

### Effect on the bill

| Scenario | 1769 MB | 512 MB |
|---|---|---|
| 500 sleeping tenants | $1.40 | **$1.05** |
| 50 active tenants | $13.96 | **$10.50** |
| 1 intensive tenant | $27.92 | **$21.00** |

With S3 now dominating even more (99% of the bill), batching now makes
it **3.9x** cheaper instead of 2.6x.
