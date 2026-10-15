---
title: Measurements
nav_order: 4
---

# Measurements

Every number on this page carries a label:

| Label | Meaning |
|---|---|
| **measured** | Comes from a committed, dated JSON artifact in [`docs/measurements/`](measurements/). Reproduce it with the command shown. |
| **derived** | Computed from measured counters (e.g. PUTs per write → monthly cost). The arithmetic is shown; the inputs are measured. |
| **hypothetical** | A projection we have NOT measured. It is a hypothesis to test, not a result. |

Anything without a label on this page is **derived**. The 2026-10-07 table
below includes a real AWS run (us-east-2); everything older predates the
v2 durability path and is linked at the bottom.

## The protocol's IO contract — measured, deterministic

The per-write cost is fixed by the protocol, not by the hardware. Pinned
by [`tests/test_io_profile.py`](https://github.com/korz-sh/tessera-db/blob/main/tests/test_io_profile.py)
in CI — a regression that adds a GET or a fourth PUT fails the suite.

| Path | Object ops per committed write | Label |
|---|---|---|
| `Store.write` warm path (retained lease) | **3 PUTs, 0 GETs** — delta + history + manifest CAS | measured |
| Owner `POST /write` (hot path) | **3 PUTs + 1 GET** — the owner re-fetches the manifest before every CAS | measured |
| `Store.write` cold path | +1 lease acquire +1 manifest GET (+hydration GETs on a cold cache) | measured |

## Micro benchmark — 2026-10-07 (300 rows, batch 100, 20 warmup, 1 client)

Same workload, same process, three backend modes. `fs` measures pure
coordination overhead (filesystem objects); `endpoint` runs the identical
`S3DynamoBackend` code path against ministack — real HTTP + conditional
writes, **not** production S3 latency; `aws` is the same backend against
real S3 + DynamoDB in `us-east-2`, measured from a client outside the
region (WAN RTT included — an in-region client would be faster).

| Workload | Engine | fs p50 ms | ministack p50 ms | AWS p50 ms | AWS p95 ms | Label |
|---|---|---|---|---|---|---|
| insert_row | sqlite | 0.03 | — | 0.02 | 0.04 | measured |
| insert_row | tessera-store | 58.4 | 30.3 | 1,215.7 | 2,135.1 | measured |
| insert_row | tessera-hot | 39.1 | 22.1 | 1,528.2 | 4,126.9 | measured |
| insert_batch (per batch) | tessera-store | 65.9 | 57.5 | 1,425.8 | 1,468.9 | measured |
| insert_batch (per batch) | tessera-hot | 4,418 | 2,607 | 155,895.9 | 174,623.2 | measured |
| point_select | tessera-store | 34.5 | 25.2 | 218.4 | 262.1 | measured |
| point_select | tessera-hot | 0.57 | 0.57 | 0.55 | 0.65 | measured |
| update_row | tessera-store | — | — | 1,211.1 | 1,779.3 | measured |
| update_row | tessera-hot | — | — | 1,435.2 | 2,212.8 | measured |
| cold_first_query_vfs | — | 282.1 | 355.9 | 2,211.3 | — | measured |

Reading the AWS column: every write pays a synchronous S3+delta+manifest
CAS round-trip over WAN (~1.4 s), so `tessera-hot`'s batch phase serializes
300 row-writes behind the publish loop (~1.56 s/row — the per-batch p50 is
100 rows through a shared lock). Warm `point_select` stays at SQLite-local
latency (0.55 ms) — reads never touch the network. `tessera-hot` write
numbers come from a same-day follow-up run (`--only tessera-hot`) after
stale-tenant cleanup; identical parameters, bucket and table — noted in the
artifact's `meta.note`.

### Final-engine run — measured (2026-10-15, WAN client, us-east-2)

Same workload (300 rows, batch 100) on the released `2.0.0` engine with
the publish-path improvements (2 RTTs per publish instead of 4, cached
manifest, transient publish retry, bounded parallel hydration):

| Workload | Engine | `per_commit` p50 ms | `windowed` p50 ms | Label |
|---|---|---:|---:|---|
| insert_row | tessera-hot | **1,102.5** | **0.68** | measured |
| insert_batch (per batch) | tessera-hot | 157,257.5 | **65.9** | measured |
| point_select | tessera-hot | 0.56 | 0.56 | measured |
| update_row | tessera-hot | 1,509.6 | **0.64** | measured |
| insert_row | tessera-store | 1,441.4 | 1,373.1 | measured |
| cold_first_query_vfs | — | 3,809.9 | **2,085.3** | measured |

`per_commit` insert_row improved **1,528 → 1,102 ms (−28%)** — the two
removed manifest GETs dropped exactly the predicted ~2 RTTs. WAN
conditions on this day were noisier (p95s up across the board), so
`tessera-store` and the `per_commit` cold-read cells regressed slightly
within run-to-run WAN variance. Under `windowed` the engine folded
963 commits into **3 publishes / 8 PUTs (0.0083 PUTs/commit)** —
the write-cost contract is unchanged on the final engine, and the
windowed cold read improved (parallel hydration).

Artifacts: [`micro-aws-2026-10-15.json`](measurements/micro-aws-2026-10-15.json),
[`micro-aws-windowed-2026-10-15.json`](measurements/micro-aws-windowed-2026-10-15.json),
[`micro-fs-2026-10-15.json`](measurements/micro-fs-2026-10-15.json).

Reproduce:

```bash
python examples/benchmark.py --type micro --rows 300 --batch 100 \
  --only sqlite,tessera-store,tessera-hot \
  --json-out docs/measurements/micro-fs-$(date +%F).json

# against a ministack endpoint (docker run -p 4566:4566 ministackorg/ministack):
python examples/benchmark.py --type micro --rows 300 --batch 100 \
  --only sqlite,tessera-store,tessera-hot \
  --tessera-backend endpoint --tessera-endpoint http://localhost:4566 \
  --json-out docs/measurements/micro-ministack-$(date +%F).json

# against real AWS (pre-provisioned scratch bucket + lease table):
python examples/benchmark.py --type micro --rows 300 --batch 100 \
  --only sqlite,tessera-store,tessera-hot \
  --tessera-backend aws --tessera-bucket <bucket> --tessera-table <table> \
  --json-out docs/measurements/micro-aws-$(date +%F).json
```

Artifacts: [`micro-fs-2026-10-07.json`](measurements/micro-fs-2026-10-07.json),
[`micro-ministack-2026-10-07.json`](measurements/micro-ministack-2026-10-07.json),
[`micro-aws-2026-10-07.json`](measurements/micro-aws-2026-10-07.json),
[`micro-aws-inregion-2026-10-07.json`](measurements/micro-aws-inregion-2026-10-07.json),
[`micro-2026-10-06.json`](measurements/micro-2026-10-06.json) (1000 rows, fs).

### WAN vs in-region (same bucket+table, us-east-2) — measured

The in-region run used a `t3.micro` in the same region (100 rows, batch 50).
WAN p50s from the table above ÷ in-region p50s: the WAN RTT (~250 ms × 4-5
serialized AWS calls per write) dominates the entire write latency.

| Workload | Engine | WAN p50 ms | in-region p50 ms | factor |
|---|---|---:|---:|---:|
| insert_row | tessera-store | 1,215.7 | 89.6 | 13.6× |
| insert_row | tessera-hot | 1,528.2 | 117.8 | 13.0× |
| insert_batch | tessera-store | 1,425.8 | 92.9 | 15.3× |
| insert_batch | tessera-hot | 155,895.9 | 6,137.2 | 25.4× |
| point_select | tessera-store | 218.4 | 12.7 | 17.2× |
| point_select | tessera-hot | 0.55 | 0.91 | local both ways |
| cold_first_query_vfs | — | 2,211.3 | 208.7 | 10.6× |

### Read path and egress — measured (2026-10-07, WAN client, us-east-2)

Backend calls counted with a wrapping probe (`Counting` proxy over
`S3DynamoBackend`), 2000-row format-2 tenant (~200 KB of payload):

| Read | Backend ops | Egress bytes | Wall (WAN) |
|---|---|---:|---:|
| Cold first query, empty cache | 2 GETs + 1 HEAD | **4,194,721 B** (~21× payload) | 2.32 s |
| Warm point-select ×10 | 1 HEAD/read, 0 GETs | 0 | 215 ms/read |
| Warm full scan (all rows) | 1 HEAD, 0 GETs | 0 | 213 ms |

Reads are nearly free in *request* terms (a HEAD freshness check per read,
~$0.40/million); the cost driver is **egress on cold hydration** — the
chunk format fetched ~4.2 MB to answer a query over ~200 KB of logical
data. At $0.09/GB that's ~$0.0004 per cold read of this tenant; it scales
with retained bytes, not with query count.

### Windowed publish amortization — measured (2026-10-08, fs backend)

`--tessera-publish-mode windowed` folds every commit inside
`publish_every_s` into ONE publish. Backend ops are counted by a
`_CountingBackend` proxy and land in the artifact's
`meta.tessera_backend_ops` (60 rows, batch 30, 2.0 s window; commits
include seeding):

| Metric | `per_commit` | `windowed` (2 s) | factor |
|---|---:|---:|---:|
| Measured PUTs / commit | 3.02 | **0.033** | **91× fewer** |
| Publishes for 242 commits | 244 | 3 | — |
| insert_row p50 | 144.0 ms | **0.66 ms** | 218× |
| insert_batch p50 (per batch) | 2,901 ms | **19.6 ms** | 148× |
| update_row p50 | 88.5 ms | **0.66 ms** | 134× |

Trade-off: `/write` ACKs at local commit; commits ahead of `published`
live only in the owner's WAL until the window flushes (durability bound =
`publish_every_s`; `sync: true` or `POST /flush` closes it on demand).
The request-side saving is proportional to commits-per-window — at this
measured ~80 commits/window the S3 write-path cost drops from ~$15.5 to
**~$0.17 per 1M writes** (derived from the measured op ratio, Ohio list
prices). Artifacts:
[`micro-fs-per-commit-2026-10-08.json`](measurements/micro-fs-per-commit-2026-10-08.json),
[`micro-fs-windowed-2026-10-08.json`](measurements/micro-fs-windowed-2026-10-08.json).
Reproduce: add `--tessera-publish-mode windowed --tessera-publish-every-s 2.0`
to the fs command above.

**Real AWS (2026-10-08, us-east-2, WAN client)**: same workload shape as
the per-commit AWS row above (300 rows, batch 100), `windowed` 2 s:

| Metric | `per_commit` (measured 10-07) | `windowed` (measured 10-08) |
|---|---:|---:|
| PUTs / commit | ~3.0 | **0.0083** (8 PUTs / 963 commits) |
| Publishes | ~965 | 3 |
| insert_row p50 | 1,528 ms | **0.68 ms** |
| insert_batch p50 (per batch) | 155,896 ms | **69.8 ms** |
| update_row p50 | 1,435 ms | **0.68 ms** |
| point_select p50 | 0.55 ms | 0.61 ms (local read, unchanged) |

361× fewer PUTs per commit measured on real S3 — the derived write cost
drops from ~$15.5 to **~$0.043 per 1M writes**. The manifest on the
resulting tenant reads `version 965` with only 2 history archives and 1
wal delta: 963 commits folded into 3 publishes, manifest versions sparse
(the version-jump contract working on real AWS). Artifact:
[`micro-aws-windowed-2026-10-08.json`](measurements/micro-aws-windowed-2026-10-08.json).

### History growth and GC on AWS — bounded since 2026-10-08

Every publish archives the predecessor manifest as `history/<v>.json`
(one object per version). Destructive GC on S3/DynamoDB is **now
supported** (`supports_fenced_gc = True`): the sweep marks the complete
reference closure (current manifest + all retained history — every
archive is written before its successor's manifest CAS), then deletes
candidates in `gc_delete_batch` chunks, each gated by
`ensure_alive()` plus a skew-margin check (`expires_at - 1 s`) so no
delete can be issued once a successor could legally take the lease.
Verified under moto (`tests/test_s3_gc.py`): sweeps shrink object count,
mid-sweep lease loss aborts with zero further deletes, expired/near-expiry
leases and cross-tenant keys are rejected.

**Live-AWS validated 2026-10-08** (us-east-2, scratch bucket, 30-version
v2 tenant, `gc_keep_history=5`): 64 objects → 38 after one sweep — 26
`history/*.json` deleted via batched `delete_objects`, dry-run deleted
nothing, post-GC reads returned all 30 rows, and the set of gone keys
matched the sweep report exactly (0 missing, 0 extra). After `compact()`,
the current manifest listed `deltas: 0`/`chunks: 1` while the oldest
retained history (v28) still listed 26 deltas — the 30 wal objects stay
correctly inside the mark closure until every retained archive post-dates
the compaction. Remaining unmeasured on AWS: concurrent write during a
sweep and crash-resume mid-sweep.

Earlier dry-run cost measured on a 60-version tenant: 2 GETs + 4 LISTs,
1.2 s — the mark pass costs ~1 GET per archived history version,
O(versions) per run. Windowed publish is what makes this tractable:
versions grow per *publish window*, not per write.

**Consequence**: retained bytes are now O(retained_versions + live data)
instead of unbounded — but only once an operator actually runs `gc()`
(or schedules it). There is still no automatic GC scheduling inside the
owner; retention policy is `gc_keep_history` + `gc.grace_s`.

### Sustained 8-hour AWS soak — measured (2026-10-09, WAN client, us-east-2)

`examples/soak.py` drove a continuous mixed workload against real
S3 + DynamoDB for **8.32 h** (`windowed` publish, 1 s window, ~10
writes/s + ~10 reads/s): fenced-owner failovers, fenced GC sweeps and
compactions every 20 min, sampled ledger verification every 2 min.
Every acknowledged write was reconciled at `data_version` before the run
closed. Artifact: [`soak-aws-8h-2026-10-09.json`](measurements/soak-aws-8h-2026-10-09.json).

| Metric | Result | Label |
|---|---:|---|
| Elapsed | 29,969 s | measured |
| Committed writes / reads | 90,189 / 92,789 | measured |
| Durability violations / unverified | **0 / 0** | measured |
| Lost unpublished writes on failover | 0 | measured |
| S3 PUTs / commit | **0.2167** | measured |
| Publishes / commit | 0.0722 | measured |
| Write p50 / p95 / p99 | 1.09 / 1,676 / 2,236 ms | measured |
| Fenced failovers (epoch 2→8) | 6, worst takeover 102 s | measured |
| Fenced GC sweeps | 6 | measured |
| Compactions (no `data_version` regression) | 5 | measured |
| Final objects / bytes | 13,028 / 602 MB | measured |

The run passes the harness's mechanical stable gate (≥8 h, ≥10k commits,
≥2 of each drill, zero violations, zero unverified, bounded object
growth) and is pinned as `stable`-stage evidence in
[`release/gates.json`](../release/gates.json). PUTs/commit stayed under
1.0 across 90k commits — windowed amortization holds at sustained
scale, not just in micro runs. GC kept object count from tracking
commits 1:1 but retained bytes still grow within a run's retention
window; long-run sawtooth behaviour needs an even longer observation
window than 8 h.

## Monthly cost model — derived

From the measured 3 PUTs/write (plus ~$0.023/GB-mo storage): the README's
three scenarios — **$1.05/mo** (500 idle tenants), **$10.50/mo** (50
active), **$21.00/mo** (1M writes one tenant), **$5.32/mo** with batching.
All **derived**, priced at Ohio list rates in
[`tessera-cost-model-b1-2026-10-03.json`](tessera-cost-model-b1-2026-10-03.json).
The model does NOT yet count read-path GETs, egress, or NAT — treat
totals as a floor, not a bound. History growth is now boundable by GC
(see the section above); under `windowed` publish both version count and
PUT cost scale per publish window, not per write.

## Live-AWS evidence (pre-v2)

Snapshots from real account runs in September 2026, restored to the tree
from git history:

- [`docs/metrics/consumer30-whole-stack-cost-2026-09-29.json`](metrics/consumer30-whole-stack-cost-2026-09-29.json)
- [`docs/metrics/lambda-billing-snapshot-live-2026-09-29.json`](metrics/lambda-billing-snapshot-live-2026-09-29.json)
- [`docs/metrics/stack-cost-snapshot-validation-2026-09-29.json`](metrics/stack-cost-snapshot-validation-2026-09-29.json)
- [`tessera-cost-model-b1-2026-10-03.json`](tessera-cost-model-b1-2026-10-03.json)
- [`turso-cost-model-2026-09-07.json`](turso-cost-model-2026-09-07.json)
- Narrative cost/benchmark reports are consolidated in
  [Economics history](ECONOMICS.html) and the
  [historical benchmarks appendix](#historical-benchmarks-pre-v2) below.

They predate the v2 durability contract. The first post-v2 AWS run is the
`aws` column of the 2026-10-07 table above
([`micro-aws-2026-10-07.json`](measurements/micro-aws-2026-10-07.json)) —
one client, us-east-2, WAN RTT. A multi-region / in-region matrix is still
**hypothetical**.

## How these numbers are guaranteed

1. **Deterministic CI gate** — `tests/test_io_profile.py` pins exact
   backend op counts per write; any protocol-level cost regression fails
   the main suite on every PR.
2. **Continuous measurement** — `bench.yml` runs the micro benchmark
   against a ministack service on every PR + weekly, archiving the JSON
   for 90 days (`bench-micro-*` artifacts).
3. **Label discipline** — this page is the contract: no number appears
   without `measured`, `derived`, or `hypothetical`, and every measured
   number names its artifact, date, and backend.

## Historical benchmarks (pre-v2)

Both documents below predate the v2 durability contract. They are kept
verbatim as evidence of what was measured then; every figure is labeled
with its date and context. Linked artifacts not present in `docs/` remain
in git history.

## v1 Store / v2 Runtime durable-write benchmark

The runner (`experiments/tessera_v1_v2_benchmark.py`) compares one acknowledged
transaction at a time. Each transaction inserts ten 32-byte values under the
same schema. v1 uses `Store.write`; v2 uses `Runtime.submit` with one-operation
batches and the default snapshot publication mode. Both return only after the
backend has published the transaction. The runner alternates engine order,
uses a fresh tenant/group for each run, closes the writer, opens a new reader
and checks every row. It also compares every v2 operation ID with the durable
receipt set in the published frontier. v1 has no equivalent durable receipt
contract, so its recovery check covers rows, not idempotent retry.

These numbers measure this serial, small-database workload. They do not predict
consumer scan throughput, multi-group scaling or a different v2 publication mode.
The workload uses a simplified table rather than the consumer app's full SQL corpus.

### RC1 AWS comparison, 2026-10-02

The source-pinned report ran the RC tag commit `45765e022e4d12991396ca950eacbcd1e30b8056` in account `REDACTED`, region `us-east-2`, against isolated tagged S3/DynamoDB resources. It alternated v1 `Store.write` and v2 `Runtime.submit` with the same schema, five transactions of five rows, two repetitions, serial concurrency, and ACK only after the published transaction.

Median throughput was **0.748 tx/s for v1** and **0.607 tx/s for v2**; v2 was **23.3% slower** in this small-transaction sample. Median ACK latency was about **1.14 s for v1** and **1.53 s for v2**. Cold recovery was 1.49–1.70 s. v2 reconciled all five durable receipts per run; v1 has no equivalent receipt contract. The exact temporary bucket and table were deleted, then independently checked as S3 404 and DynamoDB `ResourceNotFoundException`.

This is a correctness and baseline measurement, not a capacity claim or invoice. The sample is intentionally bounded; the attempted larger 20-transaction run exceeded the local runner timeout and was cleaned up successfully. The next useful comparison is a batched consumer-sized transaction and a concurrency sweep, because one-row-at-a-time v2 pays a durable receipt/publication cost that batching can amortize. A same-RC LocalBackend run is archived for debugging, but its final repetition hit severe host-I/O outliers and is not a capacity result.

### LocalBackend

Run from commit `8636c66787e25609878d63f5bfc0beca4e28d2ae`:

```bash
python -m examples.tessera_v1_v2_benchmark \
  --transactions 60 --rows-per-transaction 10 --repetitions 3
```

The raw report contains every run and
its median, p95 and p99 ACK latency. The median of three throughput runs was
**24.53 transactions/s for v1** and **14.97 transactions/s for v2**. Both
recovered all 600 rows per run; v2 also reconciled all 60 receipts per run.
This local measurement includes filesystem publication, not AWS latency.

### AWS staging

The remote variant creates a tagged, isolated S3 bucket and on-demand DynamoDB
table in account `REDACTED`, region `us-east-2`, and deletes them in a
`finally` block. It rejects any other account and supports explicit cleanup
from the saved state file. The script never points at the consumer app's deployed data.

```bash
python -m examples.tessera_v1_v2_benchmark --aws-staging \
  --transactions 20 --rows-per-transaction 10 --repetitions 2 \
  --state /tmp/tessera-v1-v2-aws-state.json \
  --output /tmp/tessera-v1-v2-aws.json \
  --source-commit 8636c66787e25609878d63f5bfc0beca4e28d2ae
```

The report records observed latency and throughput, not an AWS invoice. Billing
depends on S3 request/storage and DynamoDB request usage; no attributable
charge is claimed from this short run.

The raw staging report used 20
transactions per engine per repetition and recovered all 200 rows each time.
The median throughput across two runs was **0.839 transactions/s for v1** and
**0.648 transactions/s for v2**. The v1 median ACK latency was 1.116–1.133 s;
the v2 median was 1.501–1.512 s. v2 reconciled every durable receipt after a
fresh owner start. The v1 recovery checks took 4.76–4.84 s; v2 took
1.46–1.65 s in these runs. Recovery here means opening a new reader and
checking rows and receipts, not recovering a failed application process.

The report's `cleanup_complete` is true. Independent checks returned S3 404
and DynamoDB `ResourceNotFoundException` for the exact temporary names;
prefix-filtered bucket and table listings were empty. These results are a
small serial sample and should not be used as capacity or cost limits.

### Consumer findings stage, 2026-09-23

The findings benchmark (`experiments/consumer_findings_modes_benchmark.py`) uses
the exact repository DDL from the consumer app commit
`3f25629238bfccda913efa8afa381d9f101b9760`, including its six findings
tables and attempt manifest. It inserts 500 issues with the `scan_issue` SQL
shape from `consumer/db/scan_results.py`, then closes the writer and checks the
published rows from a new reader. v2 also reconciles every receipt. It covers
the findings-heavy persistence stage, not the complete scanner or catalog
summary. Both schema file hashes are in each machine-readable report.

From Tessera commit `8af65bbaf95836e1fd81cd45d192172f0cf9f5fc`, with
consumer checked out at the commit above:

```bash
python -m examples.consumer_findings_modes_benchmark \
  --consumer-ddl-dir /path/to/consumer-api/consumer/db/ddl \
  --findings 500 --chunk 40 --repetitions 2
python -m examples.consumer_findings_modes_benchmark \
  --consumer-ddl-dir /path/to/consumer-api/consumer/db/ddl \
  --findings 500 --chunk 64 --submit-concurrency 8 --repetitions 2
```

Add `--aws-staging --state /tmp/consumer-findings-state.json --output
/tmp/consumer-findings-report.json` for the bounded staging run. The runner
verifies account `REDACTED`, tags resources, cleans up in `finally` and
supports `--cleanup` with the state file if interrupted.

consumer v1 stores those findings in one `Store.write` transaction. The v2
`Operation` contract permits at most 64 statements and 64 KiB, so a 500-issue
scan cannot be submitted as 500 individual insert statements in one operation.
The benchmark's 500 insert statements encode to 150,823 bytes; even one
multi-row `INSERT` with the same 5,500 parameters encodes to 73,967 bytes,
above the current payload bound. Real messages may be larger, and the consumer app's
configured cap is 5,000 stored issues per scan.
The first v2 measurement used 13 sequential operations of up to 40 inserts.
Its local report and
AWS report each contain two runs
per mode:

| Mode | AWS completion, 500 issues | Operations |
|---|---:|---:|
| v1 Store | 2.58–3.02 s | 1 |
| v2 snapshot | 19.95–20.73 s | 13 |
| v2 incremental | 23.72–24.41 s | 13 |
| v2 native | 19.82–20.29 s | 13 |

The next local measurement submitted eight operations of up to 64 inserts
concurrently. Its raw report
shows one publication batch and one shared published version for all eight
receipts in each successful v2 run. Native completed in 0.12–0.20 s versus
v1's 0.07–0.08 s. This is a useful performance path, but a publication batch
does **not** make the eight operations one atomic scan: when one operation
fails, the others can still publish. A local injected-conflict rehearsal
returned seven published receipts and one failed operation, with only 373
issues visible. the consumer app's current one-transaction findings guarantee would need
an explicit replacement before routing a real scan through this path. That
work is tracked in [issue #71](https://github.com/korz-sh/tessera-db/issues/71).

The batched AWS report repeated
the eight concurrent operations twice. All eight receipts shared one
published version in each run, and all 500 issues survived a cold owner
restart. Completion times were **2.47–2.96 s for v1**, **1.78–1.79 s for v2
snapshot**, **1.98–2.10 s for incremental**, and **1.89–1.90 s for native**.
Those are successful-case timings; the partial-failure boundary above still
prevents routing a consumer scan through this form of batching.

Both AWS campaigns used independent tagged resources in the staging account and
removed them. Independent exact-name checks returned S3 404 and DynamoDB
`ResourceNotFoundException`; prefix-filtered bucket and table listings were
empty. No billed cost was attributed to these short tests. The remaining
capacity question is whether a **single atomic v2 scan operation** can support
the full consumer payload within documented limits, then how that operation
performs in each publication mode.

### Atomic consumer findings path, 2026-09-24

`BulkOperation` admits up to 8,192 managed statements and 8 MiB of canonical
payload. It runs as one SQL transaction and produces one durable receipt. The
benchmark accepts `--atomic` to submit one operation per scan, with a bounded
queue sized for that payload. Its report includes encoded operation bytes and
retained physical object bytes per publication mode.

```bash
python -m examples.consumer_findings_modes_benchmark \
  --consumer-ddl-dir /path/to/consumer-api/consumer/db/ddl \
  --atomic --scans 2 --findings 5000 --repetitions 1 \
  --output /tmp/tessera-v2-atomic-local.json
```

The two-repetition local report
encoded **1,523,174 bytes per 5,000-finding operation**. All three v2 modes
published exactly two versions per repetition and recovered all 10,000 issue
rows and both receipts. Median completion over two scans was 0.85–1.04 s
snapshot, 0.92–1.15 s incremental and 0.58–0.68 s native; v1 was 0.06–0.20 s.
Retained local objects after two scans occupied 3.16 MB snapshot,
2.45 MB incremental and 2.06 MB native, including initial images and history.
These are small local samples, not AWS capacity or monthly cost estimates.

The owner test injects a duplicate-key error at issue 373 of 500 and verifies
that no issue or receipt publishes, then retries a valid complete operation.
The benchmark covers the issue-heavy findings stage; the consumer app's attempt manifest,
baseline reads, other findings tables and catalog publication still need a
consumer-level trial before production routing. Remote physical GC remains
disabled, so retained bytes grow until that lifecycle is safely implemented.

The 12-scan native cycle
exposed an engine log rewrite at the fifth 5,000-finding scan. Native
publication now falls back to a verified full checkpoint when the local log
does not extend the published prefix. The repaired run recovered all 60,000
rows and 12 receipts, with checkpoints at versions 5 and 10. It retained
24.89 MB of local objects after 12 scans. This workload checkpoints more
often than the nominal eight-segment schedule, so cost projections must use
measured checkpoint frequency rather than assuming one checkpoint per nine
publications. The injected-log-rewrite regression also verifies cold recovery
of both receipts after the fallback.

### Atomic findings on AWS staging, RC1, 2026-10-02

The RC1 report used the real consumer repository DDL (schema hashes recorded in the report), two atomic 5,000-finding scans, and isolated tagged staging resources. Native v2 and v1 both recovered 10,000 rows. Median scan ACK was **2.89 s for native v2** versus **2.79 s for v1**; p95 was **2.98 s versus 4.10 s** in this one-repetition sample. V2 reconciled two durable receipts and retained **2,062,615 S3 bytes** across five objects; each encoded operation was 1,523,174 bytes. Cleanup and independent absence checks passed.

This is the first RC1 measurement on the atomic consumer-shaped path. It is a baseline, not a capacity SLO or monthly invoice; the next run should repeat it and add concurrency before setting a production target.

The two-repetition follow-up repeated the same workload from commit `c8a0bea9dc9d45d8d221df8c88da0b5e9666240a`: four 5,000-finding scans per mode, serial submission, and ACK after the published transaction. Across the two runs, median ACK was **2.503 s for v1** and **2.868 s for native v2** (v2 **14.6% slower**); per-run medians were 2.366–2.641 s for v1 and 2.785–2.951 s for v2. The observed p95 range was 3.140–3.857 s for v1 and 2.929–3.006 s for v2. Every run recovered 10,000 rows; v2 reconciled two receipts per run, encoded 1,523,174 bytes per operation, and retained 2,062,615 S3 bytes across five objects. Cleanup completed with no errors and the exact temporary resources were removed.

This reduces the single-run noise in the first RC1 baseline, but it remains a serial, four-scan sample. The next measurement is a bounded non-atomic concurrency sweep to quantify throughput and its failure boundary; it must not be presented as the consumer app's atomic transaction guarantee.

The bounded batched run used the same 5,000-finding scans with v2 chunks of 64 and submit concurrency 8. v1 still executes each scan atomically and completed two scans in **2.676 s median**. Native v2 used **158 operations**, 20 publication batches and two checkpoints; its scan completion was **21.517 s median** (22.808 s max), while all 158 receipts reconciled and all 10,000 rows recovered. The run retained **4,744,834 S3 bytes** across 41 objects. Cleanup and independent absence checks passed.

This path is roughly 8x slower for the full scan because the 5,000 findings fan out to 79 operations per scan. It also has partial-failure semantics between operations, so it is a throughput diagnostic and not a candidate for the consumer app's atomic findings guarantee.

The 12-scan native cycle repeated the atomic 5,000-finding operation twelve times on the current RC1 source. It recovered **60,000 rows** and all 12 receipts, checkpointed at published versions **5 and 10**, and completed scans in **3.256 s median** (5.914 s maximum; ACK p95 5.816 s). Before cleanup it retained **24,885,653 S3 bytes** across 25 objects (13 images and 12 history records). This is the measured storage floor before physical GC, not a monthly bill; it gives us the checkpoint frequency needed for the cost model. At the documented S3 Standard assumption of $0.023/GiB-month, 24,885,653 bytes is about **$0.00053/month** of raw object storage; the 25 image/history PUTs are about **$0.000125** at $0.005 per 1,000 PUTs. DynamoDB capacity, reads, retries, logs, egress and GC are not included, so this is a storage/request floor rather than an all-in scan price.

The native fan-out sweep kept the same two 5,000-finding scans and 64-row chunks while varying submit concurrency. It is intentionally non-atomic and measures throughput/retention only:

| Submit concurrency | Scan median | Publication batches | Checkpoints | Retained S3 |
|---:|---:|---:|---:|---:|
| 1 | 133.431 s | 158 | 79, 158 | 20,541,266 B |
| 4 | 37.634 s | 40 | 20, 40 | 6,703,354 B |
| 8 | 21.517 s | 20 | 10, 20 | 4,744,834 B |

The c1 and c4 reports are archived here and here; c8 is in the batched RC1 report. All 158 receipts per run and all 10,000 rows recovered. Higher concurrency materially reduces wall time and retained history, but the path can partially publish a scan when an operation fails, so it remains unsuitable for the consumer app's atomic findings guarantee.

### Atomic findings on AWS staging, 2026-09-24

The two-repetition AWS report
used two atomic 5,000-finding scans per mode against isolated S3 and DynamoDB
resources in account `REDACTED`, region `us-east-2`. Every v2 mode
published one receipt and version per scan; a cold owner recovered all 10,000
rows and both receipts. Median scan completion by repetition was **3.33–3.77 s
snapshot**, **3.22–6.04 s incremental**, and **2.75 s native**. The v1
comparison ranged **2.26–6.65 s**. This is a small, variable sample, so the
ranges are observations rather than latency guarantees.

The separate 12-scan native AWS cycle
recovered 60,000 rows and all 12 receipts. It checkpointed at versions 5 and
10, finished a scan in 2.55 s median (4.89 s maximum), and retained
24,885,653 S3 bytes across 25 objects before cleanup. Both reports mark
`cleanup_complete=true` with no errors. Independent exact-name checks returned
S3 404 and DynamoDB `ResourceNotFoundException` for both temporary resource
pairs after the runs.

---

## Bulk scan benchmark baseline

On 2026-09-28, `experiments/turso_cloud_consumer_benchmark.py` ran from the development client against a disposable Turso Cloud database in `aws-us-east-2`. The run used the consumer app's `repository.sql` and `attempt_manifest.sql`, five sequential scans of 500 synthetic findings, 13 multirow INSERT statements per scan (40 rows per statement), and one commit per scan. It verified 2,500 rows and forced transaction rollback. The disposable database and group were destroyed and `turso db list` showed only the pre-existing `qedge` database afterward.

The raw result records every sample. Its write median is 5,100.307 ms; the nearest-rank p95 is 5,745.988 ms, which is the maximum of only five samples. Count and filtered-count read medians are 358.733 ms and 355.890 ms. These are external client round trips with a single sequential client, so they do not establish in-region service latency, throughput, p99, or a monthly bill.

The current Tessera deployed bulk drill uses a reduced findings table and only one scan. A direct performance or cost comparison would be misleading until both services run the same DDL, scan sequence, client placement and operation accounting. The Turso result here is a reproducible baseline for that matched experiment, not a winner declaration.

### Shared synthetic workload and batching sensitivity

`examples/cloud_matched_scan_benchmark.py` now generates one shared synthetic schema and the same scan, count reads and forced rollback for both services. Turso Cloud was tested from the same external development client against disposable databases in `aws-us-east-2`; the databases and groups were destroyed after each run. Python `libsql==0.1.11` issued one SQL `execute` per statement and a commit per scan. Each variant wrote 500 findings per scan.

| Statements per scan | Scans | Write p50 | Write p95 | Write p99 | Full sequential loop |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 13 (40 rows/statement) | 5 | 5,105 ms | 5,278 ms | 5,278 ms | 0.169 scans/s |
| 1 (500 rows/statement) | 30 | 963 ms | 2,210 ms | 2,219 ms | 0.518 scans/s |

The 13-statement raw report and single-statement raw report retain all samples, schema hash, payload size and read timings. The 13-statement p99 is the largest of five observations; the 30-scan p99 is the largest of 30. These are exploratory external round trips, not service-side latency distributions. The one-statement variant makes fewer remote `execute` calls; the large difference is consistent with network overhead, but these samples do not isolate its cause.

The shared harness also passed local Tessera HTTP runs with the synthetic schema and with the private consumer DDL, including 500-row transactions and rollback. Local backend timings are not AWS timings. The next deployed experiment must run the same single-statement and 13-statement variants against Tessera in Ohio, then collect AWS request counts and retained storage to build the complete monthly cost model.

### Real consumer DDL, one statement per scan

A fresh disposable Turso database in `aws-us-east-2` ran 30 scans × 500 findings from the same external client with the exact `repository.sql` and `attempt_manifest.sql` hashes above, using one multirow INSERT and commit per scan. The raw result reports 15,000 verified rows, forced rollback, write p50 **835 ms**, p95 **2,316 ms**, p99 **2,341 ms**, and a full sequential loop of **0.537 scans/s**. Count-read p50 was **373 ms**. The p99 is the largest of 30 observations. The database and group were destroyed; `turso db list` again showed only the pre-existing `qedge`. This is the closest Turso baseline for the pending Tessera deployed run, but it still covers only the findings stage of consumer and no monthly cost.

### Matched Tessera AWS run, 2026-09-29 UTC

The raw Tessera report ran the same consumer DDL hashes, 30 sequential scans × 500 findings, one multirow INSERT per scan, two count reads per scan and a forced rollback from the same external development client. Both services were deployed in Ohio. The deployment evidence binds that report and the AWS call summary by SHA-256 to the 75-file public source archive from commit `2508c30f3f6611d8a7c36dd7eb1a3d7b469af92a` and its Lambda ZIP. Its deployed Terraform module enabled opt-in AWS call telemetry. The first setup attempt had an administrator token containing a newline and was rejected with 401; a later local runner timed out before reporting. A new clean 30-scan run generated the values below. The earlier attempt may have left data in the temporary stack; it is excluded from the timed result and from the selected per-operation telemetry, but the final bucket inventory is not isolated to the successful run.

| External client measurement | Turso Cloud | Tessera v2 AWS |
| --- | ---: | ---: |
| Write p50 / p95 / p99 | 835 / 2,316 / 2,341 ms | 1,842 / 2,494 / 2,719 ms |
| Count read p50 | 373 ms | 2,104 ms |
| Filtered read p50 | 372 ms | 2,108 ms |
| Complete sequential loop | 0.537 scans/s | 0.164 scans/s |
| Correctness | 15,000 rows and rollback verified | 15,000 rows and rollback verified |

Tessera's write median was about **2.2×** Turso's and its sequential loop about **3.3×** slower in this one run. These are client-observed round trips and 30 samples, not controlled service-side distributions or concurrency capacity. The p99 in each report is the largest observation. The matched AWS call summary selected exactly 30 successful transaction requests and the first 60 successful query requests: writes made 231 S3 GETs and 60 PUTs; reads made 408 S3 GETs. Lambda billed 118,286 ms at 512 MiB for those 90 requests. These are logical SDK calls, excluding retry attempts and DynamoDB capacity units. The read path's repeated S3 fetches are a concrete optimization target; the current evidence does not isolate their individual latency contribution.

The temporary stack's final S3 inventory was 15 objects and 681,107 bytes, including data from the interrupted attempt and maintenance. After measurement, Terraform destroyed all 20 managed resources. Its state was empty; independent AWS checks found no test bucket, DynamoDB table, Lambda, API Gateway API or EventBridge rules, and the isolated administrator secret was deleted. This was an existing staging account, not a fresh-account install.

### Bounded warm read cache experiment, 2026-09-29 UTC

The new raw report repeats the same 30 × 500 consumer DDL workload and client placement with a 16 MiB, single-database verified frontier cache per warm Lambda environment. Each read still fetches the published pointer; the second read of an unchanged frontier can reuse local verified SQLite. The deployment evidence binds the report and AWS call summary to source commit `3264d3ab98b0d7f34bf5c444d113af122303c451` and its public archive and ZIP hashes.

| Same 30-scan workload | Tessera before cache | Tessera with cache | Turso Cloud baseline |
| --- | ---: | ---: | ---: |
| Write p50 | 1,842 ms | 1,835 ms | 835 ms |
| Count read p50 | 2,104 ms | 1,962 ms | 373 ms |
| Filtered read p50 | 2,108 ms | 1,538 ms | 372 ms |
| Complete sequential loop | 182.5 s | 162.6 s | 55.9 s |
| S3 GET for 60 reads | 408 | 266 | — |
| Lambda billed duration for 30 writes and 60 reads | 118.286 s | 98.921 s | — |

The cache cut read-path logical S3 GETs **35%** and this campaign's Lambda billed duration **16%**. It improved the complete loop **11%**; the write median was effectively unchanged. The paired reports are separate sequential external-client runs, not randomized or simultaneous trials, and do not establish a stable percentage under other concurrency, database sizes or cold-start rates. The new loop is still about **2.9× slower** than the Turso baseline, so the read reconstruction architecture remains a larger product issue. The newer stack ended with 62 objects and 8,104,903 bytes in S3; this is a point-in-time inventory after a different sweep schedule, not a storage efficiency comparison. Its 20 Terraform resources and isolated secret were destroyed, with empty state and independent absence checks.

### Verified native log catch-up, 2026-09-29 UTC

The third raw report repeats the same 30 × 500 consumer DDL workload from the external client. The deployment evidence binds it and the AWS call summary to source commit `62a42fe6551e0ae90de9486ecf6af0e942878ee6`, the 76-file public archive and its Lambda ZIP. When a new manifest extends the same native checkpoint, the reader hashes the exact cached bytes it copies, fetches and verifies only the new segments, and validates the reconstructed SQLite and receipt frontier. Rebase or invalid local bytes fall back to full restore; a corrupt current remote frontier still fails closed. The public archive now includes the published-reader safety tests and passed 96 tests from its extracted tree.

| Same 30-scan workload | Tessera original | Warm cache | Native catch-up | Turso Cloud |
| --- | ---: | ---: | ---: | ---: |
| Write p50 | 1,842 ms | 1,835 ms | 1,776 ms | 835 ms |
| Count read p50 | 2,104 ms | 1,962 ms | 1,898 ms | 373 ms |
| Filtered read p50 | 2,108 ms | 1,538 ms | 1,439 ms | 372 ms |
| Complete sequential loop | 182.5 s | 162.6 s | 154.5 s | 55.9 s |
| S3 GET for 60 reads | 408 | 266 | 151 | — |
| Lambda billed duration for 30 writes and 60 reads | 118.286 s | 98.921 s | 91.993 s | — |

Native catch-up cut read-path logical S3 GETs another **43%** against the warm-cache run, and **63%** against the original. The complete loop improved **5%** against warm cache and **15%** against the original; it remains about **2.8× slower** than Turso in this sequential external-client sample. That smaller latency gain than GET reduction is evidence that network/API, DynamoDB and local SQLite work need separate profiling; the run does not isolate their shares. As before, these are three separate exploratory 30-scan campaigns, not a controlled concurrency or monthly cost result. The third stack ended with 62 S3 objects and 8,104,903 bytes, the same point-in-time inventory as the warm-cache run. Terraform destroyed all 20 resources, state was empty, and independent AWS checks found no test bucket, table, Lambda, API, EventBridge rules or secret.
