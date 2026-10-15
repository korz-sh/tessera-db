---
title: Economics history
parent: Reference
nav_order: 16
---

# Cost economics — pre-v2 evidence and models

All four documents in this page **predate the v2 durability contract** and
the pluggable-benchmark measurements. They are kept verbatim as the
reasoning trail behind the current cost claims on
[Measurements](measurements.md) — model names, pricing sources and caveats
are the historical record, not today's numbers. Linked evidence artifacts
that no longer exist in `docs/` are preserved in git history.

**2026-10-08 update:** the two dominant caveats in this trail — per-commit
PUT cost and unbounded history — now have measured answers on
[Measurements](measurements.md): windowed publish amortizes PUTs per
commit (~91× at ~80 commits/window, fs-measured) and fenced destructive
GC on S3/DynamoDB bounds retained history (moto-verified; live-AWS
validation pending).

# Tessera v2: cost and latency of ORM-shaped transactions

**Historical baseline:** this probe ran at commit `48b3024`, before the v2
SQLAlchemy bridge and fenced remote GC were implemented in #75. The failure
and missing-GC statements below describe that measured revision, not current
main. See the later ORM and GC evidence. Neither
report establishes full hosted-service cost.

Measured 2026-09-24 in isolated `staging` resources, `us-east-2`. No
production or deployed consumer data was modified. The raw
report and the exact
[harness](../experiments/v2_callback_cost_probe.py) are included. Harness SHA-256:
`1ff73606406e8e3e0c9a55060f2f1f9ebfe358b26df1b32f39bf943fd17e59f4`.
The Tessera code was upstream commit
`48b3024` (same application code as the `2.0.0a3` consumer bridge), from a clean
working tree before adding this report. The client ran outside AWS, in
Argentina. The isolated bucket and table were deleted by the harness and
independently confirmed absent (S3 404, DynamoDB `ResourceNotFoundException`).

## What was actually exercised

The deployed v2 `CallbackStore` bridge was called once per committed SQL
transaction. Each transaction inserted 1, 10, or 100 rows. This reproduces
the persistence boundary of an ORM unit of work; **the test did not execute
SQLAlchemy ORM**. The existing `TesseraORM` facade cannot currently use the
v2 bridge: `create_tenant` expects a v1 manifest object but receives a dict,
and bypassing creation still fails because the Turso connection does not
provide SQLAlchemy's expected `create_function` DBAPI method. This is a
release/useability gap, not a measured ORM overhead.

| Rows/commit | Commits | Recovered rows | Commit p50 / p95 | Read p50 / p95 |
|---:|---:|---:|---:|---:|
| 1 | 60 | 60 | 3,407 / 4,361 ms | 190 / 1,559 ms |
| 10 | 30 | 300 | 3,343 / 4,442 ms | 192 / 962 ms |
| 100 | 12 | 1,200 | 3,525 / 4,534 ms | 192 / 1,239 ms |

Reads are 12 sequential `SELECT COUNT(*)` calls after the writes, with a
warm local cache. The p95 of 12 reads is effectively the maximum. There were
no failed calls; each final row count and published version matched the
number of transactions. These are **client-observed** times that include the
Argentina-to-Ohio path; they are not Lambda server-side write durations or
an offered-concurrency throughput result.

Botocore `before-call` events counted the actual remote API calls in each
write phase. Per committed transaction, the shape was stable:

| API | Calls/commit, 1 row | Calls/commit, 10 rows | Calls/commit, 100 rows |
|---|---:|---:|---:|
| S3 `PutObject` | 2 | 2 | 2 |
| S3 `GetObject` | 4.85 | 4.70 | 4.25 |
| DynamoDB `GetItem` | 5 | 5 | 5 |
| DynamoDB `UpdateItem` | 3 | 3 | 3 |
| DynamoDB `TransactWriteItems` | 1 | 1 | 1 |

The current bridge creates and closes an owner per write. It also restores a
verified frontier for each owner. Batching more rows into one callback saves
remote calls per row but does not eliminate the fixed round trips per commit.
The two S3 PUTs are one image/segment and one historical manifest. Pointer
publication uses the DynamoDB transaction.

## USD model for the *current* path

Use published Ohio rates: S3 Standard $5/million PUT and $0.40/million GET,
DynamoDB Standard on-demand $0.625/million write units and $0.125/million
strong read units, S3 Standard storage $0.023/GB-month. No free tier, tax,
retry, compression, cache hit improvement, NAT, or discounts. The DynamoDB
transaction contains one lease condition and one pointer put. Pointer JSON
was 587–2,697 bytes across 60 local versions, before DynamoDB item overhead,
so model its put as 1–3 KiB. AWS bills two write units per transactional KiB
and for the condition check.

At the observed ~4.85 S3 GETs/commit, **1 million commits cost approximately
$17–19.5 in storage API requests**: $10 S3 PUT, $1.94 S3 GET, $1.88 lease
updates, $0.63 strong reads, and $2.5–5.0 for the transaction. DynamoDB read
consistency, pointer size, lease retries, object list operations and actual
S3 request class can move the total. The prior $13.25/million figure omitted
the measured restore GETs and some DynamoDB item-size sensitivity.

This is a **per-commit**, not per-row, cost. One million rows committed 100 at
a time yield 10,000 commits: about **$0.17–0.20 in these requests**. One
million rows committed individually yield one million commits: **$17–19.5**.
CPU, memory, network paths, API ingress and retained bytes are additional.

Storage does not yet have a steady-state remote price. `NativeLogStore`
rebases at most every nine publications; `LifecycleManager.gc` explicitly
refuses remote physical deletion. In the probe, 60 one-row commits retained
283,534 bytes in 121 objects (61 images and 60 historical manifests), despite
only 60 rows of data. For a **fixed 10 MiB database** updated one million
times per month, the checkpoint algorithm alone writes about 1,085 GiB of
historical full images each month. With writes spread uniformly, that adds
roughly **$12.5 to the first month's average S3 bill and $25/month to the
following month's average**, cumulatively until safe remote GC is available.
At a fixed 100 MiB database, multiply those amounts by ten. This projection
uses the code's nine-publication rebase rule, not a measured 1M-commit run;
log segments and other history add bytes.

An owner in another region or outside AWS also downloads the checkpoint and
segments for each cold write. Same-region S3-to-AWS-service transfer is free,
but NAT or internet egress can dominate for larger databases. Gateway
endpoints for S3 and DynamoDB avoid NAT processing charges when correctly
routed. The exact consumer network path was not cost-attributed in this run.

## Deployed API read cost and missing write compute number

The separate 160-request deployed consumer read benchmark used eight warmups.
CloudWatch reported
**168 Lambda durations** in that minute: p50 531 ms,
p95 1,147 ms, average 832 ms and maximum 6,520 ms. The function has 512 MiB
memory and x86_64 architecture. At the published $0.0000166667/GB-second
rate and $0.20/million requests, **one million reads with this same duration
mix would cost about $7.14 in Lambda**, before API Gateway, logs, S3 and
DynamoDB. This is an extrapolation from one bounded staging read endpoint,
not a general read price or a write benchmark.

We have **not** measured server-side Lambda duration for these small writes.
For scale, one million writes lasting 0.5, 1, or 3 seconds in a 512 MiB
Lambda would add about $4.37, $8.53, or $25.20 respectively including the
request charge. The 3.3–3.5 second client p50 must not be used as Lambda
duration because it includes cross-continent network round trips.

## Decision at the measured revision

At this revision, the bridge was not a cheap hosted-database alternative for
normal small ORM transactions: there was no working v2 ORM facade, fixed
per-commit remote work was high, and remote GC could not bound storage. #75
subsequently added the ORM bridge and a fenced manual GC drill; bounded,
scheduled GC and full-service cost measurement remain open. A persisted owner,
fewer round trips and matched application workloads still need measurement
before comparing total v2 cost and latency with Turso Cloud.

Sources: [AWS S3 pricing](https://aws.amazon.com/s3/pricing/),
[DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/),
[DynamoDB request-unit rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html),
[Lambda pricing](https://aws.amazon.com/lambda/pricing/),
[S3 transfer rules](https://aws.amazon.com/s3/faqs/),
[gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html).

---

# Tessera v2 BYOC cost ledger

**Status, refreshed 2026-09-29 UTC:** pricing inputs, an isolated empty-stack idle sample and three deployed, matched consumer findings campaigns are measured: original, bounded warm cache and verified native log catch-up. This is still not a complete Tessera monthly quote. The current HTTP service restores an owner per transaction. The measured AWS call counts are logical SDK operations, not billed request attempts or DynamoDB capacity units. A local or Moto count must not be used as an AWS bill.

## What must be counted

For a 30-day month in one Ohio region, the Tessera total is:

`admin secret + HTTP API requests + Lambda requests/duration + S3 PUT/GET/LIST/storage + DynamoDB read/write capacity/storage/PITR + CloudWatch logs + network egress + backups + operator time`.

Free tiers, organizational discounts and taxes are excluded from the comparison because they depend on the customer's account. Each term needs a measured usage quantity and a dated unit price. `enable_aws_call_metrics` now emits both *logical* SDK call counts and `sdk_http_attempts`, calculated from each completed botocore response's `RetryAttempts + 1`. Historical samples have only logical calls. An SDK HTTP attempt is not necessarily a billable AWS request: service-specific error charging, transport failures without a parsed response and DynamoDB capacity units remain outside this counter. The observer also does not report billed Lambda duration or bytes. S3 usage pages report retained bytes for one database but exclude catalog objects, backups and other account storage. The isolated stack's CloudWatch reports and S3 inventory must fill those gaps.

| Meter | Current public reference | Evidence still needed for v2 |
| --- | --- | --- |
| Turso plan and overages | [Turso monthly pricing](https://turso.tech/pricing?frequency=monthly): Free $0/5 GB/1-day PITR; Developer $5.99/9 GB/10-day PITR; Scaler $29/24 GB/30-day PITR, plus listed row/storage/sync overages | consumer scan and purge row usage, reads, retained storage, plan selected |
| API Gateway HTTP API | [AWS pricing](https://aws.amazon.com/api-gateway/pricing/): first 300 million requests at $1/million, with request-size metering and data transfer out | Exact HTTP request count and response bytes; client retry distribution |
| Lambda x86 | [AWS pricing](https://aws.amazon.com/lambda/pricing/): $0.20/million requests and first-tier $0.0000166667/GB-second before account free tier | Matched campaign billed duration measured; cold-start mix and other workloads remain |
| S3 Standard | [AWS pricing](https://aws.amazon.com/s3/pricing/): about $0.023/GB-month in US East, $5/million PUT/LIST, $0.40/million GET | Matched campaign logical GET/PUT counts measured; retries, bytes over time, history/GC/checkpoint traffic, catalog and backups remain |
| DynamoDB Standard on demand | [AWS pricing](https://aws.amazon.com/dynamodb/pricing/): $0.625/million WRUs and $0.125/million RRUs; transactional operations and item size change units | Isolated campaign CloudWatch capacity sums measured below; request-level ConsumedCapacity, table bytes and PITR size remain |
| Secrets Manager | [AWS pricing](https://aws.amazon.com/secrets-manager/pricing/): $0.40/secret-month and $0.05/10,000 API calls | Secret count and cold-start retrievals; one admin secret is the current module floor |
| CloudWatch Logs | [AWS pricing](https://aws.amazon.com/cloudwatch/pricing/): US East examples use $0.50/GB ingest and $0.03/GB archived beyond shared free tier | Isolated campaign Lambda/API ingest measured below; retention and other logs remain |
| Data transfer and operation | Region/account-dependent AWS network tariffs and an explicit operator-hour rate | Client location, bytes out, NAT/VPC endpoint choice, backups, upgrades, incident and recovery rehearsal time |

## Concrete idle work in the current Terraform module

The [module](../deploy/aws/terraform/main.tf) schedules deletion every minute and retention every five minutes. That is **43,200 + 8,640 = 51,840 Lambda invocations in a 30-day month**, even before SQL traffic, assuming every scheduled event invokes the function once. At $0.20/million, the request-only component is **$0.010368/month** before any free tier. The one admin secret adds a $0.40/month listed price before API calls. The measured, small-sample duration and SDK call scenarios follow; their actual monthly frequency, retry mix and billed account total are unmeasured.

### Isolated empty-stack sample

The deployment record binds a reproducible public-source ZIP to an isolated Ohio stack with **zero databases and zero SQL requests**. From its redacted Lambda events, `python3 experiments/cloud_idle_meter.py docs/metrics/idle-sweep-events-2026-09-29.json` joins request-scoped SDK summaries to Lambda `REPORT` billed duration. Six deletion sweeps and one retention sweep were observed over five minutes. The first deletion invocation fetched the admin secret and billed **3,514 ms**; the five subsequent deletion invocations billed **67–88 ms**, mean **75.6 ms**. The retention invocation billed **21 ms**. Every deletion sweep made one logical DynamoDB `GetItem`, one `PutItem` and one S3 `ListObjectsV2`, even with an empty catalog. Retention made one `GetItem` and one S3 `ListObjectsV2`. These are logical SDK calls, not billed request attempts. CloudWatch's broader ten-minute window reported 8 DynamoDB read units, 7 write units and 7,383 Lambda log bytes; that window also includes setup and teardown and cannot be divided by seven as an invoice-grade per-sweep rate. The stack, secret and Terraform state were destroyed and independently checked absent.

At the listed first-tier rates, repeating the **warm-only sample** for a 30-day month gives about **$0.039 Lambda requests plus duration**, while repeating this tiny sample's **one-in-six cold deletion mix** gives about **$0.245 Lambda**. The cold-start share over a month is unknown, so these are scenarios, not bounds. The observed one LIST per scheduled invocation would be **51,840 logical LIST calls or $0.259/month** at the [S3 Standard LIST rate](https://aws.amazon.com/s3/pricing/) of $0.005/1,000 before retries. Assuming one DynamoDB read unit per sweep and one write unit per deletion sweep gives another **$0.033/month** at the [on-demand rates](https://aws.amazon.com/dynamodb/pricing/); this is consistent with, but not proven by, the short CloudWatch totals. Linearly repeating the observed log volume would be about **$0.027/month** at the [CloudWatch ingest rate](https://aws.amazon.com/cloudwatch/pricing/). Together with one $0.40 admin secret, those assumptions produce roughly **$0.76/month in the warm-only scenario** and **$0.97/month with the observed cold mix**, before free tiers, discounts and taxes. These are **partial empty-stack scenarios**, not a complete bill or a price comparison with Turso: they omit PITR and table storage, secret API calls, EventBridge metering, S3 storage and retries, log storage, network, backups and operator time. The per-minute S3 LIST is the largest measured variable idle meter and a concrete target for a durable indexed deletion queue; changing the sweep frequency alone would also change deletion lag.

The pending-deletion index candidate changes this schedule. Its empty-stack query/LIST projection is about **$0.0144/month** versus **$0.216/month** for the old deletion LIST alone, a projected **$0.202/month** reduction in these request meters. This is arithmetic from listed rates, not a new AWS measurement; Lambda duration, GSI writes/storage and a complete bill must be remeasured after deployment. The historical $0.76/$0.97 scenarios above must not be presented as the new implementation's cost.

## Performance evidence that must match the cost inputs

The [shared scan report](measurements.md#historical-benchmarks-pre-v2) shows that Turso Cloud's external-client p50 for 500 synthetic findings changes from about 5.1 seconds with 13 remote SQL statements to 0.96 seconds with one multirow statement. The matched 30-scan consumer-DDL one-statement run now exists for both services in Ohio from the same external client: Turso write p50 835 ms and 0.537 scans/s; Tessera before cache write p50 1,842 ms and 0.164 scans/s, with warm cache 1,835 ms and 0.185 scans/s, and with native catch-up 1,776 ms and 0.194 scans/s. Each 30-scan p99 is the maximum of 30 samples, so it is exploratory. Full recovery time, write/read concurrency, real consumer scan distribution and monthly row counts remain unmeasured on the managed API. Do not turn client latencies into Lambda GB-seconds: they include network and service overhead.

## Measured request subset and price floor

The deployment evidence binds the raw Tessera result and selected AWS call summary to a public archive and ZIP hash. Selection is exactly 30 successful writes plus the first 60 successful reads, corresponding to two count queries per scan. It excludes setup, forced rollback, deletion and scheduled maintenance. CloudWatch recorded **118.286 billed Lambda seconds at 512 MiB** for these 90 invocations. The SDK observer counted **639 S3 GET**, **60 S3 PUT**, **180 DynamoDB GetItem**, **90 UpdateItem** and **30 TransactWriteItems** logical calls. S3 retries and DynamoDB consumed capacity were not captured. The observed runtime's S3 inventory at the end was 681,107 bytes across 15 objects, but it includes a prior interrupted attempt and maintenance, so it cannot determine steady retained GB-months.

At the public first-tier list rates in the table, ignoring account free tiers, discounts and taxes, the **measured subset** computes as follows: Lambda duration $0.00098572, Lambda requests $0.000018, S3 GET $0.0002556, S3 PUT $0.0003, and 90 HTTP API calls $0.00009. Sum: **$0.00164932 for 30 scans**, or **$0.00005498 per scan** for only these five meters. This is a priced subset, not a billed account total or an all-in unit cost. Multiplying the same traffic and duration by 10,000 scans gives **$0.55/month for those five meters alone**. That projection does not establish throughput or fit within the current 64 MiB per-database frontier limit. It excludes DynamoDB capacity and PITR, the secret, sweeps, storage, logs, network egress, backups, retries and operator time. Turso's published Free plan has 10 million monthly rows written and 5 GB storage, so 10,000 × 500 findings would fit its *write quota*; total Turso price still depends on its read and storage meters. No price winner is established.

The bounded warm-cache repeat measured the same 90 request classes: 98.921 billed Lambda seconds, 497 logical S3 GET and 60 PUT. The same five-meter arithmetic gives **$0.00143114 per 30 scans**, or **$0.48 per 10,000 scans** if that exact pattern scaled linearly. That is a **13% reduction in the priced subset** and remains far from an all-in monthly bill. The cache also cut read-path S3 GETs from 408 to 266 and improved the full sequential loop by 11%; actual AWS charges still need billed S3 attempts, DynamoDB capacity, retained GB-months, logs, idle work and egress. The two deployments ended at different moments in their retention/deletion sweep cycles, so their point-in-time S3 inventories cannot be compared as storage savings.

The native catch-up repeat measured 91.993 billed Lambda seconds, 382 logical S3 GET and 60 PUT for the same 90 request classes. The same five-meter arithmetic gives **$0.00132741 per 30 scans**, or **$0.44 per 10,000 scans** under the same linear projection. Read-path S3 GETs fell to 151, but write-path calls were unchanged. The complete loop improved 5% against warm cache, much less than the 43% drop in read GETs; the remaining latency needs profiling before another architecture change. This remains a partial list-price calculation, not a customer bill.

## Whole-stack DynamoDB and log meters from the catch-up campaign

After the catch-up stack was destroyed, its CloudWatch metrics remained queryable. The four raw responses are DynamoDB reads, DynamoDB writes, Lambda log bytes and API log bytes. They query `ConsumedReadCapacityUnits` and `ConsumedWriteCapacityUnits` for table `tessera-cu-a5995d0d66a4-coordination`, and `IncomingBytes` for log groups `/aws/lambda/tessera-cu-a5995d0d66a4-sql` and `/aws/apigateway/tessera-cu-a5995d0d66a4-sql`. Each came from `aws cloudwatch get-metric-statistics` in `us-east-2`, with `--start-time 2026-09-29T02:37:00Z --end-time 2026-09-29T02:42:00Z --period 60 --statistics Sum` and the table or log-group dimension. [AWS documents](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html) that the table capacity metric's one-minute `Sum` covers on-demand consumption, but warns that a short spike can be underrepresented. The raw responses are kept verbatim; `python3 experiments/cloud_campaign_meter_summary.py` validates the labels, units, timestamps and duplicate minutes, then recomputes the totals and list-price arithmetic.

Across that **five-minute whole-stack window**, DynamoDB reported **191 read and 325 write capacity units**; Lambda and API log groups reported **72,096 and 7,104 ingested bytes**. At the listed first-tier rates, these correspond to an indicative **$0.00022700 DynamoDB** and **$0.00003960 log ingestion**, **$0.00026660 combined**. The window includes database setup, the 30-scan benchmark, rollback, deletion and scheduled invocations. It is broader than the 90-request selected subset above, so its meters cannot be interpreted as per-scan costs. It is also a CloudWatch estimate, not an invoice or a complete whole-stack cost. In particular, table PITR/storage, log storage, S3 retry attempts, idle work outside this window, egress and secret retrieval are still missing.

The next cost report should capture request-level DynamoDB `ConsumedCapacity` or billing records, S3 retry attempts, retained bytes over time, idle-sweep duration/calls, egress and account billing for an isolated tag or account. It should also exercise concurrency and a retention horizon long enough to estimate GB-months. The benchmark stack and isolated secret were destroyed; Terraform state was empty and independent AWS lookups found no named resources.

The read-only `experiments/cloud_stack_cost_snapshot.py` collects a bounded current S3 inventory, table and GSI CloudWatch capacity, API `Count`/`DataProcessed`, Lambda invocations and measured `Duration`, log-ingest metrics, and PITR status for a selected minute window. It records minutes without datapoints and refuses to call a truncated S3 listing complete. The S3 scan also stops after 1,000 LIST requests or a soft 30-second deadline by default because [S3 can return fewer keys than `MaxKeys`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html); `complete: false` means its bytes are a partial inventory, not a total. It scans Lambda [REPORT billing fields](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs-view.html) through bounded [FilterLogEvents pagination](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_FilterLogEvents.html) to sum billed milliseconds, memory and GB-seconds for the same window. The API access log now records [HTTP API response payload length](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-logging-variables.html), and the collector sums it through a separate bounded log scan. Each scan stops at 10,000 events, 100 requests or 30 seconds by default. It exposes pagination completion and comparison with invocation/request metrics, but late log delivery can still leave a complete scan short of actual traffic. API response payload length excludes protocol overhead and is not an internet-transfer bill; [API Gateway documents](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-metrics.html) `DataProcessed` as processed bytes, not a direct egress meter. [DynamoDB documents](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/API_DescribeTables_v20111205.html) that `TableSizeBytes` updates about every six hours. Neither value can by itself close the monthly storage or network line item.

`experiments/cloud_storage_timeline.py` integrates repeated complete S3 inventories of the same bucket using linear interpolation between scan midpoints. Its byte-hours and fractional 30-day GB-month equivalents cover only the observed interval. It reports both decimal GB and [AWS's 1,073,741,824-byte GB](https://aws.amazon.com/s3/faqs/) explicitly. It also splits byte-hours at UTC calendar-month boundaries, divides by each month's actual hours, and marks whether the sample span covers a complete month. That flag does not establish a dense sampling cadence. The output reports sample cadence and never claims a bill or a bound on storage changes between scans. Sample across a full retention horizon, then price the relevant calendar-month estimates only after checking the bucket's storage class; this inventory currently combines classes. Separately account for non-S3 storage, backups, requests and network transfer.

An isolated 1,000-publication GC drill storage timeline validates that calculation against three complete inventories from the same unversioned bucket. Current objects grew from 875 to 2,028 and sampled bytes from 2,808,868 to 10,500,424 over an observed **1,101.745 seconds**. The interpolated area is **2,032,411.84 byte-hours**, equivalent to **0.000002823 decimal GB-months** or **0.000002629 AWS GB-months** using a 30-day divisor, within those 18.36 minutes. The largest gap between sample midpoints was 960.55 seconds. This is a short rising-storage interval, not a retention horizon, monthly run rate, charged AWS storage or TCO. The inventories added six S3 LIST requests outside the GC drill's own SDK counters. The drill later destroyed the bucket and table, so this sample cannot be extended in place.

The same disposable drill's DynamoDB CloudWatch capacity series records **2,209 consumed read and 8,185 consumed write capacity units** over a 35-minute window, with datapoints in 34 minutes. An AWS Pricing `GetProducts` query for the Ohio on-demand table returned **$0.125 per million read units** (SKU `SGMZJPDKSWXTY4VE`) and **$0.625 per million write units** (SKU `GGQSXTNTRBHRVS56`), consistent with the [published DynamoDB pricing examples](https://aws.amazon.com/dynamodb/pricing/). Multiplication gives an **indicative $0.00539175 table request-capacity subtotal** for that window, before any free-tier or discount treatment. The series covers setup, publication, mark, index, sweep, recovery and teardown; it cannot isolate GC from writes or be divided into per-publication cost. These are CloudWatch consumption metrics and list-rate arithmetic, not a charged-request record or an invoice. S3, Lambda, API, logs, DynamoDB storage/PITR, data transfer and idle time outside the window remain excluded. The table was destroyed after the drill.

### Live snapshot validation, 2026-09-29

The first current-main 30-scan synthetic campaign completed its workload, five fresh-process recoveries and S3 usage inventory, but the cost collector rejected API Gateway `Count`: CloudWatch returned `Unit="None"`, while the collector expected `Count`. The report retains the benchmark and records this collector failure. A corrected collector then passed a separate [two-scan isolated stack validation](metrics/stack-cost-snapshot-validation-2026-09-29.json). Terraform state was empty after both runs; the second run's bucket, DynamoDB table, Lambda and admin secret were independently checked absent.

The validation inventory was complete for that moment: 131 S3 objects and 250,544 current bytes, with DynamoDB PITR enabled. For its two-minute UTC window, CloudWatch had only one minute of datapoints for API requests (19), API `DataProcessed` (4,548 bytes), table reads (28 capacity units), table writes (42 capacity units), Lambda invocations (18), measured Lambda duration (7,179 ms), and log ingestion (15,011 Lambda bytes and 962 API bytes). The pending-deletion GSI had one write datapoint and no read datapoint. Minutes without datapoints may represent idle time or delayed metrics. This short validation includes setup and maintenance and **cannot** be divided into a per-scan bill or projected to a monthly cost. A later 30-scan whole-stack snapshot follows; retained GB-months and full TCO remain open.

### Consumer DDL 30-scan whole-stack snapshot, 2026-09-29

The [sanitized evidence](metrics/consumer30-whole-stack-cost-2026-09-29.json) binds a 128-file public source archive from commit `e50f197` and its extracted-tree Lambda ZIP to a successful isolated Ohio deployment in the existing staging account. The workload used the consumer app's `repository.sql` and `attempt_manifest.sql`: 30 sequential scans, 500 findings and one write statement per scan, two reads per scan, plus five fresh-process recoveries. Write p50 was **1,794 ms**, the complete loop took **160.275 s** (**0.187 scans/s**), and fresh-process recovery p50 was **7,370 ms**. SDK telemetry selected 97 HTTP samples by time window and includes setup and a forced rollback; its counts must not be divided by 30 as an exact scan price.

For the five-minute **whole-stack** window, CloudWatch reported 203 DynamoDB table read units, 435 table write units, eight pending-deletion GSI read units and no GSI write units. Lambda logs yielded 106 REPORTs and **55.974 billed GB-seconds** at 512 MiB. API access logs yielded 102 requests and 6,586 response payload bytes. CloudWatch recorded 87,450 Lambda and 9,830 API log-ingest bytes. DynamoDB capacity had all five minute buckets; API, Lambda and log metrics had only four of five, so those sums may be low. PITR was enabled. The complete S3 inventory at one moment held 187 objects and 8,132,262 bytes; it is not a GB-month measure. `DescribeTable` reported zero bytes, a stale size estimate that must not be interpreted as free storage.

At the first-tier list rates above, multiplying only those observed whole-stack meters gives an **indicative $0.001403**: Lambda duration $0.000933, Lambda requests $0.000021, DynamoDB table plus GSI capacity $0.000298, HTTP API requests $0.000102 and log ingestion $0.000049. This is a partial five-minute arithmetic subtotal, not an invoice, a per-scan price or a monthly forecast. The window includes setup and scheduled work; missing CloudWatch minutes, all whole-stack S3 requests, retained storage/PITR size, Secrets Manager API calls, internet transfer, backups and operator time still prevent an all-in Tessera/Turso comparison. Terraform state was empty after teardown and independent AWS listings found no test bucket, table, Lambda, API, EventBridge rule or secret.

### Reproducible monthly scenarios, 2026-09-29

The [monthly model](../experiments/cloud_monthly_cost_model.py) now prices every
line item in the ledger from explicit meters: API requests, Lambda requests and
GB-seconds, DynamoDB capacity/storage/PITR, S3 request classes and GB-months,
CloudWatch ingest/archive, Secrets Manager, network egress and operator time.
Run it with `python3 experiments/cloud_monthly_cost_model.py`; the JSON labels each
scenario as measured, derived from the 30-scan report, or hypothetical. It uses
the dated first-tier inputs in this document and does not apply account free
tiers, credits, discounts or taxes.

| Scenario | AWS subtotal/month | Operator assumption | Total with operator |
| --- | ---: | ---: | ---: |
| One idle stack | **$0.7591** | 0 h | **$0.7591** |
| Small tenant | **$0.7751** | 0.25 h at $100/h | **$25.7751** |
| 100 mostly idle tenants in one stack | **$0.9641** | 2 h at $100/h | **$200.9641** |
| Hot consumer tenant, 10,000 scans/month | **$1.6090** | 4 h at $100/h | **$401.6090** |

The idle row is derived from the measured schedule: one Secrets Manager secret,
51,840 scheduled invocations, 51,840 S3 LISTs, and the observed warm Lambda
durations. The hot row scales the measured 30-scan campaign counters linearly
and assumes 8 decimal GB of retained S3 data, 0.05 GB of DynamoDB/PITR data and
6,586 response bytes per 30 scans as a visible egress proxy. Those are explicit
scenarios, not capacity limits or invoices. The model deliberately separates
the AWS subtotal from operator time: at this traffic level labor dominates the
total, while the serverless resource subtotal remains below two dollars. Replace
the hypothetical storage, egress and operator inputs with a full retention
horizon and billing export before making a purchase decision.

The tenant request-budget live report
adds a measured 30-scan campaign after the admission meter. The matching 30
writes and 63 reads incurred 93 extra DynamoDB updates and 93 extra S3 GETs;
the request-only list-price scenario is about $1.025 per million tenant HTTP
requests if each small counter update consumes one WRU. The metered stack's
five-minute whole-stack snapshot had 453 DynamoDB write units, but cannot
attribute them to the meter. Four of five API/Lambda metric minutes arrived
before teardown, and a one-time S3 inventory is not retained GB-months.
The full bill and an attributable latency impact remain unmeasured.

### Billed Lambda duration validation, 2026-09-29

The [isolated two-scan validation](metrics/lambda-billing-snapshot-live-2026-09-29.json)
used the new REPORT collector in the existing Ohio staging account. It parsed
25 Lambda REPORT events with no unmatched events: **9,507 billed ms at 512 MiB,
or 4.7535 billed GB-seconds**, across setup, two synthetic scans and scheduled
work. CloudWatch's invocation metric also showed 25, while only one of the
three queried UTC minutes had datapoints, so the collector deliberately left
`matches_cloudwatch_invocations` unknown. Its log pagination completed in two
requests; delayed log delivery remains possible. The simultaneous inventory
contained 133 current S3 objects and 329,920 bytes, not GB-months. The stack
was destroyed with empty Terraform state and independent active-resource
absence checks for its bucket, table, Lambda and secret. This closes one
instrumentation gap; egress, retained GB-months, account billing and operator
time still prevent an all-in monthly quote.

### API response payload validation, 2026-09-29

The isolated two-scan validation
used the HTTP API access-log `responseLength` field in an existing Ohio
staging account. Its bounded scan parsed all **24** access events it found,
with zero unparsed lengths, and summed **2,309 response payload bytes**.
CloudWatch also showed 24 API requests, but only two of three queried UTC
minutes had datapoints; the collector therefore left the coverage comparison
unknown. The simultaneous `DataProcessed` metric showed 5,781 bytes, which
includes a different traffic scope and must not be substituted for outbound
payload. The stack was destroyed with empty Terraform state and independent
active-resource absence checks for bucket, table, Lambda and secret. Response
payload bytes omit HTTP/TLS overhead, retries outside the API, other AWS data
paths and any transfer pricing, so this is a measured traffic component, not
a billed egress line item or a per-scan cost.

---

# Lowering Tessera's cost: decisions and alternatives

Research from September 7, 2026. Recommendation: simplify coordination in AWS, resolve retention and reads, and evaluate a hybrid journal inside AWS. Changing the engine alone does not remove the cost of persisting every commit. This document proposes changes; none replaces the current adapter or constitutes production validation.

Later decision: stay inside AWS. Comparisons against B2 and other providers remain as background, outside the chosen scope. The shared-journal spike (since removed) contained the local tests and the EBS/EFS deployment comparison.

## Fix the baseline first

The earlier estimate of USD 7.25 per million publications understated DynamoDB: the `ConditionCheck` inside `TransactWriteItems` consumes transactional write capacity. For items up to 1 KiB, the current path consumes one S3 PUT, four WRUs for the two-item transaction, one renewal WRU and one RRU to read the manifest: **USD 8.25 per million**, before other costs. [DynamoDB consumption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html).

Rates used: S3 Standard Ohio USD 5/million PUT, USD 0.40/million GET and USD 0.023/GB-month; DynamoDB Standard on-demand Ohio USD 0.625/million WRU and USD 0.125/million RRU. Verified against the public regional price lists for [S3](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-2/index.json) and [DynamoDB](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/us-east-2/index.json). No free tier, discounts, secondary indexes or retries.

The full formula must include publication requests, retained bytes, queries, recovery, compute, network and human operation. One publication can confirm several operations, but only if they actually coincide within its group and window.

## Comparable persistence options

Prices modeled per **million published batches**, with a coordination head <=1 KiB. These are marginal request costs; the limit columns matter as much as the number.

| Option | USD/M batches | What changes / limit |
|---|---:|---|
| Current prototype S3 + two DynamoDB items | 8.25 | Corrected baseline, implemented and tested with Moto |
| S3 + unified lease/pointer | 5.625 | One conditional write; coordination integration and migration pending |
| DynamoDB journal, delta item <=1 KiB | 2.50 | Delta + head transaction; checkpoints, reads and retention extra |
| DynamoDB journal, delta item <=3 KiB | 5.00 | Size includes keys and metadata |
| DynamoDB journal, 4 KiB delta item | 6.25 | Already above the simplified S3 path |
| B2 + unified DynamoDB head | 0.625 | Free object API; cross-provider network and bytes extra |
| R2 + unified DynamoDB head | 5.125 | Small request saving; cross-provider network extra |
| S3 Express + unified DynamoDB head | 1.755 | Single AZ; extra per-byte and storage charges |

The target prices assume the owner keeps its known version and uses CAS as validation; removing the preceding GET requires changing incremental storage to accept the known published reference. The local cache never replaces the remote CAS. Errors and ambiguous responses still require reconciliation.

### 1. Unify lease, epoch, version and pointer

Storing all those fields in one persistent item allows `UpdateItem` conditioned on owner, epoch, version and expiry; the same update publishes and extends the lease. Acquisition by another owner modifies that same item, preserving version/pointer. AWS documents atomicity of these operations and conditions. [Item contract](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html).

This removes the two-item transaction and the separate per-batch renewal. The item cannot carry TTL or be deleted on lease release: it holds durable state. Renewals during inactivity, acquisitions and releases have their own costs; they are not amortized in the table. Migration, maximum metadata size, response uncertainty and clocks need testing. The hard protection remains against completed takeover, not an evaluation of the server clock.

The local spike tested publish+renew, identity recovery after a lost response, takeover preserving the pointer, and rejection of the old owner even with a valid version. It does not measure real AWS isolation or billing.

### 2. Hybrid journal: small deltas in DynamoDB, large ones in S3

Commit via a transaction that inserts an immutable segment and updates the head with fencing. Materialize Turso from checkpoint + segments. Compact to S3 off the ACK path and retire segments only when a published checkpoint covers them and readers no longer need them.

With `k=ceil(total_item_size/1024)`, the request cost is `1.25 × (k+1)` USD/M batches, counting two WRUs for the segment and two for the head. A reasonable initial boundary to study is <=3 KiB; at 4 KiB S3 wins in this model. Adding DynamoDB storage, journal reads, checkpointing and deletion may lower the profitable boundary. The per-item limit is 400 KiB. [Limits](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html).

The current format changes 4 KiB blocks and adds metadata: many deltas will not fit the profitable range. Compression and/or a finer native segment would be needed. Measure p50/p95/p99 sizes with data of different entropy before implementing. Repeated text in a benchmark does not represent real data.

Overwriting a `last_delta` field is not enough: operations committed since the last checkpoint would be lost. Appending an unbounded delta list to the same item is also wrong: each update is billed for the whole item. Recording only the SQL to re-run it demands solving nondeterministic values, ordering, conflicts and engine changes; prefer a recoverable native record. The Moto smoke verifies segment+head atomicity, but uses sample bytes and does not implement Turso replay.

### 3. Pack several groups in one object

An immutable object can hold segments from eight groups, with offsets and hashes. Keeping independent heads and publishing them through one transaction gives `5/g + 1.25`: **USD 1.875/M group batches at g=8**. Fencing is preserved per head and each group contributes its expected version. A conflict may abort the pack and requires splitting/retrying the publication without re-executing SQL.

A deeper alternative uses a single head per shard: `(5+0.625)/g`, or **USD 0.703/M with eight groups per pack**. It requires per-shard assignment and ownership, a recovery index outside the small head, and controlled contention. It increases blast radius and couples group progress. The publisher may serialize durable commits while SQL workers execute concurrently. This does not automatically grant SQL transactions across independent databases.

It is an option for sufficient aggregate traffic. It is not worth keeping an always-on service to save cents on nearly idle groups.

## Switching provider

### Backblaze B2: the most distinct candidate

Since May 1, 2026, standard API calls are free; storage went to USD 6.95/TB-month. PUT/GET are included; Event Notifications is billed separately. The announcement also specifies usage conditions and that service commitments are unchanged. [Official change](https://www.backblaze.com/blog/backblaze-pricing-and-product-updates/).

The current price list includes free egress up to three times the average monthly storage and USD 0.01/GB beyond it, with partner exceptions. The API table carries an old note of USD 0.005/GB storage; for storage, prefer the current pricing page and the dated announcement. [Current prices](https://www.backblaze.com/cloud-storage/pricing), [API classes](https://www.backblaze.com/cloud-storage/transaction-pricing).

If the owner stays in AWS, sending deltas to B2 may generate AWS egress. Using USD 0.09/GB marginal and GiB conversion for this calculation, with no free allowance, a million batches costs roughly USD 0.97 at 4 KiB average, USD 3.37 at 32 KiB and USD 6.12 at 64 KiB, counting the unified head. The boundary against simplified S3 is around **58 KiB average uploaded per batch**, including checkpoints; NAT lowers that boundary. These examples exclude storage and reads. [Official Ohio networking example](https://aws.amazon.com/vpc/pricing/).

Before choosing B2: test ACK→GET from a new process, intact bodies, lost responses, p95/p99 latency and API limits. The PutObject page consulted does not document `If-None-Match`; do not assume full compatibility or silently drop that condition. References to immutable versions verified by an external coordinator can be investigated. Also, do not equate advertised durability with the same per-AZ failure isolation as S3 Standard. [B2 PutObject](https://www.backblaze.com/apidocs/s3-put-object), [regions](https://www.backblaze.com/docs/cloud-storage-data-regions).

### R2 and S3 Express

R2 Standard charges USD 4.50/M class-A writes, USD 0.36/M class-B reads and USD 0.015/GB-month, with no R2 egress. Switching from AWS just to save USD 0.50/M PUT does not seem sufficient; it may pay off if compute also moves or user-facing egress dominates. [R2](https://developers.cloudflare.com/r2/pricing/).

S3 Express Ohio offers PUT at USD 1.13/M, but stores in one AZ, charges USD 0.11/GB-month, USD 0.0032/GB upload and USD 0.0006/GB retrieval. Consider it for staging or an explicit durability mode, not as an equivalent replacement for Standard. Append still bills every PutObject and caps at 10,000 parts; it does not remove per-commit cost. [Regional catalog](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-2/index.json), [append](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html).

## Save work and bytes before changing infrastructure

**Retention and checkpoints.** `MAX_DEPTH=8` forces a full image every nine publications when no earlier fallback exists. For a stable 10 MiB base and a million publications, that leaves ~1,085 GiB in rebases alone: USD 24.96 if all retained for a full month, on top of deltas. If generated uniformly during the first month from zero, average occupancy is roughly half. Without GC, earlier cohorts keep adding up. This is an algorithm projection, not a measurement of real traffic.

Change the policy based on accumulated bytes, age and recovery budget, and eventually use references to shared blocks/checkpoints. Raising depth without bound shifts cost to recovery. GC needs verified checkpoints, reader references or an explicit horizon, fencing and a receipt policy: deleting receipts may allow re-execution of old IDs.

**Reads.** The prototype rebuilds the image for each read/lookup. A version-checked image cache, preceded by a strong pointer read when freshness is required, removes repeated downloads and opens of the same version. Do not read the writer's speculative connection. Read-your-writes can use a version token; bounded staleness must be a visible option.

**CPU and memory.** Replace VACUUM with correctly sealed native segments, with checkpoint/truncation/SIGKILL tests; compress after measuring entropy and CPU; load groups on demand with memory limits. An idle database should not reserve a whole process. Engine optimization and transport optimization are different jobs.

**AWS networking.** If there is NAT toward S3/DynamoDB, gateway endpoints avoid that path at no endpoint charge. The consumer deployment was not inspected and NAT use was not confirmed. This is a deployment check, not a saving already found. [Gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html).

**Honest batching.** A million uniform operations over 30 days is 0.386/s. The Poisson model without service time gives a mean batch of 1.002 at 5 ms; almost no saving. At 100 ops/s per group it gives 1.5 operations at 5 ms, 3.5 at 25 ms and 11 at 100 ms. Real traffic with bursts, execution and queues changes those results; accumulation delay adds to ACK latency. Measure effective batch, not the configured maximum.

## Alternatives to the current product

| Alternative | When it may win | What changes |
|---|---|---|
| Turso Cloud | Few small databases, many simple row writes; avoid operating storage | Managed service; measure rows, scans, sync and compatibility |
| Durable Objects + SQLite | Per-entity state that can live on Cloudflare | Different runtime and execution model; each object is single-threaded |
| Turso/SQLite on persistent disk | Sustained writes that amortize servers | Disk ACK; own backups and failover; one volume does not replicate across AZs |
| Conventional DB with synchronous replica | Strong need for multiwriter SQL and wide transactions | Compare the operating cost of an existing database against building a distributed journal |

Turso Cloud publishes Free with 5 GB, 10 M written and 500 M read rows; monthly Developer at USD 5.99 with 9 GB and 25 M written rows, plus overage. One operation may write several rows, including receipts, and aborted transactions may also consume rows. Do not automatically transfer the price to our embedded engine or assume identical guarantees. [Plans](https://turso.tech/pricing?frequency=monthly), [usage metering](https://docs.turso.tech/help/usage-and-billing).

Durable Objects SQLite includes 50 M written rows/month in Workers Paid, then USD 1/M; requests, duration and storage add up. The Workers Paid plan has a monthly base plus other consumption. The paid limit per object is 10 GB and its execution is single-threaded: it does not by itself resolve the objection to one writer per unit. [Pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [Workers](https://developers.cloudflare.com/workers/platform/pricing/), [limits](https://developers.cloudflare.com/durable-objects/platform/limits/).

With persistent disk, a hypothetical extra USD 30/month budget equals the requests of about 5.33 M batches at the simplified AWS price. It is only an illustrative boundary: existing compute, replication, backups, operations and capacity change the math. EBS replicates within one AZ; committing locally and uploading to S3 afterwards changes the protection of what was committed against zone loss. Keeping zero RPO across zones requires durable replication before the ACK. [EBS](https://aws.amazon.com/ebs/features/).

## Next experiments, in order

1. **Integrate the unified head.** Criterion: one conditional write on the hot path, metadata <=1 KiB and tests for takeover, expiry, lost response, recovery and migration; record real requests without claiming Moto-AWS equivalence.
2. **Published cache + retention.** Criterion: repeat reads of the same version without downloading the image; growth bounded by configured retention; SIGKILL/old readers/safe receipts during GC.
3. **Measure native segments and compression.** Criterion: p50/p95/p99 bytes, CPU and ACK; ten thousand batches without per-commit VACUUM and exact recovery across crash/checkpoint. That decides whether the DynamoDB journal has economic room.
4. **Compare B2 with S3.** Criterion: the complete modeled bill, including egress/checkpoints/reads, and tests of the object contract. Simulated contract first; the real measurement is separate work with an explicit budget.
5. **Hybrid journal or shared packs depending on traffic.** For genuinely small deltas, journal; for high aggregate concurrency and small groups, packs. Keep Turso Cloud as the total-cost reference before building a service of our own.

## Reproducible evidence

`python -m examples.turso_cost_model --output /tmp/turso-cost-model.json` generates formulas, idealized batching, boundary sizes, retention and network sensitivity. [Saved result](turso-cost-model-2026-09-07.json). Proposal numbers are not deployment results.

`/tmp/tessera-review-venv/bin/python -m examples.turso_coordination_cost_spike` ran **6 correct checks** with Moto and fake credentials. Dependencies: boto3 and moto. It tested request semantics for the unified head and journal; the payload is deliberately a placeholder, not engine replay. No real AWS was used, no production files or consumer-app data were changed. Cloud cost of the work: USD 0.

Rollback for this unit: remove the two research scripts, this document and the generated JSON; there are no runtime protocol changes. Validation was running both scripts and reviewing their assertions/results; the production suite was not re-run for a research-only change.

---

# Cost of DynamoDB authority over a shared EFS journal

Research dated 2026-09-08 for `us-east-2`. The proposed protocol writes an immutable pack durably to Regional EFS, then atomically updates one DynamoDB head per participating group plus one operation receipt. ACK follows the DynamoDB transaction. Prices are marginal and exclude free tiers, taxes, discounts, NAT, backups and conflict retries unless called out.

## Finding

For a predictable journal workload, Regional EFS Bursting is materially cheaper than the Elastic Throughput mode used by the fault probe. Bursting includes baseline throughput with Standard storage; at seven-day hot retention, 4 KiB deltas cost USD 1.81–1.93 per million committed group mutations at pack widths 8–32, before compute. The same path under Elastic costs USD 2.28–2.79, while the optimized S3 reference costs about USD 5.66.

The 300 GB canonical database stays in S3. EFS stores only the recent journal. Batching lowers DynamoDB calls and latency pressure, while bounded retention controls the EFS storage bill. Elastic remains the fallback for workloads whose peaks or burst-credit consumption exceed Bursting capacity.

## Verified Ohio prices

| Resource | Marginal price used |
|---|---:|
| DynamoDB Standard on-demand write request unit | USD 0.625 / million WRU |
| DynamoDB Standard on-demand read request unit | USD 0.125 / million RRU |
| DynamoDB Standard storage | USD 0.25 / GB-month |
| EFS Regional Standard storage | USD 0.30 / GB-month |
| EFS Elastic Throughput write | USD 0.06 / GB |
| EFS Elastic Throughput read | USD 0.03 / GB |
| EFS Bursting throughput | Included with Standard storage capacity |
| Fargate Linux/x86 | USD 0.04048 / vCPU-hour + USD 0.004445 / GB-hour |

Sources: the public regional [DynamoDB catalog](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/us-east-2/index.json), [EFS catalog](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEFS/current/us-east-2/index.json), and [ECS/Fargate catalog](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/us-east-2/index.json). DynamoDB charges two request units per transactional item up to 1 KiB, including transactions cancelled by a failed condition. Transactions support up to 100 unique items and 4 MB. See [DynamoDB request consumption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html) and [transaction limits](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html). AWS documents a 50 KiB/s per GiB Bursting baseline, a 1 MiB/s minimum after credits are exhausted, and a 100 MiB/s minimum while credits are available; monitor `BurstCreditBalance` and `PercentIOLimit`. See [EFS throughput modes](https://docs.aws.amazon.com/efs/latest/ug/performance.html).

## Model

For a pack containing `g` group mutations, the commit transaction updates `g` group heads and inserts one durable receipt. With every item at or below 1 KiB:

```text
DynamoDB transactional write = 2 × (g + 1) WRU per pack
DynamoDB strong pre-read      = g RRU per pack
```

Per million group mutations, the DynamoDB request floor is therefore `1.25 × (1 + 1/g) + 0.125` dollars. It approaches USD 1.375 as the pack grows; it cannot be batched away while DynamoDB validates every group independently.

The EFS estimate is calibrated from the live 300-pack probe: 4,100 mutations with 512-byte payloads produced 3,481,505 logical bytes, 22,761,472 metered write bytes and 2,719,744 metered read bytes. At the verified prices that run cost USD 0.001447 in EFS throughput. The model decomposes it into the observed encoded bytes plus 64,267 fixed write bytes and 9,066 fixed read bytes per pack. This exactly reproduces the measurement, but the future immutable-file layout has not been metered and can change the per-pack overhead.

Payload growth retains the current JSON/base64 encoding as a conservative assumption. A compact binary segment would reduce both I/O and storage. Elastic totals include the calibrated per-byte charge; Bursting totals remove that charge and retain Standard storage. The cost advantage is conditional on staying within Bursting throughput and IOPS.

## Unit economics

USD per one million committed group mutations, seven days of hot retention, no compute:

| Payload | Groups/pack | DynamoDB | Hot storage | Bursting total | Elastic total | Optimized S3 |
|---:|---:|---:|---:|---:|---:|---:|
| 0.5 KiB | 1 | 2.625 | 0.119 | **2.744** | 6.923 | 5.630 |
| 0.5 KiB | 8 | 1.531 | 0.067 | **1.598** | 2.165 | 5.630 |
| 0.5 KiB | 32 | 1.414 | 0.061 | **1.475** | 1.655 | 5.630 |
| 4 KiB | 1 | 2.625 | 0.454 | **3.079** | 7.544 | 5.655 |
| 4 KiB | 8 | 1.531 | 0.402 | **1.933** | 2.787 | 5.655 |
| 4 KiB | 32 | 1.414 | 0.396 | **1.810** | 2.277 | 5.655 |
| 32 KiB | 1 | 2.625 | 3.130 | **5.755** | 12.514 | 5.860 |
| 32 KiB | 8 | 1.531 | 3.077 | **4.609** | 7.756 | 5.860 |
| 32 KiB | 32 | 1.414 | 3.072 | **4.486** | 7.246 | 5.860 |

The S3 reference is the improved path, one Standard PUT plus one DynamoDB WRU per group publication, with equivalent retained encoded bytes at USD 0.023/GB-month. It is deliberately stricter than comparing against Tessera's current USD 8.25 per million path.

At 32 KiB, Bursting remains cheaper with seven-day retention but crosses the S3 reference at roughly ten days. Packs should be compacted into larger archive/checkpoint objects before moving cold; applying EFS IA lifecycle directly to many small files introduces its 128 KiB billing floor and retrieval charges.

## Compute

Reusing already-paid consumer workers makes incremental journal compute approximately zero. A dedicated always-on Fargate task with 0.25 vCPU and 0.5 GB costs USD 9.01/month; two tasks cost USD 18.02/month at 730 hours. For 4 KiB payloads and seven-day retention, the break-even volume against optimized S3 is:

| Groups/pack | One Fargate task | Two Fargate tasks |
|---:|---:|---:|
| 8 | 2.42 M mutations/month | 4.84 M mutations/month |
| 32 | 2.34 M mutations/month | 4.69 M mutations/month |

An event-driven 512 MiB Lambda lasting 100 ms adds USD 0.129/M mutations at width 8 and USD 0.032/M at width 32. At 500 ms it adds USD 0.546/M and USD 0.136/M respectively. These figures include USD 0.20/M Lambda requests and exclude the free tier. [Lambda pricing](https://aws.amazon.com/lambda/pricing/).

Example monthly bill for 4 KiB payloads, width 8 and seven-day hot retention:

| Volume | Reused consumer compute | Lambda, 100 ms | One Fargate | Two Fargate | Optimized S3 |
|---:|---:|---:|---:|---:|---:|
| 1 M | 1.93 | 2.06 | 10.94 | 19.95 | 5.66 |
| 10 M | 19.33 | 20.62 | 28.34 | 37.35 | 56.55 |
| 100 M | 193.28 | 206.20 | 202.29 | 211.30 | 565.52 |

## Single 300 GB database, 500 concurrent users

Concurrency is not request rate. A concrete planning case uses 100 queries/s total, 10 writes/s, 4 KiB deltas, seven-day hot retention and existing consumer compute. One unified conditional DynamoDB head publishes each group-commit pack; durable operation IDs live in the pack chain. The 300 GB canonical image costs USD 6.90/month in S3, and the seven-day EFS journal occupies about 34 GB for USD 10.21/month.

| Maximum group-commit wait | Mean writes/pack | DynamoDB/month | EFS hot/month | S3 base/month | Storage + authority |
|---:|---:|---:|---:|---:|---:|
| 25 ms | 1.25 | 15.55 | 10.21 | 6.90 | **32.67** |
| 100 ms | 2.00 | 9.72 | 10.21 | 6.90 | **26.83** |
| 250 ms | 3.50 | 5.55 | 10.21 | 6.90 | **22.67** |
| 1,000 ms | 11.00 | 1.77 | 10.21 | 6.90 | **18.88** |

The mean pack sizes use an idealized Poisson arrival model. They expose the latency/cost tradeoff rather than promise batching. S3 cache misses, a second checkpoint copy, logs and response egress remain outside this table. No dedicated Fargate tasks are required when query execution, caches and the journal client run in existing consumer workers.

## The cheaper architecture

Even the best EFS/DynamoDB row above is too expensive for a low-cost Tessera tier. Both services sit on the commit path: the seven-day EFS copy costs USD 10.21/month and DynamoDB charges for every publication. The cheaper design removes both from ordinary commits:

1. Existing consumer workers append to a short local WAL.
2. A commit is acknowledged after two of three workers in separate Availability Zones fsync the same ordered log entry.
3. The leader can fail and be re-elected. Every worker may submit writes, but one consensus log still defines the SQLite write order.
4. One log entry establishes a cross-tenant atomic visibility boundary; materializers expose it only after every participating database reaches that committed index.
5. A closed WAL segment is uploaded to S3 every minute. Periodic compaction folds archived segments into the canonical checkpoint.

At the same 10 writes/s and 4 KiB logical payload, the WAL carries 106.17 GB/month. Two remote replicas plus 25% protocol overhead transfer 265.42 GB across AZ boundaries before compression. AWS charges Regional data transfer on both sides of an EC2 cross-AZ transfer, modeled here as USD 0.02/GB total. One day of closed segments remains in S3 Standard before compaction.

| Configuration | Monthly cost |
|---|---:|
| S3 Standard base + three 10 GB gp3 WAL volumes | USD 14.91 |
| S3 Standard base + included Fargate ephemeral storage | USD 12.51 |
| Previous row + 50% WAL compression target | **USD 9.81** |
| Monthly S3 Standard-IA recovery snapshot + 50% compression | USD 7.23 |

The last row is a recovery archive, not an active read origin. It keeps a monthly full snapshot in S3 Standard-IA and an average of 15 days of compressed segments in S3 Standard. Standard-IA has a 30-day minimum, a 128 KiB minimum object size and a retrieval fee; restoring all 300 GB adds about USD 3.00. If the workload reads the checkpoint frequently, Standard-IA is the wrong class.

Fargate 1.4+ includes 20 GiB of ephemeral storage per task. The zero-disk-cost rows require three already-paid consumer tasks, enough free space after the image and logs, placement across three AZs, and rolling operations that preserve quorum. If consumer does not meet those conditions, use gp3 and add USD 2.40/month. See [Fargate task storage](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html), [Amazon EBS pricing](https://aws.amazon.com/ebs/pricing/), [EC2 data transfer pricing](https://aws.amazon.com/ec2/pricing/on-demand-backup/), and [S3 storage-class constraints](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html).

The USD 9.81 result is a target to validate. It assumes the WAL compresses by 50%; raw WAL costs USD 12.51. It excludes logs, backup history, public response egress, cache misses and the engineering cost of consensus. The active working set matters: 300 GB of cold data is cheap, while 300 GB of random hot reads needs more local cache or pays S3 request and retrieval costs.

There is a cheaper durability tier: acknowledge locally and upload a closed WAL segment to S3 once per minute. Its modeled AWS floor is USD 7.20/month with one day of segments before compaction. It can lose up to 60 seconds if the writer or its AZ dies before upload.

| Tier | Approximate monthly cost | Commit guarantee |
|---|---:|---|
| Async S3 | USD 7-9 | Local durability; RPO up to 60 seconds on host/AZ loss |
| Replicated WAL on existing tasks | USD 10-15 | Two-of-three fsync; survives one node/AZ failure with RPO 0 |
| DynamoDB + EFS | USD 19-33 | Managed Regional journal and conditional authority |

The current experimental `Owner` is not this design yet. Its request handlers share one process lock across reads and writes, so a single Owner cannot provide 500 concurrent queries. The implementation needs an established consensus component for the write log and local read connections that apply the committed log index without taking the writer lock.

## Conflict sensitivity

Every failed multi-group condition can consume the DynamoDB transaction and leave an EFS orphan. Modeling independent retries for the 4 KiB, width-8, seven-day case gives:

| Transaction conflict rate | Attempts/success | USD/M before compute |
|---:|---:|---:|
| 0% | 1.000 | 2.787 |
| 1% | 1.010 | 2.811 |
| 5% | 1.053 | 2.912 |
| 20% | 1.250 | 3.383 |

This remains cheaper than the optimized S3 request path in the modeled range, but latency and wasted Turso preparation will become the larger problem before request cost does. Pack formation should avoid combining unrelated hot groups when one group repeatedly causes transaction cancellation.

## Product decision

The shared journal should start as a bounded hot path with explicit admission gates:

- Bursting throughput for predictable load, guarded by `BurstCreditBalance`, `PercentIOLimit` and ACK latency alarms.
- Elastic throughput only when measured peaks or exhausted credits justify its per-byte charge.
- Encoded delta p95 and actual group-commit width measured in the consumer app.
- Checkpoint and GC operational before enabling production ACKs.
- Existing consumer compute reused initially; no dedicated six-task read/write fleet.
- Large deltas use a shorter EFS horizon or publish directly to S3.

This suggests an adaptive Tessera product rather than one storage mode for every tenant: aggregate hot workloads and cross-tenant commits use DynamoDB-authorized EFS packs; sparse or large workloads use the object path. The next measurement needed from the consumer app is the real distribution of effective pack width, encoded delta bytes, conflict overlap and required hot-recovery horizon.

## Reproduction

Run:

```bash
python -m examples.shared_journal_authority_cost \
  --output docs/shared-journal-authority-cost-2026-09-08.json
```

The checked-in JSON contains all 27 combinations of payload size, pack width and retention, plus Lambda/Fargate and conflict sensitivity. This research fetched public pricing catalogs and created no AWS resources; cloud cost was USD 0.
