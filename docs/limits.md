---
title: Capacity and limits
nav_order: 7
---

# Capacity, cost and operating limits

Guidance for planning a deployment. Every number carries a label —
**measured** (committed dated artifact), **derived** (arithmetic on measured
counters), **hypothetical** (unmeasured projection). Sources and repro
commands live on the [Measurements](measurements.html) page; guarantees and
their honest limits live in [Guarantees](GUARANTEES.html).

## Write throughput (one tenant)

One tenant has exactly one writer at a time — the lease is the fencing
token, not a performance knob.

| Publish mode | Write latency | Effective ceiling | Label |
|---|---|---|---|
| `per_commit` (default) | one S3+delta+manifest CAS round-trip per write — p50 **1,528 ms** over WAN, **117.8 ms** in-region | ~0.7 writes/s WAN, ~8.5 writes/s in-region | measured |
| `windowed` (opt-in) | local WAL commit — p50 **0.68 ms** measured over WAN | bounded by local commit rate, not network; hundreds of commits per publish window | measured |

`windowed` trades a bounded durability window for that throughput: a write
is acked at local commit and becomes durable at the next publish
(`--publish-every-s`, default 1 s). `sync: true` or `POST /flush` closes
the window on demand. An owner crash loses at most the commits inside the
open window — never a write acknowledged `version <= published`.

## Read throughput

| Read | Latency | Label |
|---|---|---|
| warm owner `POST /read` | p50 **0.55 ms** (SQLite-local; never touches the network) | measured |
| independent `Store.read`, warm cache | one manifest HEAD + local data | measured |
| cold first read (S3 hydration) | ~**2.2 s** for a small tenant; scales with downloaded bytes | measured |

Cold reads historically downloaded ~4.2 MB for ~200 KB of logical data —
chunked VFS + incremental compaction bound that to live pages plus the
delta chain. `min_version` reads resolve against the manifest's committed
frontier (`data_version`) and poll until it is reached or `read_wait_ms`
elapses.

## Cost per workload shape

Derived from measured op counts at public S3/DynamoDB rates (us-east-2,
first tier, no free tier). Inputs are measured; the dollar figures are
**derived**, never billed totals.

| Workload | Backend ops | Derived cost |
|---|---|---|
| 1M writes, `per_commit` | ~3.0 PUTs + 1 GET per write | ~**$15.5** |
| 1M writes, `windowed` (2 s window) | **0.0083 PUTs** per commit — measured on S3 | ~**$0.043** |
| warm reads | 0 object ops (local SQLite) | ~$0 |
| cold reads | 1 HEAD warm + hydration GETs once | cents |
| storage, post-GC | retained versions + live data | $0.023/GB-month |

Idle tenants bill nothing but storage bytes — that is the product point.

## Operating limits

| Limit | Value | Mechanism |
|---|---|---|
| Writers per tenant | 1 | lease epoch fencing; a displaced owner CAS-fails |
| Write durability window (`windowed`) | `publish_every_s` (default 1 s) | commits inside the window are lossy on crash |
| Lease TTL / renewal | `ttl_ms` — Store 10 s, Owner 30 s / renews at TTL/3 | renewal failure at expiry fences the owner; transient errors do not |
| Publish error budget | `publish_error_budget_s` (default 30 s) | transient backend errors retry with jittered backoff; fencing signals (`CASFailed`, `LeaseLost`, `TenantBusy`, `NotFound`, `ManifestError`) and budget exhaustion fence the owner |
| Skew margin | ≥1 s before expiry | deletes and takeover checks stop ahead of the bound |
| History retention | `gc_keep_history` count ∪ `history_days` | union policy; archived manifests are restorable |
| Object growth | bounded by retained versions + live data | fenced destructive GC deletes outside the mark closure |
| GC scheduling | **manual** | no built-in scheduler — an operator or cron must call `gc()`; this is a real gap, not hidden |
| Grace window for young objects | `gc_delta_age_s` | unreferenced objects younger than it are never deleted |
| Tenant isolation | manifest CAS is the commit point | one conditional write decides each durable frontier |
| Multi-region | none | single-region S3 + DynamoDB today |

## What is NOT yet bounded

- Sustained operation past the measured window: the
  [8-hour AWS soak](measurements/soak-aws-8h-2026-10-09.json) covered
  90,189 writes, 92,789 reads, 6 fenced failovers and 6 GC sweeps with
  zero durability violations — longer horizons are unmeasured.
- Concurrent write-during-sweep and crash-resume mid-sweep on real AWS —
  structurally argued via mark-under-lease + skew bound, not yet measured.
- Per-tenant size ceiling: no hard cap in code; practical bound is cold
  hydration time and compaction cost, which scale with retained bytes.
- DynamoDB capacity: lease ops are tiny items; no measured contention
  ceiling yet.
