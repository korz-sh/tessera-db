---
title: Home
nav_order: 1
description: "One complete SQLite per tenant on S3 — real transactions, real SQL, billed per work done."
permalink: /
---

# tessera-db

**One complete SQLite database per tenant, stored on S3, with real
transactions and real SQL.** Not a shared table with `WHERE tenant_id = ?`:
every tenant gets an isolated `.sqlite` file, its own WAL history, and a
conditional manifest that is the single source of truth.

An idle tenant reserves **no compute**. Its bytes sit on S3 and cost what
bytes cost. When a write arrives, work happens; when nothing arrives,
nothing is billed beyond storage.

## Why it exists

Giving each tenant a full database is expensive at scale — one Postgres per
customer does not multiply well. Sharing one database and filtering by
`tenant_id` does not actually isolate anything: one bad `UPDATE` without a
`WHERE`, one slow migration, one connection string leak, and tenants bleed
into each other.

tessera picks a third option:

| | One DB per tenant (RDS) | Shared DB + `tenant_id` | **tessera** |
|---|---|---|---|
| Isolation | ✅ real | ❌ none | ✅ real (separate files) |
| Idle cost | $$$ per tenant, always | low | ~$0 (S3 bytes only) |
| Per-tenant restore | snapshot juggling | row surgery | point the manifest back |
| Per-tenant export | dump | filter & pray | it *is* a file — download it |

The crossover against one RDS `t4g.micro` per tenant is around **2 tenants**
([measured](/tessera-db/measurements)). Below that, run a normal database —
it is simpler and cheaper.

## What you get

- **Durable writes** — every committed transaction is a write-once delta on
  S3; the manifest advances with a conditional `PutObject` (CAS), so two
  writers can never silently lose each other's commits.
- **Read-your-writes** — `min_version` reads never see a stale state.
- **Point-in-time recovery** — snapshots + delta replay; `read_at`,
  `restore`, `clone`, `export` are first-class operations.
- **Fenced ownership** — a hot owner holds a DynamoDB-fenced lease and
  batches publishes (~0.6 ms/write locally, measured); when it dies, the
  next owner takes over at epoch+1 and the fenced one cannot write.
- **History that survives GC** — archived manifests keep deleted deltas
  addressable for `history_days` days.

## What it is not

- Not a general-purpose production database (yet — v2.0.0).
- Not cheaper than a normal DB for one big tenant.
- Not magic: writes go through S3, so a single `Store.write` costs tens of
  milliseconds. Batch or use the owner when that matters.

## Status

`2.0.0` — the v2 experimental runtime (journal, Turso adapter, cloud
surface) is packaged behind `tessera.experimental` with explicit opt-in
gates. The stable v1 surface (`Store`, `Owner`, `LocalBackend`,
`S3DynamoBackend`) is what the [measurements](/tessera-db/measurements)
cover.
