---
title: How it works
nav_order: 3
---

# How it works

Everything is objects in a backend namespace (`LocalBackend` for dev,
`S3DynamoBackend` on AWS). There is exactly **one mutable object** per
tenant — the manifest — and every mutation is a conditional write.

## Object layout

```mermaid
graph LR
    subgraph "tenant-a/"
        M["manifest.json<br/>the ONE mutable object<br/>(CAS if-match)"]
        S["snap/42.sqlite<br/>write-once snapshot (v1)"]
        C["chunks/ab/cd1234...<br/>content-addressed chunks (v2)"]
        W["wal/00000003-00000007.delta<br/>write-once deltas (TSD1)"]
        H["history/40.json<br/>archived manifests"]
    end
    M --> S & C & W
    H -.-> S & C
```

The manifest pins a snapshot (v1) or a content-addressed chunk set (v2)
plus the delta chain since. Deltas are `TSD1` frame sets — WAL frames
captured *before* the committing connection checkpoints and deletes them.

## Write path (`Store.write`)

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant S as Store
    participant B as Backend (S3)
    C->>S: write(tenant, fn)
    S->>B: acquire/renew lease (fence)
    S->>B: HEAD manifest — reject if last_epoch > lease.epoch
    S->>S: hydrate snapshot + deltas → local db.sqlite
    S->>S: fn(conn) — caller commits inside
    S->>B: PUT wal/epoch-seq.delta (create-only)
    S->>B: PUT history/<v>.json (archive old manifest)
    S->>B: PUT manifest.json (if-match: old etag)
    alt CAS lost
        S->>S: discard local dir, raise Conflict
        Note over S: fn is NEVER retried
    else CAS won
        S-->>C: (new manifest, version)
    end
```

On CAS failure the local copy is destroyed before `Conflict` propagates —
the local database can never drift ahead of the committed manifest.

## Read path

```mermaid
flowchart TD
    R[read tenant, fn, min_version] --> H[HEAD manifest<br/>If-None-Match etag cache]
    H --> V{data_version >= min_version?<br/>commit frontier, not the object generation}
    V -- no --> P[poll until deadline<br/>else StaleRead]
    V -- yes --> F{tenant format}
    P --> H
    F -- "v1 / v2 fully local" --> HY["hydrate snapshot + deltas<br/>into local cache → sqlite3"]
    F -- "v2, not local" --> VFS["TesseraVFS: fetch base chunks<br/>PAGE BY PAGE on demand"]
    HY --> FN["fn(conn)"]
    VFS --> FN
```

Reads never touch leases. VFS reads need the `vfs` extra (APSW); without
it, v2 reads fall back to full hydration — slower, still correct.

## Hot owner (fenced write path)

For sustained writes a per-tenant `Owner` holds a DynamoDB-fenced lease
and serves HTTP `/write`. Two publish modes:

- `per_commit` (default): `/write` commits locally **and** waits for the
  delta + history + manifest CAS round-trip before ACKing — the response
  is durable on return.
- `windowed`: `/write` ACKs at local commit; the background loop folds
  every commit inside `publish_every_s` into ONE publish (bounded
  durability window — commits ahead of the `published` floor are lost if
  the owner's WAL is discarded before the flush). `sync: true` on
  `/write` or `POST /flush` forces an immediate publish.

```mermaid
sequenceDiagram
    participant App
    participant O as Owner (HTTP)
    participant D as DynamoDB lease
    participant B as S3
    O->>D: acquire lease (epoch n, fencing token)
    App->>O: POST /write {sql, sync?}
    alt per_commit (or sync/flush)
        O->>B: PUT delta + history, CAS manifest
        O-->>App: 200 {version, published}
    else windowed
        O-->>App: 200 {version > published}
        loop publish tick
            O->>B: ONE delta+history+manifest per window
        end
    end
    Note over O,D: lease lost → fenced:<br/>stop serving, never publish again
    Note over D: next owner acquires at epoch n+1
```

A fenced owner can keep its HTTP server but stops accepting writes (409)
and exits its loop — a stale epoch can never CAS over a newer manifest.

## Recovery

- `read_at(tenant, version)` / `history(tenant)` — archived manifests under
  `history/` keep deleted deltas addressable for `gc_keep_history` days.
- `restore` — copies the tenant to `<tenant>@v<n>` and hydrates that
  version; the live tenant is untouched.
- `clone` / `export` — materialize any version as a standalone sqlite file.
- `gc` — mark & sweep: objects referenced by the live manifest OR any
  archived manifest survive; the rest is deleted past a grace window.
