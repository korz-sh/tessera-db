---
title: Cache and GC
parent: Reference
nav_order: 4
---

# Cache coordination and GC safety

## Sharing a Store cache

Use a separate `root` per worker for parallel local work. Multiple Store
instances may share a root on a local POSIX filesystem when they all run
this implementation and use the same backend — their mutable cache
operations are serialized across threads and processes.

```python
worker_a = Store(backend, root="/tmp/tessera/worker-a")
worker_b = Store(backend, root="/tmp/tessera/worker-b")
```

The remote tenant lease still decides who may publish; the cache lock
only protects local hydration, write callbacks, cleanup and eviction.

### Operating constraints

- Sharing a root serializes local work even for different tenants. Lock
  waits are blocking and unbounded; use separate worker roots when
  independent progress matters.
- `.tessera-cache.lock` is a persistent advisory-lock file — do not
  delete it while any worker runs. A dedicated root is required: never
  store application files or backend objects in it.
- Create Stores in fresh processes; forking with active Stores/threads
  is unsupported. Do not nest Store operations from callbacks.
- After abrupt worker death, the next lock holder discards mutable
  tenant directories and rehydrates from published manifests; immutable
  `chunks/` remain reusable. This recovers from process death, not disk
  failure.

## History survives collection

GC protects snapshots, deltas and chunks referenced by **every retained
history manifest**, not just the current one.

### Retention contract

- Keep the union of the newest `gc_keep_history` versions and archives
  within `history_retention_s`. Unknown archive mtimes are retained when
  time retention is enabled.
- Mark all history before pruning it. An object referenced only by a
  pruned archive becomes eligible on a subsequent GC run.
- `gc_keep_snapshots` is a floor, not a limit on snapshots needed by
  retained history — keeping more history can retain more storage.
- An unreferenced object must also exceed `gc_delta_age_s`; unknown
  mtimes are preserved. Object age does **not** pin a reader.

An unreadable or malformed listed archive aborts marking before any
deletion — GC never silently ignores damaged metadata.

### Deletion fencing

Destructive GC requires `Backend.supports_fenced_gc` plus
`gc_delete`/`gc_delete_batch`:

- **LocalBackend** holds its lifetime lease flock and prevents release
  until each delete finishes — atomic exclusion.
- **S3DynamoBackend** uses complete-mark, skew-bounded deletion: the mark
  covers the live manifest plus every retained history archive (each
  archive is CAS-ordered before its successor, so the closure contains
  everything a successor can reference), and every delete batch is gated
  by `ensure_alive()` plus a check that the lease is not within 1 s of
  `expires_at`. Deletes therefore precede the earliest legal takeover by
  ≥1 s — the same skew bound the fencing protocol already assumes.
  Cross-tenant keys and expired/near-expiry leases are rejected outright.

A failure after earlier deletes does not roll them back. Backends without
the capability reject destructive GC before acquiring a lease.

`store.gc(tenant, dry_run=True)` reports advisory `*_candidates` lists
without leases, deletions or pruning — a plan is an observation, not
authorization to execute it later.
