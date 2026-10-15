---
title: Guarantees and their limits
parent: Reference
nav_order: 5
---

# Guarantees and their limits

Each guarantee with the exact mechanism that delivers it and the honest limit it has. If a row has no limit, it is because it has none, not because we did not look for one.

| Guarantee | How it is delivered | Limit |
|---|---|---|
| **Mutual exclusion per tenant** | DynamoDB lease with conditional write; monotonic epoch that grows on each acquire | The lease TTL (Store 10 s / Owner 30 s defaults) is the failover window: after a crash, that is the maximum time the tenant stays blocked |
| **Fencing** | The epoch rides in the manifest CAS (`last_epoch`); a zombie with an old epoch fails the CAS and gets `Conflict` | Tolerates clock skew up to 1 s: a lease counts as expired 1 s before `expires_at` |
| **Owner failover** | In the default `per_commit` publish mode a successful ACK waits for the durable CAS; in `windowed` mode it waits for the local commit and the receipt exposes the gap (`version` vs `published`) | Recovery waits until the lease can be acquired. A lost response leaves the outcome unknown; forwarding does not replay the operation |
| **Read-your-writes** | `version`, `published` and `min_version` live on the commit-frontier axis (`manifest.data_version`) — a `/write` receipt's `version` is always a valid `min_version` and `read(min_version=N)` polls until a manifest *covering* commit N exists; sparse frontiers are legal. The manifest's `version` is a different axis — an object generation moved by every CAS, including compaction — and is deliberately never what `min_version` compares against | Maximum wait `read_wait_ms` (500 ms default), then `StaleRead` |
| **Durability** | S3 (11 nines) and all data objects write-once: there is never an UPDATE over data | The manifest is mutable, but it is a single small JSON |
| **Integrity** | sha256 in each TSD1 delta trailer, re-verified before applying; SQLite `integrity_check` in the suite | — |
| **Cold reads of large tenants** | The apsw VFS serves the database by pages: a page miss downloads only the content-addressed chunk containing it | M6 acceptance: an indexed query on a 200 MB DB touches <5% of chunks |
| **Tenant isolation** | Independent key prefixes; per-tenant leases and GC | No code path crosses two tenants |
| **Owner authentication** | The owner generates a session token at startup and publishes it in the lease item; it demands `Authorization: Bearer` (constant-time compare) on `/write` and `/read`. Whoever can read the lease is already IAM-authorized to operate the tenant | `/health` stays open on purpose (ECS probe, exposes no data). The token lives as long as the lease: no long-lived credential, no revocation to execute |

## Limits worth reading twice

**GC grace is a bet, not a promise.** A reader that takes longer than `gc_delta_age_s` (1 h default) to apply the deltas of a manifest it already downloaded can lose them: GC deletes them once grace passes.

**GC deletion on S3 is skew-bounded, not atomic.** LocalBackend serializes deletes against lease takeover with file locks; S3 has no conditional delete, so `S3DynamoBackend` instead (a) marks the complete reference closure under the lease — current manifest plus every archived history, each archive CAS-ordered before its successor — and (b) gates every delete batch behind `ensure_alive()` plus a skew-margin check that refuses deletes within 1 s of `expires_at`. Every issued delete therefore precedes the earliest legal takeover by ≥1 s of wall clock — the same skew assumption the fencing protocol already makes. A delete delayed >1 s in transit could theoretically land after a successor's PUT; even then it can only remove objects outside the closure, which no successor's manifest can reference. Cross-tenant keys and expired/near-expiry leases are rejected outright.

**"One writer per tenant" is the guarantee, not a bug.** There is no intra-tenant multi-writer. If you need real parallelism inside a tenant, the pattern is sharding the tenant, not forcing the design.

**The ACK waits for durable publication — unless you opt into the window.** `Store.write()` and a successful owner `/write` in the default `per_commit` mode mean the data is persisted and the manifest passed the CAS. The replaced manifest is preserved before the CAS. With `publish_mode="windowed"` the ACK acknowledges only the local commit: commits ahead of `published` live solely in the owner's WAL and are lost if the local directory is discarded before the next publish (bounded by `publish_every_s`). The write receipt carries the contract (`version` = committed sequence, `published` = durable floor); `sync: true` on `/write` or `POST /flush` closes the window on demand. Fencing, mutual exclusion and fail-closed semantics are identical in both modes.

**A transport error can leave an unknown outcome.** `WriteOutcomeUnknown` requires reconciling the operation; it is neither a rollback nor authorization for automatic replay. A post-commit maintenance error is logged without changing the successful write outcome.

**Exact recovery is identified by version.** `restore(version=...)` publishes full schema and data as a new version. `at=` uses approximate object timestamps: a history preservation followed by an interrupted publication can anticipate the timestamp relative to the real replacement. It is not an exact wall-clock cut.

**S3 Express (opt-in) moves to one AZ, not total durability.** With `express_bucket`, deltas live in a single AZ between publish and compaction; afterwards they are folded into a multi-AZ snapshot and GC deletes them. The window is bounded and known, but it exists.

## API stability

| Guarantee | How it is delivered | Limit |
|---|---|---|
| **The public surface does not break within a MAJOR** | Everything exported by `tessera.__all__` is frozen by `tests/test_api_compat.py`: positional signatures, defaults, exception hierarchy, and the `Backend` contract. The suite goes red on a break, with a message saying it requires a MAJOR | Covers signatures and types, not semantics: a *behavior* change with the same signature is not caught by an introspection test |
| **The on-disk format is backwards compatible** | The manifest versions the format (v1 and v2 coexist with migration); breaking it is a MAJOR even if the code compiles | The suite uses fixtures from earlier versions; it does not represent every possible combination of data |

What is **not** covered by SemVer: non-exported submodules
(`tessera.chunks`, `tessera.delta`, `tessera.vfs`, `tessera.lease`) and any
underscore-prefixed name. They can change in a PATCH. `tessera.orm` is
public, but optional: it lives behind the `[orm]` extra.
