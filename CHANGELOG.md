# Changelog

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the project follows [SemVer](https://semver.org/).

A reminder of what each number means here: breaking the **on-disk format**
or **relaxing a guarantee** (fencing, write-once, durable ACK) is a MAJOR
even if the code still compiles.

## [Unreleased]

## [2.0.0] - 2026-10-09

First stable release of the v2 durability line. Identical package code to
`2.0.0rc2`; this release is promoted on evidence, not on new features —
the `stable` stage of the release gate is verified against a pinned,
uninterrupted 8.32-hour sustained workload on live AWS (us-east-2,
`windowed` publish): 90,189 committed writes and 92,789 reads through
six fenced owner failovers, six fenced GC sweeps and five compactions,
with zero durability violations, zero unverifiable ledger entries, and
0.2167 S3 PUTs per committed write. See
`docs/measurements/soak-aws-8h-2026-10-09.json` and `docs/limits.md` for
capacity, cost and operating-limit guidance.

The soak campaign also hardened the exercise harness itself —
interruption-safe checkpointing, a dead-owner watchdog, honest failover
accounting, and unverified-vs-violation reconciliation — without
touching the shipped package.

## [2.0.0rc2] - 2026-10-08

Second release candidate of the v2 line. Bounds write-path cost and
retained storage: windowed publication batches WAL commits into one
remote publication per window, and fenced destructive GC reclaims
unreferenced objects on `S3DynamoBackend`. Validated live on AWS
(`us-east-2`): 0.0083 PUTs/commit in windowed mode and a destructive
sweep reducing a tenant from 64 to 38 objects with exact reconciliation.

### Added

- Opt-in `publish_mode="windowed"` for the HTTP owner. `/write` commits
  to the local WAL and returns `{version, published}` without a
  synchronous publication; `sync: true` and `POST /flush` close the
  window on demand. `per_commit` remains the default and is unchanged.
- Fenced destructive GC on `S3DynamoBackend`: `supports_fenced_gc`,
  `gc_delete`/`gc_delete_batch`, a complete mark closure over the current
  and archived manifests, skew-bound lease checks per delete batch, and a
  grace window for young objects. Backends without fenced deletion keep
  refusing destructive GC.
- `Manifest.data_version`: the committed-data frontier a manifest
  covers, distinct from `version` (object/CAS generation). Compaction
  carries `data_version` forward; publication stamps it with the folded
  commit sequence; `read(min_version=)` resolves against it. The field is
  additive-optional in the wire format — omitted when equal to
  `version`, so legacy manifests parse unchanged.
- Benchmark artifacts now record `meta.tessera_backend_ops` (counted
  PUTs/commits/publishes by category) plus the publish mode and cadence.
- GitHub Pages site, Apache Magpie maintenance framework, and a
  measurements page separating measured, derived and hypothetical
  claims.
- Large atomic bulk payloads across the cloud and bench harness.

### Changed

- Store and owner code decomposed into mixin packages; source comments
  standardized to English.
- `manifest.version` may now be sparse: a windowed publication stamps
  `max(version + 1, commit_seq)` so a write receipt remains a valid
  `min_version`. History and CAS semantics are unaffected.
- Documentation consolidated from 27 pages to 15.
- Expanded rc1 evidence set: AWS benchmarks, concurrency and retention
  sweeps, storage cost floor, atomic multi-group and batched consumer runs.

### Fixed

- Correctness: a background incremental compaction could CAS a manifest
  whose object `version` numerically covered an unpublished commit, so
  `read(min_version=)` could resolve without the write being durable.
  The `data_version` axis closes the hole.
- Owner no longer self-fences on publish/compaction races; transient
  lease-renewal errors no longer fence the owner.
- Coordination timeouts relaxed to survive parallel test load.

### Removed

- Obsolete documentation and benchmark artifacts.

## [2.0.0rc1] - 2026-10-02

Release candidate of the v2 line. Freezes the `tessera.experimental.turso` surface, the `tessera-turso-native-log-1` native-log format, and the `pyturso==0.7.2` engine for review. RC evidence includes sustained source-pinned load, fenced takeover, lost-response reconciliation, cold recovery, rollback exports with SQLite integrity, and process death before/after commit. Consumer staging routes v2 declaratively; production keeps v1 by default.

Installing from a public artefact is outside this gate; the local wheel and its manifests are validated.

### Added

- Experimental HTTP API for isolated v2 databases: CAS catalog, scoped
  tokens, transactions with operation ID, and durable receipt read and
  search. Includes an API Gateway HTTP API v2 adapter with the same
  dispatcher.
- Apache-2.0 license, engine and economics skills, and the BYOC product
  contract with total-cost roadmap and public release.
- Versioned `GroupBoundary` contract for the Turso runtime: an isolated
  tenant per group by default, explicit shared groups, and authorization
  of the principal before acquiring leases, hydrating caches, or running
  SQL.
- New Turso manifests persist the boundary and reject reopenings with a
  different membership. Earlier alpha manifests only reopen as the
  isolated `tenant == group` boundary.

## [2.0.0a3] - 2026-09-24

Opt-in alpha of the v2 runtime with fenced native-log publication. Pins
the durable pointer authority, verifies artefacts before reading, rejects
publications from displaced owners, and reconciles receipts after an
uncertain response. Adds `BulkOperation`, resumable migration from v1
tenants, and `CallbackStore` for the consumer app. The surface remains
under `tessera.experimental.turso`; production requires the RC gate.

## [2.0.0a2] - 2026-09-22

Defines the identified-operations contract, payload and queue limits,
durable receipts, and query states. Consolidates the bounded runtime and
the recovery mechanisms that precede native-log publication.

## [2.0.0a1] - 2026-09-08

First prerelease of the v2 line. The stable core keeps its API and
formats; what is new is an opt-in surface to evaluate the shared journal
and Turso without depending on files left outside the wheel.

### Added

- `tessera.experimental.journal`: local journal with atomic packs between
  groups, epoch fencing, durable deduplication, batching, and archival.
- `tessera.experimental.turso`: native snapshots, incremental transport,
  fenced publication, and concurrent owners with atomic receipts.
- `experimental-turso` extra, pinned to `pyturso==0.7.2`.

### Alpha status

- Existing persisted identifiers are preserved, and recovery is tested
  from the packaged code.
- Signatures inside `tessera.experimental` can change between alphas.
  There is no productive GC/retention, native incremental sealing,
  autonomous remote failover, tenant authorization, or sustained
  capacity proof yet.
- This entry prepares the artefact; publishing still requires the
  deliberate `v2.0.0a1` tag and the Release workflow.

## [1.4.0] - 2026-09-07

Version prepared; publishing requires the deliberate tag and the
workflow validations. Keeps v1/v2 formats. Operational change: remote
destructive GC is rejected because S3/DynamoDB cannot fence a DELETE with
the lease; use `gc(tenant, dry_run=True)` to inspect candidates without
deleting or pruning history.

- GC preserves historical snapshots and aborts if it cannot complete the
  mark phase.
- Shared caches coordinated between processes/threads; after a crash,
  mutable files are discarded and rebuilt from the published manifest.
- Reproducible local benchmark and costs expressed as calls/bytes.
- Wheel verification against the source code and a release manifest with
  version, commit, and SHA-256; full recovery smoke outside the checkout.

### Added

- `Store.export()` produces an independent SQLite copy without overwriting existing destinations; `Store.clone()` creates a tenant with its own objects.
- `Store.diagnostics()` inspects durable version, format, deltas, and history without acquiring leases or modifying storage.
- `[aws]` install extra with the S3/DynamoDB backend runtime dependencies.
- SDD `reliable-tenant-lifecycle`, regression tests, and an executable guide in `docs/RELIABLE-LIFECYCLE.md`.

### Fixed

- The owner confirms HTTP after publishing, with the version that includes the write. A publication failure removes it without later publishing the uncertain local state.
- Store and owner share synchronous history preservation before the CAS. A post-commit maintenance error does not change the write's successful outcome.
- Forwarding returns `WriteOutcomeUnknown` if the send may have happened, with no replay and no local fallback. Owner rejections, including `401`, propagate.
- `restore()` recovers full schema and data in a new version; readers with a warm cache rebuild their state when the database changes or the delta chain rewinds.
- The local copy is discarded on a failed callback after commit or an uncertain delta upload; the lease is re-verified after preserving history and before the CAS.

### Compatibility and validation

- Existing signatures and formats 1/2 remain supported. `publish_every_s` is kept, but no longer allows an HTTP ACK before publication: earlier owner benchmarks must be re-run.
- `version=` identifies an exact state; `at=` keeps approximate object timestamps, especially across interrupted publications.
- Local work not yet published or deployed. Local and Moto evidence does not imply new AWS validation.

## [1.3.0] - 2026-09-02

Fixes a bug that left tenants **permanently unwritable**, found in a
DynamoDB cost spike.

> [!IMPORTANT]
> This version requires the lease table to have TTL on **`ttl_at`**,
> not on `expires_at`. See the migration note below.

### Fixed

- **The DynamoDB TTL sweeper was taking the `epoch`.** The table had TTL
  on `expires_at`, which is the *lease* expiry: it lasts ~10 s and
  `release_lease` sets it to 0, so **every** item was always eligible.
  The sweeper runs up to ~48 h later, so any idle tenant lost its item —
  and with it the `epoch`, which is the fencing token. Numbering went
  back to 1, the reused epoch regenerated write-once delta keys that
  already existed, and **every write died on `Conflict`, forever**.

  Safety was never at risk (the `last_epoch` fence rejects the old
  writer); the damage was to availability — silent, and hitting exactly
  the sleeping tenants.

  The fix has two layers: `acquire_lease` writes **`ttl_at`** (a
  dedicated TTL attribute, one year out) and accepts **`min_epoch`** as a
  floor. `Store` detects the sweep and re-acquires with the manifest's
  `last_epoch` as the floor, so a tenant that already lost its item
  **recovers on its own**.

### Changed

- **`release_lease` makes 1 DynamoDB call instead of 2.** The preceding
  `get_item(ConsistentRead)` only distinguished "already released" from
  "stolen"; that comes out of the same `UpdateItem` with
  `ReturnValuesOnConditionCheckFailure=ALL_OLD`. Besides 25% fewer calls,
  it removes a TOCTOU: the decision is made on the state the condition
  evaluated.

### Migration

`Backend.acquire_lease` gains the optional kwarg `min_epoch` (default
`0`): existing implementations keep working unchanged.

The lease table needs TTL on `ttl_at`. **It is not a one-step apply**:
DynamoDB requires disabling the TTL first and applies a ~1 h rate limit
between modifications.

```bash
aws dynamodb update-time-to-live --table-name <table> \
  --time-to-live-specification "Enabled=false,AttributeName=expires_at"
# wait out the window, then enable on ttl_at
```

With the TTL disabled the system works the same: nothing is lost, the
abandoned tenant's item just stops being collected.

## [1.2.2] - 2026-09-02

Manifest serialization performance. No API or format changes: the emitted
JSON is byte-for-byte identical.

### Changed

- **`Manifest.to_json()` uses `vars()` instead of `dataclasses.asdict()`**.
  `DeltaEntry` and `ChunkEntry` are flat dataclasses (only `str`/`int`,
  no nesting), so `asdict`'s recursive copy was pure overhead — and the
  manifest is serialized on **every write**, inside the CAS. Profiling
  the write path showed 210 `asdict` calls per write.

  | Manifest | before | after | |
  |---|---|---|---|
  | v1, 5 deltas | 0.0123 ms | 0.0062 ms | 2.0x |
  | v1, 50 deltas | 0.0927 ms | 0.0312 ms | 3.0x |
  | v2, 20 deltas + 200 chunks | 0.4174 ms | 0.1452 ms | 2.9x |

  The gain grows with manifest size: a large tenant saves 0.27 ms per
  write.

### Documented

- **Protocol spike** (`examples/BENCHMARK.md`): gRPC measured at 0.318 ms
  per round-trip vs 0.215 for the current HTTP/1.1 — **slower**, verified
  with three configurations. A custom binary framing would give 6-10x,
  but **batching gives 57-82x without touching the transport**, so the
  recommendation is to batch the unit of work. Applies the same with the
  ORM: the cost is the *write* (delta + CAS), not the statement.

## [1.2.1] - 2026-09-01

Hot owner performance. No API or format changes.

### Changed

- **The owner speaks HTTP/1.1 with keep-alive** (was HTTP/1.0, the
  `BaseHTTPRequestHandler` default): the server no longer closes the
  connection after each response. **2.4x ops/s** (802 → 1949 with one
  writer), p50 0.53 → 0.28 ms.
- **Forwarding reuses the TCP connection to the owner** (per-sandbox
  pool) instead of opening a new one per write: **2.0x** (1681 → 3344
  ops/s), 0.295 ms less per forwarded write — half the forwarding cost,
  measured on loopback. A dead connection is retried once with a fresh
  one before failing open.

Context: the August benchmark attributed the owner's 296 ops/s to the
serializing lock. Measuring the same work with and without HTTP showed
**98% of the cost was the HTTP layer** (58 472 ops/s without it vs 1 285
with it); the lock has headroom to spare. Details in
`examples/BENCHMARK.md`.

## [1.2.0] - 2026-09-01

All additive: code written against 1.0.0 and 1.1.0 runs unchanged.

### Added

- **Per-tenant authentication**. The owner generates a session token at
  startup and publishes it in the lease item; it demands
  `Authorization: Bearer` (constant-time compare) on `/write` and
  `/read`. Whoever can read the lease is already IAM-authorized to
  operate the tenant, so **there is no secret to provision** — not even
  for an auto-promoted owner on Fargate — and the token dies with the
  lease. Forwarding propagates the credential with **zero extra AWS
  calls**. `/health` stays open (ECS probe).
- **API key in the reference deploy**: `X-Tessera-Key` against a Secrets
  Manager secret, cached per sandbox. Fails closed.
- **Point-in-time recovery**: `Store.history()` lists the available
  points, `Store.read_at(version=|at=)` reads the past without touching
  the present, and `Store.restore()` returns the tenant to that state.
  **A restore is just another write** (under lease, normal CAS): fencing
  stays intact, history is not rewritten, and the restore is reversible.
- **Recovery CLI**: `python -m tessera.restore --tenant X --list |
  --at "..." [--dry-run]`.
- `history_retention_s` (default 7 days): the recovery window is measured
  in **time**, not versions.
- `VersionUnavailable` on the public surface: states how far back you
  CAN go.
- **Format-compatibility fixtures**: tenants generated by the published
  1.0.0, which the suite reads on every run.
- `examples/use_cases_orm.py`: real use cases, all through the ORM.

### Changed

- The GC now retains a historical manifest if either the count **or** the
  time saves it. Previously it only counted versions, so a tenant with 10
  writes/second retained ~2 seconds of history.
- Proprietary `LICENSE` and copyright headers; `Private :: Do Not Upload`
  as a safeguard against an accidental push to public PyPI.

## [1.1.0] - 2026-09-01

Everything in this version is **additive**: code written against 1.0.0
runs unchanged (verified by `tests/test_api_compat.py`).

### Added

- **Package public surface**: `from tessera import Store,
  S3DynamoBackend, TenantBusy` instead of knowing the submodule layout.
  Resolved lazily (PEP 562): `import tessera` pulls in neither `boto3`
  nor `apsw`, and costs 0.35 ms.
- **Optional SQLAlchemy integration** (`tessera.orm`): declarative models
  on the same engine, without touching lease, fencing, or CAS.
  Installed with `pip install "tessera-db[orm]"`.
- **Optional S3 Express One Zone for deltas** (`express_bucket=`): the
  hot-path PUT drops from 28 to 11 ms; the complete warm write from 70 to
  55 ms p50. The manifest, snapshots, and chunks stay in standard
  multi-AZ S3.
- **Owner auto-promotion on Fargate Spot** under write contention, with
  configurable capacity provider (`OWNER_CAPACITY_PROVIDER`, default
  `FARGATE_SPOT`).
- `advertise_host` and `acquire_wait_s` on `Owner`, with task-IP
  auto-detection via ECS metadata (the endpoint published with bind
  `0.0.0.0` was unusable from outside).
- **Compatibility safety net** (`tests/test_api_compat.py`): freezes the
  1.0.0 public signatures and fails with an explicit message if a change
  requires a MAJOR.

### Changed

- History archiving on compaction is now asynchronous: it leaves the
  write's critical path.
- `Development Status` moves from `4 - Beta` to `5 - Production/Stable`,
  matching what the README already said.
- Documentation reorganized: [`docs/index.md`](docs/index.md) as the
  tracking dashboard and [`docs/GUARANTEES.md`](docs/GUARANTEES.md) with
  the guarantees and their limits.

### Fixed

- The latency tables attributed the manifest CAS to DynamoDB. It is a
  conditional `PutObject` to **S3**; with the sticky lease, a warm write
  **does not touch DynamoDB**. The README cost table pointed at the wrong
  component for the same reason.

### Performance notes

All measured against the real stack in us-east-2, with methodology in
[`examples/BENCHMARK.md`](examples/BENCHMARK.md):

| Scenario | p50 |
|---|---|
| Serverless warm write | 70 ms (55 with S3 Express) |
| Write with hot owner | 0.6 ms |
| Reads via CloudFront from South America | −53% |

## [1.0.0] - 2026-08-30

First stable version. The four roadmap levels (M0–M8) implemented and
validated against real AWS.

### Added

- Byte-versioned **TSD1** delta format, with sha256 trailer re-verified
  before applying.
- **Single manifest per tenant** written with CAS (`If-Match`): the only
  mutable object in all of storage.
- **Per-tenant lease with monotonic epoch** (fencing): a writer with an
  old epoch cannot commit.
- **Read-your-writes by version** (`read(min_version=N)`).
- Compaction with `VACUUM INTO` and GC with a grace window and K-snapshot
  retention.
- Interchangeable backends: `LocalBackend` (filesystem with `flock`) and
  `S3DynamoBackend` (S3 + DynamoDB).
- **Content-addressed 4 MiB chunks + manifest v2**, with page-level reads
  via the apsw VFS: a page miss downloads only the chunk containing the
  page.
- Writes over VFS with a local WAL.
- **Hot owner** (level 2): retains the lease and serves writes over HTTP.

[Unreleased]: https://github.com/korz-sh/tessera-db/compare/v2.0.0a1...HEAD
[2.0.0a1]: https://github.com/korz-sh/tessera-db/compare/v1.4.0...v2.0.0a1
[1.4.0]: https://github.com/korz-sh/tessera-db/compare/v1.3.0...v1.4.0
[1.2.2]: https://github.com/korz-sh/tessera-db/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/korz-sh/tessera-db/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/korz-sh/tessera-db/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/korz-sh/tessera-db/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/korz-sh/tessera-db/releases/tag/v1.0.0
