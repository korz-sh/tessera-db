# TesseraDB

**One complete SQLite database per tenant, stored on S3, with real
transactions and real SQL.** Every tenant gets an isolated `.sqlite` file,
its own WAL history, and a conditional manifest that is the single source
of truth — not a shared table with `WHERE tenant_id = ?`.

An idle tenant reserves **no compute**. Its bytes sit on S3 and cost what
bytes cost. When a write arrives, work happens; when nothing arrives,
nothing is billed beyond storage.

| | One DB per tenant (RDS) | Shared DB + `tenant_id` | **tessera** |
|---|---|---|---|
| Isolation | ✅ real | ❌ none | ✅ real (separate files) |
| Idle cost | $$$ per tenant, always | low | ~$0 (S3 bytes only) |
| Per-tenant restore | snapshot juggling | row surgery | point the manifest back |
| Per-tenant export | dump | filter & pray | it *is* a file — download it |

Status: `2.0.0` — stable release, verified by an 8-hour live-AWS soak
(90k writes, zero durability violations). [Measured evidence](docs/measurements.md)
— every cost and latency claim on this site is labeled measured, derived,
or hypothetical.

## Quickstart

```bash
pip install 'tessera-db'          # core: no mandatory dependencies
pip install 'tessera-db[aws]'     # + S3DynamoBackend (boto3)
```

```python
from tessera import LocalBackend, Store

backend = LocalBackend("./objects")      # dev; production: S3DynamoBackend
store = Store(backend, root="/tmp/tessera-cache")

store.create("acme", "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
_, v = store.write("acme", lambda c: (c.execute(
    "INSERT INTO kv VALUES ('lang', 'en')"), c.commit()))
store.read("acme", lambda c: c.execute("SELECT v FROM kv").fetchone(),
           min_version=v)                # read-your-writes
```

## How it works

Everything is objects in a backend namespace. There is exactly **one
mutable object per tenant** — the manifest — and every mutation is a
conditional write (CAS). Committed WAL frames become write-once delta
objects; the manifest CAS is the only commit point, so two writers can
never silently lose each other's commits.

- **Durable writes** — `Store.write` ACKs after the delta lands and the
  manifest passes CAS. A hot per-tenant `Owner` (HTTP) batches publishes.
- **Point-in-time recovery** — snapshots + delta replay; `read_at`,
  `restore`, `clone`, `export` are first-class.
- **Fenced ownership** — a DynamoDB lease epoch fences every write; a
  displaced owner CAS-fails and can never overwrite a newer frontier.
- **Bounded cost** — one commit ≈ 3 PUTs + 0 GETs on the warm path
  (protocol-pinned in CI); `windowed` publish amortizes to <0.01 PUTs
  per commit.

Full protocol walkthrough with diagrams: [docs/how-it-works.md](docs/how-it-works.md).

## What it is not

- Not a multi-writer database — one tenant, one writer (the lease is the
  contract, not a limitation).
- Not cheaper than a normal DB for one big tenant — the crossover against
  one `t4g.micro` per tenant is around **2 tenants** (measured).
- Not multi-region — single-region S3 + DynamoDB.
- Not magic — writes go through S3, so `per_commit` publish costs a WAN
  round-trip. Use the owner with `windowed` mode when that matters.

## Deploying on AWS

The [Terraform module](deploy/aws/README.md) creates API Gateway, Lambda,
a private S3 bucket, DynamoDB, and the deletion/retention sweeps in your
account. The service API is documented in [docs/CLOUD-API.md](docs/CLOUD-API.md).

## Docs

- [Install & usage](docs/install.md)
- [How it works](docs/how-it-works.md)
- [Guarantees and their limits](docs/GUARANTEES.md)
- [Capacity, cost and limits](docs/limits.md)
- [Measurements](docs/measurements.md)

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
