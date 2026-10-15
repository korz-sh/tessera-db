---
title: Install
nav_order: 2
---

# Install

```bash
pip install 'tessera-db'
```

The core package has **no mandatory third-party dependencies** — every
optional surface is an opt-in extra:

| Extra | Pulls in | Needed for |
|---|---|---|
| `aws` | `boto3` | `S3DynamoBackend` (the production backend) |
| `vfs` | `apsw` | Page-level VFS reads of format-2 tenants; without it, v2 reads fall back to full hydration (slower, still correct) |
| `orm` | `sqlalchemy` | `tessera.orm` SQLAlchemy integration |
| `experimental-turso` | `pyturso==0.7.2` | `tessera.experimental.turso` v2 runtime |
| `bench` | `psycopg2-binary`, `pymysql` | `examples/benchmark.py` engines |
| `dev` | all of the above + pytest + moto | running the test suite |

## From source

```bash
git clone https://github.com/korz-sh/tessera-db.git
cd tessera-db
pip install -e '.[dev]'
```

## Usage

```python
from pathlib import Path
from tessera import LocalBackend, Store

store = Store(LocalBackend("./objects"), root="./cache")
try:
    store.create("acme", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

    def add_user(conn):
        conn.execute("INSERT INTO users VALUES (?, ?)", (1, "Ada"))
        conn.commit()

    _, saved_version = store.write("acme", add_user)
    names = lambda conn: conn.execute("SELECT name FROM users ORDER BY id").fetchall()
    assert store.read("acme", names, min_version=saved_version) == [("Ada",)]

    store.export("acme", Path("./acme.sqlite"))      # standard SQLite file
    store.clone("acme", "acme-investigation")
    print(store.diagnostics("acme"))
finally:
    store.close()
```

`fn` receives an open WAL-mode `sqlite3.Connection` and must commit inside
itself — committed frames become a write-once delta object, then the
manifest CAS publishes them atomically.

### What a response means

| Result | Interpretation |
|---|---|
| `Store.write()` returns `(result, version)` | The version is published and contains the write |
| Owner `POST /write` → 200 | `per_commit` mode: already durable (`version == published`). `windowed` mode: committed locally; durable once `version <= published` — `sync: true` or `POST /flush` forces it |
| `min_version=N` on `read` | Polls until a manifest covering commit N exists, then `StaleRead` after `read_wait_ms` |
| Failed CAS / lost response | `fn` is never retried; reconcile the operation — do not replay it |

### Recovery and copies

`restore(version=...)` rebuilds full schema and data at an exact version
(new published version — the replaced frontier stays recoverable).
`export()` and `clone()` accept the same selectors. `diagnostics()`
inspects durable state without acquiring leases.

### Owner: the hot write path

For sustained writes, a per-tenant `Owner` holds the fenced lease and
serves HTTP `/write`, `/read`, `/flush`, `/health`. The container
entrypoint `python -m tessera.owner` is env-driven (`TESSERA_TENANT`,
`TESSERA_BUCKET`, `TESSERA_TABLE`, `TESSERA_PUBLISH_MODE`, ...); for a
local run, construct `Owner` in Python the same way `Store` is built.

Two publish modes: `per_commit` (default — every ACK is durable on
return) or `windowed` (`publish_every_s` — ACK at local commit, fold
the window into one publish; bounded durability window, ~0.01 PUTs per
commit).
