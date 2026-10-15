"""tessera use cases with the SQLAlchemy ORM.

Runnable as-is, no AWS needed:

    pip install "tessera-db[orm]"
    python examples/use_cases_orm.py

Covers what an internal service actually does: one database per tenant,
recovery from a human error, and the hot owner for write bursts. All
through the ORM, which is how 95% of application code gets written.
"""

from __future__ import annotations

import tempfile
import time

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, select
from sqlalchemy.orm import declarative_base, relationship

from tessera import LocalBackend, Store
from tessera.orm import TesseraORM

Base = declarative_base()


# @spec examples/project
class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    tasks = relationship("Task", back_populates="project")


# @spec examples/task
class Task(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    done = Column(Integer, default=0)
    project_id = Column(Integer, ForeignKey("projects.id"))
    project = relationship("Project", back_populates="tasks")


def _banner(text: str) -> None:
    print(f"\n{'=' * 68}\n{text}\n{'=' * 68}")


# ---------------------------------------------------------------------------
# 1. One database per tenant
# ---------------------------------------------------------------------------


# @spec examples/case-1-database-per-tenant
def case_1_database_per_tenant(db: TesseraORM) -> None:
    """Every customer gets their own SQLite: real isolation, not a WHERE.

    A bug in a query cannot return another tenant's data, because the
    other tenant's data lives in a different file, under a different S3
    prefix.
    """
    _banner("1. One database per tenant (real isolation)")

    for tenant in ("acme", "globex"):
        db.create_tenant(tenant)

    def seed_acme(session):
        project = Project(name="Migration to tessera")
        project.tasks = [Task(title="Design"), Task(title="Implementation")]
        session.add(project)

    def seed_globex(session):
        session.add(Project(name="Portal redesign"))

    db.write("acme", seed_acme)
    db.write("globex", seed_globex)

    def list_projects(session):
        return [p.name for p in session.scalars(select(Project))]

    print("  acme  :", db.read("acme", list_projects))
    print("  globex:", db.read("globex", list_projects))
    print("  -> no query can cross the boundary: they are different databases")


# ---------------------------------------------------------------------------
# 2. Read-your-writes
# ---------------------------------------------------------------------------


# @spec examples/case-2-read-your-writes
def case_2_read_your_writes(db: TesseraORM) -> None:
    """Read immediately after writing, with a guarantee.

    In a distributed system this is NOT free: the read waits to see the
    version the write published, or fails with StaleRead instead of
    silently returning stale data.
    """
    _banner("2. Read-your-writes (no stale data)")

    version = db.write("acme", lambda s: s.add(Task(title="Deploy", project_id=1)))
    tasks = db.read(
        "acme",
        lambda s: [t.title for t in s.scalars(select(Task).order_by(Task.id))],
        min_version=version,  # <- the guarantee
    )
    print(f"  write published version {version}")
    print(f"  the read sees: {tasks}")


# ---------------------------------------------------------------------------
# 3. Recovering from a human error
# ---------------------------------------------------------------------------


# @spec examples/case-3-recovery
def case_3_recovery(db: TesseraORM, store: Store) -> None:
    """The DELETE without a WHERE, and how to get out of it.

    Data is write-once: the old state was never overwritten by anyone, it
    is still in S3. Recovering is time travel over objects that already
    exist, not restoring a cold backup.
    """
    _banner("3. Point-in-time recovery (the DELETE without WHERE)")

    before = time.time()
    time.sleep(1.05)

    def disaster(session):
        for task in session.scalars(select(Task)):
            session.delete(task)

    db.write("acme", disaster)
    print("  after the accident:", db.read("acme", lambda s: s.scalars(select(func.count(Task.id))).one()), "tasks")

    # First LOOK, without touching anything.
    previous = store.read_at(
        "acme",
        lambda conn: [r[0] for r in conn.execute("SELECT title FROM tasks")],
        at=before,
    )
    print(f"  read_at(before) shows: {previous}  <- without modifying current state")

    store.restore("acme", at=before)
    recovered = db.read(
        "acme", lambda s: [t.title for t in s.scalars(select(Task).order_by(Task.id))]
    )
    print(f"  after the restore: {recovered}")

    points = store.history("acme")
    print(f"  recovery points available: {[p['version'] for p in points]}")
    print("  -> the 'bad' state stays archived: the restore is reversible too")


# ---------------------------------------------------------------------------
# 4. Hot owner
# ---------------------------------------------------------------------------


# @spec examples/case-4-batch-writes
def case_4_batch_writes(db: TesseraORM) -> None:
    """The most common performance mistake, and why.

    What costs in tessera is the WRITE (write-once delta + manifest CAS),
    not the statement. One `db.write()` per row pays that cost N times;
    the same loop inside ONE write pays it once.
    """
    _banner("4. Batch the unit of work (tens of x)")

    db.create_tenant("bench")
    names = [f"task-{i}" for i in range(30)]

    start = time.perf_counter()
    for n in names[:10]:
        db.write("bench", lambda s, n=n: s.add(Project(name=n)))
    slow = (time.perf_counter() - start) / 10 * 1000

    start = time.perf_counter()

    def all_at_once(session):
        for n in names[10:]:
            session.add(Project(name=n))  # idiomatic ORM, nothing special

    db.write("bench", all_at_once)
    fast = (time.perf_counter() - start) / 20 * 1000

    print(f"  one db.write() per row      : {slow:7.2f} ms/row")
    print(f"  the loop INSIDE one write   : {fast:7.2f} ms/row")
    print(f"  -> {slow / fast:.0f}x faster, no special API")
    print("\n  OK   db.write(t, lambda s: [s.add(X(...)) for x in batch])")
    print("  BAD  for x in batch: db.write(t, lambda s: s.add(X(...)))")


# @spec examples/case-5-hot-owner
def case_5_hot_owner() -> None:
    """When a tenant writes often, a process retains the lease.

    Drops from ~70 ms to ~0.6 ms per write. Under contention, the client
    can run this owner ONLY on Fargate Spot and shut it down once idle.
    """
    _banner("5. Hot owner (write bursts)")
    print("""  from tessera import Owner

  owner = Owner("acme", backend, publish_every_s=1)
  owner.serve_in_background()

  # The owner demands its session token, published in the lease:
  #   Authorization: Bearer <owner.session_token>
  # A client discovers it by reading the lease — the same GetItem it was
  # already doing to find the endpoint, no extra AWS calls.

  owner.stop()   # publishes what is pending and releases the lease

  -> 0.6 ms per write vs 70 ms on the serverless path.""")


# @spec examples/use-cases-orm-main
def main() -> None:
    tmp = tempfile.mkdtemp()
    # In production: S3DynamoBackend(bucket=..., table_name=..., region=...)
    store = Store(LocalBackend(f"{tmp}/objects"), root=f"{tmp}/cache")
    db = TesseraORM(store, Base)
    try:
        case_1_database_per_tenant(db)
        case_2_read_your_writes(db)
        case_3_recovery(db, store)
        case_4_batch_writes(db)
        case_5_hot_owner()
        _banner("Done")
        print("  See README.md and docs/ for the details of each capability.")
    finally:
        store.close()


if __name__ == "__main__":
    main()
