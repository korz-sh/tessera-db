"""tessera + SQLAlchemy ORM: models instead of hand-written SQL.

Runnable as-is (uses the local backend, no AWS needed):

    pip install "tessera-db[orm]"
    python examples/orm_example.py

Shows the complete cycle: schema derived from the models, writes with
relationships, read-your-writes, and what happens when a write fails.
"""

import tempfile

from sqlalchemy import Column, ForeignKey, Integer, String, select
from sqlalchemy.orm import declarative_base, relationship

from tessera.backends.local import LocalBackend
from tessera.orm import TesseraORM
from tessera.store import Store

Base = declarative_base()


# @spec examples/author
class Author(Base):
    __tablename__ = "authors"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    books = relationship("Book", back_populates="author")


# @spec examples/book
class Book(Base):
    __tablename__ = "books"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    author_id = Column(Integer, ForeignKey("authors.id"))
    author = relationship("Author", back_populates="books")


# @spec examples/main
def main() -> None:
    tmp = tempfile.mkdtemp()
    # In production: S3DynamoBackend(bucket=..., table_name=..., region=...)
    store = Store(LocalBackend(f"{tmp}/objects"), root=f"{tmp}/cache")
    db = TesseraORM(store, Base)

    # 1. One tenant = one database. The schema comes from the models.
    db.create_tenant("acme")
    print("tenant created with the models' schema")

    # 2. Writing: runs under the tenant lease; on return, tessera uploads
    #    the write-once delta and does the manifest CAS.
    def seed(session):
        ada = Author(name="Ada Lovelace")
        ada.books = [Book(title="Notes on the Engine"), Book(title="Note G")]
        session.add(ada)

    version = db.write("acme", seed)
    print(f"write published, version={version}")

    # 3. Read with read-your-writes: waits to see that version.
    def list_books(session):
        author = session.scalars(select(Author)).one()
        return author.name, [b.title for b in author.books]

    name, titles = db.read("acme", list_books, min_version=version)
    print(f"read: {name} -> {titles}")

    # 4. A regular ORM update.
    def rename(session):
        session.scalars(select(Author)).one().name = "A. Lovelace"

    version = db.write("acme", rename)
    print("renamed ->", db.read("acme", lambda s: s.scalars(select(Author)).one().name,
                               min_version=version))

    # 5. If the callback fails, NOTHING is published: nothing left halfway.
    def broken(session):
        session.add(Author(name="ghost"))
        raise RuntimeError("something went wrong in the business logic")

    try:
        db.write("acme", broken)
    except RuntimeError as exc:
        print(f"write failed ({exc}) — nothing was published")

    authors = db.read("acme", lambda s: [a.name for a in s.scalars(select(Author))])
    print("final authors:", authors)

    # 6. Multi-tenant: another tenant, same model classes, separate database.
    db.create_tenant("globex")
    db.write("globex", lambda s: s.add(Author(name="Grace Hopper")))
    print("acme :", db.read("acme", lambda s: [a.name for a in s.scalars(select(Author))]))
    print("globex:", db.read("globex", lambda s: [a.name for a in s.scalars(select(Author))]))


if __name__ == "__main__":
    main()
