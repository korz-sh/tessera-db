# Experimental cloud API

API Gateway terminates HTTPS for the AWS stack. Requests and responses use JSON and `Authorization: Bearer <token>`. The administrator token is stored in Secrets Manager; database tokens are returned once and persisted only as SHA-256 digests.

| Method and path | Token | Purpose |
|---|---|---|
| `POST /v1/tenants` | Root | Create a tenant with `name`, stable `request_id`, and optional limits |
| `GET /v1/tenants` | Root | Page tenants |
| `GET /v1/tenants/{id}` | Root | Inspect tenant limits, reservation count, and management token IDs |
| `PUT /v1/tenants/{id}/rate-limits` | Root | Replace the tenant's fixed-minute request and incoming-body budgets |
| `POST /v1/tenants/{id}/tokens` | Root | Issue a tenant management token, returned once |
| `DELETE /v1/tenants/{id}/tokens/{token_id}` | Root | Revoke a tenant management token |
| `POST /v1/databases` | Tenant manager; root replay only in strict mode | Create a database with `name`, `schema` and a stable `request_id` |
| `GET /v1/databases` | Root or tenant manager | Page active databases in scope with `limit` and `cursor` |
| `GET /v1/databases/{id}` | Root or owning tenant manager | Inspect active/deleting/deleted state |
| `GET /v1/databases/{id}/history` | Root or owning tenant manager | Page retained version numbers and current version |
| `POST /v1/databases/{id}/restore` | Root or owning tenant manager | Copy one retained version into a new database ID |
| `GET /v1/databases/{id}/limits` | Root or owning tenant manager | Read database limits |
| `PUT /v1/databases/{id}/limits` | Root or owning tenant manager | Replace database limits |
| `DELETE /v1/databases/{id}` | Root or owning tenant manager | Revoke tokens and start asynchronous deletion |
| `POST /v1/databases/{id}/tokens` | Root or owning tenant manager | Create a read or write token |
| `DELETE /v1/databases/{id}/tokens/{token_id}` | Root or owning tenant manager | Revoke a token |
| `POST /v1/databases/{id}/transactions` | Write | Execute parameterized statements under a stable `operation_id` |
| `POST /v1/databases/{id}/query` | Read or write | Execute a parameterized read; optional `min_version` |
| `GET /v1/databases/{id}/operations/{operation_id}` | Read or write | Look up a durable receipt |
| `GET /v1/databases/{id}/usage` | Root or owning tenant manager | Page current S3 group bytes; optional `logical=true` cold-opens SQLite |

Tenant creation accepts `{"name":"team-a","request_id":"stable-tenant-id","limits":{"max_databases":32,"max_frontier_bytes":1073741824,"max_operation_bytes":1048576,"max_tokens":64},"rate_limits":{"max_requests_per_minute":300,"max_body_bytes_per_minute":67108864}}`. Both limit objects are optional; when supplied, each requires all of its fields. Allowed database-limit ranges are 1–256 reserved databases, 65,536–1,073,741,824 frontier bytes per database, 1,024–1,048,576 operation bytes per database, and 1–64 SQL tokens per database. Rate limits allow 1–600 requests and 1,024–1,073,741,824 request-body bytes per fixed UTC minute. Defaults are 300 requests and 64 MiB of bodies per minute. Up to eight tenant management tokens may be active. A tenant manager must supply `request_id` for database creation; it may manage only its own databases. Other tenants' database IDs return 404 on management routes. Database SQL tokens remain scoped to one database and cannot manage the tenant.

The database-count quota reserves a slot before catalog creation and releases it only after the deletion worker confirms an unversioned bucket, a permanently retired pointer, an empty S3 group, and durable `deleted` state. Interrupted creation can hold a slot until retried or reconciled. The BYOC Terraform stack disables new root-created databases by default: root can replay an existing legacy `request_id`, while new databases require a tenant management token. Root restore of an unowned legacy source also requires migration or an existing replay target. Older root-created databases remain outside quotas until the checkpointed migration assigns them to a tenant. The explicit compatibility switch `allow_legacy_root_create=true` restores the old, ungoverned creation path and must not be used to claim complete tenant admission. This alpha has no total billed-byte or storage quota.

The AWS API charges one atomic DynamoDB admission update before every authenticated tenant management or SQL request. The counter key is tenant ID plus fixed UTC minute; an exhausted count or body-byte budget returns HTTP 429 with `Retry-After` and `retry_after_seconds`. Rejected requests do not execute SQL. A retry counts as another request; for an uncertain write, look up or retry the **same** operation ID and payload after the delay. Root operator requests and legacy unowned databases are outside tenant budgets. The local loopback server has no DynamoDB meter. The per-minute byte limit counts incoming HTTP bodies, not response bytes, S3 history, backups, or billed AWS bytes. Existing tenants without a stored `rate_limits` field receive the defaults; their stable creation request IDs still replay. Root can replace a tenant's rate limits with `{"rate_limits":{"max_requests_per_minute":300,"max_body_bytes_per_minute":67108864}}`. Changing a limit affects the current minute's next request; it does not reset counters. The global API Gateway throttle is separate.

`POST /v1/databases` accepts `{"name":"app","schema":["CREATE TABLE ..."],"request_id":"stable-id"}`. A retry with the same request ID and identical content returns the same database ID. `POST /v1/databases/{id}/transactions` accepts `{"operation_id":"stable-id","statements":[["INSERT INTO kv VALUES (?, ?)",[1,"value"]]]}`. A successful response includes a published version. After an uncertain response, look up the receipt or retry the **same** operation ID and payload; do not create a new ID for an uncertain write.

For Python clients, create a tenant with root, then create a database and issue a scoped write token with that tenant's management token. Connect using the HTTPS API URL and database ID:

```python
from tessera.experimental.cloud_client import CloudSqlClient, CloudWriteOutcomeUnknown

db = CloudSqlClient(api_url, database_id, write_token)
statements = [["INSERT INTO kv VALUES(?, ?)", [1, "one"]]]
try:
    receipt = db.write("business-event-1", statements)
except CloudWriteOutcomeUnknown as exc:
    state = db.lookup(exc.operation_id)
    if state["state"] == "absent":
        # Only resend the exact same operation ID and statements.
        receipt = db.write(exc.operation_id, statements)
    else:
        if state["fingerprint"] != db.fingerprint(statements):
            raise RuntimeError("operation ID belongs to different SQL statements")
        receipt = state
rows = db.read("SELECT v FROM kv WHERE k=?", [1], min_version=receipt["version"])
```

`lookup()` returning `absent` is an observation at that moment, not proof that an in-flight write cannot publish later. Retrying the identical operation ID and payload is safe; generating a fresh ID is not. Compare a published receipt's fingerprint with `db.fingerprint(statements)` before treating it as this write's ACK. The client raises `CloudWriteOutcomeUnknown` for transport failures, HTTP 202/3xx/408/429/5xx, and malformed or mismatched write replies. It never automatically retries writes or follows redirects with the bearer token. It requires HTTPS except when local HTTP is explicitly enabled for loopback experiments.

`GET /v1/databases?limit=100&cursor=...` returns at most 100 active databases and a `next_cursor`. Follow it until it is `null`, including across empty filtered pages and a final empty S3 page. Root cursors point into catalog key order; tenant cursors use that tenant's reserved database IDs. Concurrent creates or deletions can change later pages, so this is an inventory, not an atomic snapshot. The default limit is 100.

The usage endpoint returns one bounded S3 list page and a `next_cursor`. Follow cursors until an empty page returns `next_cursor: null`; a short nonempty S3 page does not prove completion. Its page byte count includes history and orphaned objects under that database group. It excludes DynamoDB, catalog, logs, backups and request charges. Pages are not an atomic snapshot if writers or GC run concurrently. `logical=true` reports SQLite `page_count × page_size` at a verified frontier and can be expensive for a large cold database.

The default limits per database are `max_frontier_bytes: 67108864`, `max_operation_bytes: 65536`, and `max_tokens: 64`. Pass all three fields under `limits` at creation or in a `PUT /limits` body. Allowed ranges are 65,536–1,073,741,824 frontier bytes, 1,024–1,048,576 operation bytes and 1–64 tokens. The HTTP body cap is 1 MiB and each transaction has at most 64 statements. Raise `max_operation_bytes` explicitly for a larger atomic scan; it still produces one durable receipt and publication. Managed write results are limited to 1 MiB and SQL execution to 15 seconds. The frontier is the published checkpoint plus native log, not total S3 storage; retained history, orphan objects and other AWS charges are outside this quota. New writes that grow the frontier past the cap return HTTP 413 before durable publication. Published operation replays remain valid when limits are reduced. Updates may return 409 while a writer holds the lease. There is no total billed storage quota. Database deletion waits at least 60 seconds, then EventBridge advances a bounded purge; there is no production deletion latency SLO. The service does not speak Turso Hrana/libSQL wire protocol.

`GET /history?limit=100&cursor=...` returns retained historical version numbers, `current_version`, and an optional `next_cursor`. Follow the cursor through the final empty page; retention can remove a listed version before it is copied. `POST /restore` accepts `{"source_version":1,"name":"recovered","request_id":"stable-id"}` and optional full `limits`; it returns a **new database ID** with the source's SQL data at that version. The source stays unchanged. The copy has no tokens or old operation receipts, so issue a new token. Its limits inherit the source's current limits unless explicitly supplied. Retry an uncertain restore with the same request ID and body. A physically deleted database requires an independent backup; this endpoint cannot recover it. Large copies may exceed the synchronous Lambda timeout.
