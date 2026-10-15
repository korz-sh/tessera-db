---
title: Turso operator diagnostics
parent: Reference
nav_order: 12
---

# Turso operator diagnostics

The experimental runtime exposes an operator facade without exposing SQL,
parameters, result rows, lease tokens, session credentials, or backend
credentials:

```python
ops = runtime.operator()
ops.doctor()
ops.inspect_group()
ops.lookup_operation("orders", "operation-123")
ops.history()
ops.export(42, "orders-v42.sqlite")
```

`doctor()` separates admission and collection pressure, execution latency,
publication latency, owner renewal health, cache rebuilds, and retained recovery
state. Metric dimensions are fixed; operation IDs only appear in an explicit
point lookup. Runtime latency samples are capped at 2,048 and process-local
status entries at 4,096 (or twice the configured admission bound).

Recovery is deliberately two-step. `ops.restore(version)` returns a
`RestorePlan` containing the exact group, source artifacts, observed current
version, resulting version, and confirmation digest. Execution requires that
same still-current plan:

```python
plan = ops.restore(42)  # dry run; no mutation
result = ops.restore(42, dry_run=False, plan=plan)
```

If the frontier changes between those calls, execution fails and a new plan is
required. Export is create-only and restore creates a new published version, so
the replaced frontier remains recoverable under the configured retention
policy.

## Timeout incident runbook

1. Call `lookup_operation(group, operation_id)` with only the exact group and
   operation ID. `publishing` means local execution committed but the durable
   frontier is not yet acknowledged; `published` includes the receipt version.
2. Inspect `doctor()["runtime"]`. Queue depth/collection time isolate admission
   delay; stage latency separates execution from publication delay.
3. Check owner renewal health and remaining lease time, then cache corruption
   counters and the recovery/GC report.
4. Retry reconciliation with the same operation ID. Never create a replacement
   ID while the outcome is `admitted`, `executing`, `committed_local`,
   `publishing`, or `unknown`.
5. Export before recovery when practical. Always review a fresh restore
   preflight; never reuse a stale plan.
