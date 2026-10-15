# Contributing

Thanks for the interest. This file covers how to set up, what we expect
from a change, and where the project's guardrails live.

## Setup

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest tests/ -q
```

Tests run against `LocalBackend` (filesystem) and moto for S3/DynamoDB —
no AWS account needed. The suite has real sleeps; a full run takes a
while.

## What a good change looks like

- **Small and focused.** One concern per PR. If it needs a paragraph to
  describe, it probably wants splitting or an openspec change (below).
- **Tests first where it matters.** Bug fixes ship a failing regression
  test before the fix. Format or protocol changes update the pinned
  compatibility tests (`tests/test_api_compat.py`,
  `tests/test_format_compat.py`, `tests/fixtures/`).
- **Measured claims get artifacts.** Any number in docs is labeled
  `measured` / `derived` / `hypothetical` (see `docs/measurements.md`).
  "Validated on AWS" means it ran on real AWS — say the region. moto and
  ministack are emulators; label them as such.
- **No Devin/AI coauthor trailers** — commit as yourself.

## Guardrails you will hit

- **Durability invariants are load-bearing.** The manifest CAS is the
  commit point; lease epochs fence owners; `data_version` is the commit
  frontier and `version` the object generation. Relaxing any guarantee
  (fencing, write-once, durable ack) is a MAJOR version change even if
  the code still compiles — see `CHANGELOG.md`.
- **Release gates.** `release/gates.json` pins evidence by SHA-256. A
  release tag requires the gate green: wheel/source identity plus
  digested evidence files.
- **Spec-driven changes.** Substantial behavior changes go through
  `openspec/changes/` (proposal → spec → design → tasks → apply → verify
  → archive). Small fixes don't need this; contract changes do.

## Reporting bugs and security issues

- Bugs: open a GitHub issue with a repro (test or script) and the version.
- Security: see [SECURITY.md](SECURITY.md) — please don't file public
  issues for vulnerabilities.

## License

Apache-2.0. Contributions are submitted under the same license; keep the
SPDX header pattern used by existing files.
