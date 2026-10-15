# Copyright 2026 korz
# SPDX-License-Identifier: Apache-2.0
"""Shared owner constants."""

# Local database file name inside the tenant cache dir.
_DB_NAME = "db.sqlite"

# Compaction trigger: mirror the Store's inline thresholds (and the scheduled
# compactor in examples/handler.py) so the owner folds exactly the chains the
# inline trigger would have folded had the writes not been batched.
_COMPACT_DELTAS = 200
_COMPACT_BYTES = 16 * 1024 * 1024

# Lease acquisition backoff: full jitter, 50 ms base doubling per attempt,
# 5 attempts total before giving up and raising TenantBusy.
_ACQUIRE_ATTEMPTS = 5
_ACQUIRE_BASE_S = 0.05

# Main-loop poll granularity (seconds): publish/compact/idle timers are
# checked on ~100 ms ticks.
_TICK_S = 0.1
