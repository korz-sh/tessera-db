"""Consolidated synchronous facade for the candidate v2 Turso runtime."""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeout
from collections import OrderedDict, deque
from dataclasses import dataclass
import math
import resource
import threading
import time

from .api import (
    Operation,
    OperationOutcomeUnknown,
    OperationState,
    OperationStatus,
    OperationTimeout,
    Receipt,
)
from .owner import BatchOwner
from .queue import QueuedOwner
from .reads import PublishedReader


@dataclass(frozen=True)
# @spec turso/runtime-limits
class RuntimeLimits:
    max_batch: int = 32
    max_pending: int = 128
    max_pending_bytes: int = 1_048_576
    max_result_bytes: int = 1_048_576
    max_sql_seconds: float | None = None
    batch_window_ms: float = 5

    def __post_init__(self):
        if type(self.max_batch) is not int or not 1 <= self.max_batch <= 128:
            raise ValueError("max_batch must be 1..128")
        if type(self.max_pending) is not int or self.max_pending < self.max_batch:
            raise ValueError("max_pending must be at least max_batch")
        if type(self.max_pending_bytes) is not int or self.max_pending_bytes < 1:
            raise ValueError("max_pending_bytes must be positive")
        if type(self.max_result_bytes) is not int or self.max_result_bytes < 1:
            raise ValueError("max_result_bytes must be positive")
        if (self.max_sql_seconds is not None
                and (not isinstance(self.max_sql_seconds, (int, float))
                     or isinstance(self.max_sql_seconds, bool)
                     or self.max_sql_seconds <= 0)):
            raise ValueError("max_sql_seconds must be positive or None")
        if (not isinstance(self.batch_window_ms, (int, float))
                or isinstance(self.batch_window_ms, bool)
                or not 0 <= self.batch_window_ms <= 1000):
            raise ValueError("batch_window_ms must be 0..1000")


# @spec turso/runtime
class Runtime:
    """One group owner with bounded admission, batching and durable completion.

    Groups intentionally use independent scheduler threads. A blocked group can
    consume its own configured capacity but cannot occupy another group's queue.
    """

    def __init__(self, backend, group, *, schema=(), limits=None,
                 read_cache_root=None, read_poll_ms=10, **owner_options):
        self.limits = RuntimeLimits() if limits is None else limits
        if not isinstance(self.limits, RuntimeLimits):
            raise TypeError("limits must be RuntimeLimits")
        if type(read_poll_ms) is not int or not 1 <= read_poll_ms <= 1000:
            raise ValueError("read poll interval must be 1..1000 ms")
        self._owner = BatchOwner(
            backend,
            group,
            schema=schema,
            max_result_bytes=self.limits.max_result_bytes,
            max_sql_seconds=self.limits.max_sql_seconds,
            stage_callback=self._stage,
            **owner_options,
        )
        self._queue = QueuedOwner(
            self._owner,
            max_batch=self.limits.max_batch,
            max_pending=self.limits.max_pending,
            max_bytes=self.limits.max_pending_bytes,
            window_ms=self.limits.batch_window_ms,
        )
        self._reader = PublishedReader(
            self._owner.storage, cache_root=read_cache_root, poll_ms=read_poll_ms
        )
        self._lock = threading.Lock()
        self._status = OrderedDict()
        self._stage_times = {}
        self._latencies = deque(maxlen=2048)
        self._stage_latencies = {
            name: deque(maxlen=2048)
            for name in ("queue", "execution", "publication", "total")
        }
        self._outcomes = {name: 0 for name in ("published", "failed", "unknown")}
        self._events = {name: 0 for name in ("conflicts", "retries", "reconciliations")}
        self._completed = 0
        self._closed = False
        self._started = time.monotonic()

    def _stage(self, operation_id, stage):
        state = OperationState(stage)
        now = time.monotonic()
        with self._lock:
            current = self._status.get(operation_id)
            if current is not None and not current.outcome_known:
                self._status[operation_id] = OperationStatus(
                    operation_id, state, fingerprint=current.fingerprint
                )
                self._stage_times.setdefault(operation_id, {})[state.value] = now

    def submit(self, operation: Operation, *, timeout: float | None = None) -> Receipt:
        if not isinstance(operation, Operation):
            raise TypeError("Operation required")
        if (timeout is not None
                and (not isinstance(timeout, (int, float))
                     or isinstance(timeout, bool) or timeout < 0)):
            raise ValueError("timeout must be nonnegative or None")
        started = time.monotonic()
        with self._lock:
            if self._closed:
                raise RuntimeError("runtime is closed")
            self._status[operation.operation_id] = OperationStatus(
                operation.operation_id,
                OperationState.ADMITTED,
                fingerprint=operation.fingerprint,
            )
            self._status.move_to_end(operation.operation_id)
            while len(self._status) > max(4096, self.limits.max_pending * 2):
                self._status.popitem(last=False)
            self._stage_times[operation.operation_id] = {"admitted": started}
        try:
            future = self._queue.submit(operation)
        except Exception:
            with self._lock:
                self._status.pop(operation.operation_id, None)
                self._stage_times.pop(operation.operation_id, None)
            raise

        def completed(done):
            try:
                receipt = done.result()
            except OperationOutcomeUnknown:
                status = OperationStatus(
                    operation.operation_id,
                    OperationState.UNKNOWN,
                    fingerprint=operation.fingerprint,
                )
            except Exception:
                status = OperationStatus(
                    operation.operation_id,
                    OperationState.FAILED,
                    fingerprint=operation.fingerprint,
                )
            else:
                status = OperationStatus(
                    operation.operation_id,
                    OperationState.PUBLISHED,
                    fingerprint=operation.fingerprint,
                    receipt=receipt,
                )
            with self._lock:
                self._status[operation.operation_id] = status
                self._status.move_to_end(operation.operation_id)
                while len(self._status) > max(4096, self.limits.max_pending * 2):
                    self._status.popitem(last=False)
                ended = time.monotonic()
                elapsed = (ended - started) * 1000
                self._latencies.append(elapsed)
                self._stage_latencies["total"].append(elapsed)
                stages = self._stage_times.pop(operation.operation_id, {})
                executing = stages.get("executing")
                committed = stages.get("committed_local")
                publishing = stages.get("publishing")
                if executing is not None:
                    self._stage_latencies["queue"].append((executing - started) * 1000)
                if executing is not None and committed is not None:
                    self._stage_latencies["execution"].append((committed - executing) * 1000)
                if publishing is not None:
                    self._stage_latencies["publication"].append((ended - publishing) * 1000)
                self._outcomes[status.state.value] += 1
                self._completed += 1

        future.add_done_callback(completed)
        try:
            return future.result(timeout=timeout)
        except FutureTimeout as exc:
            if future.done():
                raise
            raise OperationTimeout(
                "operation remains admitted; resolve with lookup or the same operation ID"
            ) from exc

    def read(self, sql: str, params: tuple | list = (), *, min_version=None,
             receipt=None, timeout=None) -> list:
        with self._lock:
            if self._closed:
                raise RuntimeError("runtime is closed")
        return self._reader.read(
            sql, params, min_version=min_version, receipt=receipt, timeout=timeout
        )

    def lookup(self, operation_id: str) -> OperationStatus:
        with self._lock:
            closed = self._closed
            self._events["reconciliations"] += 1
        durable = (self._owner.lookup(operation_id) if closed
                   else self._reader.lookup(operation_id))
        if durable is not None:
            return OperationStatus(
                operation_id,
                OperationState.PUBLISHED,
                fingerprint=durable.fingerprint,
                receipt=durable,
            )
        with self._lock:
            return self._status.get(
                operation_id,
                OperationStatus(operation_id, OperationState.ABSENT),
            )

    @staticmethod
    def _percentile(values, percentile):
        if not values:
            return 0.0
        ordered = sorted(values)
        return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]

    def metrics(self):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        with self._lock:
            latencies = list(self._latencies)
            completed = self._completed
            stage_latencies = {name: list(values) for name, values in self._stage_latencies.items()}
            outcomes = dict(self._outcomes)
            events = dict(self._events)
        stats = self._queue.stats()
        return {
            **stats,
            "completed": completed,
            "throughput_per_second": completed / max(time.monotonic() - self._started, 1e-9),
            "latency_ms": {
                "p95": self._percentile(latencies, 0.95),
                "p99": self._percentile(latencies, 0.99),
            },
            "stage_latency_ms": {
                name: {
                    "p95": self._percentile(values, 0.95),
                    "p99": self._percentile(values, 0.99),
                }
                for name, values in stage_latencies.items()
            },
            "outcomes": outcomes,
            "events": events,
            "cpu_seconds": usage.ru_utime + usage.ru_stime,
            "rss_bytes": usage.ru_maxrss * 1024,
        }

    def read_metrics(self):
        return self._reader.metrics()

    def operator(self):
        from .operations import Operator
        return Operator(self)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._queue.close()
        finally:
            self._reader.close()

    def abort(self) -> None:
        """Reject queued work and quiesce the already executing batch.

        Python cannot safely terminate an in-flight database call. Therefore
        abrupt shutdown is exact for queued admission and bounded by the
        currently executing batch for process-local cleanup.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._queue.abort()
        finally:
            self._reader.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
