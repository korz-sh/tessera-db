"""Bounded automatic admission and durable batch completion."""

from collections import deque
from concurrent.futures import Future
import threading
import time

from .api import AdmissionLimitExceeded, OperationFailed
from .owner import Request


# @spec turso/queue-full
class QueueFull(AdmissionLimitExceeded):
    pass


# @spec turso/queued-owner
class QueuedOwner:
    def __init__(self, owner, *, max_batch=32, max_pending=128, max_bytes=1048576, window_ms=5):
        if (type(max_batch) is not int or not 1 <= max_batch <= 128
                or type(max_pending) is not int or max_pending < max_batch
                or type(max_bytes) is not int or max_bytes < 1
                or not 0 <= window_ms <= 1000):
            raise ValueError("invalid queue bounds")
        self.owner, self.max_batch = owner, max_batch
        self.max_pending, self.max_bytes = max_pending, max_bytes
        self.window = window_ms / 1000
        self._condition = threading.Condition()
        self._queue = deque()
        self._count = self._bytes = 0
        self._peak_count = self._peak_queue_depth = 0
        self._batches = self._batch_items = 0
        self._collection_ms = 0.0
        self._closing = False
        self._thread = threading.Thread(target=self._run, name="turso-batches", daemon=True)
        self._thread.start()

    def submit(self, request):
        if not isinstance(request, Request):
            raise TypeError("Request required")
        size = len(request.payload) + len(request.operation_id.encode())
        with self._condition:
            if self._closing:
                raise RuntimeError("queue is closing")
            if self._count >= self.max_pending or self._bytes + size > self.max_bytes:
                raise QueueFull("admitted request limit exceeded")
            future = Future()
            # Accepted requests cannot be cancelled while their durable outcome is pending.
            future.set_running_or_notify_cancel()
            self._queue.append((request, future, size))
            self._count += 1
            self._bytes += size
            self._peak_count = max(self._peak_count, self._count)
            self._peak_queue_depth = max(self._peak_queue_depth, len(self._queue))
            self._condition.notify()
            return future

    def _collect(self):
        with self._condition:
            while not self._queue and not self._closing:
                self._condition.wait()
            if not self._queue:
                return []
            started = time.monotonic()
            deadline = started + self.window
            batch = [self._queue.popleft()]
            while len(batch) < self.max_batch:
                if self._queue:
                    batch.append(self._queue.popleft())
                elif self._closing or time.monotonic() >= deadline:
                    break
                else:
                    self._condition.wait(deadline - time.monotonic())
            self._batches += 1
            self._batch_items += len(batch)
            self._collection_ms += (time.monotonic() - started) * 1000
            return batch

    def _run(self):
        while batch := self._collect():
            try:
                outcomes = self.owner.commit([r for r, _, _ in batch])
            except Exception as exc:
                outcomes = [exc] * len(batch)
            with self._condition:
                self._count -= len(batch)
                self._bytes -= sum(size for _, _, size in batch)
            for (_, future, _), outcome in zip(batch, outcomes):
                if isinstance(outcome, Exception):
                    future.set_exception(outcome)
                else:
                    future.set_result(outcome)

    def close(self):
        if threading.current_thread() is self._thread:
            raise RuntimeError("close must not run from a completion callback")
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()
        self.owner.close()

    def abort(self):
        """Reject work that is still queued, then quiesce any executing batch."""
        if threading.current_thread() is self._thread:
            raise RuntimeError("abort must not run from a completion callback")
        with self._condition:
            self._closing = True
            rejected = list(self._queue)
            self._queue.clear()
            self._count -= len(rejected)
            self._bytes -= sum(size for _, _, size in rejected)
            self._condition.notify_all()
        for _, future, _ in rejected:
            future.set_exception(OperationFailed("runtime aborted before execution"))
        self._thread.join()
        self.owner.close()

    def stats(self):
        with self._condition:
            return {
                "admitted": self._count,
                "admitted_bytes": self._bytes,
                "queue_depth": len(self._queue),
                "peak_admitted": self._peak_count,
                "peak_queue_depth": self._peak_queue_depth,
                "batches": self._batches,
                "mean_batch_width": (
                    self._batch_items / self._batches if self._batches else 0.0
                ),
                "mean_collection_ms": (
                    self._collection_ms / self._batches if self._batches else 0.0
                ),
                "closing": self._closing,
            }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
