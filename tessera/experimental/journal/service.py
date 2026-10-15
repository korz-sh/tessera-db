"""Bounded in-process front end for the single-host journal spike."""

from concurrent.futures import Future
import queue
import threading
import time

from .local import Mutation


# @spec journal/service
class Service:
    def __init__(self, journal, *, max_batch=32, window_ms=5, max_pending=128):
        if (type(max_batch) is not int or not 1 <= max_batch <= 64
                or type(max_pending) is not int or max_pending < max_batch
                or not 0 <= window_ms <= 1000):
            raise ValueError("invalid service bounds")
        self.journal, self.max_batch, self.window = journal, max_batch, window_ms / 1000
        self._slots = threading.BoundedSemaphore(max_pending)
        self._queue = queue.Queue()
        self._lock, self._closing = threading.Lock(), False
        self.batches = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, mutation):
        if not isinstance(mutation, Mutation):
            raise TypeError("Mutation required")
        with self._lock:
            if self._closing:
                raise RuntimeError("service closing")
            if not self._slots.acquire(blocking=False):
                raise queue.Full("journal admission full")
            future = Future()
            future.set_running_or_notify_cancel()
            self._queue.put((mutation, future))
            return future

    def _run(self):
        stopping = False
        while not stopping:
            first = self._queue.get()
            if first is None:
                return
            batch, deadline = [first], time.monotonic() + self.window
            while len(batch) < self.max_batch:
                try:
                    item = self._queue.get(timeout=max(0, deadline - time.monotonic()))
                except queue.Empty:
                    break
                if item is None:
                    stopping = True
                    break
                batch.append(item)
            try:
                results = self.journal.append([r for r, _ in batch])
                self.batches.append(len(batch))
            except Exception as exc:
                results = [exc] * len(batch)
            for (_, future), result in zip(batch, results):
                self._slots.release()
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)

    def close(self):
        if threading.current_thread() is self._thread:
            raise RuntimeError("close cannot run inside a completion callback")
        with self._lock:
            if not self._closing:
                self._closing = True
                self._queue.put(None)
        self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
