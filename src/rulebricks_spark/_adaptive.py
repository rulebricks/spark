"""Request scheduling for independent rows with a fixed row allowance."""

import logging
import math
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, CancelledError, ThreadPoolExecutor, wait
from contextlib import ExitStack

from ._batch_policy import BatchPolicy, Slot

# Byte-size seed; measured timings choose the working plan.
_INITIAL_REQUEST_BYTES = 256 * 1024


def initial_requests(row_budget, average_bytes):
    if average_bytes is None:
        return 4
    # Preserve overlap for tiny payloads; the policy also enforces byte limits.
    return min(4, max(2, math.ceil(row_budget * (average_bytes + 1) / _INITIAL_REQUEST_BYTES)))


class RecordStream:
    """Prepare only a bounded slice of input on the Spark task thread."""

    def __init__(self, batches, config, names, row_budget):
        from . import _engine as engine

        self.config = config
        self.buffer = deque()
        tune_sizes = config.tunes_batches
        self.max_rows = row_budget if tune_sizes else (config.max_batch_rows or row_budget)
        self.row_budget = row_budget
        self.average_payload_bytes = None
        self.observed_rows = 0
        self._new_rows = self._new_bytes = 0

        def records():
            for batch in batches:
                for offset in range(0, batch.num_rows, min(1000, row_budget)):
                    for values in batch.slice(offset, min(1000, row_budget)).to_pylist(
                        maps_as_pydicts="strict"
                    ):
                        source = dict(zip(names, values.values()))
                        try:
                            record = engine._make_record(source, config)
                        except Exception as exc:
                            yield engine._local_payload_failure(source, exc, config)
                        else:
                            if tune_sizes:
                                self._new_rows += 1
                                self._new_bytes += record.payload_bytes
                            yield record

        self.records = iter(records())

    def prepend(self, items):
        self.buffer.extendleft(reversed(items))

    def take(self, capacity):
        result = self._take(capacity)
        if self._new_rows:
            average = self._new_bytes / self._new_rows
            if self.average_payload_bytes is None:
                self.average_payload_bytes = average
            else:
                # Weight by newly encoded rows, rather than request count.
                # One allowance of new input replaces ~63% of the old mean.
                weight = -math.expm1(-self._new_rows / self.row_budget)
                self.average_payload_bytes += weight * (average - self.average_payload_bytes)
            self.observed_rows += self._new_rows
            self._new_rows = self._new_bytes = 0
        return result

    def _take(self, capacity):
        from . import _engine as engine

        chunk = []
        size = 2
        for _ in range(min(capacity, self.max_rows)):
            try:
                item = self.buffer.popleft() if self.buffer else next(self.records)
            except StopIteration:
                return (chunk or None, False, True)
            if isinstance(item, engine.RowOutcome):
                if chunk:
                    self.prepend([item])
                    return chunk, False, False
                return item, False, False
            extra = item.payload_bytes + (1 if chunk else 0)
            if chunk and size + extra > self.config.max_batch_bytes:
                self.prepend([item])
                return chunk, True, False
            chunk.append(item)
            size += extra
            if size > self.config.max_batch_bytes:
                return chunk, True, False
        return chunk, False, False


def row_budget_outcomes(stream, config, slots, *, row_budget):
    """Keep old allowances until requests finish, including retries and split trees."""
    from . import _engine as engine

    policy = None
    if config.tunes_batches:
        first, _, _ = stream.take(row_budget)
        if first is None:
            return
        fitted = (
            min(row_budget, stream.max_rows) if isinstance(first, engine.RowOutcome) else len(first)
        )
        stream.prepend([first] if isinstance(first, engine.RowOutcome) else first)
        width = initial_requests(row_budget, stream.average_payload_bytes)
        policy = BatchPolicy(
            row_budget,
            fitted,
            max_requests=slots,
            initial_requests=width,
            startup_trial=width < 4,
        )
    payload_profile = stream.average_payload_bytes
    profile_rows = stream.observed_rows
    local = threading.local()
    client_lock = threading.Lock()
    cancelled = threading.Event()
    pending = {}
    reserved = 0
    exhausted = False
    peak_rows = peak_requests = completed = 0
    with ExitStack() as clients:

        def execute(item):
            if cancelled.is_set():
                raise CancelledError()
            if isinstance(item, engine.RowOutcome):
                if config.on_error == "raise":
                    raise engine.ExecutionError(item.metadata["_rb_error"])
                return [item], False
            if not hasattr(local, "client"):
                with client_lock:
                    local.client = clients.enter_context(engine.sdk_client(config))
            before = [r.attempts for r in item]
            result = engine._execute_chunk(item, local.client, config, _cancelled=cancelled)
            return result, any(r.attempts != n + 1 for r, n in zip(item, before))

        pool = ThreadPoolExecutor(max_workers=slots, thread_name_prefix="rulebricks-rows")

        def refill():
            nonlocal reserved, exhausted, peak_rows, peak_requests, payload_profile, profile_rows
            while not exhausted and len(pending) < slots:
                average = stream.average_payload_bytes
                if policy is not None and average is not None:
                    if payload_profile is None:
                        payload_profile, profile_rows = average, stream.observed_rows
                    elif stream.observed_rows - profile_rows >= row_budget and (
                        average >= 2 * payload_profile or 2 * average <= payload_profile
                    ):
                        fitted_rows = None
                        if 2 * average <= payload_profile:
                            fitted_rows = max(
                                1,
                                min(
                                    stream.max_rows,
                                    int((config.max_batch_bytes - 2) / (average + 1)),
                                ),
                            )
                        policy.reconsider(fitted_rows=fitted_rows)
                        payload_profile, profile_rows = average, stream.observed_rows
                available = row_budget - reserved
                if available <= 0:
                    return
                slot = policy.claim(available) if policy is not None else Slot(0, available)
                if slot is None:
                    return
                admitted = time.monotonic()
                item, byte_limited, tail = stream.take(slot.capacity)
                if item is None:
                    exhausted = True
                    if policy is not None:
                        policy.freeze()
                    return
                n = 1 if isinstance(item, engine.RowOutcome) else len(item)
                if byte_limited and policy is not None and policy.constrain_batch_rows(n):
                    payload_profile, profile_rows = (
                        stream.average_payload_bytes,
                        stream.observed_rows,
                    )
                    stream.prepend(item)
                    continue
                # Adaptive plans reserve whole balanced slots. Manual packing
                # charges actual rows so byte-limited batches leave room for more.
                if policy is None:
                    slot = Slot(0, n)
                reserved += slot.capacity
                assert reserved <= row_budget
                pending[pool.submit(execute, item)] = {
                    "slot": slot,
                    "admitted": admitted,
                    "partial_tail": tail and n < slot.capacity,
                }
                peak_rows = max(peak_rows, reserved)
                peak_requests = max(peak_requests, len(pending))

        try:
            refill()
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                # Observe every failure before admitting any replacement work.
                for future in done:
                    future.result()
                while done:
                    future = done.pop()
                    entry = pending.pop(future)
                    result, retried = future.result()
                    slot = entry["slot"]
                    reserved -= slot.capacity
                    completed += len(result)
                    if policy is not None:
                        stable = all(
                            x["slot"].generation == policy.generation for x in pending.values()
                        )
                        policy.finish(
                            slot,
                            rows=len(result),
                            seconds=time.monotonic() - entry["admitted"],
                            correct=all(r.metadata["_rb_status"] == "ok" for r in result),
                            retried=retried,
                            stable=stable,
                            partial_tail=entry["partial_tail"],
                        )
                    # Refill before Arrow conversion or downstream consumption.
                    refill()
                    yield result
        finally:
            cancelled.set()
            for future in pending:
                future.cancel()
            # In-progress HTTP calls cannot be revoked; join before closing clients.
            pool.shutdown(wait=True, cancel_futures=True)
            logger = logging.getLogger(__name__)
            logger.info(
                "Rulebricks batch plan: automatic=%s row_allowance=%s completed_rows=%s peak_reserved_rows=%s "
                "peak_requests=%s selected_requests=%s comparisons=%s",
                config.tunes_batches,
                row_budget,
                completed,
                peak_rows,
                peak_requests,
                policy.best_width if policy is not None else None,
                policy.probes if policy is not None else 0,
            )
            if policy is not None and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Rulebricks batch timings: payload_mean_bytes=%s observed_payload_rows=%s measurements=%s",
                    stream.average_payload_bytes,
                    stream.observed_rows,
                    list(policy.events),
                )
