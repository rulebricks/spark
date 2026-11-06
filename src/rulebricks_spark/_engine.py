"""Memory-bounded partition engine shared by flows and rules."""

from __future__ import annotations

import datetime
import json
import random
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Dict, List, Optional

from ._config import RunConfig
from ._metadata import RB_PREFIX, seed_attempt_state
from ._payload import (
    build_payload,
    encode_payload,
    record_id_value,
)
from ._transport import (
    BulkResponse,
    FailureInfo,
    classify_exception,
    derive_item_execution_id,
    run_attempt,
    sdk_client,
)
from .exceptions import ExecutionError
from .responses import _evaluate

MAX_SPLIT_DEPTH = 10
MAX_ISOLATION_REQUESTS = 256


@dataclass
class RecordState:
    source: Dict[str, Any]
    payload: Dict[str, Any]
    payload_bytes: int
    record_id: Optional[str]
    history: List[Dict[str, Any]] = field(default_factory=list)
    attempts: int = 0


@dataclass
class RowOutcome:
    record: RecordState
    result: Any
    metadata: Dict[str, Any]
    raw_json: Optional[str] = None


@dataclass
class RequestBudget:
    """Shared HTTP request budget for one original chunk's split tree."""

    maximum: int
    used: int = 0

    def consume(self) -> bool:
        if self.used >= self.maximum:
            return False
        self.used += 1
        return True


class ByteChunker:
    """Accumulate records under an encoded JSON-array byte budget."""

    def __init__(self, max_bytes: int, max_records: Optional[int] = None):
        self.max_bytes = max_bytes
        self.max_records = max_records
        self._records: List[RecordState] = []
        self._bytes = 2  # opening and closing brackets

    def add(self, record: RecordState) -> List[List[RecordState]]:
        """Add a record and return any chunks completed by this addition."""

        emitted: List[List[RecordState]] = []
        item_bytes = record.payload_bytes
        extra_bytes = item_bytes + (1 if self._records else 0)

        record_cap_hit = self.max_records is not None and len(self._records) >= self.max_records
        byte_cap_hit = bool(self._records) and self._bytes + extra_bytes > self.max_bytes
        if record_cap_hit or byte_cap_hit:
            emitted.append(self._records)
            self._records = []
            self._bytes = 2
            extra_bytes = item_bytes

        if self._bytes + extra_bytes > self.max_bytes:
            emitted.append([record])
            return emitted

        self._records.append(record)
        self._bytes += extra_bytes
        return emitted

    def finish(self) -> List[List[RecordState]]:
        if not self._records:
            return []
        records = self._records
        self._records = []
        self._bytes = 2
        return [records]


def make_partition_worker(
    config: RunConfig,
    input_names: Sequence[str],
    output_schema,
    *,
    partition_id_column: Optional[str] = None,
):
    """Use Arrow directly so nullable int64 values never pass through floats."""

    def worker(batch_iter) -> Iterator[Any]:
        from pyspark import TaskContext
        from pyspark.sql.pandas.types import to_arrow_schema

        task = TaskContext.get()
        if config.state_key is not None and task is not None and task.attemptNumber() > 0:
            raise ExecutionError(
                "Spark retried a stateful task. Stop this attempt; resume its checkpoint path if configured, otherwise reconcile state before replaying."
            )
        arrow_schema = to_arrow_schema(output_schema)
        source_columns = [name for name in input_names if not name.startswith(RB_PREFIX)]
        blocked = False
        blocked_key = None

        if partition_id_column is not None:
            # Empty partitions require neither a request budget nor an HTTP client.
            first = next((batch for batch in batch_iter if batch.num_rows), None)
            if first is None:
                return
            partition_id = first.column(partition_id_column)[0].as_py()
            batch_iter = chain([first], batch_iter)
            del first
        else:
            partition_id = task.partitionId() if task is not None else 0
        slots = config.request_slots(partition_id)
        row_budget = config.row_budget(partition_id)
        if row_budget is not None:
            if config.state_key is not None or config.context_operation is not None:
                raise ExecutionError("Concurrent requests require independent rule or flow rows.")
            from ._adaptive import RecordStream, row_budget_outcomes

            stream = RecordStream(batch_iter, config, input_names, row_budget)
            outcomes_iter = row_budget_outcomes(stream, config, slots, row_budget=row_budget)
            try:
                for outcomes in outcomes_iter:
                    yield _outcomes_to_arrow(outcomes, config, source_columns, arrow_schema)
            finally:
                outcomes_iter.close()
            return

        def emit(chunk):
            nonlocal blocked, blocked_key
            if blocked or (
                config.state_key is not None and chunk[0].source[config.state_key] == blocked_key
            ):
                outcomes = []
                for record in chunk:
                    outcome = _local_payload_failure(
                        record.source,
                        ValueError(
                            "An earlier request has an uncertain outcome; reconcile the partition before resubmitting."
                            if blocked
                            else "An earlier event for this key failed; reconcile its state before submitting later events."
                        ),
                        config,
                    )
                    outcome.metadata["_rb_error_type"] = "context_blocked"
                    outcome.metadata["_rb_record_id"] = record.record_id
                    outcomes.append(outcome)
            else:
                outcomes = _execute_chunk(chunk, client, config)
                blocked = config.context_operation == "ingest" and any(
                    outcome.metadata["_rb_error_type"] in ("context_write", "protocol")
                    for outcome in outcomes
                )
                if config.state_key is not None and any(
                    outcome.metadata["_rb_status"] != "ok" for outcome in outcomes
                ):
                    blocked_key = chunk[0].source[config.state_key]
                    blocked = any(
                        outcome.metadata["_rb_error_type"]
                        not in ("execution", "schema", "response_handler")
                        for outcome in outcomes
                    )
                    # A bulk response may already contain later committed events.
                    # Stop this partition; a checkpointed caller discards the window.
                    if len(chunk) > 1:
                        blocked = True
            return _outcomes_to_arrow(outcomes, config, source_columns, arrow_schema)

        with sdk_client(config) as client:
            for item in _prepare_chunks(batch_iter, config, input_names):
                if isinstance(item, RowOutcome):
                    if config.state_key is not None:
                        blocked_key = item.record.source.get(config.state_key)
                    if config.on_error == "raise":
                        raise ExecutionError(item.metadata["_rb_error"])
                    yield _outcomes_to_arrow([item], config, source_columns, arrow_schema)
                else:
                    yield emit(item)

    return worker


def _prepare_chunks(batch_iter, config: RunConfig, input_names: Sequence[str]):
    """Read Arrow only on the task thread, with bounded decoded input."""
    chunker = ByteChunker(config.max_batch_bytes, config.max_batch_rows)
    for batch in batch_iter:
        for offset in range(0, batch.num_rows, 1000):
            rows = batch.slice(offset, 1000).to_pylist(maps_as_pydicts="strict")
            for values in rows:
                source = dict(zip(input_names, values.values()))
                try:
                    record = _make_record(source, config)
                except Exception as exc:
                    yield from chunker.finish()
                    yield _local_payload_failure(source, exc, config)
                    continue
                yield from chunker.add(record)
    yield from chunker.finish()


def _make_record(source: Dict[str, Any], config: RunConfig) -> RecordState:
    payload = build_payload(source, config)
    if config.state_key is not None:
        key = source.get(config.state_key)
        if key is None or key == "":
            raise ValueError("State key must not be null or empty.")
        if isinstance(key, int) and abs(key) > 2**53 - 1:
            raise ValueError("Use string state keys for integers outside JSON's safe range.")
        if len(encode_payload(payload).encode("utf-8")) + 2 > config.max_batch_bytes:
            raise ValueError(
                "Event exceeds max_batch_bytes; increase the limit or reduce the payload."
            )
    if config.context_operation:
        path = config.correlation_id_path or ""
        key = payload[path] if path in payload else record_id_value(payload, path)
        if key is None or key == "":
            raise ValueError("Context key must not be null or empty.")
        if config.context_operation != "lookup" and isinstance(key, int) and abs(key) > 2**53 - 1:
            raise ValueError(
                "Use a string context identity for integers outside JSON's safe range."
            )
        if len(encode_payload(payload).encode("utf-8")) + 2 > config.max_batch_bytes:
            raise ValueError(
                "Context record exceeds max_batch_bytes; increase the limit or reduce the payload."
            )
    record_id = (
        record_id_value(payload, config.correlation_id_path) if config.correlation_id_path else None
    )
    if config.context_operation:
        record_id = str(key)
    history, attempts = seed_attempt_state(source)
    return RecordState(
        source=source,
        payload=payload,
        payload_bytes=len(encode_payload(payload).encode("utf-8")),
        record_id=record_id,
        history=history,
        attempts=attempts,
    )


def _execute_chunk(
    records: List[RecordState],
    client: Any,
    config: RunConfig,
    *,
    _budget: Optional[RequestBudget] = None,
    _retry_limit: Optional[int] = None,
    _depth: int = 0,
    _exhausted_failure: Optional[FailureInfo] = None,
    _cancelled: Optional[threading.Event] = None,
) -> List[RowOutcome]:
    """Execute, retry, and if needed recursively split one chunk."""

    budget = _budget or RequestBudget(MAX_ISOLATION_REQUESTS)
    retry_limit = config.max_retries if _retry_limit is None else _retry_limit

    for retry_index in range(retry_limit + 1):
        if _cancelled is not None and _cancelled.is_set():
            raise CancelledError()
        if not budget.consume():
            failure = _exhausted_failure or FailureInfo(
                error_type="unknown",
                message="Failure isolation request budget exhausted.",
                http_status=None,
                execution_id=None,
                trace_id=None,
                retry_after=None,
                retryable=True,
            )
            return _budget_exhausted_outcomes(records, config, failure)

        chunk_id = str(uuid.uuid4())
        failed_started = _utcnow()
        try:
            success = run_attempt(client, config, [record.payload for record in records])
        except Exception as exc:
            failed_completed = _utcnow()
            failure = classify_exception(exc)
            for record in records:
                record.attempts += 1

            can_split = (
                len(records) > 1
                and _depth < MAX_SPLIT_DEPTH
                and config.state_key is None
                and config.context_operation != "ingest"
            )
            size_error = failure.error_type in ("payload_too_large", "response_too_large")
            if not size_error and failure.retryable and retry_index < retry_limit:
                _append_failure_history(records, failure, chunk_id, failed_completed)
                delay = _retry_delay(failure, retry_index)
                if _cancelled is None:
                    time.sleep(delay)
                elif _cancelled.wait(delay):
                    raise CancelledError() from None
                continue

            if can_split and (
                size_error or (failure.error_type == "server" and config.isolate_failures)
            ):
                _append_failure_history(records, failure, chunk_id, failed_completed)
                midpoint = len(records) // 2
                outcomes = []
                for half in (records[:midpoint], records[midpoint:]):
                    outcomes.extend(
                        _execute_chunk(
                            half,
                            client,
                            config,
                            _budget=budget,
                            _retry_limit=max(0, retry_limit - retry_index) if size_error else 0,
                            _depth=_depth + 1,
                            _exhausted_failure=failure,
                            _cancelled=_cancelled,
                        )
                    )
                return outcomes

            if config.on_error == "raise":
                raise ExecutionError(
                    f"{config.asset_type} {config.slug!r} failed: {failure.message}"
                ) from exc
            return [
                RowOutcome(
                    record=record,
                    result=None,
                    metadata=_terminal_failure_metadata(
                        record,
                        config,
                        failure,
                        chunk_id,
                        len(records),
                        failed_started,
                        failed_completed,
                    ),
                )
                for record in records
            ]

        for record in records:
            record.attempts += 1
        return _response_outcomes(records, success, chunk_id, config)

    raise AssertionError("retry loop must return or raise")


def _response_outcomes(
    records: Sequence[RecordState],
    success: BulkResponse,
    chunk_id: str,
    config: RunConfig,
) -> List[RowOutcome]:
    if success.protocol_error or len(success.data) != len(records):
        return _protocol_error_outcomes(records, success, chunk_id, config)

    outcomes: List[RowOutcome] = []
    for index, record in enumerate(records):
        item = success.data[index]
        common = _common_metadata(
            record=record,
            config=config,
            http_status=success.http_status,
            execution_id=success.execution_id,
            item_execution_id=(
                derive_item_execution_id(success.execution_id, index)
                if config.asset_type == "flow"
                else None
            ),
            trace_id=success.trace_id,
            chunk_id=chunk_id,
            chunk_size=len(records),
            started_at=success.started_at,
            completed_at=success.completed_at,
        )

        raw_json = _json(item) if config.responses.raw_column else None
        outcome = RowOutcome(record, None, common, raw_json=raw_json)
        if config.context_operation:
            outcome.result = _json(item)
            common["_rb_context_write_status"] = success.write_status.get(index)
            if index in success.errors:
                common.update(_row_error(*success.errors[index]))
            elif item.get("status") == "pending" or item.get("reason") == "execution_in_progress":
                common["_rb_status"] = "pending"
            if common["_rb_error"] and config.on_error == "raise":
                raise ExecutionError(common["_rb_error"])
            outcomes.append(outcome)
            continue
        value, error = _evaluate(item, config.output_schema, config.responses)
        if error:
            common.update(_row_error(*error))
        else:
            outcome.result = value if config.output_schema is not None else _json(value)
        if common["_rb_status"] != "ok" and config.on_error == "raise":
            raise ExecutionError(common["_rb_error"])
        outcomes.append(outcome)
    return outcomes


def _protocol_error_outcomes(
    records: Sequence[RecordState],
    success: BulkResponse,
    chunk_id: str,
    config: RunConfig,
) -> List[RowOutcome]:
    message = success.protocol_error or (
        f"Rulebricks returned {len(success.data)} response items for {len(records)} request items."
    )
    if config.on_error == "raise":
        raise ExecutionError(message)
    outcomes: List[RowOutcome] = []
    for record in records:
        common = _common_metadata(
            record=record,
            config=config,
            http_status=success.http_status,
            execution_id=success.execution_id,
            item_execution_id=None,
            trace_id=success.trace_id,
            chunk_id=chunk_id,
            chunk_size=len(records),
            started_at=success.started_at,
            completed_at=success.completed_at,
        )
        common.update(
            {
                "_rb_status": "protocol_error",
                "_rb_error_type": "protocol",
                "_rb_error": message,
            }
        )
        outcomes.append(RowOutcome(record, None, common))
    return outcomes


def _append_failure_history(
    records: Sequence[RecordState],
    failure: FailureInfo,
    chunk_id: str,
    completed_at: datetime.datetime,
) -> None:
    entry = {
        "at": completed_at.isoformat(),
        "http_status": failure.http_status,
        "error_type": failure.error_type,
        "error": failure.message,
        "trace_id": failure.trace_id,
        "execution_id": failure.execution_id,
        "chunk_id": chunk_id,
    }
    for record in records:
        record.history.append(dict(entry))


def _terminal_failure_metadata(
    record: RecordState,
    config: RunConfig,
    failure: FailureInfo,
    chunk_id: Optional[str],
    chunk_size: int,
    started_at: datetime.datetime,
    completed_at: datetime.datetime,
) -> Dict[str, Any]:
    metadata = _common_metadata(
        record=record,
        config=config,
        http_status=failure.http_status,
        execution_id=failure.execution_id,
        item_execution_id=None,
        trace_id=failure.trace_id,
        chunk_id=chunk_id,
        chunk_size=chunk_size,
        started_at=started_at,
        completed_at=completed_at,
    )
    metadata.update(
        {
            "_rb_status": "chunk_error",
            "_rb_error_type": failure.error_type,
            "_rb_error": failure.message,
        }
    )
    if config.context_operation == "ingest":
        metadata["_rb_context_write_status"] = "unknown"
        metadata["_rb_error_type"] = "context_write"
    return metadata


def _common_metadata(
    *,
    record: RecordState,
    config: RunConfig,
    http_status: Optional[int],
    execution_id: Optional[str],
    item_execution_id: Optional[str],
    trace_id: Optional[str],
    chunk_id: Optional[str],
    chunk_size: Optional[int],
    started_at: Optional[datetime.datetime],
    completed_at: Optional[datetime.datetime],
) -> Dict[str, Any]:
    return {
        "_rb_status": "ok",
        "_rb_error_type": None,
        "_rb_error": None,
        "_rb_http_status": http_status,
        "_rb_execution_id": execution_id,
        "_rb_item_execution_id": item_execution_id,
        "_rb_trace_id": trace_id,
        "_rb_record_id": record.record_id,
        "_rb_chunk_id": chunk_id,
        "_rb_chunk_size": chunk_size,
        "_rb_attempts": record.attempts,
        "_rb_attempt_history": _json(record.history)
        if config.diagnostics and record.history
        else None,
        "_rb_started_at": started_at,
        "_rb_completed_at": completed_at,
        "_rb_operation_id": config.operation_id,
        "_rb_slug": config.slug,
        "_rb_version": None if config.context_operation else config.version,
        "_rb_asset_type": config.asset_type,
    }


def _local_payload_failure(source: Dict[str, Any], exc: Exception, config: RunConfig) -> RowOutcome:
    history, attempts = seed_attempt_state(source)
    record = RecordState(source, {}, 2, None, history, attempts)
    if config.context_operation:
        key = next(
            (
                name
                for name, target in config.input_mapping.items()
                if target == config.correlation_id_path
            ),
            config.correlation_id_path,
        )
        value = source.get(key) if key else None
        record.record_id = str(value) if value is not None else None
    now = _utcnow()
    metadata = _common_metadata(
        record=record,
        config=config,
        http_status=None,
        execution_id=None,
        item_execution_id=None,
        trace_id=None,
        chunk_id=None,
        chunk_size=1,
        started_at=now,
        completed_at=now,
    )
    metadata.update(
        {
            "_rb_status": "chunk_error",
            "_rb_error_type": "client",
            "_rb_error": f"{type(exc).__name__}: {exc}",
        }
    )
    if config.context_operation == "ingest":
        metadata["_rb_context_write_status"] = "not_sent"
    return RowOutcome(record, None, metadata)


def _budget_exhausted_outcomes(
    records: Sequence[RecordState],
    config: RunConfig,
    originating_failure: FailureInfo,
) -> List[RowOutcome]:
    message = (
        "Failure isolation stopped after reaching max_isolation_requests="
        f"{MAX_ISOLATION_REQUESTS}. Last error: {originating_failure.message}"
    )
    failure = FailureInfo(
        error_type=originating_failure.error_type,
        message=message,
        http_status=originating_failure.http_status,
        execution_id=originating_failure.execution_id,
        trace_id=originating_failure.trace_id,
        retry_after=None,
        retryable=originating_failure.retryable,
    )
    if config.on_error == "raise":
        raise ExecutionError(message)
    now = _utcnow()
    return [
        RowOutcome(
            record=record,
            result=None,
            metadata=_terminal_failure_metadata(
                record,
                config,
                failure,
                None,
                len(records),
                now,
                now,
            ),
        )
        for record in records
    ]


def _outcomes_to_arrow(
    outcomes: Sequence[RowOutcome],
    config: RunConfig,
    source_columns: Sequence[str],
    arrow_schema,
):
    import pyarrow as pa

    rows = []
    for outcome in outcomes:
        row = {name: outcome.record.source.get(name) for name in source_columns}
        row["_rb_result"] = outcome.result
        row["_rb_raw_result"] = outcome.raw_json
        row.update(outcome.metadata)
        rows.append(row)
    return pa.RecordBatch.from_pylist(rows, schema=arrow_schema)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _row_error(error_type: str, message: str) -> Dict[str, Any]:
    return {"_rb_status": "execution_error", "_rb_error_type": error_type, "_rb_error": message}


def _retry_delay(failure: FailureInfo, retry_index: int) -> float:
    if failure.retry_after is not None:
        return failure.retry_after
    return random.uniform(0.0, min(2.0 * 2**retry_index, 60.0))


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)
