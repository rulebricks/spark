"""Bounded scheduling through the real SDK, with explicit completion gates."""

import json
import threading
import time
from collections import Counter
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rulebricks_spark import ExecutionError
from rulebricks_spark._engine import make_partition_worker
from rulebricks_spark._metadata import build_execution_schema
from tests.conftest import echo


def capture_clients(monkeypatch, handler):
    clients = []
    client_type = httpx.Client

    def create(**kwargs):
        client = client_type(transport=httpx.MockTransport(handler), **kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr("rulebricks_spark._transport.httpx.Client", create)
    return clients


def partition(config, rows=20, read_rows=None):
    config.max_inflight_rows = config.max_inflight_rows or 2
    config.execution_partitions = 1
    config.input_columns = ["id"]
    schema = build_execution_schema(StructType([StructField("id", LongType())]))

    def batches():
        for i in range(rows):
            if read_rows is not None:
                read_rows.append(i)
            yield pa.record_batch([[i]], names=["id"])

    return make_partition_worker(config, ["id"], schema)(batches())


def test_refills_before_slow_request_finishes_and_bounds_input(config, monkeypatch):
    first_started = threading.Event()
    replacement_started = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    requests = []
    active = 0
    peak = 0

    def handler(request):
        nonlocal active, peak
        item = json.loads(request.content)[0]["id"]
        with lock:
            requests.append(item)
            active += 1
            peak = max(peak, active)
        try:
            if item == 0:
                first_started.set()
                assert release.wait(10)
            elif item == 1:
                assert first_started.wait(10)
            else:
                replacement_started.set()
                assert release.wait(10)
            return echo(request)
        finally:
            with lock:
                active -= 1

    clients = capture_clients(monkeypatch, handler)
    read_rows = []
    iterator = partition(config, read_rows=read_rows)
    try:
        first = next(iterator).to_pylist()
        assert first[0]["id"] == 1
        assert replacement_started.wait(10)
        # Spark has paused output consumption: no unbounded queue of requests.
        assert sorted(requests) == [0, 1, 2]
        assert len(read_rows) <= 4
        assert peak == 2 and len(clients) == 2
    finally:
        release.set()
    rows = first + [row for batch in iterator for row in batch.to_pylist()]
    assert sorted(row["id"] for row in rows) == list(range(20))
    assert all(json.loads(row["_rb_result"])["id"] == row["id"] for row in rows)
    assert peak == 2 and len(clients) == 2
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize("raise_error", [False, True])
@pytest.mark.parametrize("manual", [False, True])
def test_cancellation_interrupts_retry_wait_and_closes_clients(
    config, monkeypatch, raise_error, manual
):
    if manual:
        config.max_batch_rows = 1
    retry_started = threading.Event()
    requests = []

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        requests.append(item)
        if item == 0:
            retry_started.set()
            return httpx.Response(429, headers={"Retry-After": "120"}, text="busy")
        assert retry_started.wait(10)
        return httpx.Response(400, text="bad input") if raise_error else echo(request)

    config.max_retries = 2
    config.on_error = "raise" if raise_error else "continue"
    clients = capture_clients(monkeypatch, handler)
    iterator = partition(config)
    before = time.monotonic()
    if raise_error:
        with pytest.raises(ExecutionError, match="bad input"):
            next(iterator)
        assert sorted(requests) == [0, 1]
    else:
        assert next(iterator).to_pylist()[0]["id"] == 1
        iterator.close()
        assert set(requests) <= {0, 1, 2}
    assert time.monotonic() - before < 10
    assert requests.count(0) == 1
    assert all(client.is_closed for client in clients)


def test_concurrent_splits_retries_and_local_errors_preserve_alignment(config, monkeypatch):
    config.max_inflight_rows = 4
    config.execution_partitions = 1
    config.max_batch_rows = 2
    config.max_batch_bytes = 19
    config.max_retries = 1
    config.payload_column = "body"
    lock = threading.Lock()
    retried = False
    requests = []

    def handler(request):
        nonlocal retried
        assert len(request.content) <= config.max_batch_bytes
        items = json.loads(request.content)
        with lock:
            requests.append(items)
            if len(items) > 1:
                return httpx.Response(413, text="split me")
            if items[0]["id"] == 0 and not retried:
                retried = True
                return httpx.Response(503, text="retry me", headers={"Retry-After": "0"})
        return echo(request)

    clients = capture_clients(monkeypatch, handler)
    schema = build_execution_schema(
        StructType([StructField("id", LongType()), StructField("body", StringType())])
    )
    bodies = [json.dumps({"id": i}) if i != 3 else "invalid json" for i in range(7)]
    batch = pa.record_batch([list(range(7)), bodies], names=["id", "body"])
    rows = {
        row["id"]: row
        for output in make_partition_worker(config, ["id", "body"], schema)(iter([batch]))
        for row in output.to_pylist()
    }
    assert sorted(rows) == list(range(7))
    assert rows[0]["_rb_attempts"] == 3
    assert rows[3]["_rb_error_type"] == "client"
    for i, row in rows.items():
        if i != 3:
            assert json.loads(row["_rb_result"]) == {"id": i}
            assert row["_rb_status"] == "ok"
    assert all(len(items) <= 2 for items in requests)
    assert all(client.is_closed for client in clients)


def test_legacy_tasks_use_one_request_without_a_row_budget(config):
    assert config.request_slots(100) == 1
    assert config.row_budget(100) is None
    assert not config.tunes_batches


@pytest.mark.parametrize("row_count", [0, 6])
def test_input_partition_marker_overrides_task_context_without_leaking(
    config, monkeypatch, row_count
):
    from pyspark import TaskContext

    monkeypatch.setattr(TaskContext, "get", lambda: SimpleNamespace(partitionId=lambda: 999))
    config.max_inflight_rows = 5
    config.execution_partitions = 2
    config.max_batch_rows = 1
    config.input_columns = ["id"]
    release = threading.Event()
    lock = threading.Lock()
    active = peak = 0
    payloads = []

    def handler(request):
        nonlocal active, peak
        with lock:
            payloads.extend(json.loads(request.content))
            active += 1
            peak = max(peak, active)
            if active == 2:
                release.set()
        try:
            assert release.wait(10)
            return echo(request)
        finally:
            with lock:
                active -= 1

    clients = capture_clients(monkeypatch, handler)
    marker = "_rb_request_partition"
    batches = iter(
        [
            pa.record_batch([[], []], names=["id", marker]),
            pa.record_batch([list(range(row_count)), [1] * row_count], names=["id", marker]),
        ]
    )
    schema = build_execution_schema(StructType([StructField("id", LongType())]))
    rows = [
        row
        for batch in make_partition_worker(config, ["id"], schema, partition_id_column=marker)(
            batches
        )
        for row in batch.to_pylist()
    ]
    assert peak == (2 if row_count else 0)
    assert sorted(payloads, key=lambda item: item["id"]) == [{"id": i} for i in range(row_count)]
    assert sorted(row["id"] for row in rows) == list(range(row_count))
    assert all(marker not in row for row in rows)
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize(
    "failure,category",
    [
        (httpx.ReadTimeout, "network"),
        (httpx.ConnectTimeout, "network"),
        (httpx.ConnectError, "network"),
        (408, "network"),
        (409, "infrastructure"),
        (429, "throttled"),
        (500, "server"),
        (502, "infrastructure"),
        (503, "infrastructure"),
        (504, "infrastructure"),
    ],
)
@pytest.mark.parametrize("recover", [False, True])
def test_concurrent_retry_limits_and_failure_metadata(
    config, monkeypatch, failure, category, recover
):
    config.max_retries = 2
    config.diagnostics = True
    monkeypatch.setattr("rulebricks_spark._engine._retry_delay", lambda *_: 0)
    first_started = threading.Event()
    sibling_started = threading.Event()
    lock = threading.Lock()
    attempts = Counter()
    active = 0
    peak = 0

    def handler(request):
        nonlocal active, peak
        item = json.loads(request.content)[0]["id"]
        with lock:
            attempts[item] += 1
            attempt = attempts[item]
            active += 1
            peak = max(peak, active)
        try:
            if item == 0 and attempt == 1:
                first_started.set()
                assert sibling_started.wait(10)
            elif item == 1:
                sibling_started.set()
                assert first_started.wait(10)
            if item == 0 and (not recover or attempt < 3):
                if isinstance(failure, int):
                    return httpx.Response(
                        failure,
                        text="temporary failure",
                        headers={"X-Rulebricks-Trace-Id": f"failed-{attempt}"},
                    )
                raise failure("temporary failure", request=request)
            return echo(request)
        finally:
            with lock:
                active -= 1

    clients = capture_clients(monkeypatch, handler)
    outputs = [row for batch in partition(config, rows=8) for row in batch.to_pylist()]
    rows = {row["id"]: row for row in outputs}
    assert len(outputs) == len(rows) == 8
    assert peak == 2
    assert attempts == Counter({0: 3, **{i: 1 for i in range(1, 8)}})
    assert rows[0]["_rb_attempts"] == 3
    history = json.loads(rows[0]["_rb_attempt_history"])
    assert len(history) == 2
    assert all(attempt["error_type"] == category for attempt in history)
    if isinstance(failure, int):
        assert [attempt["trace_id"] for attempt in history] == ["failed-1", "failed-2"]
    if recover:
        assert rows[0]["_rb_status"] == "ok"
        assert json.loads(rows[0]["_rb_result"]) == {"id": 0}
    else:
        assert rows[0]["_rb_status"] == "chunk_error"
        assert rows[0]["_rb_error_type"] == category
        assert rows[0]["_rb_http_status"] == (failure if isinstance(failure, int) else None)
        assert rows[0]["_rb_result"] is None
    for i in range(1, 8):
        assert rows[i]["_rb_status"] == "ok"
        assert json.loads(rows[i]["_rb_result"]) == {"id": i}
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize("status", [400, 401, 422])
def test_concurrent_non_retryable_errors_leave_other_rows_running(config, monkeypatch, status):
    config.max_retries = 3
    requests = []

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        requests.append(item)
        return httpx.Response(status, text="invalid request") if item == 0 else echo(request)

    clients = capture_clients(monkeypatch, handler)
    rows = {row["id"]: row for batch in partition(config, rows=8) for row in batch.to_pylist()}
    assert sorted(requests) == list(range(8))
    assert rows[0]["_rb_attempts"] == 1
    assert rows[0]["_rb_error_type"] == "client"
    assert all(rows[i]["_rb_status"] == "ok" for i in range(1, 8))
    assert all(client.is_closed for client in clients)


def test_real_socket_timeout_retries_without_blocking_other_slot(config, api, monkeypatch):
    config.base_url = api.options["base_url"]
    config.timeout_seconds = 0.2
    config.max_retries = 1
    config.diagnostics = True
    monkeypatch.setattr("rulebricks_spark._engine._retry_delay", lambda *_: 0)
    release_first = threading.Event()
    sibling_completed = threading.Event()
    lock = threading.Lock()
    attempts = Counter()

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        with lock:
            attempts[item] += 1
            attempt = attempts[item]
        if item == 0 and attempt == 1:
            assert release_first.wait(10)
        elif item == 0:
            assert sibling_completed.is_set()
            release_first.set()
        else:
            sibling_completed.set()
        return echo(request)

    api.handler = handler
    try:
        rows = {row["id"]: row for batch in partition(config, rows=8) for row in batch.to_pylist()}
    finally:
        release_first.set()
    assert attempts == Counter({0: 2, **{i: 1 for i in range(1, 8)}})
    assert all(row["_rb_status"] == "ok" for row in rows.values())
    assert rows[0]["_rb_attempts"] == 2
    assert json.loads(rows[0]["_rb_attempt_history"])[0]["error_type"] == "network"


@pytest.mark.parametrize("failure", [503, httpx.ReadTimeout])
def test_exhausted_concurrent_retries_raise_and_close_clients(config, monkeypatch, failure):
    config.max_retries = 2
    config.on_error = "raise"
    monkeypatch.setattr("rulebricks_spark._engine._retry_delay", lambda *_: 0)
    started = [threading.Event(), threading.Event()]
    attempts = Counter()
    lock = threading.Lock()

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        with lock:
            attempts[item] += 1
        assert item in (0, 1)
        started[item].set()
        assert started[1 - item].wait(10)
        if isinstance(failure, int):
            return httpx.Response(failure, text="temporary failure")
        raise failure("temporary failure", request=request)

    clients = capture_clients(monkeypatch, handler)
    with pytest.raises(ExecutionError, match="temporary failure"):
        list(partition(config, rows=8))
    assert set(attempts) == {0, 1}
    assert max(attempts.values()) == 3
    assert all(count <= 3 for count in attempts.values())
    assert all(client.is_closed for client in clients)
