"""Request scheduling and failure isolation against the real SDK transport."""

import json
from contextlib import contextmanager

import httpx
import pyarrow as pa
import pytest
from pyspark.sql.types import LongType, StructField, StructType

from rulebricks_spark import ExecutionError, _engine
from rulebricks_spark._engine import _execute_chunk, _make_record, make_partition_worker
from rulebricks_spark._metadata import build_execution_schema
from tests.conftest import echo


def execute(config, sdk, rows):
    return _execute_chunk([_make_record(row, config) for row in rows], sdk.client, config)


def test_retry_preserves_failed_attempt_and_honors_server_delay(config, sdk, monkeypatch):
    config.max_retries = 1
    config.diagnostics = True
    sleeps = []
    monkeypatch.setattr(_engine.time, "sleep", sleeps.append)
    sdk.handler = lambda request: (
        httpx.Response(
            429,
            json={"error": "busy"},
            headers={"Retry-After": "120", "X-Rulebricks-Trace-Id": "failed-trace"},
        )
        if len(sdk.requests) == 1
        else echo(request)
    )
    outcome = execute(config, sdk, [{"id": "a"}])[0]
    assert sleeps == [120.0]
    assert len(sdk.requests) == 2
    assert outcome.metadata["_rb_status"] == "ok" and outcome.metadata["_rb_attempts"] == 2
    history = json.loads(outcome.metadata["_rb_attempt_history"])
    assert history[0]["http_status"] == 429 and history[0]["trace_id"] == "failed-trace"


@pytest.mark.parametrize("status", [413, 500])
def test_split_preserves_order_and_bounds_requests(config, sdk, status, monkeypatch):
    def handler(request):
        payload = json.loads(request.content)
        if len(payload) > 1 if status == 413 else any(item["id"] == 1 for item in payload):
            return httpx.Response(status, json={"error": "cannot execute chunk"})
        return echo(request)

    sdk.handler = handler
    outcomes = execute(config, sdk, [{"id": i} for i in range(3)])
    assert [r.record.source["id"] for r in outcomes] == [0, 1, 2]
    assert [r.metadata["_rb_status"] for r in outcomes] == (
        ["ok"] * 3 if status == 413 else ["ok", "chunk_error", "ok"]
    )
    assert len(sdk.requests) == 5
    if status == 500:
        config.isolate_failures = False
        sdk.requests.clear()
        outcomes = execute(config, sdk, [{"id": i} for i in range(3)])
        assert len(sdk.requests) == 1 and all(r.result is None for r in outcomes)
        config.isolate_failures = True
        config.max_retries = 2
        monkeypatch.setattr(_engine, "MAX_ISOLATION_REQUESTS", 5)
        monkeypatch.setattr(_engine.time, "sleep", lambda _: None)
        sdk.requests.clear()
        sdk.handler = lambda request: httpx.Response(500, text="outage")
        outcomes = execute(config, sdk, [{"id": i} for i in range(8)])
        assert len(sdk.requests) == 5 and len(outcomes) == 8
        assert all(r.metadata["_rb_error_type"] == "server" for r in outcomes)


@pytest.mark.parametrize("body", [[{}], [{}, {}, {}], {"error": "request failed"}])
def test_protocol_errors_never_assign_unaligned_results(config, sdk, body):
    sdk.handler = lambda request: httpx.Response(200, json=body)
    rows = [{"id": 0}, {"id": 1}]
    outcomes = execute(config, sdk, rows)
    assert len(sdk.requests) == 1
    assert all(r.result is None and r.metadata["_rb_error_type"] == "protocol" for r in outcomes)
    assert all(r.metadata["_rb_trace_id"] == "trace-123" for r in outcomes)
    config.on_error = "raise"
    with pytest.raises(ExecutionError, match="Rulebricks"):
        execute(config, sdk, rows)


@pytest.mark.parametrize(
    "status,body", [(400, {"error": "bad input"}), (200, [{"error": "bad formula"}])]
)
def test_raise_policy_covers_http_and_item_failures(config, sdk, status, body):
    config.on_error = "raise"
    sdk.handler = lambda request: httpx.Response(status, json=body)
    with pytest.raises(ExecutionError, match="bad"):
        execute(config, sdk, [{}])
    assert len(sdk.requests) == 1


def test_connection_errors_retry_but_programming_errors_do_not(config, sdk, monkeypatch):
    config.max_retries = 1
    monkeypatch.setattr(_engine.time, "sleep", lambda _: None)
    for error, attempts, category in [
        (httpx.ConnectError("offline"), 2, "network"),
        (TypeError("parser bug"), 1, "unknown"),
    ]:

        def fail(request, error=error):
            raise error

        sdk.handler = fail
        sdk.requests.clear()
        outcome = execute(config, sdk, [{}])[0]
        assert len(sdk.requests) == attempts
        assert outcome.metadata["_rb_error_type"] == category


def test_arrow_buffers_are_bounded_and_client_closes_on_cancellation(config, sdk, monkeypatch):
    from rulebricks_spark._transport import sdk_client

    clients = []
    client_type = httpx.Client

    def capture_client(**kwargs):
        client = client_type(transport=httpx.MockTransport(echo), **kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr("rulebricks_spark._transport.httpx.Client", capture_client)
    config.input_columns = []
    config.max_batch_rows = None
    schema = build_execution_schema(StructType([StructField("id", LongType())]))
    calls = []

    @contextmanager
    def record_requests(config):
        with sdk_client(config) as client:
            client._client_wrapper.httpx_client.httpx_client.event_hooks["request"].append(
                lambda request: calls.append(len(json.loads(request.content)))
            )
            yield client

    monkeypatch.setattr(_engine, "sdk_client", record_requests)
    worker = make_partition_worker(config, ["id"], schema)
    batch = pa.record_batch([list(range(15000))], names=["id"])
    assert sum(part.num_rows for part in worker(iter([batch]))) == 15000
    assert calls == [10000, 5000] and clients[0].is_closed
    iterator = worker(iter([batch]))
    next(iterator)
    iterator.close()
    assert clients[1].is_closed
