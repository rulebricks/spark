"""A row budget counts all admitted rows, including retries and split trees."""

import inspect
import json
import pickle
import threading
import time
from collections import Counter

import httpx
import pyarrow as pa
import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rulebricks_spark import apply_flow, apply_rule, resume
from rulebricks_spark._config import DEFAULT_MAX_BATCH_ROWS
from rulebricks_spark._engine import make_partition_worker
from rulebricks_spark._metadata import build_execution_schema
from tests.conftest import echo
from tests.test_concurrency import capture_clients


def run_partition(config, rows, names=("id",)):
    config.execution_partitions = 1
    config.input_columns = list(names)
    schema = StructType(
        [StructField(name, LongType() if name == "id" else StringType()) for name in names]
    )
    batch = pa.RecordBatch.from_pylist(rows, schema=None)
    return make_partition_worker(config, names, build_execution_schema(schema))(iter([batch]))


def test_row_budget_is_divided_without_rounding_up(config):
    config.max_inflight_rows = 10
    config.execution_partitions = 3
    assert [config.row_budget(i) for i in range(3)] == [4, 3, 3]
    assert [config.request_slots(i) for i in range(3)] == [4, 3, 3]
    with pytest.raises(ValueError, match="partition"):
        config.row_budget(3)
    config.max_inflight_rows = 10000
    assert config.request_slots(0) == 32


@pytest.mark.parametrize("apply", [apply_flow, apply_rule, resume])
def test_public_api_exposes_only_the_row_budget(apply):
    parameters = inspect.signature(apply).parameters
    assert "max_inflight_rows" in parameters
    assert "target_concurrency" not in parameters
    assert "adaptive_batching" not in parameters


@pytest.mark.parametrize("rows", [DEFAULT_MAX_BATCH_ROWS, 1000, None, 4])
def test_batch_mode_survives_serialization(config, rows):
    config.max_inflight_rows = 128
    config.max_batch_rows = rows
    copied = pickle.loads(pickle.dumps(config))
    assert copied.tunes_batches is (rows is DEFAULT_MAX_BATCH_ROWS)
    assert copied.max_batch_rows == rows


@pytest.mark.parametrize(
    "budget,batch_rows,partitions,expected",
    [
        (128, 4, 6, [24, 24, 20, 20, 20, 20]),
        (130, 4, 6, [24, 24, 22, 20, 20, 20]),
        (128, 32, 3, [64, 32, 32]),
        (128, 64, 2, [64, 64]),
        (129, 128, 1, [129]),
        (7, 3, 2, [4, 3]),
        (3, 10, 1, [3]),
        (8, 4, 4, [2, 2, 2, 2]),
    ],
)
def test_manual_allowances_preserve_full_requests_when_producers_allow_it(
    config, budget, batch_rows, partitions, expected
):
    config.max_inflight_rows = budget
    config.max_batch_rows = batch_rows
    config.execution_partitions = partitions
    allowances = [config.row_budget(i) for i in range(partitions)]
    assert allowances == expected
    assert sum(allowances) == budget
    if partitions <= budget // batch_rows:
        assert (
            sum((r + batch_rows - 1) // batch_rows for r in allowances)
            == (budget + batch_rows - 1) // batch_rows
        )


def test_manual_batches_use_the_callers_ceiling_without_constructing_a_policy(
    config, monkeypatch, caplog
):
    def unexpected_policy(*args, **kwargs):
        raise AssertionError("Manual batching must not create an optimizer")

    monkeypatch.setattr("rulebricks_spark._adaptive.BatchPolicy", unexpected_policy)
    config.max_inflight_rows = 16
    config.max_batch_rows = 8
    sizes = []

    def handler(request):
        sizes.append(len(json.loads(request.content)))
        return echo(request)

    clients = capture_clients(monkeypatch, handler)
    with caplog.at_level("INFO", logger="rulebricks_spark._adaptive"):
        rows = [
            row
            for batch in run_partition(config, [{"id": i} for i in range(128)])
            for row in batch.to_pylist()
        ]
    assert sizes == [8] * 16
    assert sorted(row["id"] for row in rows) == list(range(128))
    assert "automatic=False" in caplog.text and "comparisons=0" in caplog.text
    assert "measurements=" not in caplog.text
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize("batch_rows,expected", [(None, [24000]), (20000, [20000, 4000])])
def test_manual_row_budget_has_no_hidden_batch_row_limit(config, monkeypatch, batch_rows, expected):
    config.max_inflight_rows = 24000
    config.max_batch_rows = batch_rows
    sizes = []

    def handler(request):
        sizes.append(len(json.loads(request.content)))
        return echo(request)

    capture_clients(monkeypatch, handler)
    actual = [
        r for b in run_partition(config, [{"id": i} for i in range(24000)]) for r in b.to_pylist()
    ]
    assert sorted(sizes) == sorted(expected)
    assert sorted(r["id"] for r in actual) == list(range(24000))


@pytest.mark.parametrize("batch_rows,expected", [(None, 24000), (20000, 20000), (50000, 24000)])
def test_manual_planning_uses_only_the_row_budget_and_supplied_ceiling(batch_rows, expected):
    from rulebricks_spark._config import initial_batch_rows

    assert initial_batch_rows(batch_rows, 24000) == expected


@pytest.mark.parametrize("budget", [16, 64, 1250, 8000, 24000])
def test_small_payloads_start_packed_instead_of_maximizing_threads(config, monkeypatch, budget):
    config.max_inflight_rows = budget
    sizes = []

    def handler(request):
        sizes.append(len(json.loads(request.content)))
        return echo(request)

    capture_clients(monkeypatch, handler)
    rows = [
        row
        for batch in run_partition(config, [{"id": i} for i in range(budget)])
        for row in batch.to_pylist()
    ]
    base, extra = divmod(budget, 2)
    assert sorted(sizes) == sorted([base, base + extra])
    assert sorted(row["id"] for row in rows) == list(range(budget))


def test_large_payloads_retain_the_four_request_start(config, monkeypatch):
    config.max_inflight_rows = 16
    sizes = []

    def handler(request):
        sizes.append(len(json.loads(request.content)))
        return echo(request)

    capture_clients(monkeypatch, handler)
    inputs = [{"id": i, "data": "x" * 256_000} for i in range(16)]
    rows = [
        row
        for batch in run_partition(config, inputs, names=("id", "data"))
        for row in batch.to_pylist()
    ]
    assert sizes == [4] * 4
    assert sorted(row["id"] for row in rows) == list(range(16))


def test_payload_average_forgets_earlier_sizes_without_counting_prepared_rows_twice(config):
    from rulebricks_spark._adaptive import RecordStream

    config.max_inflight_rows = 8
    inputs = [{"text": "x" * size} for size in [2048] * 8 + [15360] * 24]
    batch = pa.RecordBatch.from_pylist(inputs)
    stream = RecordStream(iter([batch]), config, ["text"], row_budget=8)
    first, _, _ = stream.take(8)
    initial = stream.average_payload_bytes
    assert stream.observed_rows == 8
    stream.prepend(first)
    stream.take(8)
    assert stream.observed_rows == 8 and stream.average_payload_bytes == initial
    for _ in range(3):
        stream.take(8)
    assert stream.observed_rows == 32
    assert 14500 < stream.average_payload_bytes < 15400


def test_later_larger_payloads_adjust_the_plan_and_keep_actual_byte_limits(config, monkeypatch):
    from rulebricks_spark import _adaptive

    policies = []
    original = _adaptive.BatchPolicy

    class CapturedPolicy(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            policies.append(self)

    monkeypatch.setattr(_adaptive, "BatchPolicy", CapturedPolicy)
    config.max_inflight_rows = 16
    config.max_batch_bytes = 64 * 1024
    sent = []

    def handler(request):
        records = json.loads(request.content)
        assert len(request.content) <= config.max_batch_bytes
        sent.extend(r["id"] for r in records)
        if any(len(r["text"]) == 15360 for r in records):
            assert len(records) <= 4
        return echo(request)

    capture_clients(monkeypatch, handler)
    inputs = [{"id": i, "text": "x" * (2048 if i < 32 else 15360)} for i in range(96)]
    rows = [
        row
        for batch in run_partition(config, inputs, names=("id", "text"))
        for row in batch.to_pylist()
    ]
    assert sorted(r["id"] for r in rows) == sorted(sent) == list(range(96))
    assert all(r["_rb_status"] == "ok" for r in rows)
    assert policies[0].minimum_width >= 4


def test_changed_payload_mix_reopens_timing_checks_without_needing_a_byte_overflow(
    config, monkeypatch
):
    from rulebricks_spark import _adaptive

    policies = []
    original = _adaptive.BatchPolicy

    class CapturedPolicy(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            policies.append(self)

    monkeypatch.setattr(_adaptive, "BatchPolicy", CapturedPolicy)
    config.max_inflight_rows = 16
    capture_clients(monkeypatch, echo)
    inputs = [{"id": i, "text": "x" * (2048 if i < 32 else 15360)} for i in range(96)]
    rows = [r for b in run_partition(config, inputs, ("id", "text")) for r in b.to_pylist()]
    assert sorted(r["id"] for r in rows) == list(range(96))
    changes = [e for e in policies[0].events if e.get("event") == "payload_size_changed"]
    assert 1 <= len(changes) <= 4


def test_return_to_smaller_payloads_can_release_an_old_byte_constraint(config, monkeypatch):
    from rulebricks_spark import _adaptive

    policies = []
    original = _adaptive.BatchPolicy

    class CapturedPolicy(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            policies.append(self)

    monkeypatch.setattr(_adaptive, "BatchPolicy", CapturedPolicy)
    config.max_inflight_rows = 16
    config.max_batch_bytes = 64 * 1024

    def handler(request):
        assert len(request.content) <= config.max_batch_bytes
        return echo(request)

    capture_clients(monkeypatch, handler)
    inputs = [{"id": i, "text": "x" * (15360 if 32 <= i < 96 else 2048)} for i in range(160)]
    rows = [r for b in run_partition(config, inputs, ("id", "text")) for r in b.to_pylist()]
    assert sorted(r["id"] for r in rows) == list(range(160))
    assert policies[0].minimum_width <= 2


def test_manual_byte_limited_batches_charge_actual_rows(config, monkeypatch):
    config.max_inflight_rows = 4
    config.max_batch_rows = 4
    config.max_batch_bytes = 20  # Exactly two of these small records fit.
    full = threading.Barrier(2, timeout=10)
    sizes = []

    def handler(request):
        size = len(json.loads(request.content))
        sizes.append(size)
        assert len(request.content) <= config.max_batch_bytes
        full.wait()  # Both two-row batches must be admitted before either completes.
        return echo(request)

    capture_clients(monkeypatch, handler)
    rows = [
        row
        for batch in run_partition(config, [{"id": i} for i in range(4)])
        for row in batch.to_pylist()
    ]
    assert sizes == [2, 2]
    assert sorted(row["id"] for row in rows) == list(range(4))


@pytest.mark.parametrize("adaptive", [True, False])
def test_refills_completed_rows_while_a_slow_batch_keeps_its_allowance(
    config, monkeypatch, adaptive
):
    config.max_inflight_rows = 7
    config.max_batch_rows = DEFAULT_MAX_BATCH_ROWS if adaptive else 3
    lock = threading.Lock()
    full = threading.Event()
    replacement = threading.Event()
    release = threading.Event()
    active = peak = 0
    seen = []

    def handler(request):
        nonlocal active, peak
        ids = [r["id"] for r in json.loads(request.content)]
        with lock:
            seen.append(ids)
            active += len(ids)
            peak = max(peak, active)
            if active == 7:
                full.set()
        try:
            if 0 in ids:
                assert release.wait(10)
            else:
                assert full.wait(10)
                if max(ids) >= 7:
                    replacement.set()
            return echo(request)
        finally:
            with lock:
                active -= len(ids)

    clients = capture_clients(monkeypatch, handler)
    iterator = run_partition(config, [{"id": i} for i in range(31)])
    try:
        first = next(iterator).to_pylist()
        assert replacement.wait(10)
        assert all(r["id"] != 0 for r in first)
        assert peak == 7
    finally:
        release.set()
    result = first + [r for batch in iterator for r in batch.to_pylist()]
    assert sorted(r["id"] for r in result) == list(range(31))
    assert sorted(i for ids in seen for i in ids) == list(range(31))
    assert max(map(len, seen)) <= (4 if adaptive else 3)
    assert peak == 7
    assert all(client.is_closed for client in clients)


@pytest.mark.parametrize("adaptive", [True, False])
@pytest.mark.parametrize("large", [False, True])
def test_packing_uses_bytes_and_row_ceiling_without_rejecting_large_records(
    config, monkeypatch, large, adaptive
):
    config.max_inflight_rows = 8
    config.max_batch_rows = DEFAULT_MAX_BATCH_ROWS if adaptive else 3
    config.max_batch_bytes = 1024
    requests = []

    def handler(request):
        records = json.loads(request.content)
        requests.append(records)
        assert len(request.content) <= config.max_batch_bytes or len(records) == 1
        return echo(request)

    capture_clients(monkeypatch, handler)
    text = "x" * (config.max_batch_bytes + 10 if large else 10)
    rows = [{"id": i, "text": text} for i in range(9)]
    actual = [r for batch in run_partition(config, rows, ("id", "text")) for r in batch.to_pylist()]
    assert sorted(r["id"] for r in actual) == list(range(9))
    assert all(r["_rb_status"] == "ok" for r in actual)
    assert max(map(len, requests)) == (1 if large else (4 if adaptive else 3))


@pytest.mark.parametrize("adaptive", [True, False])
def test_retries_splits_and_terminal_errors_keep_the_row_allowance(config, monkeypatch, adaptive):
    config.max_inflight_rows = 6
    config.max_batch_rows = DEFAULT_MAX_BATCH_ROWS if adaptive else 3
    config.max_retries = 1
    monkeypatch.setattr("rulebricks_spark._engine._retry_delay", lambda *_: 0)
    lock = threading.Lock()
    calls = Counter()
    active = peak = 0

    def handler(request):
        nonlocal active, peak
        records = json.loads(request.content)
        ids = tuple(r["id"] for r in records)
        with lock:
            calls[ids] += 1
            attempt = calls[ids]
            active += len(ids)
            peak = max(peak, active)
        try:
            if len(ids) > 1 and 0 in ids:
                return httpx.Response(413, text="split")
            if ids == (0,) and attempt == 1:
                raise httpx.ReadTimeout("uncertain completion", request=request)
            if 4 in ids and attempt == 1:
                return httpx.Response(503, text="infrastructure unavailable")
            if 8 in ids:
                return httpx.Response(400, text="invalid input")
            return echo(request)
        finally:
            with lock:
                active -= len(ids)

    clients = capture_clients(monkeypatch, handler)
    actual = {
        r["id"]: r
        for batch in run_partition(config, [{"id": i} for i in range(12)])
        for r in batch.to_pylist()
    }
    assert sorted(actual) == list(range(12))
    assert peak <= 6
    assert calls[(0,)] == 2
    assert actual[0]["_rb_status"] == "ok"
    assert actual[0]["_rb_attempts"] >= 3
    assert actual[4]["_rb_status"] == "ok" and actual[4]["_rb_attempts"] >= 2
    assert actual[8]["_rb_status"] != "ok" and actual[8]["_rb_http_status"] == 400
    assert all(client.is_closed for client in clients)


def test_row_budget_rejects_stateful_checkpoint_recovery():
    with pytest.raises(ValueError, match="max_inflight_rows.*stateful"):
        resume(
            "unused-checkpoint", api_key="test", base_url="http://localhost", max_inflight_rows=2
        )


@pytest.mark.parametrize("adaptive", [True, False])
def test_invalid_payloads_return_one_outcome_and_do_not_stall_admission(
    config, monkeypatch, adaptive
):
    config.max_inflight_rows = 4
    config.max_batch_rows = DEFAULT_MAX_BATCH_ROWS if adaptive else 3
    config.payload_column = "body"
    capture_clients(monkeypatch, echo)
    source = [
        {"id": i, "body": "invalid" if i == 3 else json.dumps({"value": i})} for i in range(9)
    ]
    actual = {
        r["id"]: r
        for batch in run_partition(config, source, ("id", "body"))
        for r in batch.to_pylist()
    }
    assert sorted(actual) == list(range(9))
    assert actual[3]["_rb_error_type"] == "client"
    assert all(json.loads(r["_rb_result"]) == {"value": i} for i, r in actual.items() if i != 3)


@pytest.mark.parametrize("adaptive", [True, False])
def test_one_row_budget_refills_before_waiting_for_output_consumption(
    config, monkeypatch, adaptive
):
    config.max_inflight_rows = 1
    config.max_batch_rows = DEFAULT_MAX_BATCH_ROWS if adaptive else 1
    replacement = threading.Event()
    release = threading.Event()

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        if item == 1:
            replacement.set()
            assert release.wait(10)
        return echo(request)

    clients = capture_clients(monkeypatch, handler)
    iterator = run_partition(config, [{"id": 0}, {"id": 1}])
    try:
        assert next(iterator).to_pylist()[0]["id"] == 0
        assert replacement.wait(10)
    finally:
        release.set()
    assert [r["id"] for batch in iterator for r in batch.to_pylist()] == [1]
    assert len(clients) == 1 and clients[0].is_closed


def test_live_plan_transitions_keep_rows_and_clients_bounded(config, monkeypatch):
    from rulebricks_spark import _adaptive as adaptive_engine

    original = adaptive_engine.BatchPolicy
    policies = []

    class FastPolicy(original):
        def _set_width(self, width, warmup=True):
            super()._set_width(width, warmup)
            self.minimum_rows = 8

        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            policies.append(self)

    monkeypatch.setattr(adaptive_engine, "BatchPolicy", FastPolicy)
    monkeypatch.setattr(adaptive_engine, "initial_requests", lambda *_: 4)
    config.max_inflight_rows = 16
    lock = threading.Lock()
    active = peak = 0
    ids = []

    def handler(request):
        nonlocal active, peak
        batch = json.loads(request.content)
        with lock:
            active += len(batch)
            peak = max(peak, active)
            ids.extend(x["id"] for x in batch)
        try:
            time.sleep(0.03 if len(batch) >= 8 else 0.01)
            return echo(request)
        finally:
            with lock:
                active -= len(batch)

    clients = capture_clients(monkeypatch, handler)
    rows = [
        r for b in run_partition(config, [{"id": i} for i in range(4096)]) for r in b.to_pylist()
    ]
    assert sorted(r["id"] for r in rows) == sorted(ids) == list(range(4096))
    assert peak <= 16
    assert len({e["width"] for e in policies[0].events}) >= 2
    assert all(c.is_closed for c in clients)


@pytest.mark.parametrize("explicit_old_limit", [False, True])
def test_default_byte_ceiling_allows_requests_above_five_mib(
    config, monkeypatch, explicit_old_limit
):
    config.max_inflight_rows = 8
    if explicit_old_limit:
        config.max_batch_bytes = 5 * 1024 * 1024
    else:
        assert config.max_batch_bytes == 64 * 1024 * 1024
    request_bytes = []

    def handler(request):
        request_bytes.append(len(request.content))
        return echo(request)

    capture_clients(monkeypatch, handler)
    rows = [{"id": i, "text": "x" * (3 * 1024 * 1024)} for i in range(4)]
    actual = [r for b in run_partition(config, rows, ("id", "text")) for r in b.to_pylist()]
    assert sorted(r["id"] for r in actual) == list(range(4))
    assert all(r["_rb_status"] == "ok" for r in actual)
    assert max(request_bytes) <= config.max_batch_bytes
    assert (max(request_bytes) > 5 * 1024 * 1024) != explicit_old_limit
