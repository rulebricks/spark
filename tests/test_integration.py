"""Public API through Spark workers, the generated SDK, and a local HTTP server."""

import json
import threading
import time
import uuid
from decimal import Decimal

import httpx
import pytest
from pyspark.sql import functions
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from rulebricks_spark import ResponseOptions, apply_flow, apply_rule, resume
from rulebricks_spark.sql import register_udf
from tests.conftest import ROOT_ID, echo

pytestmark = pytest.mark.requires_spark


def business_error(item):
    return item.get("failure", {}).get("message")


def unwrap(item):
    return item["data"]


def test_versioned_preflight_and_nested_output(spark, api):
    def decision(request):
        if request.method == "GET":
            assert dict(request.url.params) == {"slug": "decision", "version": "3"}
            return httpx.Response(
                200,
                json=[
                    {
                        "slug": "decision",
                        "published": True,
                        "request_schema": [{"key": "customer.country", "type": "string"}],
                        "response_schema": [
                            {"key": "decision", "type": "object"},
                            {"key": "decision.approved", "type": "boolean"},
                        ],
                    }
                ],
            )
        return httpx.Response(200, json=[{"decision": {"approved": True}}])

    api.handler = decision
    source = spark.createDataFrame([("US",)], ["country"])
    with pytest.raises(ValueError, match="customer.country: missing"):
        apply_rule(source, "decision", **api.options, version="3", input_validation="strict")
    assert len(api.requests) == 1 and api.requests[0].method == "GET"
    with pytest.warns(UserWarning, match="customer.country"):
        warned = apply_flow(source, "decision", **api.options, version="3", input_validation="warn")
    contract = warned.schema["rulebricks_meta"].metadata["rulebricks_contract"]
    assert contract["input_warnings"] == ["customer.country: missing declared input"]
    assert all(request.method == "GET" for request in api.requests)
    assert warned.collect()[0].rulebricks_meta.status == "ok"
    source = source.select(functions.struct("country").alias("source"))
    result = apply_rule(
        source,
        "decision",
        **api.options,
        version="3",
        input_mapping={"source": "customer"},
        input_validation="strict",
    )
    assert result.collect()[0].rulebricks_result.decision.approved is True
    assert json.loads(api.requests[-1].content) == [{"customer": {"country": "US"}}]


@pytest.mark.parametrize("apply", [apply_flow, apply_rule])
@pytest.mark.parametrize("typed", [False, True])
def test_batch_alignment_and_audit(spark, api, apply, typed):
    responses = [{"score": 1}, {"error": "formula failed"}, {"score": 3}]
    source = spark.createDataFrame(
        [(index, "original", json.dumps(response)) for index, response in enumerate(responses)],
        ["source.id", "score", "response"],
    )
    schema = StructType([StructField("score", LongType())]) if typed else None
    before = time.time()
    result = apply(
        source,
        "decision",
        **api.options,
        version="3",
        input_columns=["response"],
        output_schema=schema,
        response_options=ResponseOptions(
            result_format="auto" if typed else "json",
            raw_column="raw",
        ),
    )
    assert api.requests == []
    rows = result.collect()
    assert len(api.requests) == 1
    assert [r.rulebricks_meta.status for r in rows] == ["ok", "error", "ok"]
    assert [r["source.id"] for r in rows] == [0, 1, 2]
    assert all(r.score == "original" for r in rows)
    assert rows[1].rulebricks_result is None
    assert rows[1].rulebricks_meta.error.asDict() == {
        "type": "execution",
        "message": "formula failed",
        "http_status": 200,
    }
    assert [json.loads(r.raw) for r in rows] == responses
    assert (
        rows[2].rulebricks_result.score if typed else json.loads(rows[2].rulebricks_result)["score"]
    ) == 3
    meta = rows[2].rulebricks_meta
    assert (meta.slug, meta.version, meta.attempts, meta.trace_id) == (
        "decision",
        "3",
        1,
        "trace-123",
    )
    assert before <= meta.started_at.timestamp() <= meta.completed_at.timestamp() <= time.time()
    assert meta.execution_id == (ROOT_ID if apply == apply_flow else None)
    assert meta.item_execution_id == (
        str(uuid.uuid5(uuid.UUID(ROOT_ID), "2")) if apply == apply_flow else None
    )


def test_payload_shaping_limits_and_correlation(spark, api):
    source = spark.createDataFrame(
        [(i, "é" * 10, "unused") for i in range(5)], ["id", "text", "audit"]
    )
    rows = apply_flow(
        source,
        "decision",
        **api.options,
        input_columns=["id", "text"],
        input_mapping={"id": "customer"},
        correlation_id_path="customer",
        max_batch_rows=2,
        max_batch_bytes=90,
    ).collect()
    assert [r.rulebricks_meta.correlation_id for r in rows] == [str(i) for i in range(5)]
    assert all(r.audit == "unused" for r in rows)
    assert all(len(request.content) <= 90 for request in api.requests)
    assert all(len(json.loads(request.content)) <= 2 for request in api.requests)
    assert [item for request in api.requests for item in json.loads(request.content)] == [
        {"customer": i, "text": "é" * 10} for i in range(5)
    ]
    api.requests.clear()
    source = spark.createDataFrame(
        [('{"customer":{"id":"a"}}',), ("not JSON",), ('{"customer":{"id":"c"}}',)], ["body"]
    )
    rows = apply_flow(
        source, "decision", **api.options, payload_column="body", correlation_id_path="customer.id"
    ).collect()
    assert [r.rulebricks_meta.status for r in rows] == ["ok", "error", "ok"]
    assert rows[2].rulebricks_meta.correlation_id == "c"
    assert sum(len(json.loads(request.content)) for request in api.requests) == 2


def test_custom_responses_and_schema_drift(spark, api):
    responses = [
        {"data": [1, 2], "extra": True},
        {"failure": {"message": "bad input"}},
        {"data": ["changed"]},
        {"unexpected": True},
        {"data": []},
    ]
    source = spark.createDataFrame([(json.dumps(item),) for item in responses], ["response"])
    rows = apply_flow(
        source,
        "decision",
        **api.options,
        output_schema=ArrayType(LongType()),
        response_options=ResponseOptions(
            error_handler=business_error, transform=unwrap, raw_column="raw"
        ),
    ).collect()
    assert len(api.requests) == 1
    assert [r.rulebricks_meta.error.type if r.rulebricks_meta.error else None for r in rows] == [
        None,
        "execution",
        "schema",
        "response_handler",
        None,
    ]
    assert rows[0].rulebricks_result == [1, 2] and rows[4].rulebricks_result == []
    assert "$[0]" in rows[2].rulebricks_meta.error.message
    assert [json.loads(r.raw) for r in rows] == responses
    row = apply_flow(
        source.limit(1),
        "decision",
        **api.options,
        output_schema=StructType([StructField("data", LongType())]),
        response_options=ResponseOptions(schema_mismatch="null"),
    ).collect()[0]
    assert row.rulebricks_result.data is None and row.rulebricks_meta.status == "ok"


def test_arrow_preserves_nullable_ids_decimals_and_timestamps(spark, api):
    decimal = Decimal("123456789012345678901234567890123456.78")
    source = spark.createDataFrame(
        [
            (9007199254740993, str(decimal), '{"tier":"3"}'),
            (None, None, "{}"),
        ],
        StructType(
            [
                StructField("id", LongType()),
                StructField("amount", StringType()),
                StructField("attributes", StringType()),
            ]
        ),
    )
    source = source.withColumn(
        "attributes", functions.from_json("attributes", MapType(StringType(), StringType()))
    )
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("amount", DecimalType(38, 2)),
            StructField("attributes", MapType(StringType(), LongType())),
        ]
    )
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        source = source.withColumn(
            "event_at", functions.expr("TIMESTAMP_NTZ '2026-01-15 12:00:00'")
        )
        rows = apply_rule(source, "decision", **api.options, output_schema=schema).collect()
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
    assert rows[0].id == rows[0].rulebricks_result.id == 9007199254740993
    assert rows[1].id is None and rows[1].rulebricks_result.id is None
    assert rows[0].rulebricks_result.amount == decimal
    assert rows[0].rulebricks_result.attributes == {"tier": 3}
    payloads = json.loads(api.requests[0].content)
    assert payloads[0]["id"] == 9007199254740993 and payloads[1]["id"] is None
    assert payloads[0]["event_at"] == "2026-01-15T12:00:00"


def test_resume_reads_saved_contract_and_only_reexecutes_selected_rows(spark, api, tmp_path):
    responses = ResponseOptions(error_handler=business_error, transform=unwrap, raw_column="raw")
    headers = {"additional_headers": {"X-Workload": "nightly"}}
    api.handler = lambda request: (
        httpx.Response(503, text="unavailable")
        if json.loads(request.content)[0]["id"] == 1
        else httpx.Response(200, json=[{"data": {"score": 0}}])
    )
    source = spark.createDataFrame([(0,), (1,)], StructType([StructField("id", LongType(), False)]))
    first = apply_flow(
        source,
        "decision",
        **api.options,
        version="3",
        max_batch_rows=1,
        max_retries=0,
        output_schema=StructType([StructField("score", LongType(), False)]),
        response_options=responses,
        request_options=headers,
        diagnostics=True,
    )
    path = str(tmp_path / "run")
    first.write.parquet(path)
    restored = spark.read.parquet(path).withColumn("audit_batch", functions.lit(12))
    assert "nightly" not in json.dumps(restored.schema.jsonValue())
    with pytest.raises(ValueError, match="callbacks"):
        resume(restored, **api.options)
    with pytest.raises(ValueError, match="request_options"):
        resume(restored, **api.options, response_options=responses)
    api.requests.clear()
    api.handler = lambda request: httpx.Response(200, json=[{"data": {"score": 1}}])
    rows = sorted(
        resume(
            restored, **api.options, response_options=responses, request_options=headers
        ).collect(),
        key=lambda r: r.id,
    )
    assert len(api.requests) == 1 and json.loads(api.requests[0].content) == [{"id": 1}]
    assert api.requests[0].headers["X-Workload"] == "nightly"
    assert [r.rulebricks_result.score for r in rows] == [0, 1]
    assert [r.rulebricks_meta.attempts for r in rows] == [1, 2]
    assert all(r.audit_batch == 12 for r in rows)
    assert json.loads(rows[1].raw) == {"data": {"score": 1}}
    assert rows[1].rulebricks_meta.diagnostics.attempt_history[0].http_status == 503


@pytest.mark.parametrize("payload_format", ["json", "struct"])
def test_resume_preserves_payload_column_and_only_retries_failures(
    spark, api, tmp_path, payload_format
):
    payloads = [{"id": index, "customer": {"country": "US", "reference": None}} for index in (0, 1)]
    source = spark.createDataFrame(
        [(index, json.dumps(payload)) for index, payload in enumerate(payloads)],
        ["audit_id", "payload"],
    )
    if payload_format == "struct":
        source = source.withColumn(
            "payload",
            functions.from_json(
                "payload", "id long, customer struct<country:string,reference:string>"
            ),
        )
    api.handler = lambda request: (
        httpx.Response(503, text="unavailable")
        if json.loads(request.content)[0]["id"] == 1
        else echo(request)
    )
    path = str(tmp_path / "payload-run")
    apply_flow(
        source,
        "decision",
        **api.options,
        version="3",
        payload_column="payload",
        correlation_id_path="id",
        max_batch_rows=1,
        max_retries=0,
    ).write.parquet(path)
    saved = spark.read.parquet(path)
    initial = {row.audit_id: row for row in saved.collect()}
    assert initial[1].rulebricks_meta.error.type == "infrastructure"
    api.requests.clear()
    api.handler = echo

    rows = sorted(resume(saved, **api.options).collect(), key=lambda row: row.audit_id)

    assert len(api.requests) == 1
    assert json.loads(api.requests[0].content) == [payloads[1]]
    assert api.requests[0].url.path == "/api/v1/flows/decision/3"
    assert rows[0] == initial[0]
    assert rows[1].payload == initial[1].payload
    assert [json.loads(row.rulebricks_result) for row in rows] == payloads
    assert [row.rulebricks_meta.correlation_id for row in rows] == ["0", "1"]
    assert [row.rulebricks_meta.attempts for row in rows] == [1, 2]


@pytest.mark.parametrize("batch_options", [{}, {"max_batch_rows": 1}])
def test_resume_concurrent_failures_preserves_successes_and_history(
    spark, api, tmp_path, batch_options
):
    api.handler = lambda request: (
        httpx.Response(503, text="unavailable")
        if json.loads(request.content)[0]["id"] < 8
        else echo(request)
    )
    path = str(tmp_path / "concurrent-recovery")
    apply_flow(
        spark.range(12, numPartitions=2),
        "decision",
        **api.options,
        max_inflight_rows=4,
        **batch_options,
        max_retries=0,
        diagnostics=True,
    ).write.parquet(path)
    saved = spark.read.parquet(path)
    initial = {row.id: row for row in saved.collect()}
    api.requests.clear()
    release = threading.Event()
    lock = threading.Lock()
    active = peak = 0

    def recover(request):
        nonlocal active, peak
        size = len(json.loads(request.content))
        with lock:
            active += size
            peak = max(peak, active)
            if active == 4:
                release.set()
        try:
            assert release.wait(10)
            return echo(request)
        finally:
            with lock:
                active -= size

    api.handler = recover
    rows = resume(saved, **api.options, max_inflight_rows=4, **batch_options).collect()

    assert peak == 4
    assert sorted(item["id"] for r in api.requests for item in json.loads(r.content)) == list(
        range(8)
    )
    assert sorted(row.id for row in rows) == list(range(12))
    for row in rows:
        assert row.rulebricks_meta.status == "ok"
        assert json.loads(row.rulebricks_result) == {"id": row.id}
        assert row.rulebricks_meta.operation_id == initial[row.id].rulebricks_meta.operation_id
        if row.id < 8:
            assert row.rulebricks_meta.attempts == 2
            assert row.rulebricks_meta.diagnostics.attempt_history[0].http_status == 503
        else:
            assert row == initial[row.id]


def test_resume_rejects_replayable_lineage_and_changed_contracts(spark, api):
    api.handler = lambda request: httpx.Response(503, text="unavailable")
    pending = apply_flow(spark.range(1), "decision", **api.options, max_retries=0).cache()
    try:
        with pytest.raises(ValueError, match="cache"):
            resume(pending, **api.options)
        assert api.requests == []
    finally:
        pending.unpersist()
    first = pending.checkpoint(eager=True)
    second = apply_flow(spark.range(1), "decision", **api.options, max_retries=0).checkpoint(
        eager=True
    )
    with pytest.raises(ValueError, match="different executions"):
        resume(first.unionByName(second), **api.options)
    with pytest.raises(ValueError, match="types no longer match"):
        resume(first.withColumn("id", functions.col("id").cast("string")), **api.options)
    api.handler = echo
    row = resume(first, **api.options).collect()[0]
    assert row.rulebricks_meta.status == "ok" and row.rulebricks_meta.attempts == 2


def test_sql_registration_and_nullable_output(spark, api):
    register_udf(
        spark,
        "decide",
        "decision",
        **api.options,
        output_schema=StructType([StructField("id", LongType())]),
        max_batch_rows=2,
    )
    source = spark.createDataFrame(
        [('{"id":9007199254740993}',), ('{"error":"failed"}',)], ["response"]
    )
    rows = source.selectExpr("decide(struct(*)) AS result").collect()
    assert rows[0].result.id == 9007199254740993
    assert rows[1].result.id is None and rows[1].result._rb_error == "failed"
    source = spark.createDataFrame(
        [(9007199254740993,), (None,)], StructType([StructField("id", LongType())])
    ).coalesce(1)
    rows = source.selectExpr("decide(struct(*)) AS result").collect()
    assert "cannot guarantee precision" in rows[0].result._rb_error


def test_invalid_configuration_fails_before_requests(spark, api):
    source = spark.createDataFrame([(1, 2)], ["a", "odd`name"])
    for options, message in [
        ({"input_mapping": {"a": "odd`name"}}, "destination"),
        ({"result_column": "a"}, "already exist"),
        ({"request_options": {"additional_body_parameters": {}}}, "Unsupported"),
        ({"request_options": {"verify": "false"}}, "request_options.verify"),
        ({"max_batch_rows": 0}, "max_batch_rows"),
        ({"max_inflight_rows": 0}, "max_inflight_rows"),
        ({"max_inflight_rows": True}, "max_inflight_rows"),
        ({"max_inflight_rows": 1.5}, "max_inflight_rows"),
        ({"max_inflight_rows": 1, "execution_partitions": 2}, "partition count"),
        ({"max_inflight_rows": 1, "execution_partitions": None}, "partition count"),
        ({"output_schema": LongType()}, "output_schema"),
    ]:
        with pytest.raises((TypeError, ValueError), match=message):
            apply_flow(source, "decision", **api.options, **options)
    assert api.requests == []
    result = apply_flow(
        source.repartition(4),
        "decision",
        **api.options,
        execution_partitions=2,
        request_options={"verify": False},
    )
    assert result.rdd.getNumPartitions() == 2
    assert result.collect()[0]["odd`name"] == 2


@pytest.mark.parametrize("apply", [apply_flow, apply_rule])
@pytest.mark.parametrize("batch_options", [{}, {"max_batch_rows": 1}])
def test_row_budget_runs_multiple_requests_in_one_spark_task(spark, api, apply, batch_options):
    first_started = threading.Event()
    replacement_started = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0

    def handler(request):
        nonlocal active, peak
        item = json.loads(request.content)[0]["id"]
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            if item == 0:
                first_started.set()
                assert replacement_started.wait(10)
            elif item == 1:
                assert first_started.wait(10)
            elif item == 2:
                replacement_started.set()
            return echo(request)
        finally:
            with lock:
                active -= 1

    api.handler = handler
    result = apply(
        spark.range(12, numPartitions=1),
        "decision",
        **api.options,
        execution_partitions=1,
        max_inflight_rows=2,
        **batch_options,
        response_options=ResponseOptions(result_format="json"),
    )
    assert api.requests == [] and result.rdd.getNumPartitions() == 1
    rows = sorted(result.collect(), key=lambda row: row.id)
    assert peak == 2 and replacement_started.is_set()
    assert [row.id for row in rows] == list(range(12))
    assert all(json.loads(row.rulebricks_result) == {"id": row.id} for row in rows)
    assert all(row.rulebricks_meta.status == "ok" for row in rows)


@pytest.mark.parametrize(
    "options,partitions,allowances,tuning",
    [
        ({}, 1, [None], False),
        ({"execution_partitions": None}, 3, [None, None, None], False),
        ({"max_batch_rows": 2}, 1, [None], False),
        ({"max_batch_rows": None}, 1, [None], False),
        ({"max_inflight_rows": 5}, 1, [5], True),
        ({"max_inflight_rows": 1}, 1, [1], True),
        ({"max_inflight_rows": 5, "execution_partitions": 1}, 1, [5], True),
        ({"max_inflight_rows": 5, "execution_partitions": None}, 3, [2, 2, 1], True),
        ({"max_inflight_rows": 5, "max_batch_rows": 1000}, 1, [5], False),
        ({"max_inflight_rows": 5, "max_batch_rows": None}, 1, [5], False),
        ({"max_inflight_rows": 128, "max_batch_rows": 4}, 2, [64, 64], False),
        ({"max_inflight_rows": 128, "max_batch_rows": 32}, 1, [128], False),
        ({"max_inflight_rows": 128, "max_batch_rows": 64}, 1, [128], False),
        (
            {"max_inflight_rows": 5, "max_batch_rows": 4, "execution_partitions": 3},
            3,
            [2, 2, 1],
            False,
        ),
    ],
)
def test_request_planning_and_legacy_defaults(
    spark, api, monkeypatch, options, partitions, allowances, tuning
):
    from rulebricks_spark import _apply

    configs = []
    original = _apply.make_partition_worker

    def capture(config, *args, **kwargs):
        configs.append(config)
        return original(config, *args, **kwargs)

    monkeypatch.setattr(_apply, "make_partition_worker", capture)
    result = apply_flow(spark.range(12, numPartitions=3), "decision", **api.options, **options)
    assert result.rdd.getNumPartitions() == partitions
    assert [configs[0].row_budget(i) for i in range(partitions)] == allowances
    assert configs[0].tunes_batches is tuning
    assert api.requests == []
    rows = result.collect()
    assert sorted(row.id for row in rows) == list(range(12))
    assert all(json.loads(row.rulebricks_result)["id"] == row.id for row in rows)
    ceiling = options.get("max_batch_rows", options.get("max_inflight_rows", 1000))
    if ceiling is not None:
        assert all(len(json.loads(request.content)) <= ceiling for request in api.requests)


@pytest.mark.parametrize("batch_options", [{}, {"max_batch_rows": 1}])
def test_row_budget_with_empty_source_and_preserved_partitioning(spark, api, batch_options):
    source = spark.createDataFrame(spark.sparkContext.emptyRDD(), "id long")
    assert source.rdd.getNumPartitions() == 0
    result = apply_flow(
        source,
        "decision",
        **api.options,
        max_inflight_rows=4,
        execution_partitions=None,
        **batch_options,
    )
    assert result.collect() == [] and api.requests == []


@pytest.mark.parametrize("downstream", ["union", "coalesce", "union_coalesce"])
@pytest.mark.parametrize("batch_options", [{}, {"max_batch_rows": 1}])
def test_row_budget_preserves_input_partition_budget_under_downstream_operators(
    spark, api, downstream, batch_options
):
    result = apply_flow(
        spark.range(12, numPartitions=2),
        "decision",
        **api.options,
        execution_partitions=2,
        max_inflight_rows=5,
        **batch_options,
    )
    if "union" in downstream:
        prefix = spark.createDataFrame([], result.schema)
        result = prefix.unionByName(result)
    if "coalesce" in downstream:
        result = result.coalesce(1)
    rows = result.collect()
    assert sorted(row.id for row in rows) == list(range(12))
    assert sorted(item["id"] for r in api.requests for item in json.loads(r.content)) == list(
        range(12)
    )
    assert max(len(json.loads(r.content)) for r in api.requests) <= 3
    assert all(row.rulebricks_meta.status == "ok" for row in rows)
    assert all(json.loads(row.rulebricks_result) == {"id": row.id} for row in rows)


@pytest.mark.parametrize("batch_options", [{}, {"max_batch_rows": 1}])
def test_concurrent_spark_timeout_infrastructure_and_terminal_errors(spark, api, batch_options):
    attempts = {}
    lock = threading.Lock()
    release_first = threading.Event()

    def handler(request):
        item = json.loads(request.content)[0]["id"]
        with lock:
            attempts[item] = attempts.get(item, 0) + 1
            attempt = attempts[item]
        if item == 0 and attempt == 1:
            assert release_first.wait(10)
        elif item == 0:
            release_first.set()
        if item == 1 and attempt == 1:
            return httpx.Response(503, text="temporarily unavailable", headers={"Retry-After": "0"})
        if item == 2:
            return httpx.Response(504, text="upstream timeout", headers={"Retry-After": "0"})
        if item == 3:
            return httpx.Response(401, text="unauthorized")
        return echo(request)

    api.handler = handler
    try:
        collected = apply_flow(
            spark.range(12, numPartitions=2),
            "decision",
            **api.options,
            execution_partitions=2,
            max_inflight_rows=4,
            **batch_options,
            max_retries=1,
            timeout_seconds=1.0,
            diagnostics=True,
        ).collect()
    finally:
        release_first.set()
    rows = {row.id: row for row in collected}
    assert len(collected) == len(rows) == 12
    assert attempts == {i: 2 if i in (0, 1, 2) else 1 for i in range(12)}
    assert rows[0].rulebricks_meta.diagnostics.attempt_history[0].error_type == "network"
    assert rows[1].rulebricks_meta.diagnostics.attempt_history[0].http_status == 503
    assert rows[2].rulebricks_meta.error.type == "infrastructure"
    assert rows[2].rulebricks_meta.error.http_status == 504
    assert rows[3].rulebricks_meta.error.type == "client"
    for i in range(12):
        if i not in (2, 3):
            assert rows[i].rulebricks_meta.status == "ok"
            assert json.loads(rows[i].rulebricks_result) == {"id": i}


def test_row_budget_keeps_batching_room_on_a_wide_spark_cluster(spark, api, monkeypatch):
    from pyspark import SparkContext

    monkeypatch.setattr(SparkContext, "defaultParallelism", property(lambda self: 64))
    result = apply_flow(
        spark.range(12, numPartitions=2), "decision", **api.options, max_inflight_rows=128
    )
    assert result.rdd.getNumPartitions() == 8
    assert api.requests == []


@pytest.mark.parametrize("batch_rows,partitions", [(4, 8), (16, 2), (32, 1), (64, 1)])
def test_manual_partition_count_follows_the_request_plan(
    spark, api, monkeypatch, batch_rows, partitions
):
    from pyspark import SparkContext

    monkeypatch.setattr(SparkContext, "defaultParallelism", property(lambda self: 64))
    result = apply_flow(
        spark.range(512, numPartitions=partitions),
        "decision",
        **api.options,
        max_inflight_rows=128,
        max_batch_rows=batch_rows,
    )
    assert result.rdd.getNumPartitions() == partitions
    assert api.requests == []
    rows = result.collect()
    assert sorted(row.id for row in rows) == list(range(512))
    assert all(row.rulebricks_meta.status == "ok" for row in rows)
    assert [len(json.loads(r.content)) for r in api.requests] == [batch_rows] * (512 // batch_rows)


@pytest.mark.parametrize("count", [512, 4096])
def test_adaptive_batching_reaches_spark_tasks_and_respects_the_row_budget(spark, api, count):
    lock = threading.Lock()
    active = peak = 0
    sizes = set()

    def handler(request):
        nonlocal active, peak
        size = len(json.loads(request.content))
        with lock:
            sizes.add(size)
            active += size
            peak = max(peak, active)
        try:
            time.sleep(0.04 if size == 8 else 0.005)
            return echo(request)
        finally:
            with lock:
                active -= size

    api.handler = handler
    rows = apply_flow(
        spark.range(count, numPartitions=1),
        "decision",
        **api.options,
        max_inflight_rows=16,
        execution_partitions=1,
    ).collect()
    assert sorted(row.id for row in rows) == list(range(count))
    assert all(row.rulebricks_meta.status == "ok" for row in rows)
    assert 8 in sizes and 16 in sizes
    assert peak <= 16


@pytest.mark.parametrize("apply", [apply_flow, apply_rule])
def test_manual_batching_reaches_spark_tasks(spark, api, apply):
    rows = apply(
        spark.range(512, numPartitions=1),
        "decision",
        **api.options,
        max_inflight_rows=16,
        execution_partitions=1,
        max_batch_rows=8,
        response_options=ResponseOptions(result_format="json"),
    ).collect()
    assert sorted(row.id for row in rows) == list(range(512))
    assert all(row.rulebricks_meta.status == "ok" for row in rows)
    assert [len(json.loads(r.content)) for r in api.requests] == [8] * 64


def test_resume_uses_a_fresh_manual_batch_plan(spark, api, tmp_path):
    api.handler = lambda request: (
        httpx.Response(503, text="unavailable")
        if json.loads(request.content)[0]["id"] < 8
        else echo(request)
    )
    path = str(tmp_path / "manual-recovery")
    apply_flow(
        spark.range(12, numPartitions=1),
        "decision",
        **api.options,
        max_inflight_rows=4,
        max_batch_rows=1,
        max_retries=0,
    ).write.parquet(path)
    saved = spark.read.parquet(path)
    initial = {row.id: row for row in saved.collect()}
    api.requests.clear()
    api.handler = echo
    rows = resume(
        saved,
        **api.options,
        max_inflight_rows=8,
        max_batch_rows=4,
        execution_partitions=1,
    ).collect()
    assert sorted(row.id for row in rows) == list(range(12))
    assert [len(json.loads(r.content)) for r in api.requests] == [4, 4]
    for row in rows:
        assert row.rulebricks_meta.status == "ok"
        if row.id >= 8:
            assert row == initial[row.id]
        else:
            assert row.rulebricks_meta.attempts == 2
