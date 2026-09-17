"""Public API through Spark workers, the generated SDK, and a local HTTP server."""

import json
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
