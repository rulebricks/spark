"""Context operations through Spark, the generated SDK, and HTTP receipts."""

import json

import httpx
import pytest

from rulebricks_spark import ResponseOptions, contexts, resume

pytestmark = pytest.mark.requires_spark


def test_ingest_alignment_order_and_execution_outcomes(spark, api):
    def ingest(request):
        payload = json.loads(request.content)
        assert payload == [
            {"customer_id": "a", "amount": 1},
            {"customer_id": "a", "amount": 2},
            {"customer_id": "b", "amount": -1},
            {"customer_id": "c", "amount": 3},
            {"customer_id": "d", "amount": 4},
        ]
        assert {"positions", "executed", "status", "state"} <= set(
            request.url.params["include"].split(",")
        )
        return httpx.Response(
            200,
            json={
                "context": "customer",
                "accepted": 3,
                "rejected": 1,
                "execution_degraded": "One or more Context decisions failed",
                "results": [
                    {
                        "instance_id": "d",
                        "positions": [4],
                        "status": "complete",
                        "executed": [
                            {
                                "type": "flow",
                                "slug": "decision",
                                "status": "evaluation_error",
                                "error": "bad calculation",
                                "execution_id": "flow-d",
                            }
                        ],
                    },
                    {
                        "instance_id": "a",
                        "positions": [0, 1],
                        "status": "complete",
                        "state": {"amount": 2},
                        "executed": [
                            {
                                "type": "flow",
                                "slug": "decision",
                                "status": "success",
                                "execution_id": "flow-a",
                            }
                        ],
                    },
                    {
                        "instance_id": "c",
                        "positions": [3],
                        "status": "pending",
                        "need": ["country"],
                        "executed": [],
                    },
                ],
                "rejections": [{"position": 2, "error": "invalid amount"}],
            },
        )

    api.handler = ingest
    source = spark.createDataFrame(
        [
            ("a", 2, 2, "second"),
            ("d", 1, 4, "failed"),
            ("b", 1, -1, "rejected"),
            ("c", 1, 3, "waiting"),
            ("a", 1, 1, "first"),
        ],
        ["id", "sequence", "amount", "source_note"],
    )
    result = contexts.ingest(
        source,
        "customer",
        key="id",
        **api.options,
        input_columns=["id", "amount"],
        input_mapping={"id": "customer_id"},
        order_by=["sequence"],
        include="state",
    )
    assert api.requests == []
    rows = result.collect()
    assert len(api.requests) == 1 and api.requests[0].url.path == "/api/v1/contexts/batch/customer"
    assert [r.rulebricks_meta.status for r in rows] == ["ok", "ok", "error", "pending", "error"]
    assert [r.rulebricks_meta.write_status for r in rows] == [
        "committed",
        "committed",
        "rejected",
        "committed",
        "committed",
    ]
    assert rows[0].source_note == "first" and rows[1].source_note == "second"
    assert json.loads(rows[0].rulebricks_result)["state"] == {"amount": 2}
    assert json.loads(rows[1].rulebricks_result)["executed"][0]["execution_id"] == "flow-a"
    assert rows[3].rulebricks_meta.error is None
    assert rows[4].rulebricks_meta.error.type == "context_execution"
    assert all(r.rulebricks_meta.version is None and r.rulebricks_meta.attempts == 1 for r in rows)
    saved = spark.createDataFrame(rows, result.schema)
    with pytest.raises(ValueError, match="Reconcile context state"):
        resume(saved, **api.options)


@pytest.mark.parametrize("status", [413, 429, 503])
def test_partial_commits_are_retained_without_replay(spark, api, status):
    api.handler = lambda request: httpx.Response(
        status,
        json={
            "error": "write interrupted",
            "committed_instance_ids": ["a"],
            "failed_instance_ids": ["b"],
            "committed_count": 1,
        },
    )
    source = spark.createDataFrame([("a",), ("b",)], ["id"])
    rows = contexts.ingest(source, "customer", key="id", **api.options).collect()
    assert len(api.requests) == 1
    by_key = {r.id: r for r in rows}
    assert by_key["a"].rulebricks_meta.write_status == "committed"
    assert by_key["b"].rulebricks_meta.write_status == "unknown"
    assert all(r.rulebricks_meta.error.type == "context_write" for r in rows)
    assert json.loads(rows[0].rulebricks_result)["request_error"]["failed_instance_ids"] == ["b"]
    api.requests.clear()
    rows = contexts.ingest(
        source, "customer", key="id", **api.options, order_by=["id"], max_batch_rows=1
    ).collect()
    assert len(api.requests) == 1
    assert rows[1].rulebricks_meta.write_status == "not_sent"
    assert rows[1].rulebricks_meta.error.type == "context_blocked"
    assert rows[1].rulebricks_meta.attempts == 0

    api.requests.clear()
    rows = contexts.apply_flow(
        source, "decision", key="id", order_by=["id"], **api.options
    ).collect()
    assert len(api.requests) == 1
    assert rows[1].rulebricks_meta.attempts == 0
    assert rows[1].rulebricks_meta.error.type == "context_blocked"


@pytest.mark.parametrize("positions", [[0], [0, 0], [0, 2]])
def test_incomplete_or_ambiguous_receipt_does_not_invent_success(spark, api, positions):
    api.handler = lambda request: httpx.Response(
        200,
        json={
            "results": [
                {"instance_id": "a", "positions": positions, "status": "complete", "executed": []}
            ],
            "rejections": [],
        },
    )
    source = spark.createDataFrame([("a",), ("b",)], ["id"])
    rows = contexts.ingest(source, "customer", key="id", **api.options).collect()
    assert len(api.requests) == 1
    assert all(
        r.rulebricks_meta.error.type == "protocol" and r.rulebricks_meta.write_status == "unknown"
        for r in rows
    )
    assert all(json.loads(r.rulebricks_result)["response"]["results"] for r in rows)


def test_lookup_deduplicates_keys_and_preserves_source_rows(spark, api):
    def lookup(request):
        if request.url.path.endswith("/missing"):
            return httpx.Response(404, json={"error": "not found"})
        assert request.url.path.endswith("/a/?#")
        return httpx.Response(
            200,
            json={
                "context": "customer:a",
                "status": "pending",
                "need": ["country"],
                "state": {"visits": 2},
                "derived": {"score": 4},
            },
        )

    api.handler = lookup
    source = spark.createDataFrame(
        [("a/?#", 1), ("a/?#", 2), ("missing", 3), (None, 4)], ["customer.id", "row"]
    )
    result = contexts.lookup(source, "customer", key="customer.id", **api.options)
    assert api.requests == []
    rows = sorted(result.collect(), key=lambda r: r.row)
    assert len(api.requests) == 2 and all(r.method == "GET" for r in api.requests)
    assert [r.rulebricks_meta.status for r in rows] == ["pending", "pending", "error", "error"]
    assert json.loads(rows[0].rulebricks_result)["derived"] == {"score": 4}
    assert rows[2].rulebricks_meta.error.type == "not_found"
    assert rows[3].rulebricks_meta.attempts == 0


def test_context_rejects_oversize_record_before_writing(spark, api):
    source = spark.createDataFrame([("a", "x" * 100)], ["id", "value"])
    rows = contexts.ingest(
        source, "customer", key="id", **api.options, max_batch_bytes=30
    ).collect()
    assert api.requests == []
    assert rows[0].rulebricks_meta.write_status == "not_sent"
    assert "max_batch_bytes" in rows[0].rulebricks_meta.error.message
    assert rows[0].rulebricks_meta.correlation_id == "a"
    source = spark.createDataFrame([(2**53 + 1,)], ["id"])
    rows = contexts.ingest(source, "customer", key="id", **api.options).collect()
    assert api.requests == []
    assert rows[0].rulebricks_meta.write_status == "not_sent"
    assert rows[0].rulebricks_meta.correlation_id == str(2**53 + 1)


def test_setup_creates_reuses_and_rejects_conflicting_definitions(spark, api, monkeypatch):

    definitions = []
    client_type = httpx.Client
    client_options = []

    def capture_client(**kwargs):
        client_options.append(dict(kwargs))
        return client_type(**kwargs)

    monkeypatch.setattr(contexts.httpx, "Client", capture_client)

    def setup(request):
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["auto_execute_decisions"] is False
            assert body["on_schema_mismatch"] == "reject"
            body["schema"]["base"][0]["required"] = True
            body.update(id="context-id", slug="customer-actual")
            definitions.append(body)
            return httpx.Response(201, json=body)
        if request.url.path.endswith("/context-id"):
            return httpx.Response(200, json=definitions[0])
        assert request.url.params["name"] == "Customer state"
        return httpx.Response(200, json={"data": definitions, "cursor": None})

    api.handler = setup
    source = spark.createDataFrame([("a", 1)], ["id", "balance"])
    options = {**api.options, "request_options": {"verify": False}}
    slug = contexts.ensure("Customer state", schema=source.schema, key="id", **options)
    assert slug == "customer-actual"
    assert contexts.ensure("Customer state", schema=source.schema, key="id", **options) == slug
    assert sum(r.method == "POST" for r in api.requests) == 1
    definitions[0]["schema"]["base"][1]["type"] = "string"
    with pytest.raises(ValueError, match=r"schema.base.balance"):
        contexts.ensure("Customer state", schema=source.schema, key="id", **options)
    definitions.append(dict(definitions[0], id="other"))
    with pytest.raises(ValueError, match="Multiple contexts"):
        contexts.ensure("Customer state", schema=source.schema, key="id", **options)
    assert sum(r.method == "POST" for r in api.requests) == 1
    assert all(options["verify"] is False for options in client_options)


def test_setup_checks_all_name_matches_before_creating(api):
    from pyspark.sql.types import StructType

    def listing(request):
        assert request.method == "GET"
        if "cursor" not in request.url.params:
            return httpx.Response(
                200, json={"data": [{"id": "one", "name": "State"}], "cursor": "next"}
            )
        return httpx.Response(200, json={"data": [{"id": "two", "name": "State"}], "cursor": None})

    api.handler = listing
    with pytest.raises(ValueError, match="Multiple contexts"):
        contexts.ensure("State", schema=StructType().add("id", "string"), key="id", **api.options)
    assert len(api.requests) == 2


def test_stateful_events_wait_for_flow_and_preserve_independent_keys(spark, api):

    balances = {}

    def advance(request):
        [event] = json.loads(request.content)
        key, seq = event["account"], event["sequence"]
        assert request.url.path == "/api/v1/flows/decision/3"
        if key == "a":
            assert seq == balances.get(key, 0) + 1
            balances[key] = seq
        if key == "b":
            assert seq == 1
            body = {"error": "Calculation failed"}
        elif key in ("c", "d"):
            assert seq == 1
            body = {"failure": "Missing country"}
        else:
            body = {"balance": balances.get(key, 0)}
        return httpx.Response(200, json=[body])

    def failure(item):
        return item.get("error") or item.get("failure")

    api.handler = advance
    source = spark.createDataFrame(
        [(key, seq) for key in ["e/?#", "d", "c", "b", "a"] for seq in [2, 1]],
        ["account", "sequence"],
    )
    result = contexts.apply_flow(
        source,
        "decision",
        version="3",
        key="account",
        order_by=["sequence"],
        response_options=ResponseOptions(error_handler=failure),
        **api.options,
    )
    assert api.requests == []
    rows = result.collect()
    assert len(api.requests) == 7
    by_event = {(row.account, row.sequence): row for row in rows}
    assert [json.loads(by_event["a", seq].rulebricks_result)["balance"] for seq in (1, 2)] == [1, 2]
    for key in ("b", "c", "d"):
        assert by_event[key, 2].rulebricks_meta.error.type == "context_blocked"
        assert by_event[key, 2].rulebricks_meta.attempts == 0
    assert by_event["b", 1].rulebricks_meta.error.type == "execution"
    assert by_event["c", 1].rulebricks_meta.status == "error"
    assert by_event["b", 1].rulebricks_meta.execution_id
    assert by_event["b", 1].rulebricks_meta.item_execution_id
    assert by_event["e/?#", 2].rulebricks_meta.status == "ok"
    assert all(
        row.rulebricks_meta.asset_type == "flow" and row.rulebricks_meta.version == "3"
        for row in rows
    )
    with pytest.raises(ValueError, match="stateful flow effects"):
        resume(spark.createDataFrame(rows, result.schema), **api.options)


def test_stateful_local_rejection_blocks_later_events_for_that_key(spark, api):

    source = spark.createDataFrame(
        [("a", 1, "x" * 100), ("a", 2, "ok")], ["id", "sequence", "value"]
    )
    rows = contexts.apply_flow(
        source,
        "decision",
        key="id",
        order_by=["sequence"],
        max_batch_bytes=80,
        **api.options,
    ).collect()
    assert api.requests == []
    assert all(row.rulebricks_meta.attempts == 0 for row in rows)
    assert rows[1].rulebricks_meta.error.type == "context_blocked"
