"""End-to-end tests with a real local Spark session and fake Rulebricks clients.

Uses the ``_client_class`` test hook on ``solve()`` to inject a deterministic
in-process client that runs on Spark executors without network calls. Fake
clients are defined at **module level** so they are picklable across worker
processes.

Marked ``requires_spark`` — skipped automatically if PySpark is unavailable.
"""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

pytestmark = pytest.mark.requires_spark


# ---------------------------------------------------------------------------
# Module-level fake clients (must be picklable for Spark workers)
# ---------------------------------------------------------------------------

class _FakeRulesAPI:
    """Deterministic bulk_solve: P1 if financial_impact > 1M, else P3."""

    def bulk_solve(self, slug, request):
        return [
            {
                "priority": "P1" if row.get("financial_impact", 0) > 1_000_000 else "P3",
                "reportable": bool(row.get("customer_harm", False)),
                "risk_score": float(row.get("financial_impact", 0)) / 1_000_000.0,
            }
            for row in request
        ]


class FakeClient:
    def __init__(self, api_key=None, **kwargs):
        self.rules = _FakeRulesAPI()


class _FailingRulesAPI:
    def bulk_solve(self, slug, request):
        raise RuntimeError("simulated API failure")


class FailingClient:
    def __init__(self, api_key=None, **kwargs):
        self.rules = _FailingRulesAPI()


# Track batch call counts via a module-level list shared in local mode.
_batch_log = []


class _TrackingRulesAPI:
    def bulk_solve(self, slug, request):
        _batch_log.append(len(request))
        return [
            {
                "priority": "P3",
                "reportable": False,
                "risk_score": 0.0,
            }
            for _ in request
        ]


class TrackingClient:
    def __init__(self, api_key=None, **kwargs):
        self.rules = _TrackingRulesAPI()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("rulebricks-spark-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def rule_schema():
    return StructType([
        StructField("priority", StringType(), nullable=True),
        StructField("reportable", BooleanType(), nullable=True),
        StructField("risk_score", DoubleType(), nullable=True),
    ])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSolveEndToEnd:
    def test_basic_solve_returns_expected_columns(self, spark, rule_schema):
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [
                ("R1", 500_000, False),
                ("R2", 2_000_000, True),
                ("R3", 1_500_000, True),
            ],
            schema=["report_id", "financial_impact", "customer_harm"],
        )

        result = solve(
            df,
            rule="breach-triage",
            api_key="test-key",
            output_schema=rule_schema,
            batch_size=10,
            _client_class=FakeClient,
        )

        rows = result.orderBy("report_id").collect()
        assert [r.report_id for r in rows] == ["R1", "R2", "R3"]
        assert [r.priority for r in rows] == ["P3", "P1", "P1"]
        assert [r.reportable for r in rows] == [False, True, True]
        assert [r._rb_error for r in rows] == [None, None, None]

    def test_rb_error_column_null_on_success(self, spark, rule_schema):
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [("R1", 1_000_000, True)],
            schema=["report_id", "financial_impact", "customer_harm"],
        )
        result = solve(
            df, rule="triage", api_key="k",
            output_schema=rule_schema,
            _client_class=FakeClient,
        )
        assert all(r._rb_error is None for r in result.collect())

    def test_batching_handles_all_rows(self, spark, rule_schema):
        """Verify all 25 rows are processed correctly regardless of batch chunking."""
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [("R" + str(i), i * 100_000, i % 2 == 0) for i in range(25)],
            schema=["report_id", "financial_impact", "customer_harm"],
        ).repartition(1)

        result = solve(
            df,
            rule="triage",
            api_key="k",
            output_schema=rule_schema,
            batch_size=10,
            max_concurrent_requests_per_partition=1,
            _client_class=FakeClient,
        )

        rows = result.collect()
        # All 25 rows processed with no errors.
        assert len(rows) == 25
        assert all(r._rb_error is None for r in rows)
        assert all(r.priority in ("P1", "P3") for r in rows)

    def test_collision_overwrite_default(self, spark, rule_schema):
        """If input has a 'priority' column, rule output replaces it."""
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [("R1", 2_000_000, True, "UNKNOWN")],
            schema=["report_id", "financial_impact", "customer_harm", "priority"],
        )
        result = solve(
            df, rule="triage", api_key="k",
            output_schema=rule_schema,
            _client_class=FakeClient,
        )
        row = result.collect()[0]
        assert row.priority == "P1"  # Rule output, not the stale "UNKNOWN".

    def test_collision_error_raises(self, spark, rule_schema):
        from rulebricks_spark import solve
        from rulebricks_spark.exceptions import SchemaConflictError

        df = spark.createDataFrame(
            [("R1", 1_000_000, True, "UNKNOWN")],
            schema=["report_id", "financial_impact", "customer_harm", "priority"],
        )
        with pytest.raises(SchemaConflictError, match="priority"):
            solve(
                df,
                rule="triage",
                api_key="k",
                output_schema=rule_schema,
                on_conflict="error",
                _client_class=FakeClient,
            )

    def test_input_mapping_renames_fields(self, spark, rule_schema):
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [("R1", 2_000_000, True)],
            schema=["report_id", "fin_impact", "harm"],
        )
        result = solve(
            df,
            rule="triage",
            api_key="k",
            output_schema=rule_schema,
            input_mapping={"fin_impact": "financial_impact", "harm": "customer_harm"},
            _client_class=FakeClient,
        )
        row = result.collect()[0]
        assert row.priority == "P1"

    def test_invalid_input_mapping_raises(self, spark, rule_schema):
        from rulebricks_spark import solve

        df = spark.createDataFrame([("R1",)], schema=["report_id"])
        with pytest.raises(ValueError, match="input_mapping"):
            solve(
                df,
                rule="triage",
                api_key="k",
                output_schema=rule_schema,
                input_mapping={"nonexistent_column": "something"},
                _client_class=FakeClient,
            )


class TestErrorHandling:
    def test_bulk_solve_failure_continues(self, spark, rule_schema):
        """With on_error='continue', batch failures populate _rb_error."""
        from rulebricks_spark import solve

        df = spark.createDataFrame(
            [("R1", 1, False), ("R2", 2, True)],
            schema=["report_id", "financial_impact", "customer_harm"],
        )
        result = solve(
            df,
            rule="triage",
            api_key="k",
            output_schema=rule_schema,
            on_error="continue",
            max_retries=0,
            _client_class=FailingClient,
        )
        rows = result.collect()
        assert all(r._rb_error is not None for r in rows)
        assert all("simulated API failure" in r._rb_error for r in rows)
        assert all(r.priority is None for r in rows)
