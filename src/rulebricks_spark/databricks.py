"""SQL-callable Rulebricks UDF helpers for Spark.

Databricks-first utilities for invoking Rulebricks rules directly from SQL
cells and DataFrame expressions, without calling :func:`solve` yourself.

Example
-------
>>> from rulebricks_spark.databricks import register_udf
>>> register_udf(spark, "rulebricks_triage", rule="abc123xyz9", api_key=key)
>>> # Now callable from SQL:
>>> spark.sql(
...     "SELECT *, rulebricks_triage(struct(*)) AS r FROM incidents"
... ).display()

The registered UDF accepts a single ``struct`` argument whose fields match
the rule's request schema, and returns a struct containing the rule's
response fields plus a nullable ``_rb_error`` column.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql.types import StringType, StructField, StructType

from ._config import SolveConfig
from ._schema import fetch_rule_schema, rb_types_from_spark_schema


def register_udf(
    spark,
    name: str,
    rule: str,
    *,
    api_key: str,
    base_url: str = "https://rulebricks.com/api/v1",
    output_schema: Optional[StructType] = None,
    batch_size: int = 500,
) -> StructType:
    """Register a SQL-callable Spark UDF that invokes a Rulebricks rule.

    The UDF takes a single ``struct`` argument (typically ``struct(*)`` in
    SQL) whose fields match the rule's request schema, and returns a struct
    of the rule's response fields plus a trailing ``_rb_error`` column that
    is null on success and carries the error message on failure.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    name : str
        Name to register the UDF under. Lowercase snake_case is conventional.
        This is the identifier you'll use in SQL expressions.
    rule : str
        Rulebricks rule slug (short opaque ID from the Rulebricks UI).
    api_key : str
        Rulebricks API key. In Databricks, retrieve via
        ``dbutils.secrets.get(scope, key)``.
    base_url : str, default 'https://rulebricks.com/api/v1'
        Override for self-hosted deployments. The ``/api/v1`` suffix is
        required.
    output_schema : StructType, optional
        Explicit return schema (rule outputs only — ``_rb_error`` is appended
        automatically). Omitting this triggers a single driver-side API call
        to fetch the rule's response schema.
    batch_size : int, default 500
        Rows per ``bulk_solve`` call within each Arrow batch. Capped at 1000
        by the Rulebricks API.

    Returns
    -------
    StructType
        The full return schema of the registered UDF (rule outputs +
        ``_rb_error``), so callers can see what columns the UDF produces.

    Examples
    --------
    Register and invoke from SQL:

    >>> register_udf(spark, "triage", rule="abc123xyz9", api_key=key)
    >>> spark.sql('''
    ...   SELECT
    ...     report_id,
    ...     triage.priority,
    ...     triage.risk_score,
    ...     triage._rb_error
    ...   FROM (
    ...     SELECT *, triage(struct(*)) AS triage FROM incidents
    ...   )
    ... ''').display()

    Self-hosted Rulebricks with explicit schema (skips inference):

    >>> from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    >>> schema = StructType([
    ...     StructField("priority", StringType()),
    ...     StructField("risk_score", DoubleType()),
    ... ])
    >>> register_udf(
    ...     spark, "triage", rule="abc123xyz9",
    ...     api_key=key, base_url="https://rulebricks.acme.internal",
    ...     output_schema=schema,
    ... )
    """
    _validate_register_args(name=name, rule=rule, api_key=api_key, batch_size=batch_size)

    if output_schema is None:
        rule_output_schema, rb_type_pairs = fetch_rule_schema(rule, api_key, base_url)
    else:
        rule_output_schema = output_schema
        rb_type_pairs = rb_types_from_spark_schema(output_schema)

    return_schema = StructType(
        list(rule_output_schema.fields)
        + [StructField("_rb_error", StringType(), nullable=True)]
    )

    config = SolveConfig(
        rule_slug=rule,
        api_key=api_key,
        base_url=base_url,
        batch_size=batch_size,
        max_concurrent=1,
        input_mapping={},
        on_missing_input="default",
        on_error="continue",
        timeout_seconds=60.0,
        max_retries=3,
        output_columns=rb_type_pairs,
        input_columns=[],
    )

    udf = _build_pandas_udf(config, return_schema, rb_type_pairs)
    spark.udf.register(name, udf)
    return return_schema


def _validate_register_args(name: str, rule: str, api_key: str, batch_size: int) -> None:
    if not name or not isinstance(name, str):
        raise ValueError(f"name must be a non-empty string (got {name!r}).")
    if not rule or not isinstance(rule, str):
        raise ValueError(f"rule must be a non-empty string (got {rule!r}).")
    if not api_key or not isinstance(api_key, str):
        raise ValueError("api_key must be a non-empty string.")
    if not 1 <= batch_size <= 1000:
        raise ValueError(
            f"batch_size must be between 1 and 1000 (got {batch_size}). "
            f"Rulebricks bulk_solve hard-caps at 1000 rows per call."
        )


def _build_pandas_udf(config: SolveConfig, return_schema: StructType, rb_type_pairs):
    """Construct a pandas_udf closed over the resolved rule configuration."""
    from pyspark.sql.functions import pandas_udf

    from ._partition import (
        _build_payloads,
        _bulk_solve_with_retry,
        _coerce_output,
        _format_error,
        _make_executor_client,
        _normalize_response,
    )

    output_names = [name for name, _ in rb_type_pairs]

    @pandas_udf(return_schema)
    def _rulebricks_udf(inputs):
        import pandas as pd

        if len(inputs) == 0:
            empty = {name: [] for name in output_names}
            empty["_rb_error"] = []
            return pd.DataFrame(empty)

        n = len(inputs)
        payloads = _build_payloads(inputs, config)

        output_data = {name: [None] * n for name in output_names}
        errors = [None] * n

        client = _make_executor_client(config)

        for start in range(0, n, config.batch_size):
            batch = payloads[start : start + config.batch_size]
            batch_len = len(batch)
            try:
                response = _bulk_solve_with_retry(client, config, batch)
                results = _normalize_response(response)
            except Exception as exc:
                err = _format_error(exc)
                for j in range(batch_len):
                    errors[start + j] = err
                continue

            for j in range(batch_len):
                if j < len(results) and isinstance(results[j], dict):
                    row = results[j]
                    row_error = row.get("error") or row.get("_error")
                    if row_error:
                        errors[start + j] = str(row_error)
                    for field_name, rb_type in rb_type_pairs:
                        output_data[field_name][start + j] = _coerce_output(
                            row.get(field_name), rb_type
                        )
                else:
                    errors[start + j] = "missing_response_for_row"

        output_data["_rb_error"] = errors
        return pd.DataFrame(output_data)

    return _rulebricks_udf
