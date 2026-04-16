"""Public ``solve()`` entry point for applying Rulebricks rules to Spark DataFrames."""

from __future__ import annotations

from typing import Dict, Literal, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ._config import SolveConfig
from ._partition import make_partition_worker
from ._schema import (
    build_output_schema,
    fetch_rule_schema,
    rb_types_from_spark_schema,
)
from .exceptions import SchemaConflictError

OnConflict = Literal["error", "overwrite"]
OnMissingInput = Literal["error", "default"]
OnError = Literal["raise", "continue"]


def solve(
    df: DataFrame,
    rule: str,
    *,
    api_key: str,
    base_url: str = "https://rulebricks.com",
    batch_size: int = 500,
    max_concurrent_requests_per_partition: int = 4,
    input_mapping: Optional[Dict[str, str]] = None,
    output_schema: Optional[StructType] = None,
    on_conflict: OnConflict = "overwrite",
    on_missing_input: OnMissingInput = "default",
    on_error: OnError = "continue",
    auto_repartition: bool = False,
    timeout_seconds: float = 60.0,
    max_retries: int = 3,
    _client_class=None,
) -> DataFrame:
    """Apply a Rulebricks rule to every row of a Spark DataFrame.

    Each row is converted to a JSON payload and sent to the rule's
    ``bulk_solve`` endpoint in batches. Rule output fields are appended to the
    DataFrame as new columns. Errors are isolated per-row in a trailing
    ``_rb_error`` column (null on success).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame. Column names must match the rule's request fields,
        or be remapped via ``input_mapping``.
    rule : str
        Slug of the Rulebricks rule to apply.
    api_key : str
        Rulebricks API key. In Databricks, retrieve via
        ``dbutils.secrets.get(scope, key)``.
    base_url : str, default 'https://rulebricks.com'
        API base URL. Override for self-hosted deployments, e.g.
        ``https://rulebricks.yourcompany.com``.
    batch_size : int, default 500
        Rows per ``bulk_solve`` HTTP call. Hard-capped at 1000 by the API.
        Larger batches improve throughput until rule complexity or payload
        size dominates.
    max_concurrent_requests_per_partition : int, default 4
        How many ``bulk_solve`` calls to run in parallel *within* each Spark
        partition. Effective parallelism = executors × this value.
    input_mapping : dict, optional
        ``{dataframe_column: rule_field_name}`` mapping for columns whose
        names differ from the rule's request schema. Unmapped columns are
        passed through with their original names.
    output_schema : StructType, optional
        Explicit Spark schema for rule outputs. If omitted, the schema is
        inferred from the rule's definition via the Rulebricks workspace API.
        Provide this to get typed ``ArrayType`` / ``StructType`` outputs
        instead of the default JSON-encoded strings.
    on_conflict : 'error' | 'overwrite', default 'overwrite'
        What to do when rule output field names collide with input columns.
        ``'overwrite'`` replaces input columns with the rule's output.
    on_missing_input : 'error' | 'default', default 'default'
        How to handle rule input fields that aren't present in the
        DataFrame. ``'default'`` lets Rulebricks apply the rule's defined
        default values; ``'error'`` currently has no enforcement beyond
        relying on the API to reject the request.
    on_error : 'raise' | 'continue', default 'continue'
        ``'continue'`` writes the error message into ``_rb_error`` and
        leaves rule output columns null for the failed rows. ``'raise'``
        fails the Spark job on the first batch failure.
    auto_repartition : bool, default False
        If True, repartition a poorly-partitioned DataFrame (e.g. a single
        giant partition) before processing.
    timeout_seconds : float, default 60.0
        Passed through for future transport-level enforcement; the bundled
        SDK applies its own default timeout today.
    max_retries : int, default 3
        Retry attempts per batch on retryable failures (HTTP 408/429/5xx,
        timeouts, connection resets). Exponential backoff, capped at 30s.

    Returns
    -------
    pyspark.sql.DataFrame
        Input columns (minus collisions under ``on_conflict='overwrite'``),
        then rule output columns, then ``_rb_error``.

    Examples
    --------
    >>> from rulebricks_spark import solve
    >>> scored = solve(
    ...     spark.table("breach_reports"),
    ...     rule="breach-triage",
    ...     api_key=dbutils.secrets.get("rulebricks", "api_key"),
    ... )
    >>> scored.filter("_rb_error IS NULL").display()

    Rename input columns to match the rule schema:

    >>> solve(df, rule="eligibility",
    ...       api_key=key,
    ...       input_mapping={"fin_impact": "financial_impact"})

    Self-hosted Rulebricks with tuned concurrency:

    >>> solve(df, rule="underwriting",
    ...       api_key=key,
    ...       base_url="https://rules.acme.internal",
    ...       batch_size=1000,
    ...       max_concurrent_requests_per_partition=8)
    """
    _validate_args(
        batch_size=batch_size,
        max_concurrent=max_concurrent_requests_per_partition,
        on_conflict=on_conflict,
        on_missing_input=on_missing_input,
        on_error=on_error,
    )

    # Resolve output schema: explicit > inferred.
    if output_schema is None:
        rule_output_schema, rb_type_pairs = fetch_rule_schema(rule, api_key, base_url)
    else:
        rule_output_schema = output_schema
        rb_type_pairs = rb_types_from_spark_schema(output_schema)

    # Detect collisions.
    input_cols = set(df.columns)
    output_cols = {f.name for f in rule_output_schema.fields}
    collisions = input_cols & output_cols

    if collisions and on_conflict == "error":
        raise SchemaConflictError(
            f"Rule output columns collide with DataFrame columns: "
            f"{sorted(collisions)}. Set on_conflict='overwrite' to replace "
            f"them, or use input_mapping to rename inputs."
        )

    # Validate input_mapping references real DataFrame columns.
    if input_mapping:
        missing = set(input_mapping.keys()) - input_cols
        if missing:
            raise ValueError(
                f"input_mapping references DataFrame columns that don't exist: "
                f"{sorted(missing)}. Available columns: {sorted(input_cols)}"
            )

    full_output_schema = build_output_schema(
        input_schema=df.schema,
        rule_output_schema=rule_output_schema,
    )

    if auto_repartition:
        df = _maybe_repartition(df)

    config = SolveConfig(
        rule_slug=rule,
        api_key=api_key,
        base_url=base_url,
        batch_size=batch_size,
        max_concurrent=max_concurrent_requests_per_partition,
        input_mapping=input_mapping or {},
        on_missing_input=on_missing_input,
        on_error=on_error,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        output_columns=rb_type_pairs,
        input_columns=list(df.columns),
        _client_class=_client_class,
    )

    worker = make_partition_worker(config)
    return df.mapInPandas(worker, schema=full_output_schema)


# --- validation & helpers ----------------------------------------------------


def _validate_args(
    batch_size: int,
    max_concurrent: int,
    on_conflict: str,
    on_missing_input: str,
    on_error: str,
) -> None:
    if not 1 <= batch_size <= 1000:
        raise ValueError(
            f"batch_size must be between 1 and 1000 (got {batch_size}). "
            f"Rulebricks bulk_solve hard-caps at 1000 rows per call."
        )
    if max_concurrent < 1:
        raise ValueError(
            f"max_concurrent_requests_per_partition must be >= 1 "
            f"(got {max_concurrent})."
        )
    if on_conflict not in ("error", "overwrite"):
        raise ValueError(
            f"on_conflict must be 'error' or 'overwrite' (got {on_conflict!r})."
        )
    if on_missing_input not in ("error", "default"):
        raise ValueError(
            f"on_missing_input must be 'error' or 'default' (got {on_missing_input!r})."
        )
    if on_error not in ("raise", "continue"):
        raise ValueError(
            f"on_error must be 'raise' or 'continue' (got {on_error!r})."
        )


def _maybe_repartition(df: DataFrame) -> DataFrame:
    """Cheaply repartition DataFrames that are pathologically partitioned.

    We avoid ``.count()`` or other shuffles here — the heuristic looks only at
    the current partition count, so the cost is a metadata access.
    """
    current = df.rdd.getNumPartitions()
    if current == 1:
        spark = df.sparkSession
        target = max(spark.sparkContext.defaultParallelism, 8)
        return df.repartition(target)
    if current > 10_000:
        return df.coalesce(1000)
    return df
