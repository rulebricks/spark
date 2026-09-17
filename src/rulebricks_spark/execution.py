"""Spark transformations for Rulebricks rules and flows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from ._apply import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_ROWS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    apply_dataframe,
)
from .responses import ResponseOptions


def apply_rule(
    df: DataFrame,
    rule: str,
    *,
    api_key: str,
    base_url: str,
    version: str = "latest",
    input_columns: Optional[Sequence[str]] = None,
    payload_column: Optional[str] = None,
    input_mapping: Optional[Mapping[str, str]] = None,
    correlation_id_path: Optional[str] = None,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_batch_rows: Optional[int] = DEFAULT_MAX_BATCH_ROWS,
    execution_partitions: Optional[int] = 1,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    output_schema: Optional[DataType] = None,
    result_column: str = "rulebricks_result",
    metadata_column: str = "rulebricks_meta",
    diagnostics: bool = False,
    on_error: str = "continue",
    response_options: Optional[ResponseOptions] = None,
    request_options: Optional[Mapping[str, Any]] = None,
    isolate_failures: bool = True,
    input_validation: str = "none",
) -> DataFrame:
    """Apply a Rulebricks rule to every row of a Spark DataFrame.

    Rule outputs are returned in one result column without overwriting source
    columns. Auto mode infers the selected version's output schema.
    JSON mode skips output inference.

    Parameters
    ----------
    df:
        Source Spark DataFrame.
    rule:
        Rule slug.
    api_key:
        Rulebricks API key.
    base_url:
        Rulebricks API base URL, including ``/api/v1``.
    version:
        Published version number, release environment, or ``"latest"``.
    input_columns:
        Source columns to include in each request. Defaults to all columns.
    payload_column:
        Struct or JSON-string column containing the complete request object.
    input_mapping:
        Mapping of source column names to Rulebricks request field names.
    correlation_id_path:
        Dot path copied from the payload into metadata for correlation.
    max_batch_bytes:
        Maximum encoded request body size.
    max_batch_rows:
        Maximum rows in one Rulebricks request.
    execution_partitions:
        Spark partitions used for execution. One request is active per task.
    max_retries:
        Inline retries for transient request failures.
    timeout_seconds:
        Timeout for one synchronous Rulebricks request.
    output_schema:
        Explicit typed output schema, overriding selected-version inference.
    result_column:
        Name of the appended typed result or JSON column.
    metadata_column:
        Name of the appended execution metadata struct.
    diagnostics:
        Include chunk and attempt-history diagnostics in metadata.
    on_error:
        ``"continue"`` returns row-level errors; ``"raise"`` fails the task.
    response_options:
        Raw JSON, schema mismatch handling, original responses, and executor
        callbacks. See :class:`rulebricks_spark.ResponseOptions`.
    request_options:
        SDK additional_headers and additional_query_parameters, plus the Spark
        transport's boolean verify setting for TLS certificates. Credentials and
        request options are never stored in result schema metadata.
    isolate_failures:
        Bisect failed HTTP 500 requests after retries. Disable for flows whose
        side effects make additional executions undesirable.
    input_validation:
        ``"none"`` (default), ``"strict"``, or ``"warn"``. Preflight compares
        declared inputs with the prepared Spark schema, without reading rows.
        Strict requires every declared field, including fields with defaults.
    """

    return apply_dataframe(
        df,
        asset_type="rule",
        slug=rule,
        api_key=api_key,
        base_url=base_url,
        version=version,
        input_columns=input_columns,
        payload_column=payload_column,
        input_mapping=input_mapping,
        correlation_id_path=correlation_id_path,
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        execution_partitions=execution_partitions,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        output_schema=output_schema,
        result_column=result_column,
        metadata_column=metadata_column,
        diagnostics=diagnostics,
        on_error=on_error,
        response_options=response_options,
        request_options=request_options,
        isolate_failures=isolate_failures,
        input_validation=input_validation,
    )


def apply_flow(
    df: DataFrame,
    flow: str,
    *,
    api_key: str,
    base_url: str,
    version: str = "latest",
    input_columns: Optional[Sequence[str]] = None,
    payload_column: Optional[str] = None,
    input_mapping: Optional[Mapping[str, str]] = None,
    correlation_id_path: Optional[str] = None,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_batch_rows: Optional[int] = DEFAULT_MAX_BATCH_ROWS,
    execution_partitions: Optional[int] = 1,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    output_schema: Optional[DataType] = None,
    result_column: str = "rulebricks_result",
    metadata_column: str = "rulebricks_meta",
    diagnostics: bool = False,
    on_error: str = "continue",
    response_options: Optional[ResponseOptions] = None,
    request_options: Optional[Mapping[str, Any]] = None,
    isolate_failures: bool = True,
    input_validation: str = "none",
) -> DataFrame:
    """Apply a flow, preserving source columns and appending results and metadata.

    Results are JSON unless ``output_schema`` is supplied. All other options
    match :func:`apply_rule`. Execution is lazy, with one active HTTP request
    per Spark task.
    """

    return apply_dataframe(
        df,
        asset_type="flow",
        slug=flow,
        api_key=api_key,
        base_url=base_url,
        version=version,
        input_columns=input_columns,
        payload_column=payload_column,
        input_mapping=input_mapping,
        correlation_id_path=correlation_id_path,
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        execution_partitions=execution_partitions,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        output_schema=output_schema,
        result_column=result_column,
        metadata_column=metadata_column,
        diagnostics=diagnostics,
        on_error=on_error,
        response_options=response_options,
        request_options=request_options,
        isolate_failures=isolate_failures,
        input_validation=input_validation,
    )
