"""Shared driver-side preparation for Rulebricks Spark transformations."""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Dict, Optional

from pyspark.sql import Column, DataFrame, functions
from pyspark.sql.types import (
    ArrayType,
    DataType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from ._config import DEFAULT_MAX_BATCH_BYTES as DEFAULT_MAX_BATCH_BYTES
from ._config import DEFAULT_MAX_BATCH_ROWS as DEFAULT_MAX_BATCH_ROWS
from ._config import RunConfig, initial_batch_rows
from ._engine import make_partition_worker
from ._metadata import RB_PREFIX, build_execution_schema
from ._schema import _load_asset, _rule_output_schema, _validate_input_schema
from ._transport import _split_request_options
from .responses import ResponseOptions

DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_MAX_RETRIES = 3


class _DefaultExecutionPartitions(int):
    """Distinguish an omitted partition count from an explicit 1.

    The int value preserves the public signature's historical default. Only
    callers opting into a concurrency budget get automatic partition planning.
    """


DEFAULT_EXECUTION_PARTITIONS = _DefaultExecutionPartitions(1)

CONTRACT_METADATA_KEY = "rulebricks_contract"

ATTEMPT_HISTORY_SCHEMA = ArrayType(
    StructType(
        [
            StructField("at", StringType(), nullable=True),
            StructField("http_status", IntegerType(), nullable=True),
            StructField("error_type", StringType(), nullable=True),
            StructField("error", StringType(), nullable=True),
            StructField("trace_id", StringType(), nullable=True),
            StructField("execution_id", StringType(), nullable=True),
            StructField("chunk_id", StringType(), nullable=True),
        ]
    )
)


def apply_dataframe(
    df: DataFrame,
    *,
    asset_type: str,
    slug: str,
    api_key: str,
    base_url: str,
    version: str,
    input_columns: Optional[Sequence[str]],
    payload_column: Optional[str],
    input_mapping: Optional[Mapping[str, str]],
    correlation_id_path: Optional[str],
    max_batch_bytes: int,
    max_batch_rows: Optional[int],
    execution_partitions: Optional[int],
    max_retries: int,
    timeout_seconds: float,
    output_schema: Optional[DataType],
    result_column: str,
    metadata_column: str,
    diagnostics: bool,
    on_error: str,
    response_options: Optional[ResponseOptions] = None,
    request_options: Optional[Mapping[str, Any]] = None,
    isolate_failures: bool = True,
    input_validation: str = "none",
    context_operation: Optional[str] = None,
    context_include: Optional[str] = None,
    state_key: Optional[str] = None,
    context_key: Optional[str] = None,
    context_namespace: Optional[str] = None,
    contract: Optional[Dict[str, Any]] = None,
    max_inflight_rows: Optional[int] = None,
) -> DataFrame:
    """Prepare and return one lazy Rulebricks DataFrame transformation."""

    if not isinstance(df, DataFrame):
        raise TypeError("df must be a pyspark.sql.DataFrame.")
    if input_validation not in ("none", "warn", "strict"):
        raise ValueError("input_validation must be 'none', 'warn', or 'strict'.")
    responses = response_options if response_options is not None else ResponseOptions()
    if not isinstance(responses, ResponseOptions):
        raise TypeError("response_options must be ResponseOptions or None.")
    if responses.result_format == "json" and output_schema is not None:
        raise ValueError("output_schema cannot be combined with result_format='json'.")
    _, sdk_request_options = _split_request_options(request_options)
    unsupported = set(sdk_request_options) - {
        "additional_headers",
        "additional_query_parameters",
    }
    if unsupported:
        raise ValueError("Unsupported request_options: " + ", ".join(sorted(unsupported)))
    if any(not isinstance(value, Mapping) for value in sdk_request_options.values()):
        raise TypeError("Each request_options value must be a mapping.")
    if context_operation and "include" in (request_options or {}).get(
        "additional_query_parameters", {}
    ):
        raise ValueError("Use include instead of overriding the context include query parameter.")
    if not isinstance(isolate_failures, bool):
        raise TypeError("isolate_failures must be a bool.")
    if responses.raw_column is not None:
        if responses.raw_column in df.columns or responses.raw_column in (
            result_column,
            metadata_column,
        ):
            raise ValueError("raw_column must not collide with source or output columns.")
        if responses.raw_column.startswith(RB_PREFIX):
            raise ValueError("raw_column must not use the reserved _rb_ prefix.")
    for name, value in (
        ("slug", slug),
        ("api_key", api_key),
        ("base_url", base_url),
        ("version", version),
        ("result_column", result_column),
        ("metadata_column", metadata_column),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string.")
    if result_column == metadata_column:
        raise ValueError("result_column and metadata_column must be different.")
    reserved_outputs = [
        name for name in (result_column, metadata_column) if name.startswith(RB_PREFIX)
    ]
    if reserved_outputs:
        raise ValueError(
            f"Output columns beginning with {RB_PREFIX!r} are reserved: "
            + ", ".join(reserved_outputs)
        )
    collisions = sorted({result_column, metadata_column} & set(df.columns))
    if collisions:
        raise ValueError("Rulebricks output columns already exist: " + ", ".join(collisions))
    reserved = sorted(name for name in df.columns if name.startswith(RB_PREFIX))
    if reserved and contract is None:
        raise ValueError(
            f"Input columns beginning with {RB_PREFIX!r} are reserved: " + ", ".join(reserved)
        )
    if payload_column and input_columns is not None:
        raise ValueError("payload_column and input_columns are mutually exclusive.")
    if payload_column and input_mapping:
        raise ValueError(
            "input_mapping cannot be combined with payload_column; shape the payload "
            "object directly."
        )
    if isinstance(input_columns, (str, bytes)):
        raise TypeError("input_columns must be a sequence of column names, not a string.")
    if len(set(df.columns)) != len(df.columns):
        raise ValueError("Source DataFrame contains duplicate column names.")
    requested = list(input_columns or [])
    if len(set(requested)) != len(requested):
        raise ValueError("input_columns contains duplicate column names.")
    if payload_column:
        requested.append(payload_column)
    missing = sorted(set(requested) - set(df.columns))
    if missing:
        raise ValueError("Input columns not found in DataFrame: " + ", ".join(missing))
    mapping = dict(input_mapping or {})
    if any(
        not isinstance(key, str) or not isinstance(value, str) or not value
        for key, value in mapping.items()
    ):
        raise ValueError("input_mapping must map column names to non-empty strings.")
    destinations = [
        mapping.get(name, name)
        for name in (input_columns if input_columns is not None else df.columns)
    ]
    if len(set(destinations)) != len(destinations):
        raise ValueError("input_mapping destination names must be unique.")
    selected = set(input_columns if input_columns is not None else df.columns)
    outside = sorted(set(mapping) - selected)
    if outside:
        raise ValueError(
            "input_mapping sources must be included in input_columns: " + ", ".join(outside)
        )
    if correlation_id_path is not None and (
        not isinstance(correlation_id_path, str) or not correlation_id_path
    ):
        raise ValueError("correlation_id_path must be a non-empty string or None.")
    for name, number, minimum in (
        ("max_batch_bytes", max_batch_bytes, 3),
        ("max_batch_rows", max_batch_rows, 1),
        ("execution_partitions", execution_partitions, 1),
        ("max_inflight_rows", max_inflight_rows, 1),
        ("max_retries", max_retries, 0),
    ):
        if number is None and name in (
            "max_batch_rows",
            "execution_partitions",
            "max_inflight_rows",
        ):
            continue
        if isinstance(number, bool) or not isinstance(number, int) or number < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}.")
    if max_inflight_rows is not None and (state_key is not None or context_operation is not None):
        raise ValueError("max_inflight_rows is only supported for independent rule and flow rows.")
    if (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
    ):
        raise ValueError("timeout_seconds must be finite and positive.")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive.")
    if on_error not in ("continue", "raise"):
        raise ValueError("on_error must be 'continue' or 'raise'.")
    if output_schema is not None and not isinstance(
        output_schema, (StructType, ArrayType, MapType)
    ):
        raise TypeError("output_schema must be a StructType, ArrayType, or MapType.")
    if isinstance(output_schema, StructType):
        names = output_schema.fieldNames()
        if names == ["error"] and responses.error_handler is None:
            raise ValueError(
                "A singleton output field named 'error' is reserved for "
                "Rulebricks execution failures."
            )
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError("output_schema contains duplicate fields: " + ", ".join(duplicates))

    partitioned = (
        plan_execution(df, execution_partitions, max_inflight_rows, max_batch_rows)
        if max_inflight_rows is not None
        else None
    )
    source_columns = [
        name for name in df.columns if not (contract is not None and name.startswith(RB_PREFIX))
    ]
    selected_inputs = (
        []
        if payload_column
        else list(input_columns if input_columns is not None else source_columns)
    )
    input_warnings = []
    infer_output = (
        asset_type == "rule" and output_schema is None and responses.result_format != "json"
    )
    if contract is None and (infer_output or input_validation != "none"):
        asset = _load_asset(
            asset_type,
            slug,
            api_key=api_key,
            base_url=base_url,
            version=version,
            timeout_seconds=timeout_seconds,
            request_options=request_options,
        )
        if infer_output:
            output_schema = _rule_output_schema(asset)
        if input_validation != "none":
            input_warnings = _validate_input_schema(
                df.schema,
                asset.get("request_schema"),
                mode=input_validation,
                input_columns=selected_inputs,
                input_mapping=mapping,
                payload_column=payload_column,
            )
    operation_id = contract["operation_id"] if contract is not None else str(uuid.uuid4())
    contract = contract or {
        "contract_version": 1,
        "operation_id": operation_id,
        "asset_type": asset_type,
        "slug": slug,
        "version": None if context_operation else version,
        "input_validation": input_validation,
        "input_warnings": input_warnings,
        "context_operation": context_operation,
        "state_key": state_key,
        "input_columns": selected_inputs,
        "source_columns": source_columns,
        "source_schema": StructType([df.schema[name] for name in source_columns]).jsonValue(),
        "payload_column": payload_column,
        "input_mapping": dict(input_mapping or {}),
        "correlation_id_path": correlation_id_path,
        "output_schema": output_schema.jsonValue() if output_schema is not None else None,
        "result_column": result_column,
        "metadata_column": metadata_column,
        "diagnostics": diagnostics,
        "responses": responses.contract(),
        "isolate_failures": isolate_failures,
        "request_options_required": bool(request_options),
    }

    if partitioned is None:
        partitioned = apply_partitioning(df, execution_partitions)
    planned_partitions = (
        partitioned.rdd.getNumPartitions() if max_inflight_rows is not None else None
    )
    config = RunConfig(
        asset_type=asset_type,
        slug=slug,
        version=version,
        api_key=api_key,
        base_url=base_url.rstrip("/"),
        input_columns=selected_inputs,
        payload_column=payload_column,
        input_mapping=dict(input_mapping or {}),
        correlation_id_path=correlation_id_path,
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        max_retries=max_retries,
        isolate_failures=isolate_failures,
        timeout_seconds=timeout_seconds,
        on_error=on_error,
        output_schema=output_schema,
        responses=responses,
        request_options=dict(request_options or {}),
        operation_id=operation_id,
        diagnostics=diagnostics,
        context_operation=context_operation,
        context_include=context_include,
        state_key=state_key,
        context_key=context_key,
        context_namespace=context_namespace,
        max_inflight_rows=max_inflight_rows,
        execution_partitions=planned_partitions,
    )

    worker_schema = build_execution_schema(
        partitioned.schema,
        output_schema=output_schema,
    )
    # Spark indexes input columns by name internally, so give Arrow safe names
    # and restore literal source names on the executor.
    safe_input = partitioned.select(
        *[
            literal_column(name).alias(f"_rb_input_{index}")
            for index, name in enumerate(partitioned.columns)
        ]
    )
    partition_id_column = None
    if max_inflight_rows is not None:
        # Downstream unions/coalesces can give TaskContext a different partition
        # ID. Capture the input partition index before entering the Arrow worker.
        partition_id_column = "_rb_request_partition"
        safe_input = safe_input.withColumn(partition_id_column, functions.spark_partition_id())
    internal = safe_input.mapInArrow(
        make_partition_worker(
            config, partitioned.columns, worker_schema, partition_id_column=partition_id_column
        ),
        schema=worker_schema,
    )
    return project_result(
        internal,
        source_columns=source_columns,
        result_column=result_column,
        metadata_column=metadata_column,
        diagnostics=diagnostics,
        contract=contract,
        raw_column=responses.raw_column,
    )


def project_result(
    df: DataFrame,
    *,
    source_columns: Sequence[str],
    result_column: str,
    metadata_column: str,
    diagnostics: bool,
    contract: Dict[str, Any],
    raw_column: Optional[str] = None,
) -> DataFrame:
    """Project internal worker columns into the stable public result shape."""

    result = functions.col("_rb_result")

    error_schema = StructType(
        [
            StructField("type", StringType(), nullable=True),
            StructField("message", StringType(), nullable=True),
            StructField("http_status", IntegerType(), nullable=True),
        ]
    )
    error = functions.when(
        functions.col("_rb_status").isin("ok", "pending"),
        functions.lit(None).cast(error_schema),
    ).otherwise(
        functions.struct(
            functions.col("_rb_error_type").alias("type"),
            functions.col("_rb_error").alias("message"),
            functions.col("_rb_http_status").alias("http_status"),
        )
    )
    meta_fields = [
        functions.col("_rb_operation_id").alias("operation_id"),
        functions.when(
            functions.col("_rb_status").isin("ok", "pending"),
            functions.col("_rb_status"),
        )
        .otherwise(functions.lit("error"))
        .alias("status"),
        error.alias("error"),
        functions.col("_rb_execution_id").alias("execution_id"),
        functions.col("_rb_item_execution_id").alias("item_execution_id"),
        functions.col("_rb_trace_id").alias("trace_id"),
        functions.col("_rb_record_id").alias("correlation_id"),
        functions.col("_rb_attempts").alias("attempts"),
        functions.col("_rb_started_at").alias("started_at"),
        functions.col("_rb_completed_at").alias("completed_at"),
        functions.col("_rb_asset_type").alias("asset_type"),
        functions.col("_rb_slug").alias("slug"),
        functions.col("_rb_version").alias("version"),
    ]
    if contract.get("context_operation"):
        meta_fields.extend(
            [
                functions.lit(contract["context_operation"]).alias("operation"),
                functions.col("_rb_context_write_status").alias("write_status"),
            ]
        )
    if diagnostics:
        meta_fields.append(
            functions.struct(
                functions.col("_rb_chunk_id").alias("chunk_id"),
                functions.col("_rb_chunk_size").alias("chunk_size"),
                functions.from_json(
                    functions.col("_rb_attempt_history"),
                    ATTEMPT_HISTORY_SCHEMA,
                ).alias("attempt_history"),
            ).alias("diagnostics")
        )

    metadata = functions.struct(*meta_fields).alias(
        metadata_column,
        metadata={CONTRACT_METADATA_KEY: contract},
    )
    return df.select(
        *[literal_column(name) for name in source_columns],
        result.alias(result_column),
        metadata,
        *([functions.col("_rb_raw_result").alias(raw_column)] if raw_column else []),
    )


def plan_execution(
    df: DataFrame,
    execution_partitions: Optional[int],
    max_inflight_rows: Optional[int],
    max_batch_rows: Optional[int] = DEFAULT_MAX_BATCH_ROWS,
) -> DataFrame:
    """Distribute a request plan across producers without a coordinator."""
    if max_inflight_rows is None:
        return apply_partitioning(df, execution_partitions)
    if df.isStreaming:
        raise ValueError("Use max_inflight_rows inside foreachBatch, not on a streaming DataFrame.")
    batch_rows = initial_batch_rows(max_batch_rows, max_inflight_rows)
    full_requests, remainder = divmod(max_inflight_rows, batch_rows)
    requests = full_requests + bool(remainder)
    if isinstance(execution_partitions, _DefaultExecutionPartitions):
        # Aim for four overlapping requests per producer. Never create a
        # producer just for the partial slot: it would have a tiny allowance
        # but still receive a whole Spark input partition.
        execution_partitions = min(
            max(1, (requests + 3) // 4),
            max(1, full_requests),
            max(1, df.sparkSession.sparkContext.defaultParallelism),
        )
    if execution_partitions is None:
        execution_partitions = df.rdd.getNumPartitions()
        if execution_partitions == 0:
            return df
    if execution_partitions > max_inflight_rows:
        raise ValueError(
            "max_inflight_rows must be >= the execution partition count. "
            "Set execution_partitions <= max_inflight_rows or omit it for automatic planning."
        )
    partitioned = apply_partitioning(df, execution_partitions)
    logging.getLogger(__name__).info(
        "Rulebricks request plan: %s rows total; initial batch ceiling=%s; "
        "%s nominal requests; %s execution partitions",
        max_inflight_rows,
        batch_rows,
        requests,
        execution_partitions,
    )
    return partitioned


def apply_partitioning(df: DataFrame, execution_partitions: Optional[int]) -> DataFrame:
    """Apply the requested execution partition count without eager work."""

    if execution_partitions is None:
        return df
    if df.isStreaming:
        return df.repartition(execution_partitions)
    current = df.rdd.getNumPartitions()
    if current > execution_partitions:
        return df.coalesce(execution_partitions)
    if current < execution_partitions:
        return df.repartition(execution_partitions)
    return df


def literal_column(name: str) -> Column:
    """Resolve literal names, including dots, spaces and embedded backticks."""
    return functions.col("`" + name.replace("`", "``") + "`")
