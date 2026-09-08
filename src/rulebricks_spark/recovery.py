"""Secondary recovery API for materialized Rulebricks results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from pyspark.sql import DataFrame, functions
from pyspark.sql.types import StringType, StructType

from ._apply import (
    CONTRACT_METADATA_KEY,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_ROWS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT_SECONDS,
    apply_dataframe,
    literal_column,
)
from ._metadata import RETRYABLE_ERROR_TYPES, nullable_type
from ._schema import _data_type_from_json
from .responses import ResponseOptions


def resume(
    result_df: DataFrame | str,
    *,
    api_key: str,
    base_url: str,
    error_types: Optional[Sequence[str]] = None,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_batch_rows: Optional[int] = DEFAULT_MAX_BATCH_ROWS,
    execution_partitions: Optional[int] = 1,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    on_error: str = "continue",
    response_options: Optional[ResponseOptions] = None,
    request_options: Optional[Mapping[str, Any]] = None,
) -> DataFrame:
    """Resume a saved result DataFrame, or a stateful Checkpoint path.

    Read results from durable storage or a reliable checkpoint. A cache alone
    can be evicted and replay the original HTTP calls. The saved schema supplies
    the asset, version, input mapping, and output schema.

    error_types defaults to transient request failures. Re-supply response
    callbacks and request_options if the original run used them; neither is
    stored in table metadata. Request limits and on_error match apply_rule().

    A string path resumes a ``contexts.Checkpoint`` run using its original
    execution settings. It reconstructs plain Context state under fresh instance
    IDs and replays whole unfinished windows. Per-row error selection and the
    request-tuning overrides above do not apply to checkpoint paths.
    """

    if isinstance(result_df, str):
        from ._checkpoints import resume_checkpoint

        if error_types is not None:
            raise ValueError(
                "Stateful checkpoint recovery replays whole unfinished windows, not selected error types."
            )
        return resume_checkpoint(
            result_df,
            api_key=api_key,
            base_url=base_url,
            response_options=response_options,
            request_options=request_options,
        )

    _require_materialized_source(result_df)
    metadata_fields = [
        field
        for field in result_df.schema.fields
        if isinstance(field.metadata.get(CONTRACT_METADATA_KEY), dict)
    ]
    if len(metadata_fields) != 1:
        raise ValueError(
            "resume() requires exactly one Rulebricks metadata column with an execution contract."
        )
    metadata_column = metadata_fields[0].name
    contract = dict(metadata_fields[0].metadata[CONTRACT_METADATA_KEY])
    _validate_contract(result_df, contract, metadata_column)

    saved = contract["responses"]
    if response_options is None:
        if saved.get("error_handler") or saved.get("transform"):
            raise ValueError(
                "resume() requires the original ResponseOptions callbacks; Python code is not stored in table metadata."
            )
        response_options = ResponseOptions(
            **{k: v for k, v in saved.items() if k not in ("error_handler", "transform")}
        )
    if response_options.contract() != saved:
        raise ValueError("response_options no longer match the execution contract.")
    if contract.get("request_options_required") and not request_options:
        raise ValueError(
            "resume() requires the original request_options; headers are not stored in table metadata."
        )
    if isinstance(error_types, (str, bytes)):
        raise TypeError("error_types must be a sequence of error category names.")
    selected_types = sorted(RETRYABLE_ERROR_TYPES) if error_types is None else list(error_types)
    meta = literal_column(metadata_column)
    should_resume = (meta.getField("status") == functions.lit("error")) & meta.getField(
        "error"
    ).getField("type").isin(selected_types)
    identities = (
        result_df.filter(should_resume)
        .select(
            meta.getField("operation_id").alias("operation_id"),
            meta.getField("asset_type").alias("asset_type"),
            meta.getField("slug").alias("slug"),
            meta.getField("version").alias("version"),
        )
        .distinct()
        .limit(2)
        .collect()
    )
    if not identities:
        return result_df
    expected_identity = (
        contract["operation_id"],
        contract["asset_type"],
        contract["slug"],
        contract["version"],
    )
    if len(identities) != 1 or tuple(identities[0]) != expected_identity:
        raise ValueError("resume() cannot combine rows from different executions.")

    retry_input = _build_retry_input(
        result_df.filter(should_resume),
        contract=contract,
        metadata_column=metadata_column,
    )
    untouched = result_df.filter(~functions.coalesce(should_resume, functions.lit(False)))
    output_schema = (
        _data_type_from_json(contract["output_schema"])
        if contract.get("output_schema") is not None
        else None
    )
    retried = apply_dataframe(
        retry_input,
        asset_type=contract["asset_type"],
        slug=contract["slug"],
        api_key=api_key,
        base_url=base_url,
        version=contract["version"],
        input_columns=None if contract.get("payload_column") else contract.get("input_columns"),
        payload_column=contract.get("payload_column"),
        input_mapping=contract.get("input_mapping"),
        correlation_id_path=contract.get("correlation_id_path"),
        max_batch_bytes=max_batch_bytes,
        max_batch_rows=max_batch_rows,
        execution_partitions=execution_partitions,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        output_schema=output_schema,
        result_column=contract["result_column"],
        metadata_column=contract["metadata_column"],
        diagnostics=bool(contract.get("diagnostics")),
        isolate_failures=contract["isolate_failures"],
        on_error=on_error,
        response_options=response_options,
        request_options=request_options,
        contract=contract,
    )
    return untouched.unionByName(retried, allowMissingColumns=False)


def _build_retry_input(
    df: DataFrame,
    *,
    contract: dict[str, Any],
    metadata_column: str,
) -> DataFrame:
    meta = literal_column(metadata_column)
    error = meta.getField("error")
    excluded = {
        metadata_column,
        contract["result_column"],
        contract["responses"]["raw_column"],
    }
    columns = [literal_column(name) for name in df.columns if name not in excluded]
    columns.extend(
        [
            meta.getField("status").alias("_rb_status"),
            error.getField("type").alias("_rb_error_type"),
            error.getField("message").alias("_rb_error"),
            error.getField("http_status").alias("_rb_http_status"),
            meta.getField("execution_id").alias("_rb_execution_id"),
            meta.getField("item_execution_id").alias("_rb_item_execution_id"),
            meta.getField("trace_id").alias("_rb_trace_id"),
            meta.getField("correlation_id").alias("_rb_record_id"),
            functions.lit(None).cast(StringType()).alias("_rb_chunk_id"),
            functions.lit(None).cast("integer").alias("_rb_chunk_size"),
            meta.getField("attempts").alias("_rb_attempts"),
            _history_json(meta, bool(contract.get("diagnostics"))).alias("_rb_attempt_history"),
            meta.getField("started_at").alias("_rb_started_at"),
            meta.getField("completed_at").alias("_rb_completed_at"),
            meta.getField("operation_id").alias("_rb_operation_id"),
            meta.getField("slug").alias("_rb_slug"),
            meta.getField("version").alias("_rb_version"),
            meta.getField("asset_type").alias("_rb_asset_type"),
        ]
    )
    return df.select(*columns)


def _history_json(meta: Any, diagnostics: bool) -> Any:
    if not diagnostics:
        return functions.lit(None).cast(StringType())
    return functions.to_json(meta.getField("diagnostics").getField("attempt_history"))


def _validate_contract(
    result_df: DataFrame,
    contract: dict[str, Any],
    metadata_column: str,
) -> None:
    required = {
        "contract_version",
        "operation_id",
        "asset_type",
        "slug",
        "version",
        "source_columns",
        "source_schema",
        "result_column",
        "metadata_column",
        "responses",
        "isolate_failures",
        "output_schema",
    }
    missing = sorted(required - set(contract))
    if missing:
        raise ValueError("Invalid Rulebricks execution contract; missing: " + ", ".join(missing))
    if contract["contract_version"] != 1:
        raise ValueError(
            f"Unsupported Rulebricks execution contract {contract['contract_version']!r}."
        )
    if contract.get("state_key"):
        raise ValueError(
            "Reconcile stateful flow effects before resubmitting ordered events; resume() cannot replay them safely."
        )
    if contract["asset_type"] not in ("flow", "rule"):
        raise ValueError(
            "resume() supports rule and flow executions. Reconcile context state before submitting another ingest."
        )
    if contract["metadata_column"] != metadata_column:
        raise ValueError("Rulebricks metadata column does not match its contract.")
    raw_column = contract["responses"]["raw_column"]
    required_columns = (
        set(contract["source_columns"])
        | ({raw_column} if raw_column else set())
        | {
            contract["result_column"],
            metadata_column,
        }
    )
    missing_columns = sorted(required_columns - set(result_df.columns))
    if missing_columns:
        raise ValueError(
            "Result DataFrame is missing contracted columns: " + ", ".join(missing_columns)
        )
    expected_source = StructType.fromJson(contract["source_schema"])
    actual_source = StructType([result_df.schema[name] for name in contract["source_columns"]])
    if nullable_type(actual_source) != nullable_type(expected_source):
        raise ValueError("Source column types no longer match the execution contract.")
    result_type = result_df.schema[contract["result_column"]].dataType
    expected_result = (
        _data_type_from_json(contract["output_schema"])
        if contract.get("output_schema") is not None
        else StringType()
    )
    if nullable_type(result_type) != nullable_type(expected_result):
        raise ValueError("Result type no longer matches the execution contract.")


def _require_materialized_source(df: DataFrame) -> None:
    """Cache can be evicted. Require lineage without an external execution node.

    This package targets classic PySpark 3.5. Inspect node names rather than
    explain strings; a file read or a reliable checkpoint severs this lineage.
    """
    pending = [df._jdf.queryExecution().analyzed()]
    while pending:
        node = pending.pop()
        if node.nodeName() in {"PythonMapInArrow", "MapInArrow", "MapInPandas"}:
            raise ValueError(
                "resume() requires a durable result read or a reliable checkpoint. "
                "Write results to a run-specific Delta/Parquet path and read them back, "
                "or use checkpoint(eager=True). cache()/persist() can replay original HTTP calls."
            )
        children = node.children()
        pending.extend(children.apply(index) for index in range(children.size()))
