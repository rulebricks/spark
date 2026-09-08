"""Stable audit metadata contract for Rulebricks Spark executions."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql.types import (
    DataType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RB_PREFIX = "_rb_"

RETRYABLE_ERROR_TYPES = frozenset(("network", "infrastructure", "throttled", "server"))


def metadata_fields() -> List[StructField]:
    """Internal fields carried between the worker and Spark projection."""

    return [
        StructField("_rb_status", StringType(), nullable=False),
        StructField("_rb_error_type", StringType(), nullable=True),
        StructField("_rb_error", StringType(), nullable=True),
        StructField("_rb_http_status", IntegerType(), nullable=True),
        StructField("_rb_context_write_status", StringType(), nullable=True),
        StructField("_rb_execution_id", StringType(), nullable=True),
        StructField("_rb_item_execution_id", StringType(), nullable=True),
        StructField("_rb_trace_id", StringType(), nullable=True),
        StructField("_rb_record_id", StringType(), nullable=True),
        StructField("_rb_chunk_id", StringType(), nullable=True),
        StructField("_rb_chunk_size", IntegerType(), nullable=True),
        StructField("_rb_attempts", IntegerType(), nullable=False),
        StructField("_rb_attempt_history", StringType(), nullable=True),
        StructField("_rb_started_at", TimestampType(), nullable=True),
        StructField("_rb_completed_at", TimestampType(), nullable=True),
        StructField("_rb_operation_id", StringType(), nullable=False),
        StructField("_rb_slug", StringType(), nullable=False),
        StructField("_rb_version", StringType(), nullable=True),
        StructField("_rb_asset_type", StringType(), nullable=False),
    ]


def build_execution_schema(
    input_schema: StructType,
    *,
    output_schema: Optional[DataType] = None,
) -> StructType:
    """Build the schema emitted by the executor-side Arrow worker."""

    fields = [field for field in input_schema.fields if not field.name.startswith(RB_PREFIX)]
    fields.append(
        StructField(
            "_rb_result",
            nullable_type(output_schema) if output_schema is not None else StringType(),
            nullable=True,
        )
    )
    fields.append(StructField("_rb_raw_result", StringType(), nullable=True))
    fields.extend(metadata_fields())
    return StructType(fields)


def nullable_type(data_type):
    """Arrow output must allow error nulls, including nested optional values."""
    from pyspark.sql.types import ArrayType, MapType

    if isinstance(data_type, StructType):
        return StructType(
            [
                StructField(f.name, nullable_type(f.dataType), True, f.metadata)
                for f in data_type.fields
            ]
        )
    if isinstance(data_type, ArrayType):
        return ArrayType(nullable_type(data_type.elementType), True)
    if isinstance(data_type, MapType):
        return MapType(data_type.keyType, nullable_type(data_type.valueType), True)
    return data_type


def seed_attempt_state(row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    """Carry a previous failed run into a new phased retry."""

    history = json.loads(row["_rb_attempt_history"]) if row.get("_rb_attempt_history") else []
    attempts = row.get("_rb_attempts") or 0

    if row.get("_rb_status") and row.get("_rb_status") != "ok":
        completed_at = row.get("_rb_completed_at") or row.get("_rb_started_at")
        history.append(
            {
                "at": completed_at.isoformat() if completed_at is not None else None,
                "http_status": row.get("_rb_http_status"),
                "error_type": row.get("_rb_error_type"),
                "error": row.get("_rb_error"),
                "trace_id": row.get("_rb_trace_id"),
                "execution_id": row.get("_rb_execution_id"),
                "chunk_id": row.get("_rb_chunk_id"),
            }
        )
    return history, attempts
