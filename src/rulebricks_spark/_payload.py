"""Payload construction and Spark/JSON value coercion."""

from __future__ import annotations

import datetime
import hashlib
import json
import math
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from ._config import RunConfig
from ._metadata import RB_PREFIX


def build_payload(row: Dict[str, Any], config: RunConfig) -> Dict[str, Any]:
    """Build one Rulebricks request object from a source row dictionary."""

    if config.payload_column:
        raw = row.get(config.payload_column)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"payload_column {config.payload_column!r} contains invalid JSON"
                ) from exc
        raw = coerce_input(raw, config.spark_timezone)
        if not isinstance(raw, dict):
            raise ValueError(f"payload_column {config.payload_column!r} must contain an object")
        payload = raw
    else:
        names = (
            config.input_columns
            if config.input_columns is not None
            else [name for name in row if not name.startswith(RB_PREFIX)]
        )
        payload = {name: coerce_input(row.get(name), config.spark_timezone) for name in names}

    if config.input_mapping:
        payload = {config.input_mapping.get(name, name): value for name, value in payload.items()}
    if config.context_key and config.context_namespace:
        target = config.input_mapping.get(config.context_key, config.context_key)
        payload[target] = context_instance_key(config.context_namespace, row[config.context_key])
    return payload


def context_instance_key(namespace: str, original: str) -> str:
    """Keep physical replay identity separate from the caller's business columns."""
    return namespace + ":" + hashlib.sha256(original.encode("utf-8")).hexdigest()


def encode_payload(payload: Dict[str, Any]) -> str:
    """Encode a payload exactly as a compact JSON request item."""

    return json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def extract_dot_path(payload: Dict[str, Any], path: str) -> Any:
    """Read dotted object paths, accepting numeric list segments."""

    current: Any = payload
    for segment in path.split("."):
        if isinstance(current, dict):
            if segment not in current:
                return None
            current = current[segment]
        elif isinstance(current, (list, tuple)) and segment.isdigit():
            index = int(segment)
            if index >= len(current):
                return None
            current = current[index]
        else:
            return None
    return current


def record_id_value(payload: Dict[str, Any], path: str) -> Any:
    value = extract_dot_path(payload, path)
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def coerce_input(value: Any, spark_timezone: Optional[str] = "UTC") -> Any:
    """Recursively normalize pandas, NumPy, Spark Row, and datetime values."""

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Decimal inputs must be finite.")
        if value == value.to_integral_value():
            return int(value)
        result = float(value)
        if not math.isfinite(result) or Decimal(str(result)) != value:
            raise ValueError(
                f"Decimal value {value} cannot be represented exactly as a JSON number."
            )
        return result

    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, np.generic):
        return coerce_input(value.item(), spark_timezone)
    if isinstance(value, np.ndarray):
        return coerce_input(value.tolist(), spark_timezone)

    if isinstance(value, datetime.datetime):
        return _datetime_to_utc_iso(value, spark_timezone)
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): coerce_input(item, spark_timezone) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [coerce_input(item, spark_timezone) for item in value]

    as_dict = getattr(value, "asDict", None)
    if callable(as_dict):
        return coerce_input(as_dict(recursive=True), spark_timezone)
    return value


def _coerce_scalar(value: Any, data_type: DataType) -> Any:
    """Convert a Rulebricks response value to an Arrow-safe Python value."""

    if value is None:
        return None
    if isinstance(data_type, BooleanType):
        return _coerce_boolean(value)
    if isinstance(data_type, (ByteType, ShortType, IntegerType, LongType)):
        return _coerce_integral(value, data_type)
    if isinstance(data_type, (FloatType, DoubleType)):
        if isinstance(value, bool):
            return float(value)
        try:
            result = float(value)
            if isinstance(data_type, FloatType) and abs(result) > 3.4028234663852886e38:
                return None
            return result if math.isfinite(result) else None
        except (TypeError, ValueError, OverflowError):
            return None
    if isinstance(data_type, DecimalType):
        return _coerce_decimal(value, data_type)
    if isinstance(data_type, TimestampType):
        return _coerce_timestamp(value, timezone_aware=True)
    if isinstance(data_type, TimestampNTZType):
        return _coerce_timestamp(value, timezone_aware=False)
    if isinstance(data_type, DateType):
        parsed = _coerce_timestamp(value, timezone_aware=False)
        return parsed.date() if parsed is not None else None
    if isinstance(data_type, BinaryType):
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return None
    if isinstance(data_type, StringType):
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, separators=(",", ":"))
        if isinstance(value, bool):
            return "true" if value else "false"
        return value if isinstance(value, str) else str(value)
    return None


def _coerce_boolean(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "yes", "1"):
            return True
        if lowered in ("false", "no", "0"):
            return False
    return None


def _coerce_integral(value: Any, data_type: DataType) -> Any:
    if isinstance(value, bool):
        result = int(value)
    else:
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if not decimal_value.is_finite() or decimal_value != decimal_value.to_integral_value():
            return None
        result = int(decimal_value)
    if isinstance(data_type, ByteType):
        lower, upper = -(2**7), 2**7 - 1
    elif isinstance(data_type, ShortType):
        lower, upper = -(2**15), 2**15 - 1
    elif isinstance(data_type, IntegerType):
        lower, upper = -(2**31), 2**31 - 1
    else:
        lower, upper = -(2**63), 2**63 - 1
    return result if lower <= result <= upper else None


def _coerce_decimal(value: Any, data_type: DecimalType) -> Any:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite():
        return None
    quantum = Decimal(1).scaleb(-data_type.scale)
    try:
        with localcontext() as context:
            context.prec = max(38, data_type.precision)
            quantized = result.quantize(quantum)
    except InvalidOperation:
        return None
    if quantized != result:
        return None
    integer_digits = max(0, quantized.adjusted() + 1)
    if integer_digits > data_type.precision - data_type.scale:
        return None
    return quantized


def coerce_result(
    value: Any, data_type: DataType, *, strict: bool, path: str = "$", nullable: bool = True
) -> Any:
    """Convert typed results and identify the exact incompatible field.

    Extra fields are intentionally ignored. Nullability expresses required
    fields; optional fields may be absent as a schema evolves.
    """
    if value is None:
        if strict and not nullable:
            raise ValueError(f"{path}: required value is missing or null")
        return None
    if isinstance(data_type, StructType) and isinstance(value, dict):
        return {
            field.name: coerce_result(
                value.get(field.name),
                field.dataType,
                strict=strict,
                path=f"{path}.{field.name}",
                nullable=field.nullable,
            )
            for field in data_type.fields
        }
    if isinstance(data_type, ArrayType) and isinstance(value, (list, tuple)):
        return [
            coerce_result(
                item,
                data_type.elementType,
                strict=strict,
                path=f"{path}[{index}]",
                nullable=data_type.containsNull,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(data_type, MapType) and isinstance(value, dict):
        converted = {}
        for key, item in value.items():
            new_key = coerce_result(
                key, data_type.keyType, strict=True, path=f"{path}.<key>", nullable=False
            )
            if new_key in converted:
                raise ValueError(f"{path}: map keys collide after conversion")
            converted[new_key] = coerce_result(
                item,
                data_type.valueType,
                strict=strict,
                path=f"{path}[{key!r}]",
                nullable=data_type.valueContainsNull,
            )
        return converted
    result = _coerce_scalar(value, data_type)
    if strict and result is None:
        raise ValueError(
            f"{path}: expected {data_type.simpleString()}, received {type(value).__name__}"
        )
    return result


def _coerce_timestamp(value: Any, *, timezone_aware: bool) -> Any:
    try:
        parsed = pd.Timestamp(value)
        if timezone_aware:
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            else:
                parsed = parsed.tz_convert("UTC")
        elif parsed.tzinfo is not None:
            parsed = parsed.tz_convert("UTC").tz_localize(None)
        return parsed.to_pydatetime()
    except Exception:
        return None


def _datetime_to_utc_iso(value: datetime.datetime, spark_timezone: Optional[str]) -> str:
    if value.tzinfo is None and spark_timezone is None:
        return value.isoformat()
    if value.tzinfo is None:
        assert spark_timezone is not None
        value = value.replace(tzinfo=ZoneInfo(spark_timezone))
    return value.astimezone(datetime.timezone.utc).isoformat()
