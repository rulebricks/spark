"""Selected-version schemas used by rule inference and input preflight."""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from typing import Any, Optional, cast

import httpx
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    MapType,
    NumericType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from rulebricks import Rulebricks
from rulebricks.core.request_options import RequestOptions

from .exceptions import SchemaInferenceError

# Complex types have no nested schema in the listing; retain them as JSON.
_TYPE_MAP = {
    "number": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "date": TimestampType(),
    "list": StringType(),
    "array": StringType(),
    "object": StringType(),
}


def _rule_schema(
    rule_slug: str, api_key: str, base_url: str, version: str = "latest"
) -> StructType:
    """Infer outputs from the same version selector used for execution."""
    return _rule_output_schema(
        _load_asset("rule", rule_slug, api_key=api_key, base_url=base_url, version=version)
    )


def _load_asset(
    asset_type: str,
    slug: str,
    *,
    api_key: str,
    base_url: str,
    version: str,
    timeout_seconds: float = 300,
    request_options: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Load one published rule or flow and its selected-version schemas on the driver."""
    if asset_type not in ("rule", "flow"):
        raise ValueError("asset_type must be 'rule' or 'flow'.")
    options = dict(request_options or {})
    queries = options.get("additional_query_parameters", {})
    if set(queries) & {"slug", "id", "version", "search", "name", "folder", "labels", "user_group"}:
        raise ValueError("request_options cannot override asset schema filters.")
    try:
        with httpx.Client(timeout=timeout_seconds) as http_client:
            client = Rulebricks(
                api_key=api_key,
                base_url=base_url,
                httpx_client=http_client,
                max_retries=0,
                timeout=timeout_seconds,
            )
            assets = client.assets.rules if asset_type == "rule" else client.assets.flows
            response = assets.with_raw_response.list(
                slug=slug, version=version, request_options=cast(RequestOptions, options)
            )
            try:
                body = response._response.json()
            finally:
                response.close()
    except Exception as exc:
        raise SchemaInferenceError(
            f"Could not load {asset_type} {slug!r} version {version!r}: {exc}"
        ) from exc
    if (
        not isinstance(body, list)
        or len(body) != 1
        or not isinstance(body[0], dict)
        or body[0].get("slug") != slug
        or body[0].get("published") is not True
    ):
        raise SchemaInferenceError(
            f"Expected one published {asset_type} for {slug!r} version {version!r}."
        )
    return body[0]


def _rule_output_schema(rule: dict[str, Any]) -> StructType:
    if not rule.get("response_schema"):
        raise SchemaInferenceError("Rule has no published response schema. Supply output_schema.")
    tree: dict[Any, Any] = {}
    for field in rule["response_schema"]:
        if not isinstance(field, dict):
            raise SchemaInferenceError("Invalid output field. Supply output_schema.")
        key, kind = field.get("key"), field.get("type")
        if not isinstance(key, str) or not key or kind not in _TYPE_MAP:
            raise SchemaInferenceError(
                "Rule has an unsupported schema field. Supply output_schema."
            )
        node = tree
        for part in key.split("."):
            if not part:
                raise SchemaInferenceError("Invalid output path. Supply output_schema.")
            node = node.setdefault(part, {})
        if None in node:
            raise SchemaInferenceError("Duplicate output fields. Supply output_schema.")
        node[None] = kind

    def struct(node: dict[Any, Any]) -> StructType:
        fields = []
        for name, child in node.items():
            if name is None:
                continue
            nested = any(key is not None for key in child)
            if nested and child.get(None) not in (None, "object"):
                raise SchemaInferenceError("Conflicting output paths. Supply output_schema.")
            kind = struct(child) if nested else _TYPE_MAP[child[None]]
            fields.append(StructField(name, kind, nullable=True))
        return StructType(fields)

    return struct(tree)


def _validate_input_schema(
    source: StructType,
    fields: Any,
    *,
    mode: str,
    input_columns: Sequence[str],
    input_mapping: Mapping[str, str],
    payload_column: Optional[str],
) -> list[str]:
    """Check declared field presence and Spark types without scanning records."""
    if not isinstance(fields, list):
        raise SchemaInferenceError("The selected asset has no request_schema.")
    payload: DataType = (
        source[payload_column].dataType
        if payload_column
        else StructType(
            [
                StructField(input_mapping.get(name, name), source[name].dataType)
                for name in input_columns
            ]
        )
    )
    if isinstance(payload, (StringType, MapType)):
        raise ValueError(
            "Input preflight needs a struct or selected columns; parse JSON/map payloads into a struct first, or set input_validation='none'."
        )
    if not isinstance(payload, StructType):
        raise ValueError("Input preflight requires an object payload.")
    problems = []
    for field in fields:
        if not isinstance(field, dict):
            raise SchemaInferenceError("The selected asset has an invalid input field.")
        key, kind = field.get("key"), field.get("type")
        if not isinstance(key, str) or not key or kind not in _TYPE_MAP:
            raise SchemaInferenceError("The selected asset has an unsupported input field.")
        actual: Optional[DataType] = payload
        # Literal dotted columns are sent as dotted keys; structs send nested JSON.
        if key in payload.fieldNames():
            actual = payload[key].dataType
        else:
            for part in key.split("."):
                if not isinstance(actual, StructType) or part not in actual.fieldNames():
                    actual = None
                    break
                actual = actual[part].dataType
        if actual is None:
            problems.append(f"{key}: missing declared input")
            continue
        expected = {
            "number": (NumericType,),
            "string": (StringType,),
            "boolean": (BooleanType,),
            "date": (StringType, NumericType, DateType, TimestampType, TimestampNTZType),
            "array": (ArrayType,),
            "list": (ArrayType,),
            "object": (StructType, MapType),
        }[kind]
        if not isinstance(actual, expected):
            problems.append(f"{key}: expected {kind}, source is {actual.simpleString()}")
    if problems:
        message = "Input schema does not match the selected asset: " + "; ".join(problems)
        if mode == "strict":
            raise ValueError(message)
        warnings.warn(message, UserWarning, stacklevel=3)
    return problems


def _data_type_from_json(value: Any) -> DataType:
    """Read any supported Spark type using the public schema JSON API."""
    return (
        StructType.fromJson(
            {
                "type": "struct",
                "fields": [{"name": "result", "type": value, "nullable": True, "metadata": {}}],
            }
        )
        .fields[0]
        .dataType
    )
