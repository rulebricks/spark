"""Schema inference and Spark type mapping.

Fetches a rule's response schema from the Rulebricks workspace and converts it
into a Spark ``StructType``. Users can skip inference by passing an explicit
``output_schema`` to :func:`solve`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .exceptions import SchemaInferenceError

# Rulebricks type → Spark type.
#
# Design notes:
#   * "number" → DoubleType is the safe superset for ints and floats; users
#     wanting long/decimal can pass an explicit output_schema.
#   * "list" and "object" default to StringType with JSON encoding. Supporting
#     typed arrays or nested structs requires a per-rule schema that the rule
#     definition doesn't always expose; users can pass output_schema to get
#     typed columns back.
_TYPE_MAP: Dict[str, DataType] = {
    "number": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "date": TimestampType(),
    "list": StringType(),
    "object": StringType(),
}


def rulebricks_to_spark_type(rb_type: str) -> DataType:
    """Map a Rulebricks response type string to a Spark ``DataType``.

    Unknown types fall back to ``StringType`` so downstream pipelines don't
    break on schema surprises.
    """
    return _TYPE_MAP.get(rb_type, StringType())


def fetch_rule_schema(
    rule_slug: str, api_key: str, base_url: str
) -> Tuple[StructType, List[Tuple[str, str]]]:
    """Fetch a rule's response schema from Rulebricks.

    Returns
    -------
    (spark_schema, rb_type_pairs)
        ``spark_schema`` is a ``StructType`` matching the rule's outputs.
        ``rb_type_pairs`` is a list of ``(field_name, rulebricks_type)`` used
        internally by the partition worker for value coercion.
    """
    try:
        from rulebricks import Rulebricks
    except ImportError as e:
        raise ImportError(
            "The rulebricks package is required. Install with `pip install rulebricks`."
        ) from e

    client = _make_client(api_key, base_url)

    try:
        rules = client.assets.rules.list()
    except Exception as e:
        raise SchemaInferenceError(
            f"Failed to fetch rule list from {base_url}: {e}. "
            f"Pass an explicit `output_schema` to skip inference."
        ) from e

    rule_entry = _find_rule_by_slug(rules, rule_slug)
    if rule_entry is None:
        raise SchemaInferenceError(
            f"Rule with slug {rule_slug!r} not found in workspace. "
            f"Verify the slug or pass an explicit `output_schema`."
        )

    rule_id = _get_attr(rule_entry, "id")
    if not rule_id:
        raise SchemaInferenceError(
            f"Rule {rule_slug!r} has no id on the workspace listing response."
        )

    try:
        rule_export = client.assets.rules.pull(id=rule_id)
    except Exception as e:
        raise SchemaInferenceError(
            f"Failed to fetch rule definition for {rule_slug!r}: {e}. "
            f"Pass an explicit `output_schema` to skip inference."
        ) from e

    response_schema = _extract_response_schema(rule_export)
    if not response_schema:
        raise SchemaInferenceError(
            f"Rule {rule_slug!r} has no response schema defined. "
            f"Pass an explicit `output_schema`."
        )

    fields: List[StructField] = []
    rb_pairs: List[Tuple[str, str]] = []
    for field_def in response_schema:
        key = _get_attr(field_def, "key") or _get_attr(field_def, "name")
        rb_type = _get_attr(field_def, "type") or "string"
        if not key:
            continue
        spark_type = rulebricks_to_spark_type(rb_type)
        fields.append(StructField(str(key), spark_type, nullable=True))
        rb_pairs.append((str(key), str(rb_type)))

    if not fields:
        raise SchemaInferenceError(
            f"Rule {rule_slug!r} returned an empty response schema."
        )

    return StructType(fields), rb_pairs


def build_output_schema(
    input_schema: StructType,
    rule_output_schema: StructType,
) -> StructType:
    """Construct the full DataFrame output schema.

    Input columns that collide with rule output columns are dropped (they will
    be overwritten by rule output). A trailing ``_rb_error`` column is always
    appended.
    """
    output_names = {f.name for f in rule_output_schema.fields}

    fields: List[StructField] = []
    for f in input_schema.fields:
        if f.name in output_names:
            continue
        fields.append(f)

    for f in rule_output_schema.fields:
        fields.append(StructField(f.name, f.dataType, nullable=True))

    fields.append(StructField("_rb_error", StringType(), nullable=True))

    return StructType(fields)


def rb_types_from_spark_schema(schema: StructType) -> List[Tuple[str, str]]:
    """Derive ``(name, rulebricks_type)`` pairs from a user-provided StructType.

    This is the inverse of :func:`rulebricks_to_spark_type` used when the
    caller passes an explicit ``output_schema``. Inferred Rulebricks types drive
    value coercion in the partition worker; an unknown Spark type maps to
    ``"string"`` which JSON-encodes complex values.
    """
    pairs: List[Tuple[str, str]] = []
    for f in schema.fields:
        if isinstance(f.dataType, BooleanType):
            pairs.append((f.name, "boolean"))
        elif isinstance(f.dataType, TimestampType):
            pairs.append((f.name, "date"))
        elif isinstance(f.dataType, (DoubleType,)) or f.dataType.typeName() in (
            "double",
            "float",
            "integer",
            "long",
            "short",
            "byte",
            "decimal",
        ):
            pairs.append((f.name, "number"))
        elif isinstance(f.dataType, ArrayType):
            pairs.append((f.name, "list_typed"))
        elif isinstance(f.dataType, StructType):
            pairs.append((f.name, "struct_typed"))
        else:
            pairs.append((f.name, "string"))
    return pairs


# --- helpers -----------------------------------------------------------------


def _make_client(api_key: str, base_url: str):
    """Instantiate a Rulebricks client, accommodating SDK versions without base_url."""
    from rulebricks import Rulebricks

    if base_url and base_url != "https://rulebricks.com":
        try:
            return Rulebricks(api_key=api_key, base_url=base_url)
        except TypeError:
            # Older SDK: fall back to env var convention
            import os

            os.environ.setdefault("RULEBRICKS_API_URL", base_url)
    return Rulebricks(api_key=api_key)


def _find_rule_by_slug(rules: Any, slug: str) -> Optional[Any]:
    """Locate a rule in a list response by its slug, tolerating dict/obj shapes."""
    iterable = rules if isinstance(rules, list) else getattr(rules, "data", None) or []
    for r in iterable:
        if _get_attr(r, "slug") == slug:
            return r
    return None


def _extract_response_schema(rule_export: Any) -> Optional[List[Any]]:
    """Pull the response schema array out of a rule export payload."""
    for attr in ("responseSchema", "response_schema"):
        val = _get_attr(rule_export, attr)
        if val:
            return list(val)

    # Handle pydantic-style .dict() / .model_dump()
    for dump_method in ("model_dump", "dict"):
        fn = getattr(rule_export, dump_method, None)
        if callable(fn):
            try:
                d = fn()
            except Exception:
                continue
            if isinstance(d, dict):
                val = d.get("responseSchema") or d.get("response_schema")
                if val:
                    return list(val)

    if isinstance(rule_export, dict):
        val = rule_export.get("responseSchema") or rule_export.get("response_schema")
        if val:
            return list(val)

    return None


def _get_attr(obj: Any, name: str) -> Any:
    """Read a field whether obj is a dict, an object, or a pydantic model."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)
