"""Register Rulebricks rules as Spark SQL functions."""

from typing import Any, Optional

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

from ._config import RunConfig
from ._engine import ByteChunker, _execute_chunk, _local_payload_failure, _make_record
from ._metadata import nullable_type
from ._schema import _rule_schema
from ._transport import sdk_client


def register_udf(
    spark: SparkSession,
    name: str,
    rule: str,
    *,
    api_key: str,
    base_url: str,
    version: str = "latest",
    output_schema: Optional[StructType] = None,
    max_batch_rows: int = 1000,
) -> StructType:
    """Register a rule accepting a SQL struct and returning outputs plus _rb_error.

    Output inference uses the selected version. max_batch_rows limits each HTTP
    request. For batch audit metadata and lossless nullable integer inputs,
    use apply_rule() instead of the pandas SQL-UDF interface.
    """
    for argument, value in (
        ("name", name),
        ("rule", rule),
        ("api_key", api_key),
        ("base_url", base_url),
        ("version", version),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{argument} must be a non-empty string.")
    if (
        isinstance(max_batch_rows, bool)
        or not isinstance(max_batch_rows, int)
        or max_batch_rows < 1
    ):
        raise ValueError("max_batch_rows must be a positive integer.")
    if output_schema is None:
        output_schema = _rule_schema(rule, api_key, base_url, version)
    if not isinstance(output_schema, StructType):
        raise TypeError("output_schema must be a StructType.")
    names = output_schema.fieldNames()
    if len(set(names)) != len(names) or "_rb_error" in names:
        raise ValueError("Output fields must be unique and must not include _rb_error.")
    return_schema = StructType(
        [
            *nullable_type(output_schema).fields,
            StructField("_rb_error", StringType()),
        ]
    )
    config = RunConfig(
        asset_type="rule",
        slug=rule,
        version=version,
        api_key=api_key,
        base_url=base_url.rstrip("/"),
        max_batch_rows=max_batch_rows,
        output_schema=output_schema,
        spark_timezone=spark.conf.get("spark.sql.session.timeZone"),
    )
    spark.udf.register(name, _rule_udf(config, return_schema))
    return return_schema


def _rule_udf(config: RunConfig, return_schema: StructType):
    @pandas_udf(return_schema)  # type: ignore[call-overload]
    def execute(inputs: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(inputs, pd.DataFrame):
            raise TypeError("The Rulebricks SQL function expects a struct argument.")
        chunker = ByteChunker(config.max_batch_bytes, config.max_batch_rows)
        outcomes = []
        with sdk_client(config) as client:
            for source in inputs.to_dict(orient="records"):
                try:
                    _check_pandas_precision(source)
                    record = _make_record(source, config)
                except (TypeError, ValueError) as exc:
                    for chunk in chunker.finish():
                        outcomes.extend(_execute_chunk(chunk, client, config))
                    outcomes.append(_local_payload_failure(source, exc, config))
                    continue
                for chunk in chunker.add(record):
                    outcomes.extend(_execute_chunk(chunk, client, config))
            for chunk in chunker.finish():
                outcomes.extend(_execute_chunk(chunk, client, config))
        rows = [
            {**(outcome.result or {}), "_rb_error": outcome.metadata["_rb_error"]}
            for outcome in outcomes
        ]
        return pd.DataFrame(rows, columns=return_schema.fieldNames(), dtype=object)

    return execute


def _check_pandas_precision(value: Any) -> None:
    # Nullable longs arrive as floats; the original integer cannot be recovered.
    if isinstance(value, float) and abs(value) >= 2**53:
        raise ValueError(
            "The SQL pandas UDF cannot guarantee precision for this numeric input. "
            "Use apply_rule() or pass identifier columns as strings."
        )
    if isinstance(value, dict):
        for item in value.values():
            _check_pandas_precision(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_pandas_precision(item)
