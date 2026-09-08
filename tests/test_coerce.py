"""Precision and schema-conversion boundaries that cannot be delegated to Arrow."""

from decimal import Decimal

import pytest
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    FloatType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from rulebricks_spark._payload import coerce_input, coerce_result


@pytest.mark.parametrize(
    "value,data_type",
    [
        ("9223372036854775808", LongType()),
        ("1.5", LongType()),
        ("12.345", DecimalType(10, 2)),
        ("1e39", FloatType()),
        ({}, StructType([StructField("id", LongType(), False)])),
        ([None], ArrayType(LongType(), containsNull=False)),
    ],
)
def test_incompatible_values_fail_or_become_null_by_policy(value, data_type):
    with pytest.raises(ValueError, match=r"\$"):
        coerce_result(value, data_type, strict=True)
    result = coerce_result(value, data_type, strict=False)
    assert result is None or result == {"id": None} or result == [None]


def test_map_keys_cannot_be_lost_during_conversion():
    with pytest.raises(ValueError, match="collide"):
        coerce_result({"1": "a", "01": "b"}, MapType(LongType(), StringType()), strict=False)


def test_input_decimals_never_silently_lose_precision():
    assert coerce_input(Decimal("9007199254740993")) == 9007199254740993
    assert coerce_input(Decimal("12.34")) == 12.34
    with pytest.raises(ValueError, match="cannot be represented exactly"):
        coerce_input(Decimal("0.123456789012345678901"))
