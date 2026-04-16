"""Tests for input and output value coercion."""

import datetime
import json
import math

import pytest

from rulebricks_spark._partition import (
    _coerce_input,
    _coerce_output,
    _is_retryable,
    _normalize_response,
    _to_plain_dict,
)


class TestCoerceInput:
    def test_none_stays_none(self):
        assert _coerce_input(None) is None

    def test_nan_becomes_none(self):
        assert _coerce_input(float("nan")) is None

    def test_passthrough_primitives(self):
        assert _coerce_input("hello") == "hello"
        assert _coerce_input(42) == 42
        assert _coerce_input(3.14) == 3.14
        assert _coerce_input(True) is True

    def test_numpy_scalar_unwraps(self):
        np = pytest.importorskip("numpy")
        val = _coerce_input(np.int64(5))
        assert val == 5
        assert type(val) is int

    def test_numpy_float_unwraps(self):
        np = pytest.importorskip("numpy")
        val = _coerce_input(np.float64(3.14))
        assert isinstance(val, float)

    def test_numpy_array_becomes_list(self):
        np = pytest.importorskip("numpy")
        val = _coerce_input(np.array([1, 2, 3]))
        assert val == [1, 2, 3]

    def test_pandas_timestamp_becomes_iso(self):
        pd = pytest.importorskip("pandas")
        val = _coerce_input(pd.Timestamp("2026-04-16T10:30:00"))
        assert isinstance(val, str)
        assert val.startswith("2026-04-16T10:30:00")

    def test_pandas_nat_becomes_none(self):
        pd = pytest.importorskip("pandas")
        assert _coerce_input(pd.NaT) is None

    def test_datetime_becomes_iso_string(self):
        dt = datetime.datetime(2026, 4, 16, 12, 0, 0)
        assert _coerce_input(dt) == "2026-04-16T12:00:00"


class TestCoerceOutputNumber:
    def test_int_to_float(self):
        assert _coerce_output(5, "number") == 5.0

    def test_string_numeric_to_float(self):
        assert _coerce_output("3.14", "number") == 3.14

    def test_bool_to_float(self):
        assert _coerce_output(True, "number") == 1.0
        assert _coerce_output(False, "number") == 0.0

    def test_bad_string_returns_none(self):
        assert _coerce_output("nope", "number") is None

    def test_none_stays_none(self):
        assert _coerce_output(None, "number") is None


class TestCoerceOutputBoolean:
    @pytest.mark.parametrize("val", [True, False])
    def test_bool_passthrough(self, val):
        assert _coerce_output(val, "boolean") is val

    @pytest.mark.parametrize("val,expected", [
        ("true", True), ("True", True), ("YES", True), ("1", True),
        ("false", False), ("no", False), ("0", False),
    ])
    def test_string_parsing(self, val, expected):
        assert _coerce_output(val, "boolean") is expected

    def test_ambiguous_string_none(self):
        assert _coerce_output("maybe", "boolean") is None

    def test_numeric_conversion(self):
        assert _coerce_output(1, "boolean") is True
        assert _coerce_output(0, "boolean") is False


class TestCoerceOutputDate:
    def test_iso_string_to_datetime(self):
        result = _coerce_output("2026-04-16T10:30:00", "date")
        assert isinstance(result, datetime.datetime)
        assert result.year == 2026

    def test_invalid_string_returns_none(self):
        assert _coerce_output("not a date", "date") is None

    def test_datetime_passthrough(self):
        dt = datetime.datetime(2026, 4, 16)
        assert _coerce_output(dt, "date") == dt


class TestCoerceOutputJsonEncoded:
    def test_dict_to_json_string_for_object(self):
        result = _coerce_output({"a": 1, "b": [2, 3]}, "object")
        assert isinstance(result, str)
        assert json.loads(result) == {"a": 1, "b": [2, 3]}

    def test_list_to_json_string_for_list(self):
        result = _coerce_output([1, 2, 3], "list")
        assert json.loads(result) == [1, 2, 3]

    def test_string_passthrough_for_string_type(self):
        assert _coerce_output("hello", "string") == "hello"

    def test_dict_in_string_column_json_encoded(self):
        result = _coerce_output({"x": 1}, "string")
        assert json.loads(result) == {"x": 1}

    def test_boolean_stringified(self):
        assert _coerce_output(True, "string") == "true"
        assert _coerce_output(False, "string") == "false"


class TestCoerceOutputTypedArray:
    def test_list_passthrough(self):
        assert _coerce_output([1, 2, 3], "list_typed") == [1, 2, 3]

    def test_tuple_becomes_list(self):
        assert _coerce_output((1, 2), "list_typed") == [1, 2]

    def test_non_list_becomes_none(self):
        assert _coerce_output("foo", "list_typed") is None
        assert _coerce_output(42, "list_typed") is None


class TestNormalizeResponse:
    def test_plain_list(self):
        assert _normalize_response([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]

    def test_none_returns_empty_list(self):
        assert _normalize_response(None) == []

    def test_wrapped_data_attribute(self):
        class Wrapper:
            data = [{"x": 1}]

        assert _normalize_response(Wrapper()) == [{"x": 1}]

    def test_single_dict_wrapped_in_list(self):
        assert _normalize_response({"only": "result"}) == [{"only": "result"}]

    def test_object_items_converted_to_dicts(self):
        class Item:
            def model_dump(self):
                return {"priority": "P1"}

        result = _normalize_response([Item()])
        assert result == [{"priority": "P1"}]


class TestToPlainDict:
    def test_dict_passthrough(self):
        assert _to_plain_dict({"a": 1}) == {"a": 1}

    def test_model_dump_preferred(self):
        class Item:
            def model_dump(self):
                return {"via": "model_dump"}

            def dict(self):
                return {"via": "dict"}

        assert _to_plain_dict(Item()) == {"via": "model_dump"}

    def test_falls_back_to_dict_method(self):
        class Item:
            def dict(self):
                return {"via": "dict"}

        assert _to_plain_dict(Item()) == {"via": "dict"}

    def test_falls_back_to_instance_dict(self):
        class Item:
            def __init__(self):
                self.visible = "yes"
                self._hidden = "no"

        assert _to_plain_dict(Item()) == {"visible": "yes"}


class TestIsRetryable:
    @pytest.mark.parametrize("msg", [
        "Connection timeout occurred",
        "HTTP 502 Bad Gateway",
        "503 service unavailable",
        "Rate limit exceeded: 429",
        "Connection reset by peer",
    ])
    def test_retryable_messages(self, msg):
        assert _is_retryable(Exception(msg))

    @pytest.mark.parametrize("msg", [
        "400 Bad Request",
        "401 Unauthorized",
        "Invalid rule slug",
    ])
    def test_non_retryable_messages(self, msg):
        assert not _is_retryable(Exception(msg))

    def test_status_code_attribute_retryable(self):
        class HttpErr(Exception):
            status_code = 503

        assert _is_retryable(HttpErr("boom"))

    def test_status_code_attribute_non_retryable(self):
        class HttpErr(Exception):
            status_code = 404

        assert not _is_retryable(HttpErr("not found"))
