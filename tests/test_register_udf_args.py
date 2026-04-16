"""Tests for register_udf() argument validation.

Validates the pre-flight checks only. End-to-end registration + UDF execution
requires a running Spark session; those live under the ``requires_spark``
marker in the integration suite.
"""

import pytest

from rulebricks_spark.databricks import _validate_register_args


class TestName:
    def test_accepts_valid_name(self):
        _validate_register_args(name="my_udf", rule="slug123", api_key="k", batch_size=500)

    @pytest.mark.parametrize("bad", ["", None, 123, object()])
    def test_rejects_non_string_or_empty(self, bad):
        with pytest.raises(ValueError, match="name"):
            _validate_register_args(name=bad, rule="slug123", api_key="k", batch_size=500)


class TestRule:
    def test_accepts_valid_slug(self):
        _validate_register_args(name="my_udf", rule="abc123xyz9", api_key="k", batch_size=500)

    @pytest.mark.parametrize("bad", ["", None, 42])
    def test_rejects_non_string_or_empty(self, bad):
        with pytest.raises(ValueError, match="rule"):
            _validate_register_args(name="my_udf", rule=bad, api_key="k", batch_size=500)


class TestApiKey:
    def test_accepts_valid_key(self):
        _validate_register_args(name="my_udf", rule="slug", api_key="sk_live_abc", batch_size=500)

    @pytest.mark.parametrize("bad", ["", None, 0])
    def test_rejects_empty_or_non_string(self, bad):
        with pytest.raises(ValueError, match="api_key"):
            _validate_register_args(name="my_udf", rule="slug", api_key=bad, batch_size=500)


class TestBatchSize:
    def test_accepts_valid_range(self):
        for size in (1, 100, 500, 1000):
            _validate_register_args(name="my_udf", rule="slug", api_key="k", batch_size=size)

    @pytest.mark.parametrize("bad", [0, -1, 1001, 5000])
    def test_rejects_out_of_range(self, bad):
        with pytest.raises(ValueError, match="batch_size"):
            _validate_register_args(name="my_udf", rule="slug", api_key="k", batch_size=bad)
