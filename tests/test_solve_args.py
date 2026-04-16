"""Tests for solve() argument validation."""

import pytest

from rulebricks_spark.solve import _validate_args


class TestBatchSize:
    def test_accepts_valid_range(self):
        for size in (1, 100, 500, 1000):
            _validate_args(size, 4, "overwrite", "default", "continue")

    @pytest.mark.parametrize("bad", [0, -1, 1001, 5000])
    def test_rejects_out_of_range(self, bad):
        with pytest.raises(ValueError, match="batch_size"):
            _validate_args(bad, 4, "overwrite", "default", "continue")


class TestConcurrency:
    def test_accepts_positive(self):
        _validate_args(100, 1, "overwrite", "default", "continue")
        _validate_args(100, 32, "overwrite", "default", "continue")

    @pytest.mark.parametrize("bad", [0, -1])
    def test_rejects_non_positive(self, bad):
        with pytest.raises(ValueError, match="max_concurrent"):
            _validate_args(100, bad, "overwrite", "default", "continue")


class TestModeValues:
    def test_rejects_bad_on_conflict(self):
        with pytest.raises(ValueError, match="on_conflict"):
            _validate_args(100, 4, "skip", "default", "continue")

    def test_rejects_bad_on_missing_input(self):
        with pytest.raises(ValueError, match="on_missing_input"):
            _validate_args(100, 4, "overwrite", "fill", "continue")

    def test_rejects_bad_on_error(self):
        with pytest.raises(ValueError, match="on_error"):
            _validate_args(100, 4, "overwrite", "default", "swallow")

    def test_accepts_all_valid_combos(self):
        for conflict in ("error", "overwrite"):
            for missing in ("error", "default"):
                for err in ("raise", "continue"):
                    _validate_args(100, 4, conflict, missing, err)
