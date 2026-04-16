"""Tests for schema inference and type mapping."""

import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from rulebricks_spark._schema import (
    build_output_schema,
    rb_types_from_spark_schema,
    rulebricks_to_spark_type,
    _extract_response_schema,
    _find_rule_by_slug,
)


class TestTypeMapping:
    def test_all_known_types_map(self):
        assert isinstance(rulebricks_to_spark_type("number"), DoubleType)
        assert isinstance(rulebricks_to_spark_type("string"), StringType)
        assert isinstance(rulebricks_to_spark_type("boolean"), BooleanType)
        assert isinstance(rulebricks_to_spark_type("date"), TimestampType)
        # list and object default to StringType (JSON-encoded).
        assert isinstance(rulebricks_to_spark_type("list"), StringType)
        assert isinstance(rulebricks_to_spark_type("object"), StringType)

    def test_unknown_type_falls_back_to_string(self):
        assert isinstance(rulebricks_to_spark_type("nonexistent_type"), StringType)
        assert isinstance(rulebricks_to_spark_type(""), StringType)


class TestBuildOutputSchema:
    def test_no_collision_appends_in_order(self):
        input_schema = StructType([
            StructField("id", StringType()),
            StructField("amount", DoubleType()),
        ])
        rule_schema = StructType([
            StructField("priority", StringType()),
            StructField("reportable", BooleanType()),
        ])
        result = build_output_schema(input_schema, rule_schema)
        names = [f.name for f in result.fields]
        assert names == ["id", "amount", "priority", "reportable", "_rb_error"]

    def test_collision_drops_input_col(self):
        input_schema = StructType([
            StructField("id", StringType()),
            StructField("priority", StringType()),
        ])
        rule_schema = StructType([
            StructField("priority", StringType()),
        ])
        result = build_output_schema(input_schema, rule_schema)
        names = [f.name for f in result.fields]
        assert names == ["id", "priority", "_rb_error"]

    def test_rb_error_column_always_appended(self):
        input_schema = StructType([StructField("x", StringType())])
        rule_schema = StructType([StructField("y", StringType())])
        result = build_output_schema(input_schema, rule_schema)
        last = result.fields[-1]
        assert last.name == "_rb_error"
        assert isinstance(last.dataType, StringType)
        assert last.nullable is True

    def test_rule_outputs_nullable(self):
        input_schema = StructType([StructField("x", StringType(), nullable=False)])
        rule_schema = StructType([
            StructField("y", StringType(), nullable=False),
        ])
        result = build_output_schema(input_schema, rule_schema)
        y_field = next(f for f in result.fields if f.name == "y")
        assert y_field.nullable is True


class TestRbTypesFromSparkSchema:
    def test_infers_rb_types_from_explicit_schema(self):
        schema = StructType([
            StructField("score", DoubleType()),
            StructField("is_ok", BooleanType()),
            StructField("label", StringType()),
            StructField("when", TimestampType()),
            StructField("items", ArrayType(StringType())),
        ])
        pairs = rb_types_from_spark_schema(schema)
        assert pairs == [
            ("score", "number"),
            ("is_ok", "boolean"),
            ("label", "string"),
            ("when", "date"),
            ("items", "list_typed"),
        ]

    def test_integer_types_mapped_to_number(self):
        schema = StructType([
            StructField("a", IntegerType()),
            StructField("b", LongType()),
        ])
        pairs = rb_types_from_spark_schema(schema)
        assert all(t == "number" for _, t in pairs)

    def test_nested_struct_marked_struct_typed(self):
        inner = StructType([StructField("k", StringType())])
        schema = StructType([StructField("nested", inner)])
        pairs = rb_types_from_spark_schema(schema)
        assert pairs == [("nested", "struct_typed")]


class TestExtractResponseSchema:
    def test_extracts_from_dict_key(self, sample_rb_schema):
        export = {"responseSchema": sample_rb_schema}
        assert _extract_response_schema(export) == sample_rb_schema

    def test_extracts_from_snake_case_dict_key(self, sample_rb_schema):
        export = {"response_schema": sample_rb_schema}
        assert _extract_response_schema(export) == sample_rb_schema

    def test_extracts_from_object_attribute(self, sample_rb_schema):
        class FakeExport:
            responseSchema = sample_rb_schema  # noqa: N815

        assert _extract_response_schema(FakeExport()) == sample_rb_schema

    def test_extracts_via_model_dump(self, sample_rb_schema):
        class PydanticLike:
            def model_dump(self):
                return {"responseSchema": sample_rb_schema}

        assert _extract_response_schema(PydanticLike()) == sample_rb_schema

    def test_returns_none_when_missing(self):
        assert _extract_response_schema({}) is None
        assert _extract_response_schema(None) is None


class TestFindRuleBySlug:
    def test_finds_in_plain_list(self):
        rules = [
            {"id": "1", "slug": "rule-a"},
            {"id": "2", "slug": "rule-b"},
        ]
        match = _find_rule_by_slug(rules, "rule-b")
        assert match["id"] == "2"

    def test_finds_in_wrapped_response(self):
        class Wrapper:
            data = [{"id": "7", "slug": "wanted"}]

        match = _find_rule_by_slug(Wrapper(), "wanted")
        assert match["id"] == "7"

    def test_returns_none_if_no_match(self):
        assert _find_rule_by_slug([{"slug": "a"}], "b") is None
