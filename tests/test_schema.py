"""Selected-version schema lookup through the generated SDK."""

import httpx
import pytest
from pyspark.sql.types import BooleanType, DoubleType, StringType

from rulebricks_spark import SchemaInferenceError
from rulebricks_spark._schema import _rule_schema


@pytest.mark.parametrize("version", ["latest", "2", "production"])
def test_inference_uses_selected_version_and_never_pulls_draft(api, version):
    api.handler = lambda request: httpx.Response(
        200,
        json=[
            {
                "slug": "decision",
                "published": True,
                "response_schema": [
                    {"key": "score", "type": "number"},
                    {"key": "approved", "type": "boolean"},
                    {"key": "details", "type": "object"},
                ],
            }
        ],
    )
    schema = _rule_schema("decision", **api.options, version=version)
    assert [field.dataType for field in schema] == [DoubleType(), BooleanType(), StringType()]
    assert len(api.requests) == 1 and api.requests[0].url.path.endswith("/admin/rules/list")
    assert dict(api.requests[0].url.params) == {"slug": "decision", "version": version}


@pytest.mark.parametrize(
    "rule",
    [
        {"published": False, "response_schema": [{"key": "draft", "type": "string"}]},
        {"published": True, "response_schema": None},
        {"published": True, "response_schema": [{"name": "Display label", "type": "string"}]},
    ],
)
def test_missing_execution_schema_is_not_invented(api, rule):
    api.handler = lambda request: httpx.Response(200, json=[{"slug": "decision", **rule}])
    with pytest.raises(SchemaInferenceError):
        _rule_schema("decision", **api.options)
    assert len(api.requests) == 1
