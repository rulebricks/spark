# Response options

`ResponseOptions` controls how a received item is interpreted during execution.
Use it when an error envelope or typed conversion should affect the row's execution
outcome. If the job only needs a different downstream view, keep the default JSON
and use [Spark expressions on saved results](working-with-results.md) instead.

```{eval-rst}
.. autoapiclass:: rulebricks_spark.ResponseOptions
   :members:
```

## Fields

| Field | Default | Behavior |
| --- | --- | --- |
| `result_format` | `"auto"` | Use an explicit schema, infer rule outputs, or keep flow JSON; `"json"` always keeps JSON and skips inference |
| `schema_mismatch` | `"error"` | Record incompatible typed values as errors; `"null"` permits null substitution |
| `raw_column` | None | Optional new column retaining each original decoded response item |
| `error_handler` | None | Optional function returning a message for a failed item or `None` for success |
| `transform` | None | Optional function selecting or reshaping a successful item before conversion |

`output_schema` cannot be combined with explicit JSON mode. JSON results may be
objects, arrays, scalars, or null, while the outer bulk response must still contain
one item per input. Raw retention happens before callbacks and typed conversion,
so it also preserves standard calculation errors and unfamiliar fields. Request
failures have no aligned item to retain and leave the raw column null.

## Interpret a flow's own error envelope

A custom error handler replaces the default check for a single-property
`{"error": ...}` object. Preserve that check in your handler if the flow can return
both standard calculation errors and its own business failure shape.

```python
from rulebricks_spark import ResponseOptions


def error_message(item):
    if isinstance(item, dict):
        if "failure" in item:
            return item["failure"]["message"]
        if set(item) == {"error"}:
            return str(item["error"])
    return None


responses = ResponseOptions(
    error_handler=error_message,
    transform=lambda item: item["data"],
    raw_column="original_response",
)
```

Pass `response_options=responses` to execution. The handler runs first; a successful
item then passes through the transform and optional type conversion. Returning a
non-JSON value or raising from a callback produces a `response_handler` error.
Those local failures do not retry the HTTP request.

Define callbacks in a notebook or application as pure, serializable Python
functions. Imported dependencies need to be available on executors. The functions
receive the decoded item rather than Spark columns, and should not make external
calls or update state. Supply the same callbacks again when using `resume()`;
Python code is never stored in the result's field metadata.

## Typed conversion

Required fields are checked before Arrow output, optional fields can be missing,
and extra properties are allowed. Compatible representations, such as an integral
numeric string for a long field, can be converted. An incompatible value becomes
a `schema` error by default; `schema_mismatch="null"` is an explicit choice to
substitute null. Output types remain nullable so failed rows can be represented.

This is conversion to a Spark type, not full JSON Schema validation. Responses
containing high-precision decimals should use strings that can be converted to
`DecimalType`. Raw retention stores normalized decoded JSON, not the exact bytes
of the HTTP response.

## Customize requests

The execution functions accept `request_options` separately from response options.
They support the SDK's `additional_headers` and `additional_query_parameters`.
Body overrides are rejected because they could invalidate alignment and byte
limits. The library retains control of timeouts and retries.

```python
request_options = {
    "additional_headers": {"X-Workload": "nightly-order-decisions"},
}
```

Headers also apply to schema lookups when those are needed. Query parameters cannot
override the asset filters used for a schema lookup. For an API key without admin
read access, supply an explicit output schema or use JSON with input preflight off.
Request headers are not stored in output metadata and must be supplied again for
recovery. A static request header is not a per-record idempotency key.
