# Work with the results in Spark

A decision result belongs to your pipeline. You may need only one routing field,
a typed table for downstream joins, or the entire response for later investigation.
The library leaves those choices with Spark: retain JSON when the shape can change,
and use familiar column expressions or a supplied Spark schema when you need
structured output.

## Preserve the response before interpreting it

Flows return JSON by default because their branches can produce different fields.
Once the result has been written and read back, `get_json_object`, `from_json`,
`select`, and ordinary validation expressions can shape it without another request.
For a flat response containing a decision and a score, a typed view might look like
this:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

schema = StructType().add("decision", "string").add("score", "double")
parsed = saved.withColumn("decision", F.from_json("rulebricks_result", schema))

ready = parsed.filter("rulebricks_meta.status = 'ok'").select(
    "request_id", "decision.decision", "decision.score"
)
```

The original JSON and execution metadata remain in `parsed`. Spark's JSON parsing
has its own permissive and fail-fast behavior; a successful parse is not a complete
business validation. Add explicit expressions for fields that must be present or
within a range, and keep those checks separate from whether the Rulebricks request
succeeded.

If a flow wraps its business result in a `data` object, select that object first
with `get_json_object("rulebricks_result", "$.data")`, or describe the nested
structure in the Spark schema. Extra fields can remain in the original JSON until
a downstream consumer needs them.

## Choose typed output during execution

For a rule, the selected published version's response schema supplies the default
Spark result type. Use `output_schema` when you want direct control. The same
option gives a flow a fixed output type, including structs, arrays, and maps.

```python
from rulebricks_spark import apply_flow

results = apply_flow(
    source,
    flow_slug,
    version=flow_version,
    api_key=api_key,
    base_url=base_url,
    output_schema=schema,
)
```

A value that cannot fit the requested type becomes a row-level `schema` error by
default, preserving healthy neighboring results. `ResponseOptions` provides
permissive null substitution, raw JSON retention, and optional callbacks when
interpretation needs to happen during execution. See the
[response reference](response-configuration.md) for their exact behavior.

For entirely dynamic rule outputs, use `ResponseOptions(result_format="json")`
to skip output inference and shape the saved result with Spark.

## Check the inputs that matter to the job

Input shaping belongs in Spark too. Select the fields you intend to send, rename
source columns with `input_mapping`, or pass a struct or JSON string through
`payload_column`. The [execution reference](reference/execution.md) describes
those options and their combinations.

If you want a structural check against the published asset before execution,
set `input_validation="strict"` or `"warn"` on `apply_rule()` or `apply_flow()`.
The check loads the same version selector used for execution and compares declared
paths and compatible Spark types after selection and mapping. Strict mode stops
preparation; warn mode reports the mismatches and continues. The default is `none`.

This check examines the DataFrame schema, not the rows. It requires every declared
input, including fields with defaults, because the listing does not expose the
effective requiredness policy. It cannot inspect properties inside JSON strings
or establish that values are non-null. When every record must pass before anything
is sent, validate a stable input snapshot with your own Spark expressions first.
