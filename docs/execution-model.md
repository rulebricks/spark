# Execution model

## Spark partitions and Rulebricks requests

`apply_rule()` and `apply_flow()` use `mapInArrow`. Each Spark task reads rows
from one partition without converting nullable integer IDs through pandas,
encodes them into size-bounded requests, calls Rulebricks
synchronously, and yields aligned output rows.

`execution_partitions` sets the nominal number of execution tasks. One request
is active in each task at a time. A value of `1` provides the safest default
for a new deployment.

Spark can retry a failed task, and speculative execution can run a duplicate
task. Either behavior can repeat external requests or temporarily exceed the
partition count. Use stable correlation IDs, disable speculation where
appropriate, and design flow side effects to be idempotent.

## Request sizing

The default request limits are:

- 5 MiB encoded JSON
- 1,000 input rows

The first limit reached closes the request. Use `max_batch_bytes` and
`max_batch_rows` to tune them. An individual row larger than the byte limit is
still sent alone so the service can return a precise payload-size error.

Rulebricks Spark automatically splits a rejected oversized request. It also
uses bounded bisection for HTTP 500 failures when `isolate_failures=True` so
one failing row does not erase healthy results from the same original request.

## Response alignment

A successful bulk response must contain exactly one item for each input item.
Items are matched by position. A shorter or longer response is a protocol
error for the complete request. Extra items are never discarded silently.

An exact singleton `{"error": ...}` item is reserved as a per-item execution
error for rules and flows. Its result is null, while healthy neighboring items
remain successful. JSON `null` items are preserved; use a custom error handler
if your application treats them as failures.

For self-hosted flow orchestration, this behavior requires
`failedResponseMode="return"`. Other modes can cause a failed item to become a
request-level failure instead of an aligned HTTP 200 item.

## Result typing

By default, `apply_rule()` infers the selected version's output schema,
or accepts `output_schema`. Flow results default to compact JSON strings.
Both transformations accept explicit struct, array, or map schemas. Invalid
typed values produce a `schema` row error by default, preserving healthy
neighboring results.

Use `ResponseOptions(result_format="json")` with either transformation to
retain arbitrary JSON and skip inference. For permissive typed conversion,
choose `schema_mismatch="null"`. Custom error detection, response transforms,
and original-response retention are described in
[Response configuration](response-configuration.md).

Source columns are never overwritten. Custom `result_column` and
`metadata_column` names must be distinct and must not already exist in the
source DataFrame.

## Input preflight

`input_validation="strict"` fetches the selected rule or flow's request schema
once on the driver using `list(slug=..., version=...)`. It checks declared
field paths and compatible Spark types after input selection and mapping.
Missing columns and incompatible types stop preparation before execution calls.
Nested structs and literal dotted fact names are supported. A flow's input
schema comes from the selected graph's origin rule.

`input_validation="warn"` emits one driver warning and continues. Messages are
also saved as `input_warnings` in the `rulebricks_contract` field metadata.
`"none"`, the default, skips preflight. Lookup failures and ambiguous responses
remain errors in both validation modes; an unavailable contract is not a match.

Strict requires **every declared input**, including those with defaults. The
listing API does not expose effective requiredness or whether defaults are
enabled. This is an explicit caller policy, not a claim of exact parity with
server validation. Extra source fields are allowed.

Preflight reads Spark schema metadata, not records. It cannot prove that all
values are non-null, dates are valid, or map/JSON-string payloads have the
required properties. Parse those payloads into a struct before preflight.
Validate values with ordinary Spark expressions over a stable input snapshot
when the job must send nothing until every record passes.

Output inference uses the same selected-version listing, supports dotted
object fields, and leaves unstructured lists and objects as JSON strings.
Use an explicit output schema for collection-returning rules or other shapes
not represented by the field list. Neither listing returns a resolved version
number: `latest` and environment aliases remain mutable between calls.

## Memory and source values

The worker decodes at most 1,000 Arrow rows at a time and retains at most
10,000 records per HTTP chunk, even if `max_batch_rows=None`. Both HTTP limits
are upper bounds; partitions and the internal buffer cap can produce smaller
requests. Payload bytes exclude unused source columns and response expansion.
Select only necessary source columns for wide datasets and tune Spark's
`spark.sql.execution.arrow.maxRecordsPerBatch` separately.

Timestamps with a timezone preserve their UTC instant. `TimestampNTZType`
values keep their wall-clock value without acquiring an invented timezone.
Nullable long IDs retain their precision. Decimal strings can be projected
to DecimalType without passing through binary floats.

The synchronous request timeout is not a guarantee that server-side execution
has stopped. Disable automatic retries (`max_retries=0`) and server-error
bisection (`isolate_failures=False`) if repeated flow side effects are unsafe.
Correlation IDs provide traceability; they do not enable server-side deduplication.
