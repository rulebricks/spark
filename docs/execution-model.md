# Execution model

## Spark partitions and Rulebricks requests

`apply_rule()` and `apply_flow()` use `mapInArrow`. Each Spark task reads rows
from one partition without converting nullable integer IDs through pandas,
encodes them into size-bounded requests, calls Rulebricks, and yields aligned
output rows.

Without a row budget, `execution_partitions` sets the nominal number of
execution tasks and each task runs one request at a time. Omitting the options
retains the historical default of one partition. Explicitly passing
`execution_partitions=None` preserves the input's partition count.

## Row budget

`max_inflight_rows=B` enables concurrent execution for independent rows.
Let `M = max_batch_rows` (infinity when omitted or `None`) and
`L = max_batch_bytes` (64 MiB by default):

```text
sum(inflight request rows) <= B
batch rows <= min(M, available task rows, max(1, rows fitting in L bytes))
nominal manual requests = ceil(B / M)     # integer M; e.g. 128 / 4 = 32
```

- Omit `max_batch_rows`: automatic batching.
- Supply an integer: manual batching with that row ceiling.
- Supply `None`: manual batching without a row ceiling.

Both modes refill individual requests and retain allowances through retries and
splitting. Byte limits do not disable tuning. `apply_rule()` and DataFrame
`resume()` use the same model; use `foreachBatch` for streaming input.

Without `max_inflight_rows`, the default remains one request at a time per task
and 1,000 rows per request.

## Partition planning

The default Spark task count follows the initial request plan:

```text
m = min(4, B) in automatic mode; min(M, B) in manual mode
q = ceil(B / m)
p = min(ceil(q / 4), floor(B / m), Spark default parallelism)
sum(task row allowances) = B
```

`execution_partitions` overrides `p`; `None` preserves input partitions. The count
must be at most `B`. Each task has at most 32 request threads. Allowances remain
with their tasks when partitions finish. Byte limits and partition tails can
shorten batches; downstream `coalesce()` can reduce active execution tasks.

## Automatic batch tuning

For task allowance `R`, newly prepared row count `n`, and average encoded row
size `s`:

```text
weight = 1 - exp(-n / R)
mean_bytes += weight * (s - mean_bytes)
request_seed = min(R, clamp(ceil(R * (mean_bytes + 1) / 256 KiB), 2, 4))

minimize sum(reserved_rows * request_seconds) / correctly_completed_rows
```

The mean starts with the first batch and reuses encoded byte counts. Rows are
counted once. Actual byte packing reacts immediately to larger or smaller rows.
A doubling or halving of the mean after an allowance of new rows reopens timing
comparisons around the working plan.

Accept a batch-size change only after repeated timing comparisons show over 10%
improvement beyond observed variation. Trial work is bounded; errors or retries
stop tuning. Successful rows are never replayed. Short jobs may keep their
starting plan; tuning is local to each task.

## Measuring and bounding concurrency

Compare correctly completed rows per second, worker CPU time per correct row,
request latency, retries, and CPU distribution across nodes. For a controlled
batching comparison, hold requests multiplied by rows per request constant.
Larger batches can keep completed items waiting for the slowest item in their
request; smaller batches add request overhead. A larger row budget can increase
queueing and memory without improving throughput once the pool is busy. Measure
the complete job, including its final tail, and track asynchronous log/trace
drain separately from execution time.

These are client-side plans for one execution, not cluster-wide admission
limits. Multiple jobs have independent budgets. A client timeout does not cancel
work already accepted by the service, so a retry can overlap the original
server execution while retaining the same client allowance. Spark task retries
and speculation can duplicate external requests and temporarily exceed the plan.
Use stable correlation IDs, disable speculation where appropriate, and design
flow side effects to be idempotent. Retries, backoff, and recursive splitting
retain their original row allowance until the chunk completes.

For ordered state changes, use `contexts.apply_stateful_flow()`. The row-budget
modes are for independent rows and do not preserve cross-request event order.
The planner logs the partition count and per-task budget at INFO through
`rulebricks_spark._apply`. Tasks log a summary at INFO through
`rulebricks_spark._adaptive`; detailed timing measurements require DEBUG.

## Request sizing

`max_batch_bytes` defaults to 64 MiB. Without a row budget, `max_batch_rows`
defaults to 1,000; `None` removes that row limit. Endpoint and proxy limits still
apply. A single row larger than the byte ceiling is sent alone so the service
can return a precise payload-size error.

Rulebricks Spark automatically splits a rejected oversized request. It also
uses bounded bisection for HTTP 500 failures when `isolate_failures=True` so
one failing row does not erase healthy results from the same original request.

## Response alignment

A successful bulk response must contain exactly one item for each input item.
Items are matched by position. A shorter or longer response is a protocol
error for the complete request. Extra items are never discarded silently.
With a row budget, completed chunks can be emitted out of input order,
while each result remains attached to its original source row and metadata.
Apply an explicit downstream sort if you need ordered output. Response callbacks
can run concurrently on executor threads and must be safe for concurrent use.

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

Arrow decoding uses slices of at most 1,000 rows, further bounded by the task
allowance in row-budget mode. Request packing follows the configured row and
byte limits. Payload bytes exclude unused source columns and response expansion.
Select only necessary source columns for wide datasets and tune Spark's
`spark.sql.execution.arrow.maxRecordsPerBatch` separately.

In concurrent mode, each task retains at most its assigned number of pending
chunks (including completed results awaiting consumption), plus the current
output chunk and the bounded input decoder/chunker. Each request thread reuses
its own HTTP client. Per-task memory grows with its request slots, request size,
source columns, and response size; the byte limit is not a total memory limit.
Downstream backpressure prevents unbounded request or result queues.

Retries, retry delays, and failure-isolation splits stay inside their original
slot. With `on_error="raise"`, an observed failure stops new submissions, but
already-issued requests may have executed. Closing the iterator cancels queued
work and retry waits, then waits for running HTTP calls before closing their
clients. It cannot cancel server-side execution. A blocking HTTP call can still
take its configured timeout to return. Existing callers that omit `max_inflight_rows`
retain their sequential request and response-callback behavior.

Timestamps with a timezone preserve their UTC instant. `TimestampNTZType`
values keep their wall-clock value without acquiring an invented timezone.
Nullable long IDs retain their precision. Decimal strings can be projected
to DecimalType without passing through binary floats.

The synchronous request timeout is not a guarantee that server-side execution
has stopped. Disable automatic retries (`max_retries=0`) and server-error
bisection (`isolate_failures=False`) if repeated flow side effects are unsafe.
Correlation IDs provide traceability; they do not enable server-side deduplication.
