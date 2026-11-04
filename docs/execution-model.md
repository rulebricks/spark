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

For independent input rows, choose the maximum planned number of outstanding
rows. The library automatically chooses batching and Spark partitioning:

```python
result = apply_flow(
    df,
    "decision-flow",
    api_key=api_key,
    base_url=base_url,
    max_inflight_rows=128,
)
```

If you have measured a preferred batch size, supply `max_batch_rows` too:

```python
result = apply_flow(
    df,
    "decision-flow",
    api_key=api_key,
    base_url=base_url,
    max_inflight_rows=128,
    max_batch_rows=4,
)
```

The second example selects **manual batching**, with a nominal plan of
**32 requests × 4 rows** (`max_inflight_rows / max_batch_rows` for full batches).
The library does not automatically tune the batch size. Both modes admit
replacement work as individual requests finish, retaining each request's row
allowance through retries and splitting. Neither mode waits for a group of
requests to finish before refilling.

| Arguments | Behavior |
| --- | --- |
| `max_inflight_rows` alone | Automatically choose partitioning and tune batching |
| `max_inflight_rows` plus explicit `max_batch_rows` | Use the supplied batch ceiling without tuning; derive partitioning from the request plan |
| `max_inflight_rows` plus `execution_partitions` | Override Spark producer count; batching still tunes unless `max_batch_rows` is explicit |
| No `max_inflight_rows` | Preserve one request at a time per task, the default one partition, and the default 1,000-row ceiling |

`max_batch_bytes` remains a request ceiling, defaulting to 64 MiB; setting it
does not disable tuning. Explicit `max_batch_rows=1000` selects manual batching
even though its value equals the legacy default. Explicit `max_batch_rows=None`
also selects manual batching, retaining the existing 10,000-row safety cap.
Omitting `max_batch_rows` in automatic mode retains the 1,000-row safety ceiling.

`apply_rule()` and DataFrame `resume()` accept the same options. Recovery chooses
a fresh plan for selected failed rows; it does not inherit the original tuning
mode or concurrency budget. Execution remains lazy. Use `foreachBatch` for
streaming input.

## Partition planning

Normal callers can leave `execution_partitions` omitted. The SDK first forms a
request plan from the row budget and batch size, then distributes it across
Spark producers. Automatic mode uses four rows as an initial planning estimate;
manual mode uses the explicit ceiling, clipped to the row budget and the internal
10,000-row safety cap. The plan contains full requests plus at most one partial
request for any remainder.

The planner aims for four overlapping requests per task, capped by Spark's
default parallelism and the number of full requests. It does not create a task
solely for the partial remainder. For example, with sufficient Spark parallelism,
128 rows with four-row batches plans 32 requests across eight tasks; 128 rows
with 32-row batches plans four requests in one task. These are initial plans,
not estimates of Rulebricks worker count or a guarantee of optimal throughput.

Manual task allowances preserve full batches wherever the producer count permits.
For example, 128 rows with four-row batches and six explicit partitions gives
allowances of 24, 24, 20, 20, 20, and 20 rows: exactly 32 full request slots.
Automatic mode distributes its initial request plan in the same way, then lets
each task tune within its fixed allowance. The allowances always sum to the
configured row budget.

An explicit `execution_partitions=N` uses N producers; `None` preserves the input
partition count. Either count must fit within the row budget. An override with
more producers than full requests splits the allowance into smaller batches so
every input partition can progress. Byte limits, local validation failures, and
partition tails can also shorten requests. Leave partitioning automatic when
preserving the measured batch size matters.

Each task has at most 32 concurrent request threads. Spark's default parallelism
is a planning hint, and actual outstanding work can be lower when fewer tasks
run, input is skewed, retries back off, or partitions finish. Finished partitions
do not lend their allowances to remaining tasks. A very small producer override
can also prevent the thread limit from filling a large row budget.

Downstream `coalesce()` can reduce simultaneously running execution tasks. To
change output file counts without constraining API parallelism, save results
first and compact the saved data. Spark's Arrow buffers are separate from the
row allowance; size `spark.sql.execution.arrow.maxRecordsPerBatch` for executor
memory when payloads are large. Spark needs resources to prepare requests and
consume results, and competes with Rulebricks when they share nodes.

## Automatic batch tuning

When `max_batch_rows` is omitted, each task derives its initial concurrency from
the four-row request plan, subject to the 32-thread cap. It uses at least four
overlapping requests when its allowance permits, or more if byte ceilings require
it, dividing its allowance into balanced slots. For example, a 16-row task allowance normally starts as
four four-row requests. It can try larger batches with fewer requests or smaller
batches with more requests, keeping its allowance fixed. There are at most two
distinct candidate configurations per task. Requests refill individually
throughout the search, without replaying successful rows.

The score is **reserved row-seconds per correctly completed row**. Lower scores
mean rows occupy the allowance for less time. To avoid changing settings because
of unusually easy input or transient latency:

- Ignore initial and configuration-change warmup, mixed-generation completions,
  and partially filled final batches.
- Collect at least four groups of completions per measurement window. Each group
  contains at least one task allowance of completed rows and four completed
  requests. Slow requests remain in the overall score and in their group.
- Check the original configuration, the candidate, the original again, and the
  candidate again (**A-B-A-B**). Both candidate windows must improve the overall
  score by more than 10% against both baseline windows.
- Also require the candidate's upper quartile of group scores to be more than
  10% below the baseline's lower quartile in each comparison. Overlapping groups
  are inconclusive, even if their average scores look better.
- Reject inconclusive candidates and keep the last accepted configuration.
  Failures or retries stop exploration and restore that configuration; normal
  retry, backoff, splitting, and error reporting still apply.

These are conservative empirical guards, not confidence intervals or a guarantee
of a global optimum. Different windows execute different rows, and concurrent
requests share backend capacity. Changing input mix or correlated load can
confound measurements, so the search can reject a real but noisy improvement.
Measurement sizes depend on completed work, rather than assumed processing time.
Probes can slow shorter jobs; there is no persistent cross-job tuning cache or
extra driver service. Supply `max_batch_rows` when you prefer a measured setting.

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
`rulebricks_spark._apply`. Each task logs its selected request width and
measurement summary through `rulebricks_spark._adaptive` at INFO.

## Request sizing

The default request limits are:

- 64 MiB encoded JSON
- 1,000 input rows

These are ceilings; automatic mode can choose smaller requests. In manual or legacy mode, the first limit reached closes the request, also respecting
any remaining task row allowance. Use `max_batch_bytes` and
`max_batch_rows` to override the defaults. The byte default was previously 5 MiB;
set `max_batch_bytes=5 * 1024 * 1024` explicitly to retain that packing limit.
Endpoint and proxy limits still apply: a 64 MiB SDK ceiling does not configure
the server to accept that size. An individual row larger than the byte limit is
still sent alone so the service can return a precise payload-size error.

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

The worker decodes at most 1,000 Arrow rows at a time and retains at most
10,000 records per HTTP chunk, even if `max_batch_rows=None`. Both HTTP limits
are upper bounds; partitions and the internal buffer cap can produce smaller
requests. Payload bytes exclude unused source columns and response expansion.
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
take its configured timeout to return. Existing callers that omit the target
retain their sequential request and response-callback behavior.

Timestamps with a timezone preserve their UTC instant. `TimestampNTZType`
values keep their wall-clock value without acquiring an invented timezone.
Nullable long IDs retain their precision. Decimal strings can be projected
to DecimalType without passing through binary floats.

The synchronous request timeout is not a guarantee that server-side execution
has stopped. Disable automatic retries (`max_retries=0`) and server-error
bisection (`isolate_failures=False`) if repeated flow side effects are unsafe.
Correlation IDs provide traceability; they do not enable server-side deduplication.
