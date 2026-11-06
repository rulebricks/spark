# Stage data and process stateful decisions

Many decisions are independent: an order arrives with everything needed to choose
its route, and Rulebricks returns the answer. Other decisions depend on facts that
arrive separately, or on what happened before. A context gives those facts a place
to live inside Rulebricks, where flows can read and update them.

For a data pipeline, the two useful patterns are **input staging** and **stateful
processing**. Staging collects inputs before a decision job consumes them. Stateful
processing lets each event depend on the state left by an earlier event. Start with
ordinary bulk execution when neither pattern is needed.

| Your pipeline needs to… | Use… | Walkthrough |
| --- | --- | --- |
| Combine separately arriving facts before evaluating records | Ingestion, lookup, then a bulk flow | [Stage data](_notebooks/04-staging-data.ipynb) |
| Evaluate events against an account's changing balance or eligibility | An ordered flow that reads and writes context state | [Process stateful events](_notebooks/05-stateful-processing.ipynb) |

_While data can be stored in Rulebricks, Rulebricks should *not* be used as a 
general purpose staging environment for data not on actively processed decision 
paths._

## Choose what one stored record represents

A context definition describes the facts and retention policy for a named store.
An instance holds those facts for one business identity. A request ID therefore
creates a separate instance for each request, while an account ID lets many events
share an account's state. Missing instances are created when data is submitted.

This choice follows the business meaning of the data. For example, a staged loan
application should have its own identity even when several applications belong to
one customer. An account balance should instead be shared by that account's events.
A constant key is appropriate only when all events really share one state object;
it also makes ordered processing serial.

An instance is a bounded JSON object, not an append-only table. Repeated properties
replace earlier values, and lists are replaced rather than concatenated. Keep
large datasets and durable event history in your lake or warehouse. Context
instances have configured expiry and a 64 MiB server limit.

## Prepare a store

`contexts.ensure()` creates a context definition when an exact name does not exist,
or checks that an existing definition is compatible. A Spark schema is enough for
ordinary base facts:

```python
from rulebricks_spark import contexts

context_slug = contexts.ensure(
    "Staged customers",
    schema=source.select("customer_id", "country", "balance").schema,
    key="customer_id",
    api_key=api_key,
    base_url=base_url,
    ttl_seconds=86400,
)
```

Setup runs immediately on the driver and returns the server-assigned slug for later
operations. It requires admin read and create permissions. It checks the requested
facts, identity, retention, and execution settings before reusing a definition;
ambiguous or incompatible definitions produce an error instead of being modified.
Provision from one driver or deployment step because creation by name is not atomic.

The default is a one-day store with automatic decision execution off. Spark schemas
describe fact types; they do not specify required facts or defaults. For those,
pass the generated SDK's `ContextSchema`. The stateful notebook uses it to give a
new account a default balance without resetting an existing account.

## Stage inputs, then run a flow

Suppose one dataset supplies each customer's country and another supplies a balance.
Ingest each contribution under the same customer key, save its receipts, and check
that the writes completed before starting the next stage:

```python
receipts = contexts.ingest(
    source,
    context_slug,
    key="customer_id",
    input_columns=["customer_id", "country", "balance"],
    api_key=api_key,
    base_url=base_url,
    execution_partitions=4,
)
receipts.write.format("delta").mode("errorifexists").save(receipts_path)
saved_receipts = spark.read.format("delta").load(receipts_path)
```

Every input row is retained with its receipt. When several rows contribute to one
instance, the library uses the server's explicit positions to associate the receipt
with all contributing rows. `rulebricks_meta.status` describes the overall outcome,
while `write_status` tells you whether the write was committed, rejected, not sent,
or uncertain. A committed write can still have a later execution failure.

Use a dedicated context without automatic execution, pending evaluations, or
dependent work when staging should only store data. Ingestion can otherwise trigger
bound decisions and cascades according to the context's policy. Such bindings are
useful when the context itself should decide when enough information has arrived;
the staging notebook keeps the decision job explicit.

Once contributions are complete, send the instance keys to a flow whose Context
Operation steps read the staged facts. This keeps repeated data out of flow
payloads. `contexts.lookup()` can also retrieve state into Spark for inspection or
other transformations. The [staging notebook](_notebooks/04-staging-data.ipynb)
walks through execution and checks the saved state separately.

Lookup uses one GET per distinct key, so reduce the key set before reading a large
store. The returned JSON keeps stored `state` and computed `derived` facts separate.
Missing instances become `not_found` errors. Reads reflect live state rather than
one consistent snapshot across all instances; save them once when downstream
consumers need the same observed values.

## Let a flow read and update state

For an account ledger, each event must see the previous event's completed update.
`contexts.apply_flow()` groups events by state owner, orders them within that key,
and submits one event at a time by default:

```python
ledger = contexts.apply_flow(
    events,
    "account-balance",
    version="3",
    key="account_id",
    order_by=["event_time", "event_id"],
    correlation_id_path="event_id",
    input_columns=["account_id", "event_id", "amount"],
    execution_partitions=4,
    api_key=api_key,
    base_url=base_url,
)
ledger.write.format("delta").mode("errorifexists").save(ledger_path)
```

This uses the regular flow endpoint. Context Operation steps inside the flow read
state, calculate the next value, and write it before returning. No context binding
is required. Parallelism comes from independent state owners. Set `max_batch_rows`
to use the server's ordered bulk execution; each partition waits for its current
bulk response before sending another request. The ordering columns must define a
unique, non-null ascending order within each key. An error inside a bulk can be
reported after later events in that same bulk already ran; the SDK stops the
remaining partition instead of assuming those events were rolled back.

Batches alternate keys: `A1, B1, C1, A2, B2, C2, ...`. Each key retains its
`order_by` sequence; independent keys can share a request and run concurrently.

The result is the ordinary flow response, with execution IDs and the requested
version. It is not a context write receipt and cannot establish which writes
occurred inside a failed flow. A known event failure blocks later events for that
key. An HTTP or protocol failure leaves execution uncertain and stops the remaining
partition; other partitions may already be working. Blocked rows remain in the
output with `context_blocked` errors and zero attempts.

## Preserve ordering when something goes wrong

Context ingestion and ordered execution do not automatically retry HTTP requests
or split failed chunks. A timeout does not establish that a write was rolled back.
After an uncertain ingestion request, later chunks in that partition are retained
as blocked rows. Read-only lookup supports bounded transient retries.

For ingestion, `order_by` defines the order of patches sharing a key. Without it,
conflicting updates follow Spark's unspecified arrival order. Ingestion merges a
chunk before evaluating decisions, so it does not provide event-by-event execution.
Use the ordered flow API when every intermediate state matters.

Ordered flow execution requires `spark.speculation=false`,
`spark.task.maxFailures=1`, and `spark.stage.maxConsecutiveAttempts=1` before any
flow calls. This disables automatic task and stage retries. Rerunning an action
or restarting a job can still repeat external writes. Use exclusive writers for
overlapping keys and design flow effects to tolerate replay. Reconcile current state and execution
history before resubmitting uncertain events. `resume(saved_dataframe)` rejects
context receipts and ordered stateful runs because retrying isolated failed rows
cannot perform that reconciliation.

These operations also work inside `foreachBatch`, where each micro-batch is a
normal DataFrame. External state does not participate in Spark's checkpoint
transaction. The [API reference](reference/contexts.md) covers every parameter,
and the [metadata reference](reference/metadata.md) describes the saved evidence.

## Save recoverable plain state in your own storage

For a finite input snapshot and a flow using one plain Context, opt into a
caller-owned Delta checkpoint:

```python
checkpoint = contexts.Checkpoint(
    path="abfss://pipeline@storage.dfs.core.windows.net/ledger/run-2026-09-08",
    context="account-balances",
    context_key="context_id",
    events_per_key=100_000,
)
ledger = contexts.apply_flow(
    events,
    "account-balance",
    version="3",
    key="account_id",
    order_by=["sequence"],
    checkpoint=checkpoint,
    max_batch_rows=512,
    execution_partitions=32,
    api_key=api_key,
    base_url=base_url,
)
```

The source's `context_id` column identifies an existing instance for each account;
it is separate from the business key. Every Context step, including subflows,
must target this parameter. The SDK saves the immutable input and reads the
complete starting state before executing any events. `events_per_key` controls
checkpoint frequency independently of HTTP batch size. A window commits only
after its results and final state have been saved successfully. This opt-in call
executes immediately and returns a saved DataFrame.

After a failure, stop submitting related work and run
`resume(checkpoint.path, api_key=api_key, base_url=base_url)`. Recovery restores the
last committed window's state under fresh Context IDs and replays the whole
unfinished window and subsequent events. Effects from an uncertain attempt remain
on abandoned instances, so a delayed response cannot corrupt the replacement
state. Successful windows keep their saved results. Delta publishes the manifest
last and fences older drivers from accepting a competing window.

Use `checkpoint.state(spark)` after completion to read the business key, the
**current physical** `context_id`, and complete saved `state` JSON. Join these IDs
explicitly when starting a new job; result source columns retain their original
values. A completed `resume(path)` just reads saved results.

This requires Delta Lake on durable shared storage, exclusive access to the
declared state, a pinned flow version and unchanged dependencies. Start Spark with
`spark.speculation=false`, `spark.task.maxFailures=1`, and
`spark.stage.maxConsecutiveAttempts=1`; automatic Spark task
re-execution is rejected before another stateful request. No HTTP retries or
failure splits run. Do not run competing jobs against these instances.

Checkpoint mode supports base state only: no tracked history, derived fields,
relationships, pending/bound automatic decisions, other Context targets, or
external effects. It cannot undo those effects. Keep the Context retention longer
than the processing and recovery window. Caller storage retains inputs, attempts,
results and state; callers own its lifecycle and removal of abandoned instances.
The [fault tolerance notebook](_notebooks/06-fault-tolerance.ipynb) gives a complete
example with short setup and recovery calls.
