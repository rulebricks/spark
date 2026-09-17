# Context operations

Context functions live under `rulebricks_spark.contexts`. Setup runs immediately
on the driver, while ingestion, lookup, and ordinary ordered flow processing return
lazy batch DataFrames. Opting into a `Checkpoint` executes eagerly and returns
saved results. The [context guide](../contexts.md) explains when to choose each
operation and how its state differs from a durable decision table.

## Create or reuse a definition

```{eval-rst}
.. autoapifunction:: rulebricks_spark.contexts.ensure
```

`name` is the exact context name to create or reuse. `schema` accepts a Spark
`StructType` for base facts, or the SDK's `ContextSchema` for defaults, required
facts, dotted paths, and derived expressions. `key` names a string or numeric
identity fact in that definition. The return value is the server-assigned slug.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `api_key`, `base_url` | Required | Admin credentials and API URL ending in `/api/v1` |
| `auto_execute_decisions` | `False` | Desired automatic decision policy |
| `ttl_seconds` | `86400` | Instance retention in seconds |
| `history_limit` | `100` | Fact-history retention count |
| `timeout_seconds` | `300` | Timeout for setup requests |
| `request_options` | None | Additional headers and TLS `verify`; query and body overrides are rejected |

Existing definitions must match the requested settings and fields; additional
facts are allowed. Ambiguous names or incompatible definitions raise an error
instead of changing an asset. Creation is not atomic by name, so provision from
one driver or deployment step. The key needs admin read and create permissions.

## Stage or merge data

```{eval-rst}
.. autoapifunction:: rulebricks_spark.contexts.ingest
```

`df` is the batch source and `context` is the context slug. `key` names the source
identity column; `input_mapping` can rename it to the stored identity fact.
The returned DataFrame preserves each source record and appends its JSON receipt
and context metadata. Records contributing to the same instance share its receipt,
aligned by the server's explicit positions.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `input_columns` | All columns | Fields included in each submission |
| `input_mapping` | None | Source-to-context fact names |
| `order_by` | None | Ascending source columns defining merge order within each key |
| `execution_partitions` | `1` | Positive partition count; equal keys stay together |
| `max_batch_bytes`, `max_batch_rows` | 5 MiB, `1000` | Limits per ingestion request |
| `include` | None | Additional comma-separated API response fields, such as `state` or `execution_results` |

There are no HTTP retries or failure splits. Individual oversized records become
local errors. Context policy may trigger bound decisions and cascades; use a
dedicated context without automatic or pending execution for pure staging.

## Read stored state

```{eval-rst}
.. autoapifunction:: rulebricks_spark.contexts.lookup
```

`df`, `context`, and `key` identify the source and the instances to read. Lookup
selects distinct keys, performs one GET per distinct key, and joins the response
back to every source row. Its JSON keeps stored `state` and computed `derived`
facts separate. Missing instances have a `not_found` error.

`execution_partitions` defaults to `1`, `max_retries` to `3`, and `include` to no
additional fields. Reads are live observations rather than a transactionally
consistent snapshot across instances. Save the returned DataFrame once when
consumers need the same observed values.

## Execute ordered stateful events

```{eval-rst}
.. autoapifunction:: rulebricks_spark.contexts.apply_flow
```

`flow` is a regular flow slug. `key` names the source state owner, while the required
`order_by` sequence defines a unique, non-null order within each key. The flow's
Context Operation steps read and write that state. No context binding is required. Start Spark with `spark.speculation=false`,
`spark.task.maxFailures=1`, and `spark.stage.maxConsecutiveAttempts=1`; the function
checks these settings before executing. Repeated actions and restarted jobs can
still repeat writes.

The function defaults to one record per request. `max_batch_rows` enables ordered
bulk requests, with no HTTP retries or failure splitting.
A known event failure blocks later events for that key; an uncertain request stops
the remaining partition. The return value contains normal flow JSON and metadata,
not a context write receipt. JSON is the default; `output_schema` and
`response_options` follow ordinary flow execution semantics.

`version` defaults to `"latest"`, `execution_partitions` to `1`, and
`max_batch_bytes` to 5 MiB. `input_columns`, `input_mapping`, and
`correlation_id_path` behave as in bulk execution. Use a separate event correlation
ID when several events share one state owner. There is no `payload_column` or
`input_validation` option on this ordered API; prepare and validate the source
with Spark before calling it.

## Checkpoint plain state

```{eval-rst}
.. autoapiclass:: rulebricks_spark.contexts.Checkpoint
   :members: state
```

Pass `checkpoint=Checkpoint(path, context, context_key="context_id",
events_per_key=100_000)` to `contexts.apply_flow()`. `path` is a new caller-owned
Delta storage root; `context_key` identifies an existing string source column
separate from the business key. `events_per_key` sets the number of events per
owner in each checkpoint window independently of HTTP batch size.

This mode requires a pinned numeric flow version, Delta Lake, exclusive state
ownership, disabled Spark speculation/task and stage retries, and plain base state without
tracked history, derived fields, relationships or bound decisions. Every Context
step must use the declared input key; other stateful and external effects cannot
be recovered. See the [checkpoint guide](../contexts.md#save-recoverable-plain-state-in-your-own-storage).

`resume(checkpoint.path, api_key=..., base_url=...)` reconstructs the last committed
state under fresh physical IDs and replays unfinished windows. After completion,
`checkpoint.state(spark)` reads the saved business key, current physical
`context_key`, and full `state` JSON. Source columns in results retain their
original values. The state view reads caller storage rather than live Contexts.

## Shared options and validation

All DataFrame context operations require `api_key` and `base_url`, and accept
`timeout_seconds=300`, `result_column="rulebricks_result"`,
`metadata_column="rulebricks_meta"`, and `request_options=None`. Custom output
names must be new and distinct. Headers and extra queries follow the SDK; `verify`
controls TLS certificate verification. The library retains control over alignment,
retries, and required response fields.

Source keys must be strings or integers. Null and empty identities become local
errors; ingestion and ordered processing reject numeric identities outside JSON's
exact integer range. Use strings for larger identifiers. Streaming DataFrames
must be handled through `foreachBatch`, and ordering across jobs remains the
caller's responsibility.
