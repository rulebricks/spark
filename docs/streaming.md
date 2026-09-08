# Incremental processing with Structured Streaming

Streaming is useful when the pipeline should remember its progress through a
source that keeps growing. It also suits scheduled jobs that drain a large backlog
in bounded steps and then shut down. A fixed table of independent records can
remain an ordinary batch job; the decision about streaming should follow the
source and recovery model rather than the dataset's size alone.

Rulebricks Spark uses Spark's existing `foreachBatch` interface. The callback
receives a normal DataFrame, so the same rule and flow transformations apply.

## Separate source admission from request sizing

The streaming source determines how much input enters each micro-batch. For a
file source, `maxFilesPerTrigger` limits admitted files; other sources expose their
own controls. Inside the callback, `max_batch_rows`, `max_batch_bytes`, and
`execution_partitions` determine how that DataFrame is sent to Rulebricks.
These controls complement one another: one large file can still contain many
HTTP chunks, while several small files may produce only a few requests.

The [streaming notebook](_notebooks/03-streaming.ipynb) uses
`trigger(availableNow=True)` to drain the input available when the query starts,
possibly across several micro-batches. Spark then stops the query. This provides
checkpointed incremental processing without requiring an always-running job.
For ongoing arrivals, a processing-time trigger runs micro-batches at an interval.
This is distinct from Spark's continuous-processing mode, which does not support
`foreachBatch`. See the [Spark guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#triggers).

## Preserve one outcome per source record

Add the micro-batch ID to the source before calling the flow, and use
`input_columns` to keep it out of the request payload. It then remains beside the
result as an audit field. Write the returned DataFrame once inside the callback;
calling `count()` and then `write()` on it can evaluate the external requests twice.

A completed micro-batch may contain failed decisions because `on_error="continue"`
preserves them as rows. Inspect those outcomes in the saved table. Choose
`on_error="raise"` only when failing the Spark task, and potentially replaying its
requests, is the intended operating policy.

## Checkpoints and external effects

The checkpoint tracks Spark source progress. It does not make a Rulebricks request
part of the output transaction. If a task or micro-batch fails after Rulebricks has
responded, the request may run again before the checkpoint advances. Use stateless
decisions or make their external effects safe to replay.

The notebook appends results to Delta with a stable `txnAppId` and the micro-batch
ID as `txnVersion`. Delta uses this pair to recognize duplicate table writes.
It does not deduplicate Rulebricks execution. Keep the application ID stable for
restarts using the same checkpoint; use a new one if you replace the checkpoint
and its batch numbering begins again. These are [Delta's standard transaction
options](https://docs.delta.io/delta-streaming/#idempotent-table-writes-in-foreachbatch).

## Choose the output layout for recovery

An appended Delta table is convenient for analytics across many micro-batches.
It contains the per-row asset, version, operation ID, and error details, but its
Spark field metadata cannot hold a separate recovery contract for every operation.
`resume()` therefore cannot recover that combined table directly, even if you
filter it down to one batch after the contracts have been overwritten.

When the library's recovery helper is part of your operating procedure, write each
micro-batch to its own durable location. Read that individual result before using
`resume()`, and combine completed outputs only for analysis. Keep the source
checkpoint and any decision recovery process coordinated so they do not both
resubmit the same records independently.

Ordered stateful processing needs an additional boundary: sorting events within
one micro-batch does not establish order across late arrivals, overlapping jobs,
or retries. Use a defined business ordering and writer policy before combining
streaming input with [shared context state](contexts.md).
