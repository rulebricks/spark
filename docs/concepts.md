# Decisions, data, and state

A decision engine is useful when the policy behind a result needs a lifecycle of
its own. A data pipeline may run every hour while eligibility rules change each
week, or several applications may need the same routing policy. Rulebricks keeps
that policy in a published asset, and the Spark job chooses which version to use.
The job remains responsible for selecting the source data and deciding how to use
the returned decisions.

## Rules and flows

A rule describes a decision table or set of conditions. Each published version
has defined inputs and outputs, which makes a typed Spark result a natural default.
A flow composes steps into a broader decision process. It might invoke several
rules, choose a branch, calculate a value, or read and write operational state.
Different branches may produce different response shapes, so flow results remain
JSON unless the caller supplies an output schema.

Both assets can receive a list of input objects in one synchronous request. The
library matches results by their positions in that list and retains the source
record beside the outcome. An error returned for one item remains attached to that
item; a malformed response that cannot be aligned is a request-level problem.

## A job, a micro-batch, and a chunk

The dataset selected by your pipeline is the job's workload. In an ordinary batch
job it is a bounded DataFrame. In Structured Streaming, Spark divides newly
available input into micro-batches and presents each one to `foreachBatch` as a
normal DataFrame. Within either kind of DataFrame, Rulebricks Spark groups records
into chunks, where each chunk is one HTTP request.

These levels provide different controls. A source option can limit the files or
offsets admitted to a micro-batch. The library's byte and row limits bound each
request, while execution partitions determine how many tasks can make calls at
once. A large job does not need streaming merely because it is large; streaming
is valuable when checkpointed, incremental progress fits the source and schedule.

## Stateless and stateful decisions

Independent records can be evaluated in parallel when one decision does not
change the inputs of another. That is the model used by the bulk examples. A
stateful decision is different: the current result depends on a value left by
an earlier event, such as an account balance or a running usage total.

Rulebricks contexts provide keyed operational storage for that state. A context
definition describes the stored facts and their policy; an instance is the state
for one key within it. An account ID therefore gives one instance per account,
while a constant key gives one instance shared by the whole workload. Events
sharing an instance need ordering and coordinated writers, even if Spark has spare
parallel capacity.

## Staging inputs

A context can also hold inputs before a decision run. This is staging data: an
order producer may supply an amount now, and another producer may add country
information later under the same request ID. The decision job can then read the
assembled facts and execute when it has the inputs it needs.

A context merges facts by identity. It does not behave like an append-only table,
and its instances have configured retention. Keep durable source data and event
history in your data lake or warehouse; use the context for the bounded state
that Rulebricks needs during processing.

## Execution evidence

The source record answers what was submitted. The result answers what Rulebricks
returned. Execution metadata connects those two with the asset, requested version,
timing, attempt count, and available IDs. A correlation ID comes from your data,
a flow execution ID identifies the Rulebricks run, and an HPS trace ID identifies
the backend request path. They serve different purposes and are preserved
independently when available.

This evidence helps with reconciliation, but it does not make an external request
transactional with a Spark task. A task retry can repeat a request, and a timeout
does not prove that the server stopped. [Reliability and audit](reliability-and-audit.md)
explains how to retain useful evidence and choose an appropriate recovery boundary.
