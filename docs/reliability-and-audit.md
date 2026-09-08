# Reliability and audit

A large decision job needs more than a count of successful requests. It needs to
account for each source record, preserve the policy that was selected, and leave
enough evidence to investigate failures without reconstructing the original data.
Rulebricks Spark keeps those concerns together in the result DataFrame, while
leaving the choice of storage and recovery procedure with the pipeline.

## Save the observation before acting on it

Write an execution result once and read it back before performing multiple audits
or downstream transformations. Re-evaluating a lazy DataFrame can repeat external
requests, and a cache is not a durable boundary because Spark can evict blocks.
A reliable checkpoint can sever lineage, but it still needs a durable checkpoint
location and an appropriate operational policy.

The result retains source columns and metadata for every row, including failed
records. `operation_id` identifies the Spark transformation; the selected asset,
requested version, timestamps, and available execution IDs identify what was run.
A flow execution ID and an HPS trace ID serve different purposes and are nullable
when absent. The [metadata reference](reference/metadata.md) describes the fields.

Use one result location per independent operation when `resume()` is part of the
recovery procedure. Its contract is stored in Spark field metadata, which cannot
represent a different contract for each operation appended to one table. You can
combine completed results later for analysis.

## Distinguish a failed request from a failed decision

A transport failure or infrastructure response affects the whole request. There
may be no aligned result data, and a timeout can leave server execution uncertain.
By contrast, a standard calculation error returned inside an HTTP 200 array is
an outcome for one record. The neighboring records remain successful.

A response with the wrong number of items is a protocol error because the library
cannot safely match its contents to the source. It does not drop extra items or
invent missing responses. A callback failure or typed conversion mismatch is
local interpretation of a received item, which is why those errors do not cause
another HTTP request by themselves.

## Recover deliberately

The default execution policy retries transient request failures within a bounded
attempt count. When the first pass is complete, `resume()` can re-execute selected
failures from one saved operation while retaining successful rows and prior
attempt information.

```python
from rulebricks_spark import resume

saved = spark.read.format("delta").load(result_path)
recovered = resume(saved, api_key=api_key, base_url=base_url)
recovered.write.format("delta").mode("errorifexists").save(recovery_path)
```

Recovery selects network, throttling, infrastructure, and server errors by default.
Schema mismatches and callback failures usually require a code or contract decision,
so they are not selected automatically. Supply the original callbacks and request
options when they were part of the first execution; the saved contract records
their presence without storing credentials or executable code.

`resume(saved_dataframe)` rejects live execution lineage, mixed operations,
incompatible source schemas, context writes, and ordered stateful flow runs.
For supported plain-state flows, `resume(checkpoint.path)` instead recovers from
a caller-owned [Context checkpoint](contexts.md#save-recoverable-plain-state-in-your-own-storage).
See the
[recovery reference](reference/recovery.md) for its validation and options. Recovery
against `latest` or another mutable alias can use a different asset release; the
library records the requested selector because execution does not return the
resolved immutable version.

## Account for replay and side effects

Spark may replay a task, speculation can run a duplicate task, and an HTTP timeout
does not guarantee that Rulebricks stopped work. Correlation IDs improve tracing
but do not enable automatic server deduplication. For flows with side effects,
choose replay-safe effects or disable HTTP retries and failure bisection while
still accounting for Spark replay.

Stateful context writes need reconciliation when their outcome is uncertain.
A receipt can confirm that a write committed even if a later decision failed.
Ordered flow execution can identify the failed event but cannot roll back its
internal writes. Inspect current state and execution history before deciding which
events to submit again. The [context guide](contexts.md) describes how later events
are blocked to limit further work after a failure.

Structured Streaming adds source checkpoints and sink transactions, but those do
not include an external Rulebricks request. The [streaming guide](streaming.md)
explains the distinction and shows how to choose an output layout for its recovery
requirements.
