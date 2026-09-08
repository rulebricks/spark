# Business decisions in your data pipeline

Rulebricks Spark brings versioned business rules and flows to PySpark DataFrames.
Your pipeline prepares the records, Rulebricks makes the decisions, and the library
returns an outcome for each source row with the information needed to understand
what ran.

This separation is useful when a policy changes more often than the pipeline that
uses it. Eligibility criteria, routing decisions, and operational calculations can
be maintained in Rulebricks while the Spark job continues to read, process, and
write tables in the usual way. Selecting a published version makes the decision
policy part of the job's configuration.

## Begin with a table

For independent records, start with `apply_rule()` or `apply_flow()`. Both preserve
your source columns, divide the work into bounded synchronous requests, and keep
successful results alongside failed records. A failed calculation in one position
of a successful bulk response does not discard the other positions.

The [getting started guide](getting-started.md) takes you from a source table to
saved decisions. The [notebooks](notebooks.md) provide complete examples you can
read here or download, beginning with bulk rules and flows before introducing
incremental processing and shared state.

## Grow the pipeline when the problem calls for it

A fixed batch and an ongoing stream use the same execution functions. Structured
Streaming supplies each micro-batch as a DataFrame, so you can retain Spark's
source controls and checkpointing without learning another streaming framework.
When inputs arrive in phases, a Rulebricks context can hold them until a decision
job is ready. When events depend on earlier events, a flow can read and update
keyed state while Spark keeps each key's events in order.

These capabilities have different operational boundaries. A saved decision table
is durable evidence of a run, while a context is an expiring operational store.
A Spark checkpoint tracks source progress, while an HTTP request can still be
repeated after a task failure. The guides explain those boundaries where they
matter, and the reference records the precise controls and result fields.

```{toctree}
:maxdepth: 1
:caption: Learn

getting-started
concepts
notebooks
```

```{toctree}
:maxdepth: 1
:caption: Build a pipeline

execution-model
working-with-results
streaming
contexts
reliability-and-audit
```

```{toctree}
:maxdepth: 1
:caption: Reference

reference/execution
response-configuration
reference/contexts
reference/recovery
reference/metadata
reference/sql
reference/errors
```

```{toctree}
:maxdepth: 1
:caption: Project

contributing
```
