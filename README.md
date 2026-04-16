# Rulebricks Spark

[![PyPI](https://img.shields.io/pypi/v/rulebricks-spark.svg)](https://pypi.org/project/rulebricks-spark/)
[![Python](https://img.shields.io/pypi/pyversions/rulebricks-spark.svg)](https://pypi.org/project/rulebricks-spark/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Apache Spark integration for [Rulebricks](https://rulebricks.com). Apply decision-table based rules to Spark DataFrames at scale.

```python
from rulebricks_spark import solve

scored = solve(
    spark.table("breach_reports"),
    rule="breach-triage",
    api_key=dbutils.secrets.get("rulebricks", "api_key"),
)

scored.display()
```

![Apply a Rulebricks rule to a Spark DataFrame — input columns in, rule output columns appended](https://raw.githubusercontent.com/rulebricks/spark/main/assets/cover.png)

Rule outputs overwrite existing columns, or automatically create new ones. Works in any PySpark environment — Databricks, EMR, Synapse, or a local cluster.

## Why

Changing business & policy logic shouldn't live in notebooks or SQL CASE statements. Rulebricks enables business users to directly author & manage decision tables in a visual UI, with versioning, audit logs, and test suites. `rulebricks-spark` then lets your Spark pipelines consume those rules at scale without wrapping the HTTP API yourself.

**Edit a rule in the Rulebricks UI. Rerun your notebook. Different results, no code change.**

## Install

```bash
pip install rulebricks-spark
```

### Databricks

In a notebook cell:

```python
%pip install rulebricks-spark
dbutils.library.restartPython()
```

Or install as a cluster library (PyPI source, package `rulebricks-spark`) so every notebook on the cluster has it available.

## Quickstart

```python
from rulebricks_spark import solve

df = spark.table("claims_raw")

scored = solve(
    df,
    rule="claims-triage",
    api_key=dbutils.secrets.get("rulebricks", "api_key"),
)

scored.write.mode("overwrite").saveAsTable("claims_scored")
```

Rule output fields are appended as new columns. A trailing `_rb_error` column is `null` on success and carries the error message on failure, so you can isolate problem rows without failing the whole job.

## How it works

1. **Driver side:** fetch the rule's response schema once via the Rulebricks SDK, build the Spark output `StructType`.
2. **Executor side:** each partition's pandas chunks are split into batches (default 500 rows) and sent to `bulk_solve`. Multiple batches per partition run concurrently in a thread pool (default 4).
3. **Result assembly:** rule outputs are coerced to the declared Spark types, collided input columns are overwritten, errors land in `_rb_error`.

Effective parallelism = `num_executors × max_concurrent_requests_per_partition`.

## API

### `solve(df, rule, *, api_key, ...) -> DataFrame`

| Argument                                | Default                  | Description                                                        |
| --------------------------------------- | ------------------------ | ------------------------------------------------------------------ |
| `df`                                    | —                        | Input Spark DataFrame.                                             |
| `rule`                                  | —                        | Rulebricks rule slug.                                              |
| `api_key`                               | —                        | Your Rulebricks API key.                                           |
| `base_url`                              | `https://rulebricks.com` | Override for self-hosted deployments.                              |
| `batch_size`                            | `500`                    | Rows per `bulk_solve` call (max 1000).                             |
| `max_concurrent_requests_per_partition` | `4`                      | Concurrent batches per Spark partition.                            |
| `input_mapping`                         | `None`                   | `{df_column: rule_field}` for renaming inputs.                     |
| `output_schema`                         | `None`                   | Explicit `StructType` to skip schema inference.                    |
| `on_conflict`                           | `"overwrite"`            | `"error"` or `"overwrite"` when output names collide with inputs.  |
| `on_missing_input`                      | `"default"`              | `"error"` or `"default"` (use rule's declared defaults).           |
| `on_error`                              | `"continue"`             | `"raise"` to fail the job; `"continue"` to isolate in `_rb_error`. |
| `auto_repartition`                      | `False`                  | Repartition a single-partition DataFrame for parallelism.          |
| `timeout_seconds`                       | `60.0`                   | Per-request timeout.                                               |
| `max_retries`                           | `3`                      | Retry attempts for 408/429/5xx/timeout/reset.                      |

Returns a new DataFrame. Spark DataFrames are immutable — `solve()` does not mutate the input.

## Common patterns

### Column names don't match the rule's inputs

```python
solve(
    df,
    rule="eligibility",
    api_key=key,
    input_mapping={
        "fin_impact": "financial_impact",
        "dso": "days_since_occurrence",
    },
)
```

Unmapped columns pass through as-is.

### Call from SQL via Unity Catalog

Register the scored DataFrame as a view, or wrap `solve()` in a Python function that your analysts can use:

```python
def triage_breaches(input_table: str, output_table: str):
    df = spark.table(input_table)
    scored = solve(df, rule="breach-triage", api_key=get_key())
    scored.write.mode("overwrite").saveAsTable(output_table)
```

### Self-hosted Rulebricks

```python
solve(
    df,
    rule="underwriting",
    api_key=key,
    base_url="https://rules.acme.internal",
)
```

### High-throughput tuning

For large batch jobs against a self-hosted Rulebricks instance:

```python
solve(
    df.repartition(32),           # match executor count
    rule="score",
    api_key=key,
    batch_size=1000,              # max
    max_concurrent_requests_per_partition=8,
)
```

Effective throughput: 32 partitions × 8 concurrent × 1000 rows = 256k rows in flight.

### Inspecting errors

```python
scored = solve(df, rule="score", api_key=key, on_error="continue")

# Successful rows
scored.filter("_rb_error IS NULL")

# Failed rows, for investigation
scored.filter("_rb_error IS NOT NULL").select("report_id", "_rb_error").show(truncate=False)
```

### Explicit output schema (typed arrays / structs)

By default, rule outputs of type `list` and `object` are serialized as JSON strings. To get typed `ArrayType` or `StructType` columns, pass an explicit `output_schema`:

```python
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

schema = StructType([
    StructField("priority", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("score", DoubleType()),
])

solve(df, rule="triage", api_key=key, output_schema=schema)
```

## Performance notes

- **Batch size**: 500 is a reasonable default. Raise to 1000 for simple rules, and drop to 100–200 for rules involving large payloads.
- **Concurrency**: each executor hits the Rulebricks API in parallel. Against the managed cloud, rate and account limits may kick in before you saturate workers — watch for 429s in `_rb_error`. Against a self-hosted cluster, the autoscaler handles it. [Read more about self-hosted Rulebricks for production volumes](#self-hosted-rulebricks-for-production-volumes).
- **Partition count**: `solve()` inherits the input DataFrame's partitioning. If your DataFrame has one huge partition, either call `.repartition(n)` yourself or pass `auto_repartition=True`.
- **Schema inference cost**: a single driver-side API call at job start. Cache the schema or pass `output_schema` explicitly to skip it for repeated runs.

## Error handling

The `_rb_error` column is always present in the output.

- `on_error="continue"` (default): batch failures mark every row in the failed batch with the error string, leaving rule output columns `null`. The job completes.
- `on_error="raise"`: the Spark task fails on the first batch error, propagating to job failure.

Retries run automatically for 408 / 425 / 429 / 500 / 502 / 503 / 504, timeouts, and connection resets, using exponential backoff (2^attempt seconds, capped at 30).

## Requirements

- Python 3.9+
- PySpark 3.3+
- `rulebricks` SDK 1.0+
- `pandas` 1.5+
- `pyarrow` 10+

## Development

```bash
git clone https://github.com/rulebricks/spark
cd rulebricks-spark
pip install -e ".[dev]"
pytest
```

Run only the fast unit tests:

```bash
pytest -m "not requires_spark"
```

Run the end-to-end tests (requires PySpark and a JVM):

```bash
pytest -m requires_spark
```

## License

MIT — see [LICENSE](LICENSE).

## Links

- [Rulebricks Homepage](https://rulebricks.com)
- [Rulebricks User Guide](https://rulebricks.com/docs)
- [Python SDK reference](https://rulebricks.com/docs/api-reference#sdk/python)
- [Issues & feature requests](https://github.com/rulebricks/spark/issues)
