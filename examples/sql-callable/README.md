# SQL-Callable Example

Invoke a Rulebricks rule directly from `%sql` cells, DBSQL notebooks, and
DataFrame expressions. Useful when the team consuming rule outputs lives in
SQL, not Python.

See [`demo.py`](./demo.py) for the full Databricks notebook.

## The one-liner

```python
from rulebricks_spark.databricks import register_udf

register_udf(spark, "rulebricks_triage", rule=RULE_SLUG, api_key=api_key)
```

After registration, the UDF is callable from SQL on the same Spark session:

```sql
SELECT
  report_id,
  triage.priority,
  triage.risk_score,
  triage._rb_error
FROM (
  SELECT *, rulebricks_triage(struct(*)) AS triage
  FROM incidents
)
```

The UDF accepts a single `struct` whose fields match the rule's request
schema, and returns a struct containing every rule output plus a nullable
`_rb_error` column (null on success).

## How it works

Under the hood, `register_udf` builds a `pandas_udf` closed over the rule's
configuration and registers it via `spark.udf.register`. The UDF:

1. Receives an Arrow batch of rows.
2. Converts each row to a `bulk_solve` payload.
3. Calls Rulebricks in `batch_size`-row chunks.
4. Returns a pandas DataFrame matching the declared return schema.

Schema inference runs once on the driver when `output_schema` isn't provided.

## `register_udf(...)` reference

| Argument | Default | Description |
|---|---|---|
| `spark` | — | Active Spark session |
| `name` | — | SQL identifier the UDF registers under |
| `rule` | — | Rulebricks rule slug |
| `api_key` | — | Rulebricks API key |
| `base_url` | `https://rulebricks.com` | Override for self-hosted deployments |
| `output_schema` | `None` | Explicit `StructType` to skip inference |
| `batch_size` | `500` | Rows per `bulk_solve` call (max 1000) |

Returns the full return `StructType` (rule outputs + `_rb_error`) so you
can see what columns the UDF produces without having to `DESCRIBE` it.

## `solve()` vs `register_udf()` — which should I use?

| Use case | Use |
|---|---|
| Batch job scoring an entire table | `solve()` |
| Mixing rule outputs with SQL joins/aggregations | `register_udf()` |
| Per-row inline scoring in a dashboard query | `register_udf()` |
| Streaming pipeline to a Delta sink | `solve()` |
| Maximum throughput on a dedicated job cluster | `solve()` (intra-partition concurrency) |

Rule of thumb: `solve()` when you control the job shape, `register_udf()`
when the consumer is SQL.

## Unity Catalog Functions

For usage beyond a single Spark session — DBSQL warehouses, scheduled
queries, Unity-Catalog-governed analyst workflows — register the rule as a
**Unity Catalog Function** instead. UC Functions are governed, versioned,
and callable from any workspace that can see the catalog.

The packaging is different (Python source stored in UC, no external imports
from a `%pip`-installed library), so `register_udf` doesn't create a UC
Function directly. A typical pattern:

```python
spark.sql("""
  CREATE OR REPLACE FUNCTION main.rulebricks.triage(payload STRUCT<...>)
  RETURNS STRUCT<priority STRING, risk_score DOUBLE, reportable BOOLEAN, rationale STRING>
  LANGUAGE PYTHON
  ENVIRONMENT (
    dependencies = '["rulebricks==1.0.0"]'
  )
  AS $$
    import os
    from rulebricks import Rulebricks

    def handler(payload):
      client = Rulebricks(api_key=os.environ["RULEBRICKS_API_KEY"])
      response = client.rules.solve(slug="<your_rule_slug>", request=dict(payload))
      return (
        response.get("priority"),
        float(response.get("risk_score") or 0),
        bool(response.get("reportable") or False),
        response.get("rationale"),
      )

    return handler(payload)
  $$
""")
```

UC Functions run per-row, not in batches, so they're suited to interactive
queries against small tables, not batch scoring of millions of rows. For the
latter, stick with `solve()`.

## Errors

The UDF's return struct always includes a `_rb_error` field:

- `null` on success.
- A human-readable message on batch failure (all rows in the failing batch
  share the same error string).
- `"missing_response_for_row"` when the API returns fewer results than the
  batch contained.

Find failed rows with:

```sql
SELECT report_id, triage._rb_error
FROM (SELECT *, rulebricks_triage(struct(*)) AS triage FROM incidents)
WHERE triage._rb_error IS NOT NULL
```

## Self-hosted Rulebricks

The UDF respects `base_url`:

```python
register_udf(
    spark,
    "rulebricks_triage",
    rule=RULE_SLUG,
    api_key=api_key,
    base_url="https://rulebricks.acme.internal/api/v1",
)
```

For serious SQL-driven workloads, run against a self-hosted instance — see
the top-level [README](../../README.md#self-hosted-rulebricks-for-production-volumes)
for why and how.
