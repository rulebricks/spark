# Examples

Runnable Databricks notebooks demonstrating `rulebricks-spark` across the
three most common patterns.

| Example | Pattern |
|---|---|
| [`breach-triage/`](./breach-triage/) | Canonical batch: score a Delta table end-to-end. Start here. |
| [`streaming/`](./streaming/) | Apply a rule to a Structured Streaming DataFrame with a Delta sink. |
| [`sql-callable/`](./sql-callable/) | Register a SQL-callable UDF for use in `%sql` cells and DBSQL. |

Each folder has its own README with the rule schema the notebook expects,
where to find your rule slug and API key, and how to wire the notebook into
Databricks (secrets, cluster config, `%pip install`).

## Prerequisites (all examples)

- A published rule in your Rulebricks workspace.
- A Rulebricks API key.
- A PySpark environment — Databricks, EMR, Synapse, or a local PySpark
  install. Examples are formatted as Databricks notebooks but the core
  `solve()` and `register_udf()` calls work anywhere PySpark runs.

## Running outside Databricks

The `%pip`, `dbutils`, `display`, and `%sql` cells are Databricks-specific
conveniences. For local or non-Databricks runs:

```python
import os
api_key = os.environ["RULEBRICKS_API_KEY"]
```

Replace `display(df)` with `df.show()` and skip the `%sql` cells (or wrap
them in `spark.sql("...").show()`).

## A note on rule slugs

Rulebricks slugs are short opaque IDs — roughly 10 characters of nanoid-
style text, e.g. `abc123xyz9`. They appear in the rule editor URL and on
each rule card in the Dashboard. Copy yours into the `RULE_SLUG` constant
at the top of each notebook.
