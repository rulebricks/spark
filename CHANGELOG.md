# Changelog

## 0.1.0 (2026-04-16)

Initial release.

- `solve()` — apply a Rulebricks rule to every row of a Spark DataFrame.
- Automatic batching via `bulk_solve` (configurable, max 1000 rows/call).
- Intra-partition concurrency with configurable thread pool.
- Schema inference from rule definition, or explicit `output_schema`.
- Column collision handling: `on_conflict="overwrite"` (default) or `"error"`.
- Per-row error isolation in `_rb_error` column with `on_error="continue"`.
- `input_mapping` for renaming DataFrame columns to match rule fields.
- Exponential-backoff retries on 408/429/5xx/timeout/connection errors.
- `auto_repartition` for pathologically partitioned DataFrames.
- Works in Databricks, EMR, Synapse, Dataproc, and local PySpark.
