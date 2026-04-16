# Changelog

## 0.1.1 (2026-04-16)

Bug fix: default `base_url` now includes the required `/api/v1` path, and
the underlying Rulebricks SDK is always invoked with an explicit `base_url`.
Previous versions relied on the SDK's implicit default, which some `rulebricks`
SDK releases ship URL-encoded, causing httpx to raise "Request URL is missing
an 'http://' or 'https://' protocol" on every call.

- `solve()` default `base_url` changed from `https://rulebricks.com` to `https://rulebricks.com/api/v1`.
- `rulebricks_spark.databricks.register_udf()` default `base_url` changed to `https://rulebricks.com/api/v1`.
- Self-hosted deployments must now include the `/api/v1` suffix in `base_url`.

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
