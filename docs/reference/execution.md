# Rules and flows

`apply_rule()` and `apply_flow()` transform a batch DataFrame into a DataFrame
containing its original columns, a result column, and execution metadata. They
are lazy: requests run when Spark evaluates the output. Streaming callers use
these functions inside `foreachBatch`.

## Apply a rule

```{eval-rst}
.. autoapifunction:: rulebricks_spark.apply_rule
```

## Apply a flow

```{eval-rst}
.. autoapifunction:: rulebricks_spark.apply_flow
```

The functions share the parameters described above. `flow` selects a flow slug
where `rule` selects a rule slug. Rule output types are inferred from the selected
version by default; flow output remains JSON unless `output_schema` is supplied.
Both return `pyspark.sql.DataFrame` and preserve the input row count when errors
are returned as data.

## Defaults and accepted values

| Parameter | Default | Contract |
| --- | --- | --- |
| `version` | `"latest"` | String version selector; prefer published numbers for reproducibility |
| `input_columns` | All source columns | Sequence of top-level names to send |
| `payload_column` | `None` | One struct or JSON-string column; cannot combine with input selection or mapping |
| `input_mapping` | None | Source-to-request names; sources must be selected and destinations unique |
| `correlation_id_path` | None | Dot path in the prepared payload, retained as a string in metadata |
| `execution_partitions` | `1`, or automatic with a row budget | Advanced Spark producer override. Omission derives partitioning from the request plan when `max_inflight_rows` is supplied; otherwise keeps one partition. `None` preserves source partitions. An explicit or preserved count must fit the row budget |
| `max_inflight_rows` | `None` | Planned outstanding rows across this execution. Omit `max_batch_rows` for automatic tuning or supply it for manual batching. Both modes refill individual requests; see [row budget](../execution-model.md#row-budget) |
| `max_batch_bytes` | `67108864` (64 MiB) | Positive byte ceiling on encoded request bodies, including the outer array; does not disable tuning |
| `max_batch_rows` | `1000`, or automatic with a row budget | Integer: manual row ceiling. `None`: manual with no row ceiling. Omit to tune with a row budget, or keep 1,000 rows without one |
| `max_retries` | `3` | Nonnegative number of retries after the first attempt |
| `timeout_seconds` | `300` | Positive timeout for a synchronous request |
| `output_schema` | None | Spark DataType used for typed conversion |
| `result_column` | `"rulebricks_result"` | New result column name |
| `metadata_column` | `"rulebricks_meta"` | New metadata column name |
| `diagnostics` | `False` | Include chunk identity, size, and attempt history |
| `on_error` | `"continue"` | Return failures as rows, or `"raise"` to fail the task |
| `response_options` | Default `ResponseOptions` | JSON format, raw retention, callbacks, and conversion policy |
| `request_options` | None | SDK headers/query parameters and Spark TLS `verify` setting |
| `isolate_failures` | `True` | Allow bounded HTTP 500 bisection after retries; 413 size splitting remains enabled |
| `input_validation` | `"none"` | `"strict"` or `"warn"` checks declared input paths and compatible Spark types |

Source names beginning with `_rb_` are reserved. Duplicate source names, conflicting
output names, invalid options, and unsupported streaming DataFrames are rejected
during preparation. Input structure checks do not scan row values. See
[working with results](../working-with-results.md) for validation boundaries and
[execution and scale](../execution-model.md) for request behavior.

Preparation may raise `TypeError`, `ValueError`, or `SchemaInferenceError`.
Execution failures normally become metadata; with `on_error="raise"` they raise
`ExecutionError` inside the Spark task and are reported through Spark's task error.
