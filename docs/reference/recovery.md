# Recover a saved operation

`resume()` re-executes selected failed records from one materialized operation.
It is a recovery step after the original result has been saved and inspected,
not a scheduler for ongoing retries. Successful records pass through unchanged.

```{eval-rst}
.. autoapifunction:: rulebricks_spark.resume
```

`result_df` must come from durable storage or a reliable checkpoint that severs
live execution lineage. Cached or persisted execution DataFrames are rejected
because Spark can evict their blocks and repeat the original requests. The saved
field metadata supplies the asset, version, payload mapping, and result contract;
credentials are always supplied again by the caller.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `api_key`, `base_url` | Required | Execution credentials and API URL |
| `error_types` | `network`, `infrastructure`, `throttled`, `server` | Categories selected for re-execution |
| `max_batch_bytes`, `max_batch_rows` | 5 MiB, `1000` | Limits on recovery requests |
| `execution_partitions` | `1` | Nominal execution partition count |
| `max_retries` | `3` | Inline transient retries after each first attempt |
| `timeout_seconds` | `300` | Timeout per request |
| `on_error` | `"continue"` | Return errors as data, or raise from the task |
| `response_options` | Saved settings where possible | Original callbacks must be supplied again |
| `request_options` | None | Original request customization must be supplied again when used |

The helper validates operation identity and the source schema before submitting
selected records. It preserves attempt history and appends the new attempts.
It cannot verify that two Python callbacks from different processes are the same
implementation, so callers remain responsible for supplying the original logic.

When passed a result DataFrame, context writes, ordered stateful runs, mixed
operations, and incompatible contracts are rejected. An appended streaming table contains several operations and cannot
be passed directly to `resume()`. See [reliability and audit](../reliability-and-audit.md)
for choosing an output layout that supports recovery.

## Resume a stateful checkpoint path

Pass the string path of a `contexts.Checkpoint` instead of a DataFrame to recover
plain Context state. This uses the run's saved immutable source, committed state
and original execution settings. Unfinished windows replay under fresh physical
Context IDs; successful windows retain their saved results. A completed path
returns saved results without executing any requests.

Re-supply credentials and any original callbacks or request customization.
`error_types` is rejected for checkpoint paths, and the per-request tuning arguments
above apply only to DataFrame recovery. The stateful path uses its original
configuration and disables HTTP retries. See [Context checkpoints](contexts.md#checkpoint-plain-state)
for prerequisites and the supported state model.
