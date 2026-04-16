# Streaming Example

Apply a Rulebricks rule to a Spark **streaming DataFrame** — the same
`solve()` function you use in batch mode also works on
Structured Streaming, Auto Loader, and Delta Live Tables pipelines.

See [`demo.py`](./demo.py) for the full Databricks notebook.

## Why it works

`solve()` is implemented on top of `mapInPandas`, which Spark Structured
Streaming supports natively as a stateless per-batch transformation. Each
micro-batch is partitioned, batched into `bulk_solve` calls, and written to
the sink — no special configuration needed.

```
raw stream  →  solve()  →  scored stream  →  Delta sink
                ^
                └── per-micro-batch bulk_solve calls
```

## Example rule schema

The demo assumes a rule named something like `transaction-triage` with:

**Inputs**

| Field | Type |
|---|---|
| `tx_id` | string |
| `amount` | number |
| `merchant_category` | string |
| `cardholder_country` | string |
| `merchant_country` | string |
| `is_card_not_present` | boolean |

**Outputs**

| Field | Type |
|---|---|
| `action` | string (`allow` / `review` / `block`) |
| `fraud_score` | number |
| `reason` | string |

Feel free to substitute any rule whose inputs match your stream's columns.
Use `input_mapping={"stream_col": "rule_field"}` on the `solve()` call to
rename columns at call time rather than upstream.

## Tuning guidance

- **`trigger(processingTime="30 seconds")`** is a good starting default. Pair
  the trigger interval with a `batch_size` that produces 1–4 HTTP calls per
  micro-batch. E.g. 600 rows/sec × 30s trigger = 18,000 rows per micro-batch;
  `batch_size=1000` gives 18 batches, well within
  `max_concurrent_requests_per_partition=4` × 4 partitions.
- **Watch the `_rb_error` column.** In a long-running stream you care more
  about isolated failures than a blown pipeline — keep `on_error="continue"`
  (the default) and route rows with non-null `_rb_error` to a dead-letter
  table via `.filter("_rb_error IS NOT NULL").writeStream...`.
- **Checkpoint location** must be on durable storage (DBFS, ADLS, S3). The
  `/tmp/...` path in the demo works for an ephemeral Community Edition test
  but will be wiped on cluster restart.

## Delta Live Tables

The notebook includes a minimal DLT example. `solve()` drops into any
`@dlt.table` function with no adaptation:

```python
import dlt
from rulebricks_spark import solve

@dlt.table(name="scored_transactions")
def scored_transactions():
    raw = dlt.read_stream("raw_transactions")
    return solve(raw, rule=RULE_SLUG, api_key=api_key)
```

Set `RULE_SLUG` and `api_key` via DLT pipeline configuration. Resolve the
API key at pipeline start from a Databricks secret scope.

## Prerequisites

Same as the [breach-triage example](../breach-triage/README.md):

- A published Rulebricks rule and its slug (10-character opaque ID)
- A Rulebricks API key, ideally stored in a Databricks secret scope
- DBR 14+ for the `%pip install` + restart flow

## Self-hosted Rulebricks

For streaming workloads above a few hundred events per second you'll want to
run against a self-hosted Rulebricks instance. The managed cloud plan is
rate-limited for multi-tenant safety. See the top-level
[README](../../README.md#self-hosted-rulebricks-for-production-volumes) for
details.
