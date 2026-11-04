![Rulebricks Spark](/assets/cover.png)

# Rulebricks Spark

[![PyPI](https://img.shields.io/pypi/v/rulebricks-spark.svg)](https://pypi.org/project/rulebricks-spark/)


[Reference Documentation](https://rulebricks-spark.readthedocs.io/en/latest/)

Apply versioned Rulebricks rules and flows to PySpark DataFrames. The library
sends records in bounded requests and keeps each decision, error, and execution
ID alongside its source data.

```bash
pip install rulebricks-spark
```

```python
import os

from rulebricks_spark import apply_flow

results = apply_flow(
    source,
    "order-routing-flow",
    version="1",
    api_key=os.environ["RULEBRICKS_API_KEY"],
    base_url="https://acme.rulebricks.com/api/v1",
    correlation_id_path="request_id",
    max_inflight_rows=128,
)
results.write.format("delta").mode("errorifexists").save(result_path)
saved = spark.read.format("delta").load(result_path)
```

Flow results remain JSON by default, ready for your own Spark expressions.

For independent rows, `max_inflight_rows=128` lets the library automatically
pack, overlap, and tune requests within a planned budget of 128 outstanding rows.
To choose the batch size yourself, set `max_batch_rows`. This disables automatic
batch tuning. With full batches, the planned number of concurrent requests is
`max_inflight_rows / max_batch_rows`. In this example, `max_batch_rows=4` gives
`128 / 4 = 32` requests of four rows each.
Both modes choose Spark partitioning automatically and refill requests individually.
`execution_partitions` remains an advanced override; `max_batch_bytes` is a
64 MiB request ceiling and does not disable tuning. Omitting `max_inflight_rows`
keeps the existing execution behavior. See
[execution and scale](docs/execution-model.md#row-budget) for details.

`apply_rule()` uses the same execution model and infers a typed result from the
selected rule version. Context operations provide optional input staging and
ordered stateful processing, while Structured Streaming uses Spark's ordinary
`foreachBatch` interface.

Start with the [documentation](docs/index.md) or the [notebook walkthroughs](examples/README.md).
The guides explain how the concepts fit together; the reference covers parameters,
result fields, failure handling, and recovery. Notebook settings are defined in
the notebooks, with only credentials kept in a secret manager or environment.

The package targets classic PySpark 3.5 and requires Python 3.9+ and PyArrow 18+.
Use a Python version supported by your Spark runtime.
For a documentation preview, follow [the Sphinx build instructions](docs/contributing.md).

## Releasing

Release versions come from Git tags. Push a tag such as `v0.2.2` (or `0.2.2`)
on the commit you want to release, and GitHub Actions builds and publishes that
version to PyPI. There is no version number to edit in the source code.

The release checks the package filenames and metadata against the tag before
uploading. See [release instructions](docs/contributing.md#releasing) for details.
