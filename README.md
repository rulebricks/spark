![Rulebricks Spark](/assets/cover.png)

# Rulebricks Spark

[![PyPI](https://img.shields.io/pypi/v/rulebricks-spark.svg)](https://pypi.org/project/rulebricks-spark/)


![Reference Documentation](https://rulebricks-spark.readthedocs.io](https://rulebricks-spark.readthedocs.io/en/latest/)

Apply versioned Rulebricks rules and flows to PySpark DataFrames. The library
sends records in bounded requests and keeps each decision, error, and execution
ID alongside its source data, so the pipeline can scale without losing the ability
to explain a run.

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
    base_url="https://rulebricks.acme.com/api/v1",
    correlation_id_path="request_id",
    execution_partitions=4,
)
results.write.format("delta").mode("errorifexists").save(result_path)
saved = spark.read.format("delta").load(result_path)
```

Flow results remain JSON by default, ready for your own Spark expressions.
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
