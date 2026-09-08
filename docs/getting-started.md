# From a table to saved decisions

A Rulebricks Spark job begins with an ordinary DataFrame. You choose the rule or
flow that should process each record, select a published version, and write the
returned DataFrame to storage. The library preserves the source columns so that
every outcome remains connected to the data that produced it.

The examples below assume an existing rule or flow in your Rulebricks deployment.
If you are new to the platform, a rule expresses a business decision through a
decision table or conditions, while a flow connects several decision and processing
steps. The [bulk rules notebook](_notebooks/01-bulk-rules.ipynb) introduces the
simpler case with a small order-routing policy.

## Installation

Install the package on both the driver and the executors in a classic PySpark 3.5
runtime. The package requires Python 3.9 or newer and PyArrow 18 or newer; choose
a Python version supported by your Spark runtime. The walkthroughs were executed
with Python 3.11.

```bash
pip install rulebricks-spark
```

For development from a checkout, use `python -m pip install -e .`. This release is
validated on classic Spark; Spark Connect and other Spark versions need separate
verification. The walkthroughs use Delta Lake 3.3.2 with Spark 3.5.9, but the core
library does not require Delta. You may write its results to another durable sink
that supports the returned Spark types.

## Connect to Rulebricks

Keep the API key in your secret manager or an environment variable. The remaining
job settings can be plain values in a notebook cell, an application configuration,
or your scheduler's parameters. They do not need their own environment variables.

```python
import os

api_key = os.environ["RULEBRICKS_API_KEY"]
base_url = "https://rulebricks.acme.com/api/v1"
flow_slug = "order-routing-flow"
flow_version = "1"
result_path = "s3a://your-data/decisions/run-2026-09-06"
```

The slug is the identifier in the asset's API URL. A published version number
selects a particular policy; `latest` and environment aliases remain mutable.
Use a new output location for each independent run, and choose storage that all
executors can reach. The package uses the generated Rulebricks Python SDK to call
the deployment at your chosen API URL.

## Apply the decision process

Suppose `source` contains `request_id`, `amount`, and `country`. Pass those fields
to the flow and retain the request ID as the correlation key in execution metadata:

```python
from rulebricks_spark import apply_flow

results = apply_flow(
    source,
    flow_slug,
    version=flow_version,
    api_key=api_key,
    base_url=base_url,
    input_columns=["request_id", "amount", "country"],
    correlation_id_path="request_id",
    execution_partitions=4,
)
```

`apply_flow()` returns a DataFrame; it does not immediately send the records.
Like other Spark transformations, it runs when an action evaluates the result.
It adds `rulebricks_result`, which contains the flow's JSON response, and
`rulebricks_meta`, which describes success or failure and the corresponding run.
Both columns are added beside the original source fields.

Use `apply_rule()` in the same way for a rule. By default, it reads the selected
version's output schema and returns a typed result rather than JSON. You can also
supply a Spark `output_schema` explicitly, or choose JSON through `ResponseOptions`.
These choices are described in [working with results](working-with-results.md).

## Write once, then inspect

Write the result before performing several audits or downstream transformations.
Reading the saved table gives those consumers the same observed decisions without
causing another evaluation of the external calls.

```python
results.write.format("delta").mode("errorifexists").save(result_path)
saved = spark.read.format("delta").load(result_path)

saved.groupBy("rulebricks_meta.status").count().show()
saved.filter("rulebricks_meta.status = 'error'").select(
    "request_id", "rulebricks_meta.error", "rulebricks_meta.execution_id"
).show(truncate=False)
```

A successful Spark write means that the output was saved, including any failed
decisions returned as rows. Check the outcomes before treating the run as complete.
Caching can reduce repeated work, but it can be evicted and is not an exactly-once
boundary. Rerunning an execution cell may repeat requests even when a subsequent
write rejects an existing path.

Continue with the [bulk flow notebook](_notebooks/02-bulk-flows.ipynb) for a full
20,000-record example, or [execution and scale](execution-model.md) to understand
how request sizes, concurrency, and timeouts affect the workload.
