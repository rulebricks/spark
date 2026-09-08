# SQL integration

Saved results are ordinary Spark data and can be queried through Spark SQL without
contacting Rulebricks again. When an interactive SQL query itself needs to execute
a rule, the SQL helper registers a struct-valued function.

```{eval-rst}
.. autoapifunction:: rulebricks_spark.sql.register_udf
```

`spark` is the active SparkSession, `name` is the SQL function name, and `rule` is
the target slug. Supply `api_key` and `base_url`; `version` defaults to `"latest"`.
The helper infers the selected version's schema unless `output_schema` is supplied
as a `StructType`. `max_batch_rows` defaults to 1,000. It returns the registered
output schema, including `_rb_error`, a nullable string describing a failed row.

```python
from rulebricks_spark.sql import register_udf

register_udf(
    spark,
    "route_order",
    "order-routing",
    version="1",
    api_key=api_key,
    base_url=base_url,
)
source.createOrReplaceTempView("orders")
spark.sql("SELECT route_order(struct(*)) AS decision FROM orders").show()
```

This interface uses a pandas SQL UDF and does not provide the bulk API's full audit
metadata or lossless nullable integer path. Potentially rounded numeric inputs at
or above the exact float integer boundary are rejected. Prefer `apply_rule()` for
large auditable jobs, or supply identifiers as strings in SQL.
