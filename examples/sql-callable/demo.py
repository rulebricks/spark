# Databricks notebook source

# MAGIC %md
# MAGIC # SQL-Callable Rulebricks UDFs
# MAGIC
# MAGIC Invoke a Rulebricks rule directly from `%sql` cells and DataFrame
# MAGIC expressions — no Python `solve()` call required. Useful when your
# MAGIC analysts live in SQL Warehouses, dashboards, or DBSQL notebooks.
# MAGIC
# MAGIC **Two paths in this notebook:**
# MAGIC 1. The one-liner — `register_udf(...)` ships with `rulebricks-spark`
# MAGIC 2. The raw pattern — build your own `pandas_udf` for custom behavior

# COMMAND ----------

# MAGIC %pip install rulebricks-spark

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

RULE_SLUG = "<your_rule_slug>"

api_key = dbutils.secrets.get(scope="rulebricks", key="api_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed data
# MAGIC
# MAGIC A small table of incident reports to exercise the UDF. Swap in your
# MAGIC real table once the pattern is proven.

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField("report_id", StringType()),
    StructField("entity_name", StringType()),
    StructField("incident_type", StringType()),
    StructField("financial_impact", DoubleType()),
    StructField("customer_harm", BooleanType()),
    StructField("parties_affected", IntegerType()),
    StructField("days_open", IntegerType()),
])

data = [
    ("R-001", "Acme Holdings",     "policy_violation",    2_500_000.0,  True,  12000, 14),
    ("R-002", "Global Services",   "disclosure_failure",    180_000.0, False,    320,  7),
    ("R-003", "Meridian Capital",  "insider_activity",   15_000_000.0,  True,      3,  2),
    ("R-004", "Northwind Advisors","fiduciary_breach",    4_200_000.0,  True,   8500, 30),
    ("R-005", "Bluehaven",         "policy_violation",       45_000.0, False,     80, 45),
]

spark.createDataFrame(data, schema=schema).write.mode("overwrite").saveAsTable("incidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 1 — The one-liner
# MAGIC
# MAGIC `register_udf` registers a Spark UDF backed by the rule. Afterwards the
# MAGIC UDF is callable from any SQL context on this Spark session.

# COMMAND ----------

from rulebricks_spark.databricks import register_udf

return_schema = register_udf(
    spark,
    name="rulebricks_triage",
    rule=RULE_SLUG,
    api_key=api_key,
)

print("Registered UDF. Return schema:")
return_schema.simpleString()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call it from SQL
# MAGIC
# MAGIC The UDF accepts a single `struct` argument whose fields match the
# MAGIC rule's request schema. `struct(*)` is the most common invocation.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   report_id,
# MAGIC   entity_name,
# MAGIC   triage.priority,
# MAGIC   triage.reportable,
# MAGIC   triage.risk_score,
# MAGIC   triage._rb_error
# MAGIC FROM (
# MAGIC   SELECT *, rulebricks_triage(struct(*)) AS triage
# MAGIC   FROM incidents
# MAGIC )
# MAGIC ORDER BY triage.risk_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize into a Delta table
# MAGIC
# MAGIC The pattern composes cleanly with `CREATE TABLE AS SELECT`:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE incidents_scored AS
# MAGIC SELECT
# MAGIC   i.*,
# MAGIC   triage.priority,
# MAGIC   triage.reportable,
# MAGIC   triage.risk_score,
# MAGIC   triage.rationale,
# MAGIC   triage._rb_error
# MAGIC FROM (
# MAGIC   SELECT *, rulebricks_triage(struct(*)) AS triage FROM incidents
# MAGIC ) i

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame expression form
# MAGIC
# MAGIC The same UDF is callable from the DataFrame API via `F.expr`:

# COMMAND ----------

from pyspark.sql import functions as F

enriched = (
    spark.table("incidents")
    .withColumn("triage", F.expr("rulebricks_triage(struct(*))"))
    .select("report_id", "triage.*")
)

display(enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path 2 — The raw pattern
# MAGIC
# MAGIC If you need custom behavior — different retry logic, an alternate HTTP
# MAGIC client, bespoke logging — build your own `pandas_udf`. This is what
# MAGIC `register_udf` does internally, without the SDK convenience.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType

custom_return_schema = StructType([
    StructField("priority", StringType()),
    StructField("reportable", BooleanType()),
    StructField("risk_score", DoubleType()),
    StructField("rationale", StringType()),
])


@pandas_udf(custom_return_schema)
def custom_triage(inputs):
    import pandas as pd
    from rulebricks import Rulebricks

    client = Rulebricks(api_key=api_key)
    payloads = inputs.to_dict(orient="records")

    response = client.rules.bulk_solve(slug=RULE_SLUG, request=payloads)
    results = response if isinstance(response, list) else []

    return pd.DataFrame({
        "priority": [r.get("priority") for r in results],
        "reportable": [r.get("reportable") for r in results],
        "risk_score": [float(r.get("risk_score") or 0) for r in results],
        "rationale": [r.get("rationale") for r in results],
    })


spark.udf.register("custom_triage", custom_triage)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT report_id, custom_triage(struct(*)).priority
# MAGIC FROM incidents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - The registered UDF is scoped to the current Spark session. If you
# MAGIC   want it available across sessions or to DBSQL users, register it as
# MAGIC   a **Unity Catalog Function** — see `README.md` in this directory.
# MAGIC - UDFs bypass Spark's intra-partition thread pool that `solve()`
# MAGIC   provides. For high-throughput batch jobs, prefer `solve()`.
# MAGIC - Errors surface in the `_rb_error` field of the returned struct.
# MAGIC   Filter on `triage._rb_error IS NOT NULL` to find rows that failed.
