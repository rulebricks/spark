# Databricks notebook source

# MAGIC %md
# MAGIC # Streaming Rule Evaluation — Rulebricks + Structured Streaming
# MAGIC
# MAGIC `solve()` is a regular DataFrame transformation, which means it works
# MAGIC on **streaming DataFrames** too. This notebook shows the end-to-end
# MAGIC pattern: read from a streaming source, score each micro-batch against a
# MAGIC Rulebricks rule, write results to a Delta sink.
# MAGIC
# MAGIC The demo uses a rate source to keep the notebook self-contained. In
# MAGIC production you'd read from a Delta table, Kafka, or Auto Loader.
# MAGIC
# MAGIC **Before running:** see `README.md` in this directory for the rule
# MAGIC schema this notebook assumes.

# COMMAND ----------

# MAGIC %pip install rulebricks-spark
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

RULE_SLUG = "<your_rule_slug>"

api_key = dbutils.secrets.get(scope="rulebricks", key="api_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Generate a streaming source
# MAGIC
# MAGIC The rate source produces one row per second — enough to demonstrate
# MAGIC the pattern without burning API credits. Swap this out for
# MAGIC `spark.readStream.format("delta").table("raw_events")` or your
# MAGIC production source when you're ready.

# COMMAND ----------

from pyspark.sql import functions as F

MERCHANT_CATEGORIES = ["grocery", "fuel", "electronics", "dining", "travel", "online"]
COUNTRIES = ["US", "CA", "GB", "DE", "SG", "AU"]

merchant_arr = F.array(*[F.lit(c) for c in MERCHANT_CATEGORIES])
country_arr = F.array(*[F.lit(c) for c in COUNTRIES])

raw_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("tx_id", F.concat(F.lit("T-"), F.col("value").cast("string")))
    .withColumn("amount", (F.rand() * 5000).cast("double"))
    .withColumn(
        "merchant_category",
        merchant_arr.getItem((F.col("value") % F.lit(len(MERCHANT_CATEGORIES))).cast("int")),
    )
    .withColumn(
        "cardholder_country",
        country_arr.getItem((F.col("value") % F.lit(len(COUNTRIES))).cast("int")),
    )
    .withColumn(
        "merchant_country",
        country_arr.getItem(((F.col("value") + F.lit(1)) % F.lit(len(COUNTRIES))).cast("int")),
    )
    .withColumn("is_card_not_present", (F.col("value") % F.lit(3)) == F.lit(0))
    .select(
        "tx_id",
        "amount",
        "merchant_category",
        "cardholder_country",
        "merchant_country",
        "is_card_not_present",
        F.col("timestamp").alias("tx_time"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Apply the rule to the stream
# MAGIC
# MAGIC The call signature is identical to batch mode. `solve()` uses
# MAGIC `mapInPandas` under the hood, which Structured Streaming supports
# MAGIC natively.

# COMMAND ----------

from rulebricks_spark import solve

scored_stream = solve(
    raw_stream,
    rule=RULE_SLUG,
    api_key=api_key,
    batch_size=200,
    max_concurrent_requests_per_partition=4,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Write to a Delta sink
# MAGIC
# MAGIC Micro-batches land in a Delta table. Use a checkpoint location you
# MAGIC have write access to.

# COMMAND ----------

CHECKPOINT = "/tmp/rulebricks_spark/streaming_demo/_checkpoint"
TARGET_TABLE = "scored_transactions"

query = (
    scored_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .toTable(TARGET_TABLE)
)

print(f"Streaming query started: {query.id}")
print(f"Writing to: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Observe results
# MAGIC
# MAGIC Wait ~60 seconds for the first micro-batch to land, then peek at the
# MAGIC output table.

# COMMAND ----------

import time

time.sleep(60)

display(spark.table(TARGET_TABLE).orderBy(F.desc("tx_time")).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Stop the stream
# MAGIC
# MAGIC Always stop running queries when you're done experimenting.

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables (DLT)
# MAGIC
# MAGIC `solve()` composes inside a DLT pipeline. A minimal DLT table definition:
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from rulebricks_spark import solve
# MAGIC
# MAGIC @dlt.table(name="scored_transactions")
# MAGIC def scored_transactions():
# MAGIC     raw = dlt.read_stream("raw_transactions")
# MAGIC     return solve(raw, rule=RULE_SLUG, api_key=api_key)
# MAGIC ```
# MAGIC
# MAGIC Store `RULE_SLUG` and `api_key` in DLT pipeline configuration (or
# MAGIC resolve `api_key` via Databricks secrets at pipeline start).
