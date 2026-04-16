# Databricks notebook source

# MAGIC %md
# MAGIC # Breach Triage — Rulebricks + Databricks
# MAGIC
# MAGIC End-to-end example of applying a Rulebricks decision table to a Spark
# MAGIC DataFrame at scale. The `breach-triage` rule classifies incident reports
# MAGIC by priority, flags reportable events, and produces a composite risk
# MAGIC score plus a rationale string.
# MAGIC
# MAGIC **What you'll see:**
# MAGIC 1. A Delta table of synthetic incident reports
# MAGIC 2. One function call to apply a rule to every row
# MAGIC 3. A live rule edit in the Rulebricks UI → rerun the same cell → different results, zero code change
# MAGIC 4. Decision audit logs
# MAGIC 5. The same rule running against 100,000 records
# MAGIC
# MAGIC **Before running:** see `README.md` in this directory for how to build
# MAGIC the `breach-triage` rule in Rulebricks and retrieve its slug + API key.

# COMMAND ----------

# MAGIC %pip install rulebricks-spark
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set your rule's slug and wire up the API key. Rulebricks slugs are short
# MAGIC opaque IDs (about 10 characters) — copy yours from the Rulebricks UI.
# MAGIC See `README.md` for details.

# COMMAND ----------

RULE_SLUG = "<your_rule_slug>"

api_key = dbutils.secrets.get(scope="rulebricks", key="api_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Create synthetic incident reports
# MAGIC
# MAGIC A small, hand-authored Delta table so every tier of the decision table
# MAGIC fires at least once. Replace with your real data once you've validated
# MAGIC the end-to-end flow.

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
    ("R-001", "Acme Holdings",            "policy_violation",       2_500_000.0,  True,  12000, 14),
    ("R-002", "Global Services Co",       "disclosure_failure",       180_000.0, False,    320,  7),
    ("R-003", "Meridian Capital",         "insider_activity",      15_000_000.0,  True,      3,  2),
    ("R-004", "Northwind Advisors",       "fiduciary_breach",       4_200_000.0,  True,   8500, 30),
    ("R-005", "Bluehaven Partners",       "policy_violation",          45_000.0, False,     80, 45),
    ("R-006", "Evergreen Investments",    "disclosure_failure",       750_000.0,  True,   1200, 10),
    ("R-007", "Summit Advisory",          "compliance_gap",           320_000.0, False,    450, 60),
    ("R-008", "Ironclad Holdings",        "conflict_of_interest",   8_900_000.0,  True,  15000, 21),
    ("R-009", "Lighthouse Financial",     "fiduciary_breach",         120_000.0, False,    150, 90),
    ("R-010", "Cascade Group",            "insider_activity",       6_300_000.0,  True,      1,  5),
    ("R-011", "Atlas Insurance",          "policy_violation",       1_100_000.0,  True,   2200, 18),
    ("R-012", "Beacon Retirement",        "compliance_gap",            85_000.0, False,    600, 35),
    ("R-013", "Copperleaf Advisors",      "disclosure_failure",        50_000.0, False,     40, 120),
    ("R-014", "Delphi Securities",        "conflict_of_interest",     950_000.0,  True,    900, 12),
    ("R-015", "Everstone Holdings",       "fiduciary_breach",       3_400_000.0,  True,   5000,  8),
    ("R-016", "Fairway Capital",          "compliance_gap",           200_000.0, False,    300, 28),
    ("R-017", "Granite Peak Investments", "policy_violation",      12_000_000.0,  True,  20000, 15),
    ("R-018", "Harborline Financial",     "disclosure_failure",       400_000.0, False,    180, 55),
    ("R-019", "Ironbridge Markets",       "insider_activity",       9_500_000.0,  True,      5,  3),
    ("R-020", "Juniper Asset Mgmt",       "compliance_gap",           650_000.0,  True,   3000, 22),
]

df = spark.createDataFrame(data, schema=schema)
df.write.mode("overwrite").saveAsTable("breach_reports_raw")

display(spark.table("breach_reports_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Apply the Rulebricks decision table
# MAGIC
# MAGIC The `breach-triage` rule is authored in the **Rulebricks UI** by the
# MAGIC team that owns the business logic — not in this notebook. It evaluates
# MAGIC each report and returns:
# MAGIC
# MAGIC | Output | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `priority` | string | P1 (critical) → P4 (low) |
# MAGIC | `reportable` | boolean | Should this incident be escalated or reported externally? |
# MAGIC | `risk_score` | number | 0–100 composite risk score |
# MAGIC | `rationale` | string | Human-readable explanation |
# MAGIC
# MAGIC **The integration is one function call.**

# COMMAND ----------

from rulebricks_spark import solve

scored = solve(
    spark.table("breach_reports_raw"),
    rule=RULE_SLUG,
    api_key=api_key,
)

scored.write.mode("overwrite").saveAsTable("breach_reports_scored")

display(scored.orderBy("risk_score", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Inspect the results
# MAGIC
# MAGIC All 20 reports are now triaged. Notice the new columns — `priority`,
# MAGIC `reportable`, `risk_score`, `rationale`, and `_rb_error` (null = success).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-priority reportable incidents
# MAGIC SELECT
# MAGIC   report_id,
# MAGIC   entity_name,
# MAGIC   incident_type,
# MAGIC   financial_impact,
# MAGIC   priority,
# MAGIC   reportable,
# MAGIC   risk_score,
# MAGIC   rationale
# MAGIC FROM breach_reports_scored
# MAGIC WHERE priority IN ('P1', 'P2') AND reportable = true
# MAGIC ORDER BY risk_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · The live-edit moment
# MAGIC
# MAGIC **Switch to the Rulebricks UI.**
# MAGIC
# MAGIC 1. Open the `breach-triage` decision table
# MAGIC 2. Change the `financial_impact` threshold for P1 (e.g. **\$5M → \$2M**)
# MAGIC 3. Click **Publish**
# MAGIC
# MAGIC Then come back here and **rerun Cell 2 above**.
# MAGIC
# MAGIC The output changes — more reports are now P1 — with **zero code changes**
# MAGIC and **zero redeployment**. The business logic lives in Rulebricks, not in
# MAGIC your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Decision audit trail
# MAGIC
# MAGIC Every rule execution is logged. Query who decided what and when —
# MAGIC essential for regulatory accountability and incident review.

# COMMAND ----------

from rulebricks import Rulebricks

client = Rulebricks(api_key=api_key)

logs = client.decisions.query(
    rules=RULE_SLUG,
    statuses="200",
    limit=5,
)

for entry in logs.data:
    print(f"{entry.timestamp}  {entry.request.get('report_id', '?')} → {entry.response.get('priority', '?')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Scale
# MAGIC
# MAGIC The 20-row demo above was for narrative clarity. Real workflows run
# MAGIC against months of accumulated records. Here we generate **100,000
# MAGIC synthetic incident reports** and score the whole table in one job.
# MAGIC
# MAGIC Effective throughput scales with
# MAGIC `num_executors × max_concurrent_requests_per_partition`.

# COMMAND ----------

from pyspark.sql import functions as F

INCIDENT_TYPES = [
    "policy_violation",
    "disclosure_failure",
    "insider_activity",
    "fiduciary_breach",
    "compliance_gap",
    "conflict_of_interest",
]

incident_type_arr = F.array(*[F.lit(t) for t in INCIDENT_TYPES])

large_df = (
    spark.range(100_000)
    .withColumn("report_id", F.concat(F.lit("R-"), F.lpad(F.col("id").cast("string"), 7, "0")))
    .withColumn("entity_name", F.concat(F.lit("Entity-"), (F.col("id") % 500).cast("string")))
    .withColumn(
        "incident_type",
        incident_type_arr.getItem((F.col("id") % F.lit(len(INCIDENT_TYPES))).cast("int")),
    )
    .withColumn("financial_impact", (F.rand(seed=42) * 20_000_000).cast("double"))
    .withColumn("customer_harm", F.rand(seed=43) > F.lit(0.5))
    .withColumn("parties_affected", (F.rand(seed=44) * 20_000).cast("int"))
    .withColumn("days_open", (F.rand(seed=45) * 180).cast("int"))
    .drop("id")
)

large_df.write.mode("overwrite").saveAsTable("breach_reports_large")
print(f"Generated {spark.table('breach_reports_large').count():,} synthetic reports")

# COMMAND ----------

import time

start = time.time()

scored_large = solve(
    spark.table("breach_reports_large").repartition(8),
    rule=RULE_SLUG,
    api_key=api_key,
    batch_size=1000,
    max_concurrent_requests_per_partition=8,
)
scored_large.write.mode("overwrite").saveAsTable("breach_reports_large_scored")

elapsed = time.time() - start
print(f"Scored 100,000 reports in {elapsed:.1f}s "
      f"({100_000 / elapsed:,.0f} rows/sec)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Priority distribution across 100k reports
# MAGIC SELECT priority, COUNT(*) AS n, ROUND(AVG(risk_score), 1) AS avg_risk
# MAGIC FROM breach_reports_large_scored
# MAGIC GROUP BY priority
# MAGIC ORDER BY priority

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concern | How Rulebricks addresses it |
# MAGIC |---|---|
# MAGIC | **Databricks integration** | `pip install rulebricks-spark` → one function call |
# MAGIC | **Non-technical authorship** | Visual decision table UI — domain experts, not engineers |
# MAGIC | **Audit trail** | Every decision versioned, logged, queryable |
# MAGIC | **Performance** | Bulk batching, concurrent requests, scales with Spark executors |
# MAGIC | **Change management** | Edit a rule → publish → pipeline behavior changes instantly, no redeploy |
