# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 - run and inspect the full pipeline (Raw -> Bronze -> Silver -> Gold)
# MAGIC Run top to bottom (**Run all**). Each section shows the results needed for the report.
# MAGIC
# MAGIC Screenshots to take are marked with 📸.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
from pyspark.sql import functions as F
from src import config, etl_pipeline as etl, gold_analytics as gold, quality_checks as qc

config.configure_adls(spark)
BATCH = etl.new_batch_id()
print("Batch ID for this run:", BATCH)

# COMMAND ----------

# MAGIC %md ## 1. Bronze

# COMMAND ----------

bronze_result = etl.run_bronze(spark, mode="full", batch_id=BATCH)
print(json.dumps(bronze_result, default=str, indent=2))
bronze = spark.read.format("delta").load(config.BRONZE_PATH)
display(bronze.limit(10))   # 📸 Bronze Delta table preview

# COMMAND ----------

# MAGIC %md ### Idempotency and incremental loading

# COMMAND ----------

before = bronze.count()
etl.run_bronze(spark, mode="full", batch_id=BATCH)
march = lambda df: df.filter("booking_date BETWEEN '2024-03-01' AND '2024-03-31'").count()
march_before = march(spark.read.format("delta").load(config.BRONZE_PATH))
for _ in range(2):
    etl.run_bronze(spark, mode="incremental", start_date="2024-03-01", end_date="2024-03-31", batch_id=BATCH)
after = spark.read.format("delta").load(config.BRONZE_PATH)
print(f"Rows before: {before} | after full re-run + 2 incremental March runs: {after.count()}")
print(f"March rows before: {march_before} | after: {march(after)}")
assert after.count() == before and march(after) == march_before, "Bronze is not idempotent!"
print("✓ Bronze is idempotent and incremental loads replace only their date range")

# COMMAND ----------

# MAGIC %md ## 2. Silver and Quarantine

# COMMAND ----------

silver_result = etl.run_silver(spark, mode="full", batch_id=BATCH)
print(json.dumps(silver_result, default=str, indent=2))
silver = spark.read.format("delta").load(config.SILVER_PATH)
quarantine = spark.read.format("delta").load(config.QUARANTINE_PATH)
print(f"Silver: {silver.count()} | Quarantine: {quarantine.count()} | Bronze: {bronze.count()}")
display(silver.limit(10))       # 📸 Silver Delta table preview

# COMMAND ----------

display(quarantine.select("booking_id", "booking_date", "booking_status", "reject_reason", "quarantined_at").limit(10))  # 📸 Quarantine preview
display(quarantine.select(F.explode("reject_reasons").alias("reason")).groupBy("reason").count())

# COMMAND ----------

# MAGIC %md ## 3. Quality checks on Bronze and Silver

# COMMAND ----------

q = qc.run_quality_checks(spark, ["bronze", "silver"], BATCH)
print(json.dumps(q["summary"], indent=2))
display(spark.createDataFrame([{k: str(v) for k, v in r.items() if k != "sample_failing_records"}
                               for r in q["results"]]))   # 📸 quality check results

# COMMAND ----------

# MAGIC %md ## 4. Gold analytics

# COMMAND ----------

gold_result = gold.run_gold(spark, BATCH)
print(json.dumps(gold_result, default=str, indent=2))
for name, path in config.GOLD_TABLES.items():
    print(f"--- {name}")
    display(spark.read.format("delta").load(path).limit(10))   # 📸 one or two Gold previews

# COMMAND ----------

gq = qc.run_quality_checks(spark, ["gold"], BATCH)
print(json.dumps(gq["summary"], indent=2))
display(spark.createDataFrame([{k: str(v) for k, v in r.items() if k != "sample_failing_records"}
                               for r in gq["results"]]))

# COMMAND ----------

# MAGIC %md ## 5. Load log and Delta history

# COMMAND ----------

display(spark.read.format("delta").load(config.LOAD_LOG_PATH).orderBy(F.desc("started_at")).limit(20))
display(spark.sql(f"DESCRIBE HISTORY delta.`{config.SILVER_PATH}`").select("version", "timestamp", "operation").limit(10))
