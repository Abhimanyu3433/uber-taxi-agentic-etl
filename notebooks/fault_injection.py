# Databricks notebook source
# MAGIC %md
# MAGIC # Fault injection - Phase 1 exit criteria
# MAGIC The pipeline and quality checks must **pass on clean data** and **fail correctly on deliberately
# MAGIC corrupted data**. For each scenario this notebook:
# MAGIC 1. corrupts the raw landing zone (`src/fault_injection.py`)
# MAGIC 2. runs Bronze → Silver → quality checks
# MAGIC 3. records whether the fault was detected and by which rule
# MAGIC 4. restores the original data
# MAGIC
# MAGIC At the end the clean pipeline is rebuilt so the tables are healthy again.
# MAGIC 📸 Screenshot the final results table.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json, time
from src import config, etl_pipeline as etl, quality_checks as qc, fault_injection as fi
config.configure_adls(spark)

EXPECTED_DETECTION = {          # which rule(s) should catch each fault
    "healthy": [],
    "missing_source_file": ["SourceFileMissingError"],
    "schema_change": ["SchemaMismatchError"],
    "duplicate_records": ["bronze_duplicate_booking_id"],
    "null_spike": ["bronze_unexpected_null_spike", "completed_rides_missing_booking_value", "quarantine_rate"],
    "out_of_range_values": ["bronze_negative_value_or_distance", "bronze_rating_out_of_range", "quarantine_rate"],
    "status_flag_mismatch": ["bronze_flag_status_consistency", "quarantine_rate"],
    "row_count_drop": ["bronze_timeliness_missing_days", "bronze_volume_vs_previous_load", "bronze_volume_vs_expected"],
    "job_failure": ["ValueError"],
}

def run_scenario(name):
    desc = fi.inject(spark, name)
    started = time.time()
    batch = etl.new_batch_id()
    outcome = {"scenario": name, "description": desc}
    try:
        mode = "bogus-mode" if name == "job_failure" else "full"     # job_failure: invalid parameter
        etl.run_bronze(spark, mode=mode, batch_id=batch)
        etl.run_silver(spark, mode="full", batch_id=batch)
        out = qc.run_quality_checks(spark, ["bronze", "silver"], batch)
        failed = [r["rule"] for r in out["results"] if r["status"] == "FAIL"]
        outcome.update(pipeline_status=out["summary"]["status"], caught_by=failed,
                       hard_failed=out["summary"]["hard_failed"])
    except Exception as e:
        outcome.update(pipeline_status="STOPPED_WITH_ERROR", caught_by=[type(e).__name__],
                       hard_failed=[type(e).__name__], error=str(e)[:200])
    finally:
        fi.restore(spark)
    expected = EXPECTED_DETECTION[name]
    if name == "healthy":
        outcome["correct"] = outcome["pipeline_status"] in ("PASS", "PASS_WITH_WARNINGS") and not outcome["hard_failed"]
    else:
        outcome["correct"] = bool(outcome["hard_failed"]) and any(x in outcome["caught_by"] for x in expected)
    outcome["seconds"] = round(time.time() - started, 1)
    print(json.dumps(outcome, default=str))
    return outcome

# COMMAND ----------

results = [run_scenario(s) for s in fi.SCENARIOS]

# COMMAND ----------

display(spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in results])
        .select("scenario", "pipeline_status", "hard_failed", "caught_by", "correct", "seconds"))
print(f"Correctly handled: {sum(r['correct'] for r in results)} / {len(results)}")

# COMMAND ----------

# MAGIC %md ## Rebuild the clean pipeline

# COMMAND ----------

print(fi.restore(spark))
b = etl.new_batch_id()
etl.run_bronze(spark, "full", batch_id=b)
etl.run_silver(spark, "full", batch_id=b)
print(json.dumps(qc.run_quality_checks(spark, ["bronze", "silver"], b)["summary"], indent=2))
