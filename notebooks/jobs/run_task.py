# Databricks notebook source
# MAGIC %md
# MAGIC # Job task runner
# MAGIC One notebook used by every task of the `uber-taxi-etl` Databricks Job.
# MAGIC The task to run comes from the `task_name` parameter: `bronze`, `silver`, `quality` or `gold`.
# MAGIC
# MAGIC Job parameters (shared by all tasks):
# MAGIC * `mode` - `full` or `incremental`
# MAGIC * `start_date`, `end_date` - booking-date range for incremental loads (`yyyy-MM-dd`)
# MAGIC * `tasks` - which tasks should actually run, e.g. `bronze,silver` (others exit as SKIPPED).
# MAGIC   The agents use this to run only Bronze -> Silver.
# MAGIC
# MAGIC The result is returned with `dbutils.notebook.exit(...)` so the agents can read it
# MAGIC through the Jobs API (`get_run_output`).

# COMMAND ----------

dbutils.widgets.text("task_name", "bronze")
dbutils.widgets.text("mode", "full")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("tasks", "bronze,silver,quality,gold")
dbutils.widgets.text("batch_id", "")

task = dbutils.widgets.get("task_name").strip().lower()
mode = dbutils.widgets.get("mode").strip().lower() or "full"
start_date = dbutils.widgets.get("start_date").strip() or None
end_date = dbutils.widgets.get("end_date").strip() or None
tasks = [t.strip().lower() for t in dbutils.widgets.get("tasks").split(",") if t.strip()]
batch_id = dbutils.widgets.get("batch_id").strip() or None
print(dict(task=task, mode=mode, start_date=start_date, end_date=end_date, tasks=tasks, batch_id=batch_id))

# COMMAND ----------

import json, os, sys
sys.dont_write_bytecode = True
repo_root = os.path.dirname(os.path.dirname(os.getcwd()))   # notebooks/jobs -> repo root
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src import config, etl_pipeline, gold_analytics, quality_checks
config.configure_adls(spark)

# COMMAND ----------

def finish(result: dict):
    dbutils.notebook.exit(json.dumps(result, default=str))

if task not in tasks:
    finish({"task": task, "status": "SKIPPED", "reason": f"not in tasks={tasks}"})

if task == "bronze":
    result = etl_pipeline.run_bronze(spark, mode, start_date, end_date, batch_id)

elif task == "silver":
    result = etl_pipeline.run_silver(spark, mode, start_date, end_date, batch_id)

elif task == "quality":
    out = quality_checks.run_quality_checks(spark, ["bronze", "silver"], batch_id, start_date, end_date)
    result = {"task": "quality", **out["summary"],
              "failed_checks": [r for r in out["results"] if r["status"] == "FAIL"]}
    if out["summary"]["hard_failed"]:
        # Failing the task makes the Job fail -> retry policy + failure e-mail alert
        raise Exception(f"Hard data quality rules failed: {out['summary']['hard_failed']}")

elif task == "gold":
    result = gold_analytics.run_gold(spark, batch_id)
    out = quality_checks.run_quality_checks(spark, ["gold"], batch_id)
    result["gold_checks"] = out["summary"]
    if out["summary"]["hard_failed"]:
        raise Exception(f"Gold validation failed: {out['summary']['hard_failed']}")

else:
    raise ValueError(f"Unknown task_name {task!r}")

result["task"] = task
finish(result)
