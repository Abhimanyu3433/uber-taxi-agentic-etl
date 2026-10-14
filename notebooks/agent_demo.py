# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - multi-agent system demo
# MAGIC The supervisor routes each request to the `etl`, `quality`, `analytics` or `explain` agent
# MAGIC (LangGraph `StateGraph`, Foundry GPT for reasoning). Every run writes a JSON + markdown report
# MAGIC to `reports/runs/` and a row to the run history.
# MAGIC
# MAGIC 📸 Screenshots: the graph diagram, each run's printed summary (agent run logs), and a report file.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
from src import config
from src.agents import AgentSystem
config.configure_adls(spark)

# Human approval for destructive actions is given HERE, explicitly, by the person running the notebook.
# A full refresh overwrites Bronze/Silver, so the agents refuse it unless approved.
agents = AgentSystem(spark, approvals={"full_refresh": True})
print("LLM ready:", agents.llm.client is not None, agents.llm.error or "")

# COMMAND ----------

# MAGIC %md ## The agent graph

# COMMAND ----------

mermaid = agents.export_graph()          # also saved to reports/agent_graph.png
print(mermaid)
try:
    from IPython.display import Image, display as ipy_display
    ipy_display(Image(agents.app.get_graph().draw_mermaid_png()))   # 📸
except Exception as e:
    print("Inline PNG unavailable:", e)

# COMMAND ----------

# MAGIC %md ## 1. Full run: supervisor → etl → quality → analytics

# COMMAND ----------

full = agents.run("Run the full pipeline for all of 2024 and validate everything")

# COMMAND ----------

# MAGIC %md ## 2. Validation only: supervisor → quality → analytics

# COMMAND ----------

validate = agents.run("Validate the current Bronze and Silver data")

# COMMAND ----------

# MAGIC %md ## 3. Incremental load of one month (no approval needed: within the backfill limit)

# COMMAND ----------

inc = agents.run("Load bookings for March 2024", mode="incremental", start_date="2024-03-01", end_date="2024-03-31")

# COMMAND ----------

# MAGIC %md ## 4. Guardrail: full refresh WITHOUT human approval is blocked

# COMMAND ----------

guarded = AgentSystem(spark, approvals={}, llm=agents.llm)
blocked = guarded.run("Reload everything from scratch", mode="full")
assert blocked["status"] == "BLOCKED_PENDING_APPROVAL"

# COMMAND ----------

# MAGIC %md ## 5. A question: supervisor → explain

# COMMAND ----------

q = agents.run("Why did the last run end with warnings, and how many bookings were quarantined?")

# COMMAND ----------

# MAGIC %md ## Run history and a generated report

# COMMAND ----------

from pyspark.sql import functions as F
display(spark.read.format("delta").load(config.RUN_HISTORY_PATH)
        .select("run_id", "route", "status", "stopped_at", "nodes_run", "escalated_to_human", "tokens_used", "duration_s")
        .orderBy(F.desc("finished_at")))
print(dbutils.fs.head(full["report_files"]["markdown"], 5000))     # 📸 generated run report
