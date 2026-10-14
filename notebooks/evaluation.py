# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 - fault injection and evaluation of the agentic system
# MAGIC For every fault scenario: corrupt the landing data → ask the agents for a full run → record what they did →
# MAGIC restore the data. Then compute the metrics the spec asks for:
# MAGIC
# MAGIC | Metric | Definition used here |
# MAGIC |---|---|
# MAGIC | Detection rate | injected faults where the run ended `FAIL` |
# MAGIC | False alarm rate | healthy runs that ended `FAIL` |
# MAGIC | Recovery rate | faults stopped at the expected node **with** an explanation and recommended action |
# MAGIC | Time saved | manual validation estimate vs agent run time |
# MAGIC | Report quality | explanations printed below for manual review (score them in the report) |
# MAGIC | Cost and latency | tokens and seconds per run |
# MAGIC
# MAGIC ⏱ About 5-8 minutes per scenario (each one runs the Databricks Job). 📸 Screenshot the metrics table.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json, time
from src import config, fault_injection as fi
from src.agents import AgentSystem
config.configure_adls(spark)

MANUAL_VALIDATION_MINUTES = 45   # assumption: an engineer checking counts, nulls, dupes, logs by hand - adjust if you measured it
HEALTHY_REPEATS = 2

agents = AgentSystem(spark, approvals={"full_refresh": True})
print("LLM ready:", agents.llm.client is not None, agents.llm.error or "")

EXPECTED_STOP = {
    "missing_source_file": "etl", "schema_change": "etl", "job_failure": "etl",
    "duplicate_records": "quality", "null_spike": "quality", "out_of_range_values": "quality",
    "status_flag_mismatch": "quality", "row_count_drop": "quality",
}

# COMMAND ----------

def evaluate(scenario):
    fi.inject(spark, scenario)
    tokens_before = agents.llm.tokens_used
    t0 = time.time()
    try:
        mode = "bogus-mode" if scenario == "job_failure" else "full"
        report = agents.run(f"Run the full pipeline and validate it ({scenario} test)", mode=mode, print_summary=False)
    finally:
        fi.restore(spark)
    seconds = round(time.time() - t0, 1)
    failed = report["status"] in ("FAIL", "ERROR")
    explained = any(f.get("explanation") and f.get("recommended_action") for f in report["findings"])
    row = {"scenario": scenario, "status": report["status"], "stopped_at": report.get("stopped_at"),
           "nodes_run": " → ".join(report["nodes_run"]), "escalated": report["escalated_to_human"],
           "findings": len(report["findings"]), "tokens": agents.llm.tokens_used - tokens_before, "seconds": seconds}
    if scenario == "healthy":
        row.update(detected=None, false_alarm=failed, recovered=None)
    else:
        row.update(detected=failed, false_alarm=None,
                   recovered=failed and report.get("stopped_at") == EXPECTED_STOP[scenario] and explained)
    print(json.dumps(row, default=str))
    for f in report["findings"][:4]:
        print(f"   [{f['severity']}] {f['rule']}: {f.get('explanation')}\n      -> {f.get('recommended_action')}")
    return row

# COMMAND ----------

scenarios = ["healthy"] * HEALTHY_REPEATS + [s for s in fi.SCENARIOS if s != "healthy"]
rows = [evaluate(s) for s in scenarios]

# COMMAND ----------

faults = [r for r in rows if r["scenario"] != "healthy"]
healthy = [r for r in rows if r["scenario"] == "healthy"]
avg_s = sum(r["seconds"] for r in rows) / len(rows)
metrics = {
    "detection_rate": f"{sum(r['detected'] for r in faults)}/{len(faults)}",
    "false_alarm_rate": f"{sum(r['false_alarm'] for r in healthy)}/{len(healthy)}",
    "recovery_rate": f"{sum(bool(r['recovered']) for r in faults)}/{len(faults)}",
    "avg_agent_minutes_per_run": round(avg_s / 60, 1),
    "manual_minutes_per_run (assumed)": MANUAL_VALIDATION_MINUTES,
    "time_saved_per_run_minutes": round(MANUAL_VALIDATION_MINUTES - avg_s / 60, 1),
    "avg_tokens_per_run": round(sum(r["tokens"] for r in rows) / len(rows)),
    "total_tokens": sum(r["tokens"] for r in rows),
}
print(json.dumps(metrics, indent=2))
display(spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in rows]))      # 📸 per-scenario results
display(spark.createDataFrame([{"metric": k, "value": str(v)} for k, v in metrics.items()]))  # 📸 metrics

# COMMAND ----------

# MAGIC %md ## Leave the platform healthy

# COMMAND ----------

print(fi.restore(spark))
final = agents.run("Run the full pipeline and validate it", mode="full")
