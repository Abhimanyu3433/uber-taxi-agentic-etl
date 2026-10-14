# Databricks notebook source
# MAGIC %md
# MAGIC # Create (or update) the Databricks Job
# MAGIC Defines the `uber-taxi-etl` Job **as code** with the Databricks SDK:
# MAGIC
# MAGIC `bronze` → `silver` → `quality` → `gold`
# MAGIC
# MAGIC * every task runs `notebooks/jobs/run_task` on `uber-etl-cluster`
# MAGIC * retries: each task retries once after 1 minute; tasks time out after 30 minutes
# MAGIC * failure alerts: e-mail on failure (set `ALERT_EMAIL` below, default = you)
# MAGIC * job parameters: `mode`, `start_date`, `end_date`, `tasks`
# MAGIC
# MAGIC Running this notebook again updates the same Job instead of creating a duplicate.

# COMMAND ----------

ALERT_EMAIL = ""   # leave empty to use your own Databricks login e-mail

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from src import config

w = WorkspaceClient()   # authenticates automatically inside Databricks
me = w.current_user.me().user_name
alert_to = ALERT_EMAIL or config.ALERT_EMAIL or me

cluster = next((c for c in w.clusters.list() if c.cluster_name == config.CLUSTER_NAME), None)
assert cluster, f"Cluster {config.CLUSTER_NAME!r} not found"

repo_root = os.path.dirname(os.getcwd()).replace("/Workspace", "", 1)
task_notebook = f"{repo_root}/notebooks/jobs/run_task"
print("Cluster:", cluster.cluster_id, "| Task notebook:", task_notebook, "| Alerts to:", alert_to)

# COMMAND ----------

def task(key, depends_on=None):
    return jobs.Task(
        task_key=key,
        description=f"Uber taxi ETL - {key}",
        existing_cluster_id=cluster.cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=task_notebook,
            source=jobs.Source.WORKSPACE,
            base_parameters={"task_name": key, "batch_id": "{{job.run_id}}"},
        ),
        depends_on=[jobs.TaskDependency(task_key=d) for d in (depends_on or [])],
        max_retries=1,
        min_retry_interval_millis=60_000,
        retry_on_timeout=True,
        timeout_seconds=config.JOB_TIMEOUT_SECONDS,
    )

settings = dict(
    name=config.JOB_NAME,
    description="Uber taxi agentic ETL: Raw -> Bronze -> Silver -> quality checks -> Gold",
    tasks=[task("bronze"), task("silver", ["bronze"]), task("quality", ["silver"]), task("gold", ["quality"])],
    parameters=[
        jobs.JobParameterDefinition(name="mode", default="full"),
        jobs.JobParameterDefinition(name="start_date", default=""),
        jobs.JobParameterDefinition(name="end_date", default=""),
        jobs.JobParameterDefinition(name="tasks", default=config.DEFAULT_JOB_TASKS),
    ],
    email_notifications=jobs.JobEmailNotifications(on_failure=[alert_to]),
    max_concurrent_runs=1,
    tags={"project": "uber-taxi-agentic-etl"},
)

existing = next(iter(w.jobs.list(name=config.JOB_NAME)), None)
if existing:
    w.jobs.reset(job_id=existing.job_id, new_settings=jobs.JobSettings(**settings))
    job_id = existing.job_id
    print("Updated existing job", job_id)
else:
    job_id = w.jobs.create(**settings).job_id
    print("Created job", job_id)

print(f"\n➡ Put this in your config / .env:  ETL_JOB_ID={job_id}")
print(f"Job page: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md ## Run the Job once on demand (Phase 1 exit criteria)
# MAGIC This waits until the run finishes (about 5-10 minutes). 📸 Screenshot the Job run page / run history.

# COMMAND ----------

run = w.jobs.run_now(job_id=job_id).result()
print("Run state:", run.state.result_state, "| run page:", run.run_page_url)
for t in run.tasks:
    print(f"  {t.task_key:8s} {t.state.result_state}")
