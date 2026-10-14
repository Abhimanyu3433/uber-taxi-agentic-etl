"""
agents.py - multi-agent system that runs, validates and reports on the ETL pipeline.

LangGraph StateGraph (routing from the project spec):

    __start__ --> supervisor
    supervisor --run_etl--> etl        supervisor --quality--> quality
    supervisor --analytics--> analytics supervisor --explain--> explain
    etl --success--> quality           etl --failure--> __end__
    quality --pass--> analytics        quality --fail--> __end__
    analytics --> __end__              explain --> __end__

Design rules
  * Foundry GPT reasons (routing, severity, explanations, remediation) using structured
    JSON outputs; deterministic rules decide. A failed HARD rule always stops the run and is
    always 'critical' - the model can never override it.
  * The model only ever sees structured check results with Booking/Customer IDs masked,
    never raw tables.
  * Agents can only call approved tools, only write to reports/, every tool call is logged,
    tokens/retries/steps are capped, and destructive actions need human approval.
  * If Foundry is not configured or the token budget is spent, the agents fall back to
    deterministic explanations so the pipeline is still operated safely.
"""

import json
import operator
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Annotated, Optional, TypedDict

from src import config

ROUTES = ["run_etl", "quality", "analytics", "explain"]
SEVERITIES = ["critical", "warning", "info"]


# ===========================================================================
# Guardrail helpers
# ===========================================================================
_ID_PATTERNS = [(re.compile(r"\bCNR(\d+)"), "CNR"), (re.compile(r"\bCID(\d+)"), "CID")]


def mask_sensitive(text: str) -> str:
    """Mask Booking / Customer IDs before anything is sent to the LLM (keep last 3 digits)."""
    for pattern, prefix in _ID_PATTERNS:
        text = pattern.sub(lambda m: f"{prefix}***{m.group(1)[-3:]}", text)
    return text


def _now():
    return datetime.now(timezone.utc)


class GuardrailViolation(Exception):
    """Raised when an agent tries something outside its permissions."""


def _check_path(path: str, allowed: list, action: str):
    if not any(path.startswith(a.rstrip("/")) for a in allowed):
        raise GuardrailViolation(f"{action} not allowed on {path}")


# ===========================================================================
# Microsoft Foundry GPT client (single client shared by all agents)
# ===========================================================================
class FoundryClient:
    """
    Thin wrapper over the Azure OpenAI-compatible endpoint of a GPT deployment in Microsoft Foundry.
    Auth: Microsoft Entra ID with the project's service principal (preferred) or an API key
    from the secret scope. Every call asks for a JSON object and is counted against the token cap.
    """

    def __init__(self, spark, max_tokens_per_run: int = config.LLM_MAX_TOKENS_PER_RUN):
        self.spark = spark
        self.endpoint = config.get_setting(spark, "FOUNDRY_ENDPOINT", "foundry_endpoint", config.FOUNDRY_ENDPOINT)
        self.deployment = config.get_setting(spark, "FOUNDRY_DEPLOYMENT", "foundry_deployment", config.FOUNDRY_DEPLOYMENT)
        self.max_tokens_per_run = max_tokens_per_run
        self.tokens_used = 0
        self.calls = 0
        self.client = None
        self.error = None
        if not (self.endpoint and self.deployment):
            self.error = "Foundry endpoint/deployment not configured"
            return
        try:
            from openai import AzureOpenAI
            if config.FOUNDRY_AUTH_MODE == "key":
                self.client = AzureOpenAI(azure_endpoint=self.endpoint, api_version=config.FOUNDRY_API_VERSION,
                                          api_key=config.get_secret(spark, "foundry_api_key"))
            else:
                from azure.identity import ClientSecretCredential, get_bearer_token_provider
                cred = ClientSecretCredential(config.get_secret(spark, "tenant_id"),
                                              config.get_secret(spark, "client_id"),
                                              config.get_secret(spark, "client_secret"))
                token_provider = get_bearer_token_provider(cred, "https://cognitiveservices.azure.com/.default")
                self.client = AzureOpenAI(azure_endpoint=self.endpoint, api_version=config.FOUNDRY_API_VERSION,
                                          azure_ad_token_provider=token_provider)
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"

    @property
    def available(self) -> bool:
        return self.client is not None and self.tokens_used < self.max_tokens_per_run

    def chat_json(self, system: str, payload: dict, max_tokens: int = config.LLM_MAX_TOKENS_PER_CALL) -> Optional[dict]:
        """Send a masked, structured payload; return the parsed JSON answer (None if unavailable)."""
        if not self.available:
            return None
        user = mask_sensitive(json.dumps(payload, default=str))[:12000]
        try:
            resp = self.client.chat.completions.create(
                model=self.deployment,
                messages=[{"role": "system", "content": system + "\nRespond with a single JSON object only."},
                          {"role": "user", "content": user}],
                temperature=config.LLM_TEMPERATURE,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            self.calls += 1
            if resp.usage:
                self.tokens_used += resp.usage.total_tokens
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            self.error = f"{type(e).__name__}: {str(e)[:300]}"
            return None


# ===========================================================================
# Tools (the ONLY actions agents may take)
# ===========================================================================
class PipelineTools:
    """Pipeline operations wrapped as named, typed tools. Every call is logged."""

    APPROVED = ["run_etl_job", "get_run_status", "run_quality_checks", "get_table_stats",
                "compare_row_counts", "run_gold_analytics", "write_report"]

    def __init__(self, spark, approvals: Optional[dict] = None):
        self.spark = spark
        self.approvals = approvals or {}
        self.log = []
        self._ws = None

    # ---- infrastructure -------------------------------------------------
    @property
    def ws(self):
        if self._ws is None:
            from databricks.sdk import WorkspaceClient
            self._ws = WorkspaceClient()
        return self._ws

    def call(self, agent: str, tool: str, **kwargs):
        """Run an approved tool and log it (name, agent, args, outcome, duration)."""
        if tool not in self.APPROVED:
            raise GuardrailViolation(f"tool {tool!r} is not approved")
        started = time.time()
        entry = {"ts": _now().isoformat(), "agent": agent, "tool": tool,
                 "args": {k: v for k, v in kwargs.items() if k != "report"}}
        try:
            result = getattr(self, tool)(**kwargs)
            entry["status"] = "ok"
            return result
        except Exception as e:
            entry["status"] = f"error: {type(e).__name__}: {str(e)[:200]}"
            raise
        finally:
            entry["seconds"] = round(time.time() - started, 2)
            self.log.append(entry)
            print(f"[tool] {agent} -> {tool} {entry['args']} : {entry['status']} ({entry['seconds']}s)")

    def _job_id(self) -> int:
        if config.ETL_JOB_ID:
            return int(config.ETL_JOB_ID)
        job = next(iter(self.ws.jobs.list(name=config.JOB_NAME)), None)
        if not job:
            raise RuntimeError(f"Databricks Job {config.JOB_NAME!r} not found - run notebooks/create_job first")
        return job.job_id

    # ---- tools ------------------------------------------------------------
    def run_etl_job(self, mode: str = "incremental", start_date: str = "", end_date: str = "",
                    tasks: str = "bronze,silver") -> dict:
        """Trigger the Databricks Job. Full refreshes and large backfills need human approval."""
        if mode == "full" and not self.approvals.get("full_refresh"):
            raise GuardrailViolation("full refresh overwrites Bronze/Silver: human approval required "
                                     "(pass approvals={'full_refresh': True})")
        if mode == "incremental" and start_date and end_date:
            days = (datetime.fromisoformat(end_date) - datetime.fromisoformat(start_date)).days + 1
            if days > config.MAX_BACKFILL_DAYS and not self.approvals.get("large_backfill"):
                raise GuardrailViolation(f"backfill of {days} days exceeds the {config.MAX_BACKFILL_DAYS}-day "
                                         "limit: human approval required (approvals={'large_backfill': True})")
        params = {"mode": mode, "start_date": start_date or "", "end_date": end_date or "", "tasks": tasks}
        waiter = self.ws.jobs.run_now(job_id=self._job_id(), job_parameters=params)
        run_id = getattr(waiter, "run_id", None) or waiter.response.run_id
        return {"run_id": run_id, "job_parameters": params}

    def get_run_status(self, run_id: int, wait: bool = True, timeout_s: int = config.JOB_TIMEOUT_SECONDS) -> dict:
        """Poll a job run until it finishes (or times out) and collect each task's output / error."""
        deadline = time.time() + timeout_s
        while True:
            run = self.ws.jobs.get_run(run_id=run_id)
            life = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
            if not wait or life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR") or time.time() > deadline:
                break
            time.sleep(config.JOB_POLL_SECONDS)
        timed_out = life not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
        if timed_out:
            self.ws.jobs.cancel_run(run_id=run_id)   # stop a run that exceeded its time budget
        tasks = []
        for t in run.tasks or []:
            info = {"task": t.task_key,
                    "state": t.state.result_state.value if t.state and t.state.result_state else life,
                    "attempt": t.attempt_number}
            try:
                out = self.ws.jobs.get_run_output(run_id=t.run_id)
                if out.error:
                    info["error"] = out.error[:1500]
                    info["error_trace"] = (out.error_trace or "")[-1500:]
                if out.notebook_output and out.notebook_output.result:
                    try:
                        info["output"] = json.loads(out.notebook_output.result)
                    except ValueError:
                        info["output"] = out.notebook_output.result[:1500]
            except Exception as e:
                info["output_error"] = str(e)[:200]
            tasks.append(info)
        result_state = run.state.result_state.value if run.state and run.state.result_state else None
        return {"run_id": run_id, "life_cycle_state": life, "result_state": result_state,
                "timed_out": timed_out, "run_page_url": run.run_page_url,
                "duration_s": round((run.execution_duration or 0) / 1000 + (run.setup_duration or 0) / 1000, 1),
                "tasks": tasks}

    def run_quality_checks(self, layers: list, batch_id: str, start_date: str = None, end_date: str = None) -> dict:
        from src import quality_checks
        return quality_checks.run_quality_checks(self.spark, layers, batch_id, start_date, end_date)

    def get_table_stats(self, layer: str) -> dict:
        """Row count, date range and last update of a layer (read-only)."""
        paths = {"bronze": config.BRONZE_PATH, "silver": config.SILVER_PATH,
                 "quarantine": config.QUARANTINE_PATH, **{f"gold.{k}": v for k, v in config.GOLD_TABLES.items()}}
        path = paths[layer]
        _check_path(path, config.AGENT_READ_PATHS, "read")
        from pyspark.sql import functions as F
        df = self.spark.read.format("delta").load(path)
        stats = {"layer": layer, "rows": df.count()}
        if "booking_date" in df.columns:
            r = df.agg(F.min("booking_date").alias("lo"), F.max("booking_date").alias("hi"),
                       F.countDistinct("booking_date").alias("days")).first()
            stats.update(min_date=str(r["lo"]), max_date=str(r["hi"]), distinct_days=r["days"])
        hist = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first()
        stats["last_updated"] = str(hist["timestamp"]) if hist else None
        return stats

    def compare_row_counts(self, start_date: str = None, end_date: str = None) -> dict:
        """Bronze vs Silver + Quarantine reconciliation."""
        from pyspark.sql import functions as F
        counts = {}
        for name, path in [("bronze", config.BRONZE_PATH), ("silver", config.SILVER_PATH),
                           ("quarantine", config.QUARANTINE_PATH)]:
            df = self.spark.read.format("delta").load(path)
            if start_date and end_date:
                df = df.filter(F.col("booking_date").between(start_date, end_date))
            counts[name] = df.count()
        counts["difference"] = counts["bronze"] - counts["silver"] - counts["quarantine"]
        counts["reconciled"] = counts["difference"] == 0
        return counts

    def run_gold_analytics(self, batch_id: str) -> dict:
        from src import gold_analytics
        return gold_analytics.run_gold(self.spark, batch_id)

    def write_report(self, run_id: str, report: dict, markdown: str) -> dict:
        """Write the run report (JSON + markdown) - the only place agents may write."""
        base = config.abfss("reports", f"runs/{run_id}")
        _check_path(base, config.AGENT_WRITE_PATHS, "write")
        dbu = config.get_dbutils(self.spark)
        dbu.fs.put(f"{base}.json", json.dumps(report, indent=2, default=str), overwrite=True)
        dbu.fs.put(f"{base}.md", markdown, overwrite=True)
        return {"json": f"{base}.json", "markdown": f"{base}.md"}


# ===========================================================================
# Shared graph state
# ===========================================================================
class AgentState(TypedDict, total=False):
    run_id: str
    request: str
    route: str
    route_reason: str
    mode: str
    start_date: str
    end_date: str
    batch_id: str
    nodes_run: Annotated[list, operator.add]
    findings: Annotated[list, operator.add]
    etl_result: dict
    quality_result: dict
    analytics_result: dict
    row_counts: dict
    status: str
    stopped_at: str
    escalated_to_human: bool
    answer: str
    errors: Annotated[list, operator.add]
    started_at: str


# ===========================================================================
# Prompts
# ===========================================================================
SUPERVISOR_PROMPT = """You are the supervisor of a data-pipeline agent team for Uber ride bookings
(medallion ETL on Databricks: Bronze -> Silver -> Gold). Classify the user's request into exactly one route:
- "run_etl": run the pipeline / load or reload data (then it is validated and Gold is rebuilt)
- "quality": validate existing Bronze and Silver data without loading anything
- "analytics": rebuild and validate the Gold analytics tables only
- "explain": a question about runs, data quality, failures or results (read-only)
Also extract an optional booking-date range (yyyy-MM-dd) and load mode ("incremental" or "full").
Return JSON: {"route": ..., "mode": "incremental"|"full"|null, "start_date": str|null,
"end_date": str|null, "reason": "<one sentence>"}"""

SEVERITY_PROMPT = """You are a senior data-quality engineer. You receive structured results of FAILED
data-quality checks for an Uber ride-booking pipeline (no raw data). For each check give:
severity ("critical" | "warning" | "info"), a plain-English explanation of the likely cause (one or two
sentences, cite the metric and threshold), and a concrete recommended action.
Checks marked hard=true are blocking rules: always classify them "critical".
Return JSON: {"findings": [{"rule": str, "severity": str, "explanation": str,
"recommended_action": str}], "overall_assessment": str}"""

FAILURE_PROMPT = """You are a data-platform engineer. A Databricks Job task of the Uber ride ETL failed.
From the error message and trace, explain in plain language what went wrong (likely root cause),
and the recommended fix. Return JSON: {"root_cause": str, "explanation": str,
"recommended_action": str, "retryable": true|false}"""

EXPLAIN_PROMPT = """You are the explain agent for an Uber ride ETL platform. Answer the user's question
in plain language using ONLY the context provided (recent runs, quality results, table statistics).
If the context does not contain the answer, say so. Return JSON: {"answer": str, "sources": [str]}"""


# ===========================================================================
# The agent system
# ===========================================================================
class AgentSystem:
    def __init__(self, spark, approvals: Optional[dict] = None, llm: Optional[FoundryClient] = None):
        self.spark = spark
        self.tools = PipelineTools(spark, approvals)
        self.llm = llm or FoundryClient(spark)
        self.app = self._build_graph()

    # ---------------------------------------------------------------- nodes
    def supervisor(self, state: AgentState) -> dict:
        request = state.get("request", "")
        decision = self.llm.chat_json(SUPERVISOR_PROMPT, {"request": request}, max_tokens=200) or {}
        route = decision.get("route")
        if route not in ROUTES:                       # deterministic fallback / validation
            text = request.lower()
            if any(w in text for w in ["why", "what", "explain", "how many", "?"]):
                route = "explain"
            elif any(w in text for w in ["gold", "analytics", "dashboard"]):
                route = "analytics"
            elif any(w in text for w in ["validate", "check", "quality"]):
                route = "quality"
            else:
                route = "run_etl"
            decision["reason"] = f"keyword fallback ({'LLM unavailable' if not self.llm.available else 'invalid LLM route'})"
        update = {"route": route, "route_reason": decision.get("reason", ""), "nodes_run": ["supervisor"]}
        for k in ["mode", "start_date", "end_date"]:
            if not state.get(k) and decision.get(k):
                update[k] = decision[k]
        print(f"[supervisor] route={route} ({update['route_reason']})")
        return update

    def etl(self, state: AgentState) -> dict:
        mode = state.get("mode") or "incremental"
        start, end = state.get("start_date") or "", state.get("end_date") or ""
        if mode == "incremental" and not (start and end):
            start, end = config.EXPECTED_START_DATE, config.EXPECTED_END_DATE
            mode = "full"                            # no range given -> full refresh
        upd = {"nodes_run": ["etl"], "mode": mode, "start_date": start, "end_date": end}
        attempts, status = 0, None
        try:
            while attempts <= config.AGENT_MAX_RETRIES:
                attempts += 1
                started = self.tools.call("etl", "run_etl_job", mode=mode, start_date=start if mode != "full" else "",
                                          end_date=end if mode != "full" else "", tasks="bronze,silver")
                status = self.tools.call("etl", "get_run_status", run_id=started["run_id"])
                # retry only platform errors; a data/code failure will not fix itself
                if status["life_cycle_state"] == "INTERNAL_ERROR" and attempts <= config.AGENT_MAX_RETRIES:
                    continue
                break
        except GuardrailViolation as g:
            return {**upd, "status": "BLOCKED_PENDING_APPROVAL", "stopped_at": "etl", "escalated_to_human": True,
                    "findings": [{"rule": "guardrail", "severity": "critical", "explanation": str(g),
                                  "recommended_action": "A human must approve this action and re-run."}]}
        upd["batch_id"] = str(status["run_id"])       # the Job uses {{job.run_id}} as batch_id
        upd["etl_result"] = {**status, "attempts": attempts}
        if status["result_state"] == "SUCCESS" and not status["timed_out"]:
            upd["etl_result"]["outcome"] = "success"
            return upd

        failed = [t for t in status["tasks"] if t.get("state") not in ("SUCCESS", "SKIPPED", "EXCLUDED")]
        err = failed[0] if failed else {"task": "job", "error": status.get("result_state")}
        ai = self.llm.chat_json(FAILURE_PROMPT, {"task": err.get("task"), "error": err.get("error"),
                                                 "trace_tail": err.get("error_trace", "")[-800:],
                                                 "timed_out": status["timed_out"]}) or {}
        upd["etl_result"]["outcome"] = "failure"
        upd.update(status="FAIL", stopped_at="etl", escalated_to_human=True, findings=[{
            "rule": f"etl_job_{err.get('task')}_failed", "severity": "critical",
            "explanation": ai.get("explanation") or f"Task {err.get('task')} failed: {str(err.get('error'))[:300]}",
            "root_cause": ai.get("root_cause"),
            "recommended_action": ai.get("recommended_action") or "Inspect the task error in the Job run page, fix the "
                                  "source data or configuration, then re-run the pipeline."}])
        return upd

    def _llm_findings(self, failed_checks: list) -> tuple:
        """Combine GPT severity/explanations with deterministic rules (hard -> always critical)."""
        if not failed_checks:
            return [], "All checks passed."
        payload = [{k: c[k] for k in ["rule", "layer", "category", "hard", "metric_value", "threshold",
                                      "failing_count", "message", "sample_failing_records"]} for c in failed_checks]
        ai = self.llm.chat_json(SEVERITY_PROMPT, {"failed_checks": payload}) or {}
        by_rule = {f.get("rule"): f for f in ai.get("findings", []) if isinstance(f, dict)}
        findings = []
        for c in failed_checks:
            f = by_rule.get(c["rule"], {})
            severity = f.get("severity") if f.get("severity") in SEVERITIES else ("critical" if c["hard"] else "warning")
            if c["hard"]:
                severity = "critical"                # the model can never downgrade a hard rule
            findings.append({"rule": c["rule"], "layer": c["layer"], "severity": severity, "hard_rule": c["hard"],
                             "metric_value": c["metric_value"], "threshold": c["threshold"],
                             "explanation": f.get("explanation") or c["message"],
                             "recommended_action": f.get("recommended_action")
                             or ("Investigate the failing records before re-running." if c["hard"]
                                 else "Review the quarantined records and the source export.")})
        return findings, ai.get("overall_assessment", "LLM assessment unavailable; deterministic severities used.")

    def quality(self, state: AgentState) -> dict:
        batch = state.get("batch_id") or f"agent-{state['run_id']}"
        rng = (state.get("start_date"), state.get("end_date")) if state.get("mode") == "incremental" else (None, None)
        out = self.tools.call("quality", "run_quality_checks", layers=["bronze", "silver"], batch_id=batch,
                              start_date=rng[0], end_date=rng[1])
        counts = self.tools.call("quality", "compare_row_counts", start_date=rng[0], end_date=rng[1])
        failed = [r for r in out["results"] if r["status"] == "FAIL"]
        findings, assessment = self._llm_findings(failed)
        summary = out["summary"]
        upd = {"nodes_run": ["quality"], "batch_id": batch, "row_counts": counts, "findings": findings,
               "quality_result": {**summary, "assessment": assessment}}
        if summary["hard_failed"]:                    # deterministic stop condition
            upd.update(status="FAIL", stopped_at="quality",
                       escalated_to_human=any(f["severity"] == "critical" for f in findings))
        else:
            upd["status"] = summary["status"]
        return upd

    def analytics(self, state: AgentState) -> dict:
        batch = state.get("batch_id") or f"agent-{state['run_id']}"
        gold = self.tools.call("analytics", "run_gold_analytics", batch_id=batch)
        out = self.tools.call("analytics", "run_quality_checks", layers=["gold"], batch_id=batch)
        failed = [r for r in out["results"] if r["status"] == "FAIL"]
        findings, assessment = self._llm_findings(failed)
        prev = state.get("status")
        gs = out["summary"]["status"]
        status = "FAIL" if gs == "FAIL" else ("PASS_WITH_WARNINGS" if "WARN" in (prev or "") + gs else "PASS")
        upd = {"nodes_run": ["analytics"], "findings": findings, "status": status,
               "analytics_result": {"tables": gold.get("tables"), **out["summary"], "assessment": assessment}}
        if gs == "FAIL":
            upd.update(stopped_at="analytics", escalated_to_human=True)
        return upd

    def explain(self, state: AgentState) -> dict:
        context = {"recent_runs": self._recent_runs(5), "latest_quality_results": self._latest_quality(),
                   "tables": {}}
        for layer in ["bronze", "silver", "quarantine"]:
            try:
                context["tables"][layer] = self.tools.call("explain", "get_table_stats", layer=layer)
            except Exception as e:
                context["tables"][layer] = {"error": str(e)[:200]}
        ai = self.llm.chat_json(EXPLAIN_PROMPT, {"question": state.get("request"), "context": context}) or {}
        answer = ai.get("answer") or ("(LLM unavailable) Latest context:\n" + json.dumps(context, default=str)[:3000])
        return {"nodes_run": ["explain"], "answer": answer, "status": "ANSWERED"}

    # ------------------------------------------------------------ routing
    @staticmethod
    def _route_from_supervisor(state: AgentState) -> str:
        return state["route"]

    @staticmethod
    def _after_etl(state: AgentState) -> str:
        return "success" if state.get("etl_result", {}).get("outcome") == "success" else "failure"

    @staticmethod
    def _after_quality(state: AgentState) -> str:
        return "fail" if state.get("status") == "FAIL" else "pass"

    def _build_graph(self):
        from langgraph.graph import END, START, StateGraph
        g = StateGraph(AgentState)
        g.add_node("supervisor", self.supervisor)
        g.add_node("etl", self.etl)
        g.add_node("quality", self.quality)
        g.add_node("analytics", self.analytics)
        g.add_node("explain", self.explain)
        g.add_edge(START, "supervisor")
        g.add_conditional_edges("supervisor", self._route_from_supervisor,
                                {"run_etl": "etl", "quality": "quality", "analytics": "analytics", "explain": "explain"})
        g.add_conditional_edges("etl", self._after_etl, {"success": "quality", "failure": END})
        g.add_conditional_edges("quality", self._after_quality, {"pass": "analytics", "fail": END})
        g.add_edge("analytics", END)
        g.add_edge("explain", END)
        return g.compile()

    # ------------------------------------------------------------ history / context
    def _recent_runs(self, n: int) -> list:
        try:
            from pyspark.sql import functions as F
            rows = (self.spark.read.format("delta").load(config.RUN_HISTORY_PATH)
                    .orderBy(F.desc("finished_at")).limit(n).collect())
            return [{k: r[k] for k in ["run_id", "route", "status", "stopped_at", "nodes_run", "row_counts",
                                       "findings", "finished_at"]} for r in rows]
        except Exception:
            return []

    def _latest_quality(self) -> list:
        try:
            from pyspark.sql import functions as F
            q = self.spark.read.format("delta").load(config.QUALITY_RESULTS_PATH)
            last = q.agg(F.max("run_timestamp")).first()[0]
            return [r.asDict() for r in q.filter(F.col("run_timestamp") == last)
                    .select("rule", "layer", "status", "hard", "metric_value", "threshold", "message").collect()]
        except Exception:
            return []

    def _monitor(self, duration_s: float) -> dict:
        """Optional monitoring: compare this run's duration with the historical baseline."""
        try:
            from pyspark.sql import functions as F
            hist = (self.spark.read.format("delta").load(config.RUN_HISTORY_PATH)
                    .filter(F.col("route") == "run_etl").agg(F.avg("duration_s").alias("avg"),
                                                             F.count("*").alias("n")).first())
            if not hist["n"]:
                return {"baseline_s": None, "drift": False, "note": "no baseline yet"}
            drift = duration_s > 1.5 * hist["avg"]
            return {"baseline_s": round(hist["avg"], 1), "this_run_s": duration_s, "drift": drift,
                    "note": "run took >50% longer than baseline" if drift else "within baseline"}
        except Exception:
            return {"baseline_s": None, "drift": False, "note": "no run history yet"}

    # ------------------------------------------------------------ reporting (explain agent)
    def _report(self, state: dict, duration_s: float) -> dict:
        q = state.get("quality_result", {})
        a = state.get("analytics_result", {})
        passed = (q.get("checks_passed") or 0) + (a.get("checks_passed") or 0)
        failed = (q.get("checks_failed") or 0) + (a.get("checks_failed") or 0)
        return {
            "run_id": state["run_id"],
            "request": state.get("request"),
            "route": state.get("route"),
            "route_reason": state.get("route_reason"),
            "nodes_run": state.get("nodes_run", []),
            "status": state.get("status", "UNKNOWN"),
            "stopped_at": state.get("stopped_at"),
            "row_counts": state.get("row_counts", {}),
            "checks_passed": passed,
            "checks_failed": failed,
            "findings": state.get("findings", []),
            "escalated_to_human": bool(state.get("escalated_to_human")),
            "answer": state.get("answer"),
            "etl_job": {k: state.get("etl_result", {}).get(k) for k in ["run_id", "result_state", "run_page_url",
                                                                  "duration_s", "attempts"]} if state.get("etl_result") else None,
            "llm": {"deployment": self.llm.deployment, "calls": self.llm.calls, "tokens_used": self.llm.tokens_used,
                    "available": self.llm.client is not None, "error": self.llm.error},
            "monitoring": self._monitor(duration_s),
            "duration_s": duration_s,
            "tool_calls": self.tools.log,
            "started_at": state.get("started_at"),
            "finished_at": _now().isoformat(),
        }

    @staticmethod
    def _markdown(r: dict) -> str:
        lines = [f"# Pipeline run report `{r['run_id']}`", "",
                 f"* **Request:** {r['request']}", f"* **Route:** {r['route']} ({r['route_reason']})",
                 f"* **Nodes run:** {' → '.join(r['nodes_run'])}",
                 f"* **Status:** **{r['status']}**" + (f" (stopped at `{r['stopped_at']}`)" if r['stopped_at'] else ""),
                 f"* **Checks:** {r['checks_passed']} passed, {r['checks_failed']} failed",
                 f"* **Row counts:** {r['row_counts']}",
                 f"* **Escalated to human:** {r['escalated_to_human']}",
                 f"* **Duration:** {r['duration_s']} s | LLM calls: {r['llm']['calls']}, tokens: {r['llm']['tokens_used']}",
                 ""]
        if r.get("answer"):
            lines += ["## Answer", "", r["answer"], ""]
        if r["findings"]:
            lines += ["## Findings", "", "| Severity | Rule | Explanation | Recommended action |", "|---|---|---|---|"]
            for f in r["findings"]:
                lines.append(f"| {f['severity']} | `{f['rule']}` | {str(f.get('explanation','')).replace('|','/')} | "
                             f"{str(f.get('recommended_action','')).replace('|','/')} |")
            lines.append("")
        lines += ["## Tool calls", "", "| Agent | Tool | Status | Seconds |", "|---|---|---|---|"]
        lines += [f"| {t['agent']} | {t['tool']} | {t['status']} | {t['seconds']} |" for t in r["tool_calls"]]
        return "\n".join(lines)

    def _persist(self, report: dict):
        """Store the run in run history (Delta) so it can be inspected and explained later."""
        ddl = ("run_id string, request string, route string, status string, stopped_at string, nodes_run string, "
               "row_counts string, findings string, escalated_to_human boolean, tokens_used long, duration_s double, "
               "report_json string, finished_at timestamp")
        row = (report["run_id"], report["request"], report["route"], report["status"], report["stopped_at"],
               json.dumps(report["nodes_run"]), json.dumps(report["row_counts"], default=str),
               json.dumps(report["findings"], default=str), report["escalated_to_human"],
               int(report["llm"]["tokens_used"]), float(report["duration_s"]),
               json.dumps(report, default=str), _now())
        (self.spark.createDataFrame([row], schema=ddl).write.format("delta").mode("append")
         .option("mergeSchema", "true").save(config.RUN_HISTORY_PATH))

    # ------------------------------------------------------------ entry point
    def run(self, request: str, mode: str = None, start_date: str = None, end_date: str = None,
            print_summary: bool = True) -> dict:
        run_id = f"{_now():%Y-%m-%dT%H-%M-%S}-{uuid.uuid4().hex[:4]}"
        started = time.time()
        state = {"run_id": run_id, "request": request, "started_at": _now().isoformat(),
                 "nodes_run": [], "findings": [], "errors": []}
        for k, v in {"mode": mode, "start_date": start_date, "end_date": end_date}.items():
            if v:
                state[k] = v
        try:
            final = self.app.invoke(state, config={"recursion_limit": config.AGENT_MAX_STEPS})
        except Exception as e:                       # never lose the report because a node crashed
            final = {**state, "status": "ERROR", "escalated_to_human": True,
                     "findings": [{"rule": "agent_error", "severity": "critical",
                                   "explanation": f"{type(e).__name__}: {str(e)[:500]}",
                                   "recommended_action": "Check the agent run logs."}]}
        duration = round(time.time() - started, 1)
        report = self._report(final, duration)
        try:
            report["report_files"] = self.tools.call("explain", "write_report", run_id=run_id, report=report,
                                                     markdown=self._markdown(report))
            self._persist(report)
        except Exception as e:
            report["report_error"] = str(e)[:300]
        if print_summary:
            print(json.dumps({k: report[k] for k in ["run_id", "route", "nodes_run", "status", "row_counts",
                                                      "checks_passed", "checks_failed", "findings",
                                                      "escalated_to_human"]}, indent=2, default=str))
            if report.get("answer"):
                print("\nANSWER:", report["answer"])
        return report

    def export_graph(self) -> str:
        """Save the LangGraph diagram (PNG via mermaid) to reports/agent_graph.png; returns mermaid text."""
        mermaid = self.app.get_graph().draw_mermaid()
        try:
            png = self.app.get_graph().draw_mermaid_png()
            import base64
            dbu = config.get_dbutils(self.spark)
            local = "/tmp/agent_graph.png"
            with open(local, "wb") as f:
                f.write(png)
            dbu.fs.cp(f"file:{local}", config.abfss("reports", "agent_graph.png"))
        except Exception as e:
            print("PNG export unavailable (needs internet for mermaid.ink):", str(e)[:150])
        return mermaid
