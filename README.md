# uber-taxi-agentic-etl

Capstone Project 3: an **agentic ETL pipeline** for Uber ride-booking data (Delhi NCR, 150,000 bookings, 2024).

* **Phase 1:** a PySpark medallion pipeline on **Azure Databricks**, with data in **ADLS Gen2**:
  Raw CSV → Bronze → Silver (+ Quarantine) → Gold. It has automated data-quality checks and runs as a **Databricks Job**.
* **Phase 2:** a **LangGraph** multi-agent system powered by a **GPT model on Microsoft Foundry**. The agents run the
  Job, validate every layer, stop on failures, explain them in plain language, and write a run report.

```
ADLS Gen2 (stuberetlabhi01)                Azure Databricks (dbw-uber-etl)                  Microsoft Foundry
raw/uber_rides/*.csv ──► bronze ──► silver ──► gold/<8 tables>         GPT deployment
                                   └─► quarantine                     ▲
reports/  ◄── quality_results, run_history, runs/<run_id>.json|md ◄── agents (supervisor → etl → quality → analytics | explain)
Secrets: Azure Key Vault ──► Databricks secret scope "uber-etl-scope" (service principal sp-uber-etl)
```

## Repository layout

```
uber-taxi-agentic-etl/
├── data/                     sample_rides.csv (152-row test sample), schema.json
├── notebooks/
│   ├── setup.ipynb           Spark session, ADLS access, containers, UC schema, connectivity test
│   ├── setup_foundry.py      Foundry GPT connection test (Entra ID auth)
│   ├── profiling             data profiling (status values, null patterns, VTAT/CTAT, date format)
│   ├── run_tests.py          runs the pytest suite
│   ├── phase1_pipeline.py    runs and inspects Bronze → Silver → quality → Gold
│   ├── fault_injection.py    Phase 1 exit criteria: checks fail correctly on corrupted data
│   ├── create_job.py         creates/updates the Databricks Job as code and runs it once
│   ├── jobs/run_task.py      the notebook every Job task runs (bronze | silver | quality | gold)
│   ├── agent_demo.py         Phase 2 demo: graph, full run, validation, guardrail, question
│   └── evaluation.py         Phase 2 fault injection + evaluation metrics
├── src/
│   ├── config.py             all paths, thresholds, allowed values, model and guardrail settings
│   ├── etl_pipeline.py       Raw → Bronze → Silver (+ Quarantine)
│   ├── gold_analytics.py     Silver → 8 Gold tables, OPTIMIZE / Z-ORDER
│   ├── quality_checks.py     reusable rules returning structured results
│   ├── agents.py             Foundry client, tools, guardrails, LangGraph supervisor + 4 agents
│   └── fault_injection.py    test harness that corrupts the landing zone (human use only)
├── tests/                    pytest unit tests (Bronze, Silver, Gold, quality)
├── requirements.txt
└── .env.example              every setting by name (no secret values)
```

## Setup (one time)

| # | Where | What |
|---|---|---|
| 1 | Azure portal | Resource group `rg-uber-etl` (East US 2) |
| 2 | Azure portal | Storage account `stuberetlabhi01` with **hierarchical namespace**; containers `raw bronze silver gold quarantine reports`; CSV uploaded to `raw/uber_rides/` |
| 3 | Entra ID | App registration `sp-uber-etl` + client secret; role **Storage Blob Data Contributor** on the storage account |
| 4 | Key Vault | `kv-uber-etl-abhi01` with secrets `sp-client-id`, `sp-tenant-id`, `sp-client-secret` |
| 5 | Databricks | Workspace `dbw-uber-etl` (Premium, Hybrid); Key Vault-backed secret scope `uber-etl-scope` (`#secrets/createScope`) |
| 6 | Databricks | Cluster `uber-etl-cluster`: single node, latest LTS runtime, **Dedicated** access mode, 20-min auto-termination; PyPI libraries `azure-identity azure-storage-file-datalake databricks-sdk openai langgraph langchain-core` |
| 7 | Databricks | Git folder linked to this repo; run `notebooks/setup.ipynb` |

Secrets never appear in code. `config.configure_adls(spark)` reads them from the secret scope at runtime.

### Phase 2 setup (Microsoft Foundry)
1. In Microsoft Foundry ([ai.azure.com](https://ai.azure.com)), create a project/resource in `rg-uber-etl` and **deploy a GPT model**
   (e.g. a GPT-4o-mini / GPT-4.1-mini class model). Note the **endpoint** and the **deployment name**.
2. On the Foundry / Azure OpenAI resource, go to **Access control (IAM)** and give `sp-uber-etl` the role **Cognitive Services OpenAI User**.
   This lets the agents use Entra ID authentication instead of an API key.
3. In Key Vault, add the secrets `foundry-endpoint` and `foundry-deployment`.
4. Run `notebooks/setup_foundry.py`. It should print `✓ Foundry GPT reachable`.

## Execution order

| Step | Notebook | Result |
|---|---|---|
| 1 | `setup.ipynb` | ADLS reachable, containers and schema ready |
| 2 | `profiling` | data understood (see findings below) |
| 3 | `run_tests.py` | all unit tests pass |
| 4 | `phase1_pipeline.py` | Bronze 150,000 / Silver ≈148,767 / Quarantine ≈1,233 / 8 Gold tables; all hard checks PASS |
| 5 | `fault_injection.py` | every corrupted scenario stopped by the right rule; healthy run passes |
| 6 | `create_job.py` | Job `uber-taxi-etl` created and run once on demand → **Phase 1 complete** |
| 7 | `setup_foundry.py` | GPT reachable |
| 8 | `agent_demo.py` | agent graph, full / validation / incremental / guarded / question runs, reports in `reports/runs/` |
| 9 | `evaluation.py` | detection, false-alarm, recovery rates, time saved, tokens and latency |

## Data findings (profiling)
* 150,000 rows × 21 columns, every day from 2024-01-01 to 2024-12-30 (365 days, 355-452 bookings/day).
* **Date format is `yyyy-MM-dd`**, not the `dd-MM-yyyy` the specification states. The parser accepts both.
* IDs are stored as `"""CNR5884300"""` (embedded quotes). They are read with `escape='"'` and stripped in Bronze.
* 5 statuses: Completed 93,000, Cancelled by Driver 27,000, No Driver Found 10,500, Cancelled by Customer 10,500, Incomplete 9,000.
* Flags match the status exactly, and each reason is present exactly when its flag is set (0 inconsistencies).
* **VTAT** (vehicle turnaround time) is present for every booking except *No Driver Found*: the time for a vehicle to reach the pickup.
  **CTAT** (customer turnaround time) is present only for Completed and Incomplete rides: the trip time once the customer is on board.
* 1,224 Booking IDs appear more than once (1,233 extra rows) with **different** dates and customers. These are ID collisions, with no
  identical rows. Silver keeps the earliest booking per ID and quarantines the rest with reason `duplicate_booking_id`.

## Design decisions
* **Bronze keeps every column as text.** Silver casts the types, so a malformed value is quarantined (and visible) instead of
  silently becoming null.
* **Idempotent writes.** Full loads overwrite; incremental loads replace only their booking-date range with Delta `replaceWhere`.
* **Quarantine instead of drop.** Every rejected row keeps its original values plus a `reject_reasons` array.
* **Hard vs soft rules.** Hard rules (schema, key completeness, null spike, duplicate spike, timeliness, volume, Silver
  cleanliness, reconciliation, quarantine rate ≤ 2%) stop the run. Soft rules (individual validity and consistency issues) are
  warnings; their rows are quarantined. The LLM classifies severity and explains, but **can never downgrade a hard rule**.
* **Unity Catalog workspace.** The Hive metastore is disabled, so the Delta tables are path-based in ADLS; the schema
  `dbw_uber_etl.uber_etl` is the project namespace. Registering the tables as UC external tables is future work.

## Agents (Phase 2)
| Node | Role | Tools |
|---|---|---|
| `supervisor` | GPT classifies the request (JSON) → `run_etl` / `quality` / `analytics` / `explain`, with a keyword fallback | Foundry client |
| `etl` | triggers the Job with `tasks=bronze,silver`, polls it, collects task output/errors; GPT explains failures | `run_etl_job`, `get_run_status` |
| `quality` | runs Bronze + Silver checks, reconciles counts, GPT classifies severity; stops on any hard failure | `run_quality_checks`, `compare_row_counts` |
| `analytics` | builds Gold, validates it (reconciles with Silver, no missing dates, plausible rates) | `run_gold_analytics`, `run_quality_checks` |
| `explain` | answers questions from run history, quality results and table stats (read-only); writes every run report | `get_table_stats`, `write_report` |

**Guardrails:** approved tools only; agents may write only under `reports/`; a full refresh or a backfill over 31 days needs
explicit human approval (`AgentSystem(spark, approvals={...})`); Booking and Customer IDs are masked before prompts; every
tool call is logged; tokens are capped per call and per run; retries, steps and job time are capped.

## Cost control
A single-node cluster with 20-minute auto-termination. A small GPT model with a token cap per run. Delete the resource group `rg-uber-etl` when finished.
