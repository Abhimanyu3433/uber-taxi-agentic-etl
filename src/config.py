"""
config.py - central configuration for the uber-taxi-agentic-etl project.

Every path, threshold and setting used by the pipeline and the agents lives here,
so no value is hard-coded in pipeline code.

How values are resolved (first match wins):
  1. Environment variable (e.g. STORAGE_ACCOUNT=...)   -> easy to override per job
  2. The default written below                          -> sensible project defaults
Secrets are NEVER stored here. They are read at runtime from the Databricks
secret scope (backed by Azure Key Vault) via get_secret().
"""

import os


def _env(name, default):
    """Read a setting from an environment variable, falling back to a default."""
    return os.environ.get(name, default)


# ---------------------------------------------------------------------------
# 1. Azure storage (ADLS Gen2) and secrets
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = _env("STORAGE_ACCOUNT", "stuberetlabhi01")
ADLS_HOST = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"

SECRET_SCOPE = _env("SECRET_SCOPE", "uber-etl-scope")
# Names of the secrets inside the scope (the values live in Key Vault)
SECRET_KEYS = {
    "client_id": "sp-client-id",
    "tenant_id": "sp-tenant-id",
    "client_secret": "sp-client-secret",
    "foundry_api_key": "foundry-api-key",   # only needed if FOUNDRY_AUTH_MODE = "key"
    # not secret, but kept in Key Vault so no cluster settings need editing:
    "foundry_endpoint": "foundry-endpoint",
    "foundry_deployment": "foundry-deployment",
}

CONTAINERS = ["raw", "bronze", "silver", "gold", "quarantine", "reports"]


def abfss(container, path=""):
    """Build a full ADLS path, e.g. abfss://silver@<account>.dfs.core.windows.net/uber_rides"""
    return f"abfss://{container}@{ADLS_HOST}/{path}"


# ---------------------------------------------------------------------------
# 2. Paths for each medallion layer
# ---------------------------------------------------------------------------
DATASET = "uber_rides"

RAW_PATH = abfss("raw", f"{DATASET}/")                 # landing zone (CSV, as received)
BRONZE_PATH = abfss("bronze", DATASET)                 # Delta: raw data + ingestion metadata
SILVER_PATH = abfss("silver", DATASET)                 # Delta: cleaned, typed, deduplicated
QUARANTINE_PATH = abfss("quarantine", DATASET)         # Delta: rejected records + reason
GOLD_BASE = abfss("gold")                              # Gold tables live under gold/<name>
REPORTS_PATH = abfss("reports")                        # agent run reports (JSON / markdown)

# Gold analytics tables (name -> path)
GOLD_TABLES = {
    name: abfss("gold", name)
    for name in [
        "daily_bookings",              # bookings, completion rate, revenue per day
        "cancellations_by_reason",     # customer vs driver cancellation reasons
        "revenue_by_vehicle_payment",  # revenue by vehicle type and payment method
        "peak_hour_demand",            # bookings per hour of day / day of week
        "top_locations",               # top pickup and drop locations
        "tat_by_vehicle",              # average VTAT and CTAT by vehicle type
        "ratings_by_vehicle",          # average driver and customer ratings
        "incomplete_by_reason",        # incomplete rides by reason
    ]
}

# Operational tables (Delta) used by quality checks and agents
QUALITY_RESULTS_PATH = abfss("reports", "quality_results")   # history of every check result
RUN_HISTORY_PATH = abfss("reports", "run_history")           # history of every agent run
LOAD_LOG_PATH = abfss("bronze", "_load_log")                 # which files/dates were ingested

# Unity Catalog namespace (Hive metastore is disabled in this workspace)
CATALOG = _env("CATALOG", "dbw_uber_etl")
SCHEMA = _env("SCHEMA", "uber_etl")


# ---------------------------------------------------------------------------
# 3. Source schema: 21 columns, source name -> snake_case name used from Bronze on
#    Bronze stores every column as STRING (raw values preserved);
#    Silver casts to the types in SILVER_TYPES.
# ---------------------------------------------------------------------------
SOURCE_COLUMNS = [
    ("Date", "date"),
    ("Time", "time"),
    ("Booking ID", "booking_id"),
    ("Booking Status", "booking_status"),
    ("Customer ID", "customer_id"),
    ("Vehicle Type", "vehicle_type"),
    ("Pickup Location", "pickup_location"),
    ("Drop Location", "drop_location"),
    ("Avg VTAT", "avg_vtat"),
    ("Avg CTAT", "avg_ctat"),
    ("Cancelled Rides by Customer", "cancelled_by_customer_flag"),
    ("Reason for cancelling by Customer", "customer_cancel_reason"),
    ("Cancelled Rides by Driver", "cancelled_by_driver_flag"),
    ("Driver Cancellation Reason", "driver_cancel_reason"),
    ("Incomplete Rides", "incomplete_flag"),
    ("Incomplete Rides Reason", "incomplete_reason"),
    ("Booking Value", "booking_value"),
    ("Ride Distance", "ride_distance"),
    ("Driver Ratings", "driver_rating"),
    ("Customer Rating", "customer_rating"),
    ("Payment Method", "payment_method"),
]
SOURCE_COLUMN_NAMES = [src for src, _ in SOURCE_COLUMNS]
COLUMN_RENAMES = dict(SOURCE_COLUMNS)

SILVER_TYPES = {
    "avg_vtat": "double",
    "avg_ctat": "double",
    "cancelled_by_customer_flag": "int",
    "cancelled_by_driver_flag": "int",
    "incomplete_flag": "int",
    "booking_value": "double",
    "ride_distance": "double",
    "driver_rating": "double",
    "customer_rating": "double",
}

# CSV reading options (confirmed during profiling)
CSV_OPTIONS = {
    "header": "true",
    "nullValue": "null",   # missing values are the literal text "null"
    "escape": '"',         # IDs are stored as """CNR5884300""" in the file
    "mode": "PERMISSIVE",
}

# Profiling showed yyyy-MM-dd; the spec says dd-MM-yyyy. Both are accepted, in this order.
DATE_FORMATS = ["yyyy-MM-dd", "dd-MM-yyyy"]
TIME_FORMAT = "HH:mm:ss"


# ---------------------------------------------------------------------------
# 4. Business rules and allowed values (confirmed during profiling)
# ---------------------------------------------------------------------------
ALLOWED_STATUSES = [
    "Completed",
    "Cancelled by Driver",
    "Cancelled by Customer",
    "No Driver Found",
    "Incomplete",
]
ALLOWED_VEHICLE_TYPES = ["Auto", "Go Mini", "Go Sedan", "Bike", "Premier Sedan", "eBike", "Uber XL"]
ALLOWED_PAYMENT_METHODS = ["UPI", "Cash", "Uber Wallet", "Credit Card", "Debit Card"]

# Which flag column must be 1 for each status (all other flags must be null)
STATUS_FLAG = {
    "Cancelled by Customer": "cancelled_by_customer_flag",
    "Cancelled by Driver": "cancelled_by_driver_flag",
    "Incomplete": "incomplete_flag",
}
# Each flag is paired with its reason column (reason present exactly when flag = 1)
FLAG_REASON = {
    "cancelled_by_customer_flag": "customer_cancel_reason",
    "cancelled_by_driver_flag": "driver_cancel_reason",
    "incomplete_flag": "incomplete_reason",
}

KEY_COLUMNS = ["booking_id", "date", "time", "booking_status", "customer_id",
               "vehicle_type", "pickup_location", "drop_location"]
# Fields a Completed booking must have
COMPLETED_REQUIRED = ["booking_value", "ride_distance", "payment_method"]

RATING_MIN, RATING_MAX = 1.0, 5.0


# ---------------------------------------------------------------------------
# 5. Data quality thresholds
# ---------------------------------------------------------------------------
QUALITY_THRESHOLDS = {
    # completeness: share of nulls allowed in key columns
    "key_null_pct_max": 0.0,
    # outcome completeness: share of Completed bookings missing value/distance/payment
    "completed_missing_pct_max": 0.001,
    # uniqueness: duplicate Booking IDs allowed in Bronze (source has ~0.8% ID collisions);
    # Silver must have none
    "bronze_duplicate_pct_max": 0.01,
    "silver_duplicate_pct_max": 0.0,
    # validity / consistency: share of rows allowed to break the rule
    "invalid_pct_max": 0.0,
    "inconsistent_pct_max": 0.0,
    # quarantine: share of Bronze rows rejected before it counts as a problem
    "quarantine_pct_max": 0.02,
    # null spike: max rise in a column's null rate vs the previous load (absolute)
    "null_spike_max": 0.05,
    # volume: allowed drop vs previous load / expected daily volume
    "volume_tolerance_pct": 0.20,
    "expected_daily_rows": 411,
    # timeliness: missing days allowed in the loaded date range
    "missing_days_max": 0,
    # reconciliation: Bronze rows must equal Silver + Quarantine exactly
    "reconciliation_diff_max": 0,
    # gold plausibility: completion rate must stay within this band
    "completion_rate_min": 0.40,
    "completion_rate_max": 0.90,
}


# ---------------------------------------------------------------------------
# 6. Pipeline / Databricks Job settings
# ---------------------------------------------------------------------------
LOAD_MODE = _env("LOAD_MODE", "full")            # "full" or "incremental" (by booking date)
# Date range the data is expected to cover (timeliness check). Profiling: 2024-01-01..2024-12-30
EXPECTED_START_DATE = _env("EXPECTED_START_DATE", "2024-01-01")
EXPECTED_END_DATE = _env("EXPECTED_END_DATE", "2024-12-30")
# Tasks the Job runs by default; agents pass a subset, e.g. "bronze,silver"
DEFAULT_JOB_TASKS = "bronze,silver,quality,gold"
JOB_NAME = _env("JOB_NAME", "uber-taxi-etl")
CLUSTER_NAME = _env("CLUSTER_NAME", "uber-etl-cluster")
ALERT_EMAIL = _env("ALERT_EMAIL", "")            # failure alerts for the Databricks Job
DATABRICKS_HOST = _env("DATABRICKS_HOST", "")    # e.g. https://adb-xxxx.azuredatabricks.net
ETL_JOB_ID = _env("ETL_JOB_ID", "")              # set after the Job is created (Stage 11)
JOB_TIMEOUT_SECONDS = int(_env("JOB_TIMEOUT_SECONDS", "1800"))
JOB_POLL_SECONDS = int(_env("JOB_POLL_SECONDS", "20"))


# ---------------------------------------------------------------------------
# 7. Microsoft Foundry GPT settings (filled in Stage 12)
# ---------------------------------------------------------------------------
FOUNDRY_ENDPOINT = _env("FOUNDRY_ENDPOINT", "")          # e.g. https://<resource>.openai.azure.com/
FOUNDRY_DEPLOYMENT = _env("FOUNDRY_DEPLOYMENT", "")      # the deployment name you chose
FOUNDRY_API_VERSION = _env("FOUNDRY_API_VERSION", "2024-10-21")
FOUNDRY_AUTH_MODE = _env("FOUNDRY_AUTH_MODE", "entra")   # "entra" (preferred) or "key"
LLM_TEMPERATURE = 0.0                                    # deterministic answers
LLM_MAX_TOKENS_PER_CALL = 800
LLM_MAX_TOKENS_PER_RUN = 20000                           # cost cap per agent run


# ---------------------------------------------------------------------------
# 8. Agent guardrails
# ---------------------------------------------------------------------------
AGENT_MAX_RETRIES = 2              # retries for a failed tool call / job run
AGENT_MAX_STEPS = 12               # hard stop for the agent graph
MAX_BACKFILL_DAYS = 31             # larger backfills need human approval
AGENT_READ_PATHS = [BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH, GOLD_BASE, REPORTS_PATH]
AGENT_WRITE_PATHS = [REPORTS_PATH]  # agents may only write reports
DESTRUCTIVE_ACTIONS = ["delete", "overwrite", "backfill"]  # always require human approval
SENSITIVE_COLUMNS = ["booking_id", "customer_id"]          # masked before sending to the LLM


# ---------------------------------------------------------------------------
# 9. Runtime helpers (Databricks only)
# ---------------------------------------------------------------------------
def get_dbutils(spark):
    """Return the Databricks dbutils object from inside a .py module."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def get_secret(spark, name):
    """Read a secret from the secret scope; falls back to an environment variable (local tests)."""
    key = SECRET_KEYS[name]
    try:
        return get_dbutils(spark).secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        value = os.environ.get(key.upper().replace("-", "_"))
        if value is None:
            raise RuntimeError(f"Secret '{key}' not found in scope '{SECRET_SCOPE}' or environment")
        return value


def get_setting(spark, env_name, secret_name, default=""):
    """A setting from the environment, else the secret scope, else a default."""
    value = os.environ.get(env_name)
    if value:
        return value
    try:
        return get_secret(spark, secret_name)
    except Exception:
        return default


def configure_adls(spark):
    """Let Spark read/write ADLS as the service principal (OAuth). Call once per session."""
    client_id = get_secret(spark, "client_id")
    tenant_id = get_secret(spark, "tenant_id")
    client_secret = get_secret(spark, "client_secret")
    h = ADLS_HOST
    spark.conf.set(f"fs.azure.account.auth.type.{h}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{h}",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{h}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{h}", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{h}",
                   f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
