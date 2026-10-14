"""
quality_checks.py - reusable data quality and business-rule validation.

Every check returns a structured result:
    rule, layer, category, status (PASS/FAIL), hard (bool), metric_value, threshold,
    failing_count, sample_failing_records, message

Hard rules stop the pipeline when they fail (and the LLM can never override them).
Soft rules are reported as warnings; the affected rows are quarantined in Silver.

Categories (from the project spec): schema, completeness, outcome_completeness,
uniqueness, validity, consistency, timeliness, volume, reconciliation, plausibility.

The Bronze checks deliberately re-implement the rules independently of the Silver
transformation, so a bug in the pipeline cannot hide itself from the checks.
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config

T = config.QUALITY_THRESHOLDS
SAMPLE_COLS = ["booking_id", "booking_date", "booking_status", "vehicle_type", "booking_value",
               "ride_distance", "driver_rating", "customer_rating", "payment_method"]
SAMPLE_SIZE = 5

# Fields that must be populated for each outcome (confirmed in profiling, Stage 4)
EXPECTED_POPULATED = {
    "avg_vtat": ["Completed", "Cancelled by Driver", "Cancelled by Customer", "Incomplete"],
    "avg_ctat": ["Completed", "Incomplete"],
    "booking_value": ["Completed", "Incomplete"],
    "ride_distance": ["Completed", "Incomplete"],
    "payment_method": ["Completed", "Incomplete"],
    "driver_rating": ["Completed"],
    "customer_rating": ["Completed"],
}


@dataclass
class CheckResult:
    rule: str
    layer: str
    category: str
    status: str                 # PASS | FAIL
    hard: bool                  # hard rule -> stops the run on FAIL
    metric_value: float
    threshold: float
    failing_count: int = 0
    message: str = ""
    sample_failing_records: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _sample(df: DataFrame, cond) -> list:
    cols = [c for c in SAMPLE_COLS if c in df.columns]
    rows = df.filter(cond).select(*cols).limit(SAMPLE_SIZE).collect()
    return [{k: (None if v is None else str(v)) for k, v in r.asDict().items()} for r in rows]


def _rate_check(df, total, rule, layer, category, cond, threshold, hard, what) -> CheckResult:
    """Generic 'share of rows matching cond must be <= threshold' check."""
    failing = df.filter(cond).count()
    rate = failing / total if total else 0.0
    passed = rate <= threshold
    return CheckResult(
        rule=rule, layer=layer, category=category, status="PASS" if passed else "FAIL",
        hard=hard, metric_value=round(rate, 6), threshold=threshold, failing_count=failing,
        message=f"{failing} of {total} rows ({rate:.2%}) {what}; allowed {threshold:.2%}",
        sample_failing_records=[] if passed or failing == 0 else _sample(df, cond))


def _days_between(start: str, end: str) -> int:
    return (date.fromisoformat(end) - date.fromisoformat(start)).days + 1


def _previous_metric(spark: SparkSession, rule: str, batch_id: str):
    """Most recent stored metric for a rule from an earlier batch (None if no history)."""
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, config.QUALITY_RESULTS_PATH):
        return None
    row = (spark.read.format("delta").load(config.QUALITY_RESULTS_PATH)
           .filter((F.col("rule") == rule) & (F.col("batch_id") != batch_id))
           .orderBy(F.desc("run_timestamp")).select("metric_value").first())
    return None if row is None else row["metric_value"]


def _previous_successful_load(spark: SparkSession, layer: str, batch_id: str):
    """Rows written by the previous successful full load of a layer (volume baseline)."""
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, config.LOAD_LOG_PATH):
        return None
    row = (spark.read.format("delta").load(config.LOAD_LOG_PATH)
           .filter((F.col("layer") == layer) & (F.col("status") == "SUCCESS")
                   & (F.col("mode") == "full") & (F.col("batch_id") != batch_id))
           .orderBy(F.desc("finished_at")).select("rows_written").first())
    return None if row is None else row["rows_written"]


def _expected_range(start_date, end_date):
    return (start_date or config.EXPECTED_START_DATE, end_date or config.EXPECTED_END_DATE)


def _missing_days_check(df, layer, rule, start, end) -> CheckResult:
    expected = _days_between(start, end)
    present = (df.filter(F.col("booking_date").between(start, end))
                 .select("booking_date").distinct().count())
    missing = expected - present
    missing_dates = []
    if missing > 0:
        have = {r["booking_date"] for r in df.select("booking_date").distinct().collect()}
        d0 = date.fromisoformat(start)
        missing_dates = [str(d0 + timedelta(days=i)) for i in range(expected)
                         if (d0 + timedelta(days=i)) not in have][:SAMPLE_SIZE]
    passed = missing <= T["missing_days_max"]
    return CheckResult(
        rule=rule, layer=layer, category="timeliness", status="PASS" if passed else "FAIL",
        hard=True, metric_value=missing, threshold=T["missing_days_max"], failing_count=missing,
        message=f"{present} of {expected} expected days present ({start} to {end})",
        sample_failing_records=[{"missing_date": d} for d in missing_dates])


# ---------------------------------------------------------------------------
# BRONZE checks
# ---------------------------------------------------------------------------
def bronze_checks(spark, bronze: DataFrame, batch_id: str, start_date=None, end_date=None) -> list:
    from src.etl_pipeline import standardize

    results = []
    layer = "bronze"
    expected_cols = [dst for _, dst in config.SOURCE_COLUMNS] + ["source_file", "ingestion_timestamp", "batch_id"]
    missing_cols = [c for c in expected_cols if c not in bronze.columns]
    results.append(CheckResult(
        rule="bronze_schema_columns", layer=layer, category="schema",
        status="PASS" if not missing_cols else "FAIL", hard=True,
        metric_value=len(missing_cols), threshold=0, failing_count=len(missing_cols),
        message="all expected columns present" if not missing_cols else f"missing columns: {missing_cols}"))
    if missing_cols:
        return results

    df = standardize(bronze).cache()
    total = df.count()
    status = F.col("booking_status")

    # completeness: key columns must never be null
    key_null = None
    for c in config.KEY_COLUMNS:
        key_null = F.col(c).isNull() if key_null is None else (key_null | F.col(c).isNull())
    results.append(_rate_check(df, total, "bronze_key_completeness", layer, "completeness",
                               key_null, T["key_null_pct_max"], True, "have a null key field"))

    # outcome completeness: Completed bookings must have value, distance and payment method
    completed = df.filter(status == "Completed")
    n_completed = completed.count()
    for c in config.COMPLETED_REQUIRED:
        results.append(_rate_check(completed, n_completed, f"completed_rides_missing_{c}", layer,
                                   "outcome_completeness", F.col(c).isNull(),
                                   T["completed_missing_pct_max"], False, f"Completed with null {c}"))

    # null spike: fields that should be populated for the booking's outcome but are null
    expected_cells, null_cells = F.lit(0), F.lit(0)
    for c, statuses in EXPECTED_POPULATED.items():
        should = status.isin(statuses)
        expected_cells = expected_cells + should.cast("int")
        null_cells = null_cells + (should & F.col(c).isNull()).cast("int")
    agg = df.agg(F.sum(expected_cells).alias("e"), F.sum(null_cells).alias("n")).first()
    null_rate = (agg["n"] or 0) / agg["e"] if agg["e"] else 0.0
    prev = _previous_metric(spark, "bronze_unexpected_null_spike", batch_id)
    rise = null_rate - (prev or 0.0)
    passed = null_rate <= T["null_spike_max"] and rise <= T["null_spike_max"]
    any_unexpected = None
    for c, statuses in EXPECTED_POPULATED.items():
        cond = status.isin(statuses) & F.col(c).isNull()
        any_unexpected = cond if any_unexpected is None else (any_unexpected | cond)
    results.append(CheckResult(
        rule="bronze_unexpected_null_spike", layer=layer, category="completeness",
        status="PASS" if passed else "FAIL", hard=True, metric_value=round(null_rate, 6),
        threshold=T["null_spike_max"], failing_count=int(agg["n"] or 0),
        message=(f"{null_rate:.2%} of outcome fields unexpectedly null "
                 f"(previous run {'n/a' if prev is None else f'{prev:.2%}'})"),
        sample_failing_records=[] if passed else _sample(df, any_unexpected)))

    # uniqueness: duplicate Booking IDs (source has ~0.8% ID collisions)
    dup_rows = (df.groupBy("booking_id").count().filter("count > 1")
                  .agg(F.sum(F.col("count") - 1)).first()[0]) or 0
    dup_rate = dup_rows / total if total else 0.0
    passed = dup_rate <= T["bronze_duplicate_pct_max"]
    dup_ids = [] if passed else [r["booking_id"] for r in df.groupBy("booking_id").count()
                                 .filter("count > 1").orderBy(F.desc("count")).limit(SAMPLE_SIZE).collect()]
    results.append(CheckResult(
        rule="bronze_duplicate_booking_id", layer=layer, category="uniqueness",
        status="PASS" if passed else "FAIL", hard=True, metric_value=round(dup_rate, 6),
        threshold=T["bronze_duplicate_pct_max"], failing_count=dup_rows,
        message=f"{dup_rows} extra rows share a Booking ID ({dup_rate:.2%}); allowed {T['bronze_duplicate_pct_max']:.2%}",
        sample_failing_records=[{"booking_id": i} for i in dup_ids]))

    # validity (soft: offending rows go to quarantine; the quarantine-rate check is hard)
    vt = T["invalid_pct_max"]
    validity = {
        "invalid_booking_status": status.isNotNull() & ~status.isin(config.ALLOWED_STATUSES),
        "invalid_vehicle_type": F.col("vehicle_type").isNotNull()
                                & ~F.col("vehicle_type").isin(config.ALLOWED_VEHICLE_TYPES),
        "invalid_payment_method": F.col("payment_method").isNotNull()
                                  & ~F.col("payment_method").isin(config.ALLOWED_PAYMENT_METHODS),
        "rating_out_of_range": (F.col("driver_rating").isNotNull()
                                & ~F.col("driver_rating").between(config.RATING_MIN, config.RATING_MAX))
                               | (F.col("customer_rating").isNotNull()
                                  & ~F.col("customer_rating").between(config.RATING_MIN, config.RATING_MAX)),
        "negative_value_or_distance": (F.col("booking_value") < 0) | (F.col("ride_distance") < 0)
                                      | (F.col("avg_vtat") < 0) | (F.col("avg_ctat") < 0),
        "invalid_date_or_time": (F.col("date").isNotNull() & F.col("booking_date").isNull())
                                | (F.col("booking_date").isNotNull() & F.col("booking_timestamp").isNull()),
    }
    non_numeric = None
    for c in config.SILVER_TYPES:
        cond = F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull()
        non_numeric = cond if non_numeric is None else (non_numeric | cond)
    validity["non_numeric_value"] = non_numeric
    for rule, cond in validity.items():
        results.append(_rate_check(df, total, f"bronze_{rule}", layer, "validity", cond, vt, False, rule))

    # consistency: flags must match the status, reasons present exactly when a flag is set
    ct = T["inconsistent_pct_max"]
    flag_mismatch, reason_mismatch = None, None
    for s, flag in config.STATUS_FLAG.items():
        reason = config.FLAG_REASON[flag]
        fm = F.coalesce(F.col(flag) == 1, F.lit(False)) != F.coalesce(status == s, F.lit(False))
        rm = F.col(flag).isNull() != F.col(reason).isNull()
        flag_mismatch = fm if flag_mismatch is None else (flag_mismatch | fm)
        reason_mismatch = rm if reason_mismatch is None else (reason_mismatch | rm)
    results.append(_rate_check(df, total, "bronze_flag_status_consistency", layer, "consistency",
                               flag_mismatch, ct, False, "have a cancellation/incomplete flag that contradicts the status"))
    results.append(_rate_check(df, total, "bronze_reason_flag_consistency", layer, "consistency",
                               reason_mismatch, ct, False, "have a reason without its flag (or a flag without a reason)"))

    # timeliness: data for every expected date
    start, end = _expected_range(start_date, end_date)
    results.append(_missing_days_check(df, layer, "bronze_timeliness_missing_days", start, end))

    # volume: vs previous successful load, and vs expected daily volume
    tol = T["volume_tolerance_pct"]
    prev_rows = _previous_successful_load(spark, "bronze", batch_id) if not start_date else None
    if prev_rows:
        drop = (prev_rows - total) / prev_rows
        passed = drop <= tol
        msg = f"{total} rows vs {prev_rows} in the previous load ({-drop:+.1%})"
        metric = round(drop, 6)
    else:
        passed, metric, msg = True, 0.0, "no previous full load to compare with (baseline run)"
    results.append(CheckResult(
        rule="bronze_volume_vs_previous_load", layer=layer, category="volume",
        status="PASS" if passed else "FAIL", hard=True, metric_value=metric, threshold=tol,
        failing_count=0 if passed else int(prev_rows - total), message=msg))

    expected_rows = T["expected_daily_rows"] * _days_between(start, end)
    shortfall = (expected_rows - total) / expected_rows
    passed = shortfall <= tol
    results.append(CheckResult(
        rule="bronze_volume_vs_expected", layer=layer, category="volume",
        status="PASS" if passed else "FAIL", hard=True, metric_value=round(shortfall, 6),
        threshold=tol, failing_count=0 if passed else int(expected_rows - total),
        message=f"{total} rows vs about {expected_rows} expected ({T['expected_daily_rows']}/day)"))

    df.unpersist()
    return results


# ---------------------------------------------------------------------------
# SILVER checks (all hard: Silver must be clean)
# ---------------------------------------------------------------------------
def silver_checks(spark, silver: DataFrame, quarantine: DataFrame, bronze: DataFrame,
                  batch_id: str, start_date=None, end_date=None) -> list:
    layer = "silver"
    results = []
    total = silver.count()
    status = F.col("booking_status")

    key_null = None
    for c in ["booking_id", "booking_timestamp", "booking_date", "booking_status", "customer_id",
              "vehicle_type", "pickup_location", "drop_location"]:
        key_null = F.col(c).isNull() if key_null is None else (key_null | F.col(c).isNull())
    results.append(_rate_check(silver, total, "silver_key_completeness", layer, "completeness",
                               key_null, 0.0, True, "have a null key field"))

    dups = (silver.groupBy("booking_id").count().filter("count > 1")
                  .agg(F.sum(F.col("count") - 1)).first()[0]) or 0
    results.append(CheckResult(
        rule="silver_unique_booking_id", layer=layer, category="uniqueness",
        status="PASS" if dups == 0 else "FAIL", hard=True, metric_value=dups,
        threshold=T["silver_duplicate_pct_max"], failing_count=dups,
        message=f"{dups} duplicate Booking IDs in Silver"))

    invalid = (~status.isin(config.ALLOWED_STATUSES)
               | ~F.col("vehicle_type").isin(config.ALLOWED_VEHICLE_TYPES)
               | (F.col("payment_method").isNotNull() & ~F.col("payment_method").isin(config.ALLOWED_PAYMENT_METHODS))
               | (F.col("booking_value") < 0) | (F.col("ride_distance") < 0)
               | ~F.coalesce(F.col("driver_rating").between(1, 5), F.lit(True))
               | ~F.coalesce(F.col("customer_rating").between(1, 5), F.lit(True)))
    results.append(_rate_check(silver, total, "silver_validity", layer, "validity",
                               invalid, 0.0, True, "break a validity rule"))

    inconsistent = None
    for s, flag in config.STATUS_FLAG.items():
        reason = config.FLAG_REASON[flag]
        cond = ((F.coalesce(F.col(flag) == 1, F.lit(False)) != (status == s))
                | (F.col(flag).isNull() != F.col(reason).isNull()))
        inconsistent = cond if inconsistent is None else (inconsistent | cond)
    results.append(_rate_check(silver, total, "silver_consistency", layer, "consistency",
                               inconsistent, 0.0, True, "have flags/reasons inconsistent with the status"))

    outcome = (status == "Completed") & (F.col("booking_value").isNull() | F.col("ride_distance").isNull()
                                         | F.col("payment_method").isNull())
    results.append(_rate_check(silver, total, "silver_completed_outcome", layer, "outcome_completeness",
                               outcome, 0.0, True, "are Completed without value/distance/payment"))

    b, q = bronze.count(), quarantine.count()
    diff = b - (total + q)
    results.append(CheckResult(
        rule="bronze_silver_reconciliation", layer=layer, category="reconciliation",
        status="PASS" if abs(diff) <= T["reconciliation_diff_max"] else "FAIL", hard=True,
        metric_value=diff, threshold=T["reconciliation_diff_max"], failing_count=abs(diff),
        message=f"Bronze {b} = Silver {total} + Quarantine {q} (difference {diff})"))

    q_rate = q / b if b else 0.0
    reasons = []
    if q:
        reasons = [{"reason": r["reason"], "rows": r["count"]} for r in
                   quarantine.select(F.explode("reject_reasons").alias("reason"))
                   .groupBy("reason").count().orderBy(F.desc("count")).limit(SAMPLE_SIZE).collect()]
    results.append(CheckResult(
        rule="quarantine_rate", layer=layer, category="validity",
        status="PASS" if q_rate <= T["quarantine_pct_max"] else "FAIL", hard=True,
        metric_value=round(q_rate, 6), threshold=T["quarantine_pct_max"], failing_count=q,
        message=f"{q} of {b} Bronze rows quarantined ({q_rate:.2%}); allowed {T['quarantine_pct_max']:.2%}",
        sample_failing_records=reasons))
    return results


# ---------------------------------------------------------------------------
# GOLD checks
# ---------------------------------------------------------------------------
def gold_checks(spark, batch_id: str) -> list:
    layer = "gold"
    results = []
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    tables = {n: spark.read.format("delta").load(p) for n, p in config.GOLD_TABLES.items()}

    empty = [n for n, df in tables.items() if df.limit(1).count() == 0]
    results.append(CheckResult(
        rule="gold_tables_not_empty", layer=layer, category="completeness",
        status="PASS" if not empty else "FAIL", hard=True, metric_value=len(empty), threshold=0,
        failing_count=len(empty), message="all gold tables have rows" if not empty else f"empty: {empty}"))

    daily = tables["daily_bookings"]
    s_rows = silver.count()
    d = daily.agg(F.sum("total_bookings").alias("b"), F.sum("revenue").alias("r"),
                  F.sum("completed").alias("c")).first()
    diff = (d["b"] or 0) - s_rows
    results.append(CheckResult(
        rule="gold_bookings_reconcile_silver", layer=layer, category="reconciliation",
        status="PASS" if diff == 0 else "FAIL", hard=True, metric_value=diff, threshold=0,
        failing_count=abs(diff), message=f"daily_bookings total {d['b']} vs Silver {s_rows}"))

    s_rev = silver.filter("is_completed").agg(F.sum("booking_value")).first()[0] or 0.0
    rev_diff = round((d["r"] or 0.0) - s_rev, 2)
    results.append(CheckResult(
        rule="gold_revenue_reconcile_silver", layer=layer, category="reconciliation",
        status="PASS" if abs(rev_diff) <= 1.0 else "FAIL", hard=True, metric_value=rev_diff,
        threshold=1.0, failing_count=0, message=f"daily revenue {(d['r'] or 0.0):.2f} vs Silver {s_rev:.2f}"))

    start, end = _expected_range(None, None)
    results.append(_missing_days_check(daily, layer, "gold_no_missing_dates", start, end))

    rate = (d["c"] or 0) / (d["b"] or 1)
    ok = T["completion_rate_min"] <= rate <= T["completion_rate_max"]
    results.append(CheckResult(
        rule="gold_completion_rate_plausible", layer=layer, category="plausibility",
        status="PASS" if ok else "FAIL", hard=True, metric_value=round(rate, 4),
        threshold=T["completion_rate_min"],
        message=f"overall completion rate {rate:.2%} (plausible band "
                f"{T['completion_rate_min']:.0%}-{T['completion_rate_max']:.0%})"))

    prev = _previous_metric(spark, "gold_completion_rate_plausible", batch_id)
    change = 0.0 if prev is None else rate - prev
    results.append(CheckResult(
        rule="gold_completion_rate_vs_history", layer=layer, category="plausibility",
        status="PASS" if abs(change) <= 0.05 else "FAIL", hard=False, metric_value=round(change, 4),
        threshold=0.05,
        message="no history yet" if prev is None else f"completion rate moved {change:+.2%} vs previous run"))

    r = tables["ratings_by_vehicle"].agg(F.min("avg_driver_rating").alias("dmin"), F.max("avg_driver_rating").alias("dmax"),
                                         F.min("avg_customer_rating").alias("cmin"), F.max("avg_customer_rating").alias("cmax")).first()
    vals = [v for v in r if v is not None]
    ok = all(config.RATING_MIN <= v <= config.RATING_MAX for v in vals)
    results.append(CheckResult(
        rule="gold_ratings_plausible", layer=layer, category="plausibility",
        status="PASS" if ok else "FAIL", hard=True, metric_value=round(min(vals), 3) if vals else 0.0,
        threshold=config.RATING_MIN, message=f"average ratings between {min(vals) if vals else None} and {max(vals) if vals else None}"))

    tat = tables["tat_by_vehicle"].agg(F.max("avg_vtat_min").alias("v"), F.max("avg_ctat_min").alias("c")).first()
    ok = (tat["v"] or 0) <= 60 and (tat["c"] or 0) <= 120
    results.append(CheckResult(
        rule="gold_turnaround_plausible", layer=layer, category="plausibility",
        status="PASS" if ok else "FAIL", hard=False, metric_value=tat["v"] or 0.0, threshold=60,
        message=f"max average VTAT {tat['v']} min, CTAT {tat['c']} min"))
    return results


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def summarize(results: list) -> dict:
    failed = [r for r in results if r.status == "FAIL"]
    hard_failed = [r for r in failed if r.hard]
    status = "FAIL" if hard_failed else ("PASS_WITH_WARNINGS" if failed else "PASS")
    return {"status": status, "checks_total": len(results),
            "checks_passed": len(results) - len(failed), "checks_failed": len(failed),
            "hard_failed": [r.rule for r in hard_failed],
            "soft_failed": [r.rule for r in failed if not r.hard]}


def save_results(spark: SparkSession, results: list, batch_id: str) -> None:
    """Append results to the quality history table (used for trends, volume and the agents)."""
    ddl = ("run_timestamp timestamp, batch_id string, rule string, layer string, category string, "
           "status string, hard boolean, metric_value double, threshold double, failing_count long, "
           "message string, sample_json string")
    now = datetime.now(timezone.utc)
    rows = [(now, batch_id, r.rule, r.layer, r.category, r.status, r.hard, float(r.metric_value),
             float(r.threshold), int(r.failing_count), r.message,
             json.dumps(r.sample_failing_records, default=str)) for r in results]
    spark.createDataFrame(rows, schema=ddl).write.format("delta").mode("append").save(config.QUALITY_RESULTS_PATH)


def run_quality_checks(spark: SparkSession, layers=("bronze", "silver"), batch_id: str = None,
                       start_date: str = None, end_date: str = None, save: bool = True) -> dict:
    """Run the checks for the requested layers and return structured results + a summary."""
    from src.etl_pipeline import new_batch_id

    batch_id = batch_id or new_batch_id()
    rng = None if not (start_date and end_date) else F.col("booking_date").between(start_date, end_date)
    load = lambda p: spark.read.format("delta").load(p)

    results = []
    bronze = load(config.BRONZE_PATH)
    if rng is not None:
        bronze = bronze.filter(rng)
    if "bronze" in layers:
        results += bronze_checks(spark, bronze, batch_id, start_date, end_date)
    if "silver" in layers:
        silver, quarantine = load(config.SILVER_PATH), load(config.QUARANTINE_PATH)
        if rng is not None:
            silver, quarantine = silver.filter(rng), quarantine.filter(rng)
        results += silver_checks(spark, silver, quarantine, bronze, batch_id, start_date, end_date)
    if "gold" in layers:
        results += gold_checks(spark, batch_id)

    if save:
        save_results(spark, results, batch_id)
    return {"batch_id": batch_id, "layers": list(layers), "summary": summarize(results),
            "results": [r.to_dict() for r in results]}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--layers", default="bronze,silver")
    p.add_argument("--batch-id")
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    out = run_quality_checks(spark, a.layers.split(","), a.batch_id)
    print(json.dumps(out["summary"], indent=2))
