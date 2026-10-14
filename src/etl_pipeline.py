"""
etl_pipeline.py - Raw -> Bronze -> Silver.

Bronze (raw data + ingestion metadata):
  * explicit 21-column schema, all STRING (raw values preserved exactly)
  * the literal text "null" is read as a real null
  * embedded quotes stripped from Booking ID and Customer ID
  * metadata: ingestion_timestamp, source_file, batch_id (+ booking_date for incremental loads)
  * idempotent (re-runs never duplicate) and incremental by booking date

Silver (cleaned, typed, deduplicated, enriched):
  * Date + Time -> booking_timestamp; numeric columns cast to their types
  * Booking Status, vehicle type and payment method standardised to canonical values
  * invalid or contradictory records -> quarantine/ with the reason(s) they were rejected
  * duplicate Booking IDs removed (earliest booking kept, the rest quarantined)
  * derived: booking_date, booking_hour, day_of_week, is_cancelled, cancelled_by,
    cancellation_reason (plus is_completed)
"""

import csv
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

from src import config


class SchemaMismatchError(Exception):
    """A source file's header does not match the expected 21 columns."""


class SourceFileMissingError(Exception):
    """There are no CSV files to ingest."""


NUMERIC_COLUMNS = list(config.SILVER_TYPES.keys())


# ===========================================================================
# Shared helpers
# ===========================================================================
def new_batch_id() -> str:
    """A unique, sortable ID for one pipeline run, e.g. 20261014T101500-a1b2c3."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def raw_schema() -> StructType:
    """Explicit 21-column schema using the SOURCE column names, all strings."""
    return StructType([StructField(n, StringType(), True) for n in config.SOURCE_COLUMN_NAMES])


def parse_booking_date(col_name: str = "date"):
    """
    Parse the Date column into a real date. Accepts yyyy-MM-dd (what the file uses)
    and dd-MM-yyyy (what the spec says). Bad values become null instead of crashing.
    """
    c = F.col(col_name)
    iso = F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"),
                 F.expr(f"try_to_timestamp(`{col_name}`, 'yyyy-MM-dd')"))
    dmy = F.when(c.rlike(r"^\d{2}-\d{2}-\d{4}$"),
                 F.expr(f"try_to_timestamp(`{col_name}`, 'dd-MM-yyyy')"))
    return F.coalesce(iso, dmy).cast("date")


def _date_predicate(start_date: str, end_date: str) -> str:
    return f"booking_date >= '{start_date}' AND booking_date <= '{end_date}'"


def _write_delta(df: DataFrame, path: str, mode: str, start_date=None, end_date=None,
                 partition_by=None) -> None:
    """
    Write a Delta table idempotently.
    full        -> overwrite the whole table
    incremental -> replace only the given booking-date range (replaceWhere)
    """
    from delta.tables import DeltaTable

    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if mode == "incremental" and DeltaTable.isDeltaTable(df.sparkSession, path):
        writer = writer.option("replaceWhere", _date_predicate(start_date, end_date))
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def write_load_log(spark: SparkSession, entry: dict) -> None:
    """Append one row describing a load (used by volume checks and agents)."""
    ddl = ("batch_id string, layer string, mode string, start_date string, end_date string, "
           "source_files string, rows_read long, rows_written long, rows_unparseable_date long, "
           "rows_quarantined long, started_at timestamp, finished_at timestamp, "
           "status string, message string")
    cols = [c.split()[0] for c in ddl.split(", ")]
    row = tuple(entry.get(c) for c in cols)
    (spark.createDataFrame([row], schema=ddl)
     .write.format("delta").mode("append").option("mergeSchema", "true")
     .save(config.LOAD_LOG_PATH))


def _validate_args(mode, start_date, end_date):
    if mode not in ("full", "incremental"):
        raise ValueError(f"mode must be 'full' or 'incremental', got {mode!r}")
    if mode == "incremental" and not (start_date and end_date):
        raise ValueError("incremental mode needs start_date and end_date (yyyy-MM-dd)")


# ===========================================================================
# BRONZE
# ===========================================================================
def list_raw_files(spark: SparkSession, raw_path: str = None) -> list:
    """Return the full paths of all CSV files in the raw landing zone."""
    raw_path = raw_path or config.RAW_PATH
    dbutils = config.get_dbutils(spark)
    try:
        files = [f.path for f in dbutils.fs.ls(raw_path) if f.name.lower().endswith(".csv")]
    except Exception as e:
        raise SourceFileMissingError(f"Raw path not readable: {raw_path} ({e})")
    if not files:
        raise SourceFileMissingError(f"No CSV files found in {raw_path}")
    return sorted(files)


def validate_headers(spark: SparkSession, files: list) -> None:
    """Schema-change detection: each file's header must equal the expected 21 columns."""
    expected = config.SOURCE_COLUMN_NAMES
    for path in files:
        first_line = spark.read.text(path).first()
        if first_line is None:
            raise SchemaMismatchError(f"{path} is empty")
        header = [h.strip() for h in next(csv.reader([first_line[0].lstrip("﻿")]))]
        if header != expected:
            missing = [c for c in expected if c not in header]
            extra = [c for c in header if c not in expected]
            raise SchemaMismatchError(
                f"Header mismatch in {path.rsplit('/', 1)[-1]}. Missing: {missing}. "
                f"Unexpected: {extra}. Order changed only: {not missing and not extra}")


def read_raw(spark: SparkSession, files: list) -> DataFrame:
    """Read the raw CSV files with the explicit schema and capture the source file name."""
    reader = spark.read.schema(raw_schema())
    for key, value in config.CSV_OPTIONS.items():
        reader = reader.option(key, value)
    return reader.csv(files).select("*", F.col("_metadata.file_path").alias("source_file"))


def to_bronze(raw_df: DataFrame, batch_id: str) -> DataFrame:
    """Rename to snake_case, strip quotes from IDs, add ingestion metadata."""
    df = raw_df
    for src, dst in config.SOURCE_COLUMNS:
        df = df.withColumnRenamed(src, dst)
    for id_col in ["booking_id", "customer_id"]:
        cleaned = F.trim(F.regexp_replace(F.col(id_col), '"', ""))
        df = df.withColumn(id_col, F.when(cleaned == "", None).otherwise(cleaned))
    return (df
            .withColumn("booking_date", parse_booking_date("date"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id)))


def run_bronze(spark: SparkSession, mode: str = None, start_date: str = None,
               end_date: str = None, batch_id: str = None) -> dict:
    """Ingest raw CSVs into Bronze. Returns a summary dict (also written to the load log)."""
    mode = (mode or config.LOAD_MODE).lower()
    batch_id = batch_id or new_batch_id()
    log = {"batch_id": batch_id, "layer": "bronze", "mode": mode, "start_date": start_date,
           "end_date": end_date, "started_at": datetime.now(timezone.utc)}
    try:
        _validate_args(mode, start_date, end_date)
        files = list_raw_files(spark)
        validate_headers(spark, files)
        log["source_files"] = ",".join(f.rsplit("/", 1)[-1] for f in files)

        bronze = to_bronze(read_raw(spark, files), batch_id)
        rows_read = bronze.count()
        unparseable = bronze.filter(F.col("booking_date").isNull()).count()
        to_write = bronze if mode == "full" else bronze.filter(_date_predicate(start_date, end_date))
        _write_delta(to_write, config.BRONZE_PATH, mode, start_date, end_date)

        log.update(rows_read=rows_read, rows_written=to_write.count(),
                   rows_unparseable_date=unparseable, status="SUCCESS",
                   message=f"{len(files)} file(s) ingested")
        return log
    except Exception as e:
        log.update(status="FAILED", message=f"{type(e).__name__}: {e}"[:1000])
        raise
    finally:
        log["finished_at"] = datetime.now(timezone.utc)
        try:
            write_load_log(spark, log)
        except Exception as log_error:
            print(f"WARNING: could not write load log: {log_error}")


# ===========================================================================
# SILVER
# ===========================================================================
def _canonical(col_name: str, allowed: list):
    """
    Map a text column to its canonical spelling, ignoring case and extra spaces
    (e.g. ' completed ' -> 'Completed'). Unknown values are kept (cleaned) so
    the validity rules can reject them.
    """
    cleaned = F.trim(F.regexp_replace(F.col(col_name), r"\s+", " "))
    mapping = F.create_map(*[x for v in allowed for x in (F.lit(v.lower()), F.lit(v))])
    return F.coalesce(mapping[F.lower(cleaned)], cleaned)


def standardize(bronze_df: DataFrame) -> DataFrame:
    """
    Type and standardise Bronze data WITHOUT rejecting anything.
    Used by Silver and (independently) by the Bronze quality checks.
    Keeps the original numeric text in _raw_<col> so cast failures can be detected.
    """
    df = bronze_df
    text_cols = ["pickup_location", "drop_location", "customer_cancel_reason",
                 "driver_cancel_reason", "incomplete_reason"]
    for c in text_cols:
        trimmed = F.trim(F.col(c))
        df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))

    df = (df
          .withColumn("booking_status", _canonical("booking_status", config.ALLOWED_STATUSES))
          .withColumn("vehicle_type", _canonical("vehicle_type", config.ALLOWED_VEHICLE_TYPES))
          .withColumn("payment_method", _canonical("payment_method", config.ALLOWED_PAYMENT_METHODS)))

    for c, t in config.SILVER_TYPES.items():
        df = (df.withColumn(f"_raw_{c}", F.col(c))
                .withColumn(c, F.expr(f"try_cast(trim(`{c}`) AS {t})")))

    time_ok = F.col("time").rlike(r"^\d{2}:\d{2}:\d{2}$")
    df = (df
          .withColumn("booking_date", parse_booking_date("date"))
          .withColumn("booking_timestamp",
                      F.when(time_ok & F.col("booking_date").isNotNull(),
                             F.expr("try_to_timestamp(concat(cast(booking_date AS string), ' ', time), "
                                    "'yyyy-MM-dd HH:mm:ss')"))))
    return df


def reject_reason_exprs() -> list:
    """Every Silver rule as (reason_name, condition_that_means_reject)."""
    rules = []
    for c in config.KEY_COLUMNS:
        rules.append((f"missing_{c}", F.col(c).isNull()))
    rules.append(("invalid_date", F.col("date").isNotNull() & F.col("booking_date").isNull()))
    rules.append(("invalid_time", F.col("time").isNotNull() & F.col("booking_date").isNotNull()
                  & F.col("booking_timestamp").isNull()))
    rules.append(("invalid_status", F.col("booking_status").isNotNull()
                  & ~F.col("booking_status").isin(config.ALLOWED_STATUSES)))
    rules.append(("invalid_vehicle_type", F.col("vehicle_type").isNotNull()
                  & ~F.col("vehicle_type").isin(config.ALLOWED_VEHICLE_TYPES)))
    rules.append(("invalid_payment_method", F.col("payment_method").isNotNull()
                  & ~F.col("payment_method").isin(config.ALLOWED_PAYMENT_METHODS)))
    for c in NUMERIC_COLUMNS:
        rules.append((f"non_numeric_{c}", F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull()))
    for c in ["booking_value", "ride_distance", "avg_vtat", "avg_ctat"]:
        rules.append((f"negative_{c}", F.col(c) < 0))
    for c in ["driver_rating", "customer_rating"]:
        rules.append((f"{c}_out_of_range",
                      F.col(c).isNotNull() & ~F.col(c).between(config.RATING_MIN, config.RATING_MAX)))
    for c in config.COMPLETED_REQUIRED:
        rules.append((f"completed_missing_{c}",
                      (F.col("booking_status") == "Completed") & F.col(c).isNull()))
    for status, flag in config.STATUS_FLAG.items():
        reason = config.FLAG_REASON[flag]
        flag_set = F.coalesce(F.col(flag) == 1, F.lit(False))
        status_match = F.coalesce(F.col("booking_status") == status, F.lit(False))
        rules.append((f"invalid_value_{flag}", F.col(flag).isNotNull() & (F.col(flag) != 1)))
        rules.append((f"status_mismatch_{flag}", flag_set != status_match))
        rules.append((f"reason_mismatch_{flag}", F.col(flag).isNull() != F.col(reason).isNull()))
    return rules


def add_reject_reasons(df: DataFrame) -> DataFrame:
    """Add reject_reasons: an array of every rule the row breaks (empty = valid)."""
    whens = [F.when(cond, F.lit(name)) for name, cond in reject_reason_exprs()]
    return df.withColumn("reject_reasons", F.filter(F.array(*whens), lambda x: x.isNotNull()))


def flag_duplicates(df: DataFrame) -> DataFrame:
    """
    Among valid rows, keep the earliest booking per Booking ID.
    Later rows with the same ID get the reason 'duplicate_booking_id'.
    """
    valid = F.size("reject_reasons") == 0
    w = Window.partitionBy("booking_id", "_is_valid").orderBy(
        F.col("booking_timestamp").asc_nulls_last(), "ingestion_timestamp", "source_file")
    return (df
            .withColumn("_is_valid", valid)
            .withColumn("_rn", F.row_number().over(w))
            .withColumn("reject_reasons",
                        F.when(F.col("_is_valid") & (F.col("_rn") > 1),
                               F.concat("reject_reasons", F.array(F.lit("duplicate_booking_id"))))
                         .otherwise(F.col("reject_reasons")))
            .drop("_is_valid", "_rn"))


def add_derived(df: DataFrame) -> DataFrame:
    """Derived columns for analytics."""
    status = F.col("booking_status")
    return (df
            .withColumn("booking_hour", F.hour("booking_timestamp"))
            .withColumn("day_of_week", F.date_format("booking_date", "EEEE"))
            .withColumn("is_completed", status == "Completed")
            .withColumn("is_cancelled", status.isin("Cancelled by Customer", "Cancelled by Driver"))
            .withColumn("cancelled_by",
                        F.when(status == "Cancelled by Customer", "customer")
                         .when(status == "Cancelled by Driver", "driver"))
            .withColumn("cancellation_reason",
                        F.when(status == "Cancelled by Customer", F.col("customer_cancel_reason"))
                         .when(status == "Cancelled by Driver", F.col("driver_cancel_reason"))))


SILVER_COLUMNS = [
    "booking_id", "customer_id", "booking_timestamp", "booking_date", "booking_hour", "day_of_week",
    "booking_status", "vehicle_type", "pickup_location", "drop_location",
    "avg_vtat", "avg_ctat",
    "cancelled_by_customer_flag", "customer_cancel_reason",
    "cancelled_by_driver_flag", "driver_cancel_reason",
    "incomplete_flag", "incomplete_reason",
    "booking_value", "ride_distance", "driver_rating", "customer_rating", "payment_method",
    "is_completed", "is_cancelled", "cancelled_by", "cancellation_reason",
    "source_file", "batch_id", "ingestion_timestamp",
]


def to_silver(bronze_df: DataFrame, batch_id: str):
    """Return (silver_df, quarantine_df) from a Bronze DataFrame."""
    checked = flag_duplicates(add_reject_reasons(standardize(bronze_df)))
    processed = F.current_timestamp()

    silver = (add_derived(checked.filter(F.size("reject_reasons") == 0))
              .select(*SILVER_COLUMNS)
              .withColumn("silver_batch_id", F.lit(batch_id))
              .withColumn("processed_timestamp", processed))

    bronze_cols = [c for c in bronze_df.columns]
    quarantine = (checked.filter(F.size("reject_reasons") > 0)
                  .select(*[F.col(f"_raw_{c}").alias(c) if c in NUMERIC_COLUMNS else F.col(c)
                            for c in bronze_cols],
                          "reject_reasons")
                  .withColumn("reject_reason", F.array_join("reject_reasons", ","))
                  .withColumn("quarantine_batch_id", F.lit(batch_id))
                  .withColumn("quarantined_at", processed))
    return silver, quarantine


def run_silver(spark: SparkSession, mode: str = None, start_date: str = None,
               end_date: str = None, batch_id: str = None) -> dict:
    """Build Silver and Quarantine from Bronze. Returns a summary dict."""
    mode = (mode or config.LOAD_MODE).lower()
    batch_id = batch_id or new_batch_id()
    log = {"batch_id": batch_id, "layer": "silver", "mode": mode, "start_date": start_date,
           "end_date": end_date, "started_at": datetime.now(timezone.utc)}
    try:
        _validate_args(mode, start_date, end_date)
        bronze = spark.read.format("delta").load(config.BRONZE_PATH)
        if mode == "incremental":
            bronze = bronze.filter(_date_predicate(start_date, end_date))

        silver, quarantine = to_silver(bronze, batch_id)
        _write_delta(silver, config.SILVER_PATH, mode, start_date, end_date)
        _write_delta(quarantine, config.QUARANTINE_PATH, mode, start_date, end_date)

        rows_in = bronze.count()
        rows_silver = spark.read.format("delta").load(config.SILVER_PATH).filter(
            "true" if mode == "full" else _date_predicate(start_date, end_date)).count()
        q = spark.read.format("delta").load(config.QUARANTINE_PATH)
        if mode == "incremental":
            q = q.filter(_date_predicate(start_date, end_date))
        rows_q = q.count()
        top = (q.select(F.explode("reject_reasons").alias("reason"))
                .groupBy("reason").count().orderBy(F.desc("count")).limit(10).collect())

        log.update(rows_read=rows_in, rows_written=rows_silver, rows_quarantined=rows_q,
                   status="SUCCESS",
                   message="quarantine reasons: " + ", ".join(f"{r['reason']}={r['count']}" for r in top))
        log["quarantine_reasons"] = {r["reason"]: r["count"] for r in top}
        return log
    except Exception as e:
        log.update(status="FAILED", message=f"{type(e).__name__}: {e}"[:1000])
        raise
    finally:
        log["finished_at"] = datetime.now(timezone.utc)
        try:
            write_load_log(spark, log)
        except Exception as log_error:
            print(f"WARNING: could not write load log: {log_error}")


# ===========================================================================
# Entry point for a Databricks Job "Python script" task
# ===========================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Uber taxi ETL")
    parser.add_argument("--layer", default="bronze", choices=["bronze", "silver"])
    parser.add_argument("--mode", default=config.LOAD_MODE)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--batch-id")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    runner = run_bronze if args.layer == "bronze" else run_silver
    print(runner(spark, args.mode, args.start_date, args.end_date, args.batch_id))
