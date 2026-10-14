"""
fault_injection.py - test harness that corrupts the raw landing zone on purpose.

Used by notebooks/fault_injection (Phase 1 exit criteria) and notebooks/evaluation
(Phase 2 agent evaluation). Each scenario:
  1. moves the real CSV(s) aside to raw/uber_rides_original/ (outside the landing folder)
  2. writes a corrupted copy into raw/uber_rides/
  3. restore() puts the original data back

This is a human-run test tool: the agents themselves can never call it.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src import etl_pipeline as etl

BACKUP_PATH = config.abfss("raw", "uber_rides_original/")
TMP_PATH = config.abfss("raw", "_tmp_fault/")

SCENARIOS = {
    "healthy": "No fault: the original file (used to measure false alarms)",
    "missing_source_file": "The landing folder has no CSV file",
    "schema_change": "Column 'Booking Value' renamed to 'Fare' in the header",
    "duplicate_records": "5% of rows duplicated",
    "null_spike": "Booking Value, Ride Distance and ratings set to null for 30% of Completed rides",
    "out_of_range_values": "4% of Completed rides get a negative Booking Value or a Driver Rating of 6.5",
    "status_flag_mismatch": "4% of Completed rides get 'Cancelled Rides by Driver' = 1",
    "row_count_drop": "All bookings from March to May removed (about 25% of rows)",
    "job_failure": "The Job is started with an invalid parameter so the Bronze task crashes",
}


def _dbutils(spark):
    return config.get_dbutils(spark)


def _ensure_backup(spark) -> list:
    """Move the original CSVs out of the landing folder once; return backup file paths."""
    dbu = _dbutils(spark)
    try:
        existing = [f.path for f in dbu.fs.ls(BACKUP_PATH) if f.name.endswith(".csv")]
    except Exception:
        existing = []
    if existing:
        return existing
    files = etl.list_raw_files(spark)
    dbu.fs.mkdirs(BACKUP_PATH)
    for f in files:
        dbu.fs.mv(f, BACKUP_PATH + f.rsplit("/", 1)[-1])
    return [f.path for f in dbu.fs.ls(BACKUP_PATH) if f.name.endswith(".csv")]


def _clear_landing(spark):
    dbu = _dbutils(spark)
    try:
        for f in dbu.fs.ls(config.RAW_PATH):
            if f.name.endswith(".csv"):
                dbu.fs.rm(f.path)
    except Exception:
        pass


def restore(spark: SparkSession) -> str:
    """Put the original data back in the landing folder."""
    dbu = _dbutils(spark)
    try:
        backups = [f for f in dbu.fs.ls(BACKUP_PATH) if f.name.endswith(".csv")]
    except Exception:
        backups = []
    if not backups:
        return "nothing to restore (original data still in place)"
    _clear_landing(spark)
    for f in backups:
        dbu.fs.mv(f.path, config.RAW_PATH + f.name)
    dbu.fs.rm(BACKUP_PATH, recurse=True)
    return f"restored {len(backups)} file(s)"


def _read_original(spark, backups) -> DataFrame:
    reader = spark.read.schema(etl.raw_schema())
    for k, v in config.CSV_OPTIONS.items():
        reader = reader.option(k, v)
    return reader.csv(backups)


def _write_csv(spark, df: DataFrame, name: str, header_override: list = None):
    """Write df as ONE csv file named <name> in the landing folder (same format as the source)."""
    dbu = _dbutils(spark)
    if header_override:
        df = df.toDF(*header_override)
    (df.coalesce(1).write.mode("overwrite")
       .option("header", "true").option("nullValue", "null").option("escape", '"')
       .csv(TMP_PATH))
    part = next(f.path for f in dbu.fs.ls(TMP_PATH) if f.name.startswith("part-"))
    dbu.fs.cp(part, config.RAW_PATH + name)
    dbu.fs.rm(TMP_PATH, recurse=True)


def inject(spark: SparkSession, scenario: str, seed: int = 42) -> str:
    """Replace the landing data with a corrupted version for the given scenario."""
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose from {list(SCENARIOS)}")
    restore(spark)                      # always start from the clean original
    if scenario in ("healthy", "job_failure"):
        return SCENARIOS[scenario]      # data untouched (job_failure is injected via Job parameters)

    backups = _ensure_backup(spark)
    _clear_landing(spark)
    if scenario == "missing_source_file":
        return SCENARIOS[scenario]

    df = _read_original(spark, backups)
    status = F.col("Booking Status")
    completed = status == "Completed"
    pick = lambda frac: F.rand(seed) < frac

    if scenario == "schema_change":
        header = [("Fare" if c == "Booking Value" else c) for c in df.columns]
        _write_csv(spark, df, "rides_schema_change.csv", header_override=header)
    elif scenario == "duplicate_records":
        _write_csv(spark, df.unionByName(df.sample(fraction=0.05, seed=seed)), "rides_duplicates.csv")
    elif scenario == "null_spike":
        hit = completed & pick(0.30)
        for c in ["Booking Value", "Ride Distance", "Driver Ratings", "Customer Rating"]:
            df = df.withColumn(c, F.when(hit, F.lit(None)).otherwise(F.col(c)))
        _write_csv(spark, df, "rides_null_spike.csv")
    elif scenario == "out_of_range_values":
        r = F.rand(seed)
        df = (df.withColumn("Booking Value", F.when(completed & (r < 0.02), F.lit("-250")).otherwise(F.col("Booking Value")))
                .withColumn("Driver Ratings", F.when(completed & (r >= 0.02) & (r < 0.04), F.lit("6.5"))
                                              .otherwise(F.col("Driver Ratings"))))
        _write_csv(spark, df, "rides_out_of_range.csv")
    elif scenario == "status_flag_mismatch":
        df = df.withColumn("Cancelled Rides by Driver",
                           F.when(completed & pick(0.04), F.lit("1")).otherwise(F.col("Cancelled Rides by Driver")))
        _write_csv(spark, df, "rides_flag_mismatch.csv")
    elif scenario == "row_count_drop":
        month = F.substring(F.col("Date"), 6, 2)
        _write_csv(spark, df.filter(~month.isin("03", "04", "05")), "rides_row_drop.csv")
    return SCENARIOS[scenario]
