"""
Shared pytest fixtures.

Tests run on Databricks (notebooks/run_tests) against the small sample file in data/,
built into DataFrames in memory, so they are fast and never touch production tables.
"""

import copy
import csv
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src import config  # noqa: E402
from src import etl_pipeline as etl  # noqa: E402

SAMPLE_FILE = os.path.join(REPO_ROOT, "data", "sample_rides.csv")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = SparkSession.builder.getOrCreate()
    try:
        config.configure_adls(session)   # some checks read history tables in ADLS
    except Exception:
        pass
    return session


@pytest.fixture(scope="session")
def sample_records():
    """The sample file as a list of dicts keyed by SOURCE column names ('null' -> None)."""
    with open(SAMPLE_FILE, newline="") as f:
        reader = csv.DictReader(f)
        return [{k: (None if v == "null" else v) for k, v in row.items()} for row in reader]


def records_to_bronze(spark, records, batch_id="test-batch"):
    """Build a Bronze DataFrame from source-style records (mimics read_raw + to_bronze)."""
    from pyspark.sql import functions as F
    rows = [tuple(r.get(c) for c in config.SOURCE_COLUMN_NAMES) for r in records]
    raw = spark.createDataFrame(rows, schema=etl.raw_schema()).withColumn(
        "source_file", F.lit("sample_rides.csv"))
    return etl.to_bronze(raw, batch_id)


@pytest.fixture
def records(sample_records):
    """A fresh, editable copy of the sample records for each test."""
    return copy.deepcopy(sample_records)


@pytest.fixture
def bronze(spark, records):
    return records_to_bronze(spark, records)


def first_with_status(records, status):
    """First record with this status whose Booking ID is unique in the sample."""
    counts = {}
    for r in records:
        counts[r["Booking ID"]] = counts.get(r["Booking ID"], 0) + 1
    return next(r for r in records if r["Booking Status"] == status and counts[r["Booking ID"]] == 1)
