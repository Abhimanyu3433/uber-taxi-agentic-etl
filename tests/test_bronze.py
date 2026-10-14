"""Unit tests for Bronze transformations."""

from pyspark.sql import functions as F

from conftest import records_to_bronze
from src import config
from src import etl_pipeline as etl


def test_bronze_has_all_columns_and_metadata(bronze):
    expected = [dst for _, dst in config.SOURCE_COLUMNS] + [
        "source_file", "booking_date", "ingestion_timestamp", "batch_id"]
    assert set(expected) <= set(bronze.columns)


def test_bronze_keeps_every_row(bronze, sample_records):
    assert bronze.count() == len(sample_records)


def test_quotes_stripped_from_ids(bronze):
    assert bronze.filter(F.col("booking_id").contains('"') | F.col("customer_id").contains('"')).count() == 0
    assert bronze.filter(~F.col("booking_id").rlike(r"^CNR\d+$")).count() == 0


def test_literal_null_is_real_null(bronze):
    nd = bronze.filter(F.col("booking_status") == "No Driver Found")
    assert nd.filter(F.col("booking_value").isNotNull()).count() == 0


def test_values_stay_as_raw_strings(bronze):
    types = dict(bronze.dtypes)
    assert types["booking_value"] == "string" and types["driver_rating"] == "string"


def test_parse_booking_date_accepts_both_formats(spark):
    df = spark.createDataFrame([("2024-03-23",), ("23-03-2024",), ("2024-13-45",), ("garbage",), (None,)], ["date"])
    out = [r["d"] for r in df.select(etl.parse_booking_date("date").alias("d")).collect()]
    assert str(out[0]) == "2024-03-23" and str(out[1]) == "2024-03-23"
    assert out[2] is None and out[3] is None and out[4] is None


def test_blank_id_becomes_null(spark, records):
    records[0]["Booking ID"] = '""'
    b = records_to_bronze(spark, records)
    assert b.filter(F.col("booking_id").isNull()).count() == 1
