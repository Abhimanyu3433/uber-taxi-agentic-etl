"""Unit tests for Silver: typing, standardisation, rejection rules, dedup, derived columns."""

import pytest
from pyspark.sql import functions as F

from conftest import first_with_status, records_to_bronze
from src import etl_pipeline as etl


def silver_of(spark, records):
    return etl.to_silver(records_to_bronze(spark, records), "test-batch")


def reasons_for(quarantine, booking_id):
    row = quarantine.filter(F.col("booking_id") == booking_id).first()
    return [] if row is None else list(row["reject_reasons"])


def bid(record):
    return record["Booking ID"].replace('"', "")


def test_clean_sample_reconciles(spark, records):
    silver, quarantine = silver_of(spark, records)
    assert silver.count() + quarantine.count() == len(records)


def test_duplicate_booking_id_keeps_earliest(spark, records):
    silver, quarantine = silver_of(spark, records)
    assert silver.groupBy("booking_id").count().filter("count > 1").count() == 0
    dup = quarantine.filter(F.array_contains("reject_reasons", "duplicate_booking_id"))
    assert dup.count() == 1                      # the sample contains one real ID collision
    kept = silver.filter(F.col("booking_id") == dup.first()["booking_id"]).first()
    assert str(kept["booking_date"]) < str(dup.first()["booking_date"])


def test_types_and_timestamp(spark, records):
    silver, _ = silver_of(spark, records)
    types = dict(silver.dtypes)
    assert types["booking_value"] == "double" and types["incomplete_flag"] == "int"
    assert types["booking_timestamp"] == "timestamp" and types["booking_date"] == "date"
    assert silver.filter(F.col("booking_timestamp").isNull()).count() == 0


def test_standardisation_accepts_messy_case(spark, records):
    r = first_with_status(records, "Completed")
    r["Booking Status"], r["Vehicle Type"], r["Payment Method"] = "  completed ", "go  sedan", "upi"
    silver, _ = silver_of(spark, records)
    row = silver.filter(F.col("booking_id") == bid(r)).first()
    assert (row["booking_status"], row["vehicle_type"], row["payment_method"]) == ("Completed", "Go Sedan", "UPI")


@pytest.mark.parametrize("field,value,reason", [
    ("Booking Value", "-120", "negative_booking_value"),
    ("Ride Distance", "-3.5", "negative_ride_distance"),
    ("Driver Ratings", "6.0", "driver_rating_out_of_range"),
    ("Customer Rating", "0", "customer_rating_out_of_range"),
    ("Booking Value", "abc", "non_numeric_booking_value"),
    ("Payment Method", None, "completed_missing_payment_method"),
    ("Booking Value", None, "completed_missing_booking_value"),
    ("Booking Status", "Teleported", "invalid_status"),
    ("Vehicle Type", "Helicopter", "invalid_vehicle_type"),
    ("Customer ID", None, "missing_customer_id"),
    ("Date", "2024-02-30", "invalid_date"),
    ("Time", "25:99:00", "invalid_time"),
])
def test_bad_values_are_quarantined_with_reason(spark, records, field, value, reason):
    r = first_with_status(records, "Completed")
    r[field] = value
    silver, quarantine = silver_of(spark, records)
    assert silver.filter(F.col("booking_id") == bid(r)).count() == 0
    assert reason in reasons_for(quarantine, bid(r))


def test_flag_contradicting_status_is_quarantined(spark, records):
    r = first_with_status(records, "Completed")
    r["Cancelled Rides by Driver"], r["Driver Cancellation Reason"] = "1", "Personal & Car related issues"
    _, quarantine = silver_of(spark, records)
    assert "status_mismatch_cancelled_by_driver_flag" in reasons_for(quarantine, bid(r))


def test_reason_without_flag_is_quarantined(spark, records):
    r = first_with_status(records, "Cancelled by Customer")
    r["Cancelled Rides by Customer"] = None
    _, quarantine = silver_of(spark, records)
    reasons = reasons_for(quarantine, bid(r))
    assert "reason_mismatch_cancelled_by_customer_flag" in reasons
    assert "status_mismatch_cancelled_by_customer_flag" in reasons


def test_quarantine_keeps_original_raw_value(spark, records):
    r = first_with_status(records, "Completed")
    r["Booking Value"] = "abc"
    _, quarantine = silver_of(spark, records)
    assert quarantine.filter(F.col("booking_id") == bid(r)).first()["booking_value"] == "abc"


def test_derived_columns(spark, records):
    silver, _ = silver_of(spark, records)
    cust = silver.filter(F.col("booking_status") == "Cancelled by Customer").first()
    drv = silver.filter(F.col("booking_status") == "Cancelled by Driver").first()
    done = silver.filter(F.col("booking_status") == "Completed").first()
    assert cust["is_cancelled"] and cust["cancelled_by"] == "customer"
    assert cust["cancellation_reason"] == cust["customer_cancel_reason"]
    assert drv["cancelled_by"] == "driver" and drv["cancellation_reason"] == drv["driver_cancel_reason"]
    assert done["is_completed"] and not done["is_cancelled"] and done["cancelled_by"] is None
    assert done["booking_hour"] == done["booking_timestamp"].hour
    assert done["day_of_week"] == done["booking_date"].strftime("%A")
