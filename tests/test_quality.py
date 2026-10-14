"""
Unit tests for quality_checks: rules PASS on clean sample data and FAIL on corrupted data.
"""

import pytest
from pyspark.sql import functions as F

from conftest import first_with_status, records_to_bronze
from src import etl_pipeline as etl
from src import quality_checks as qc

RANGE = ("2024-01-01", "2024-12-30")


def bronze_results(spark, records):
    b = records_to_bronze(spark, records)
    return {r.rule: r for r in qc.bronze_checks(spark, b, "test-batch", *RANGE)}


def test_results_have_required_fields(spark, records):
    r = next(iter(bronze_results(spark, records).values())).to_dict()
    for key in ["rule", "layer", "status", "metric_value", "threshold", "sample_failing_records"]:
        assert key in r


CLEAN_RULES = ["bronze_schema_columns", "bronze_key_completeness", "bronze_unexpected_null_spike",
               "bronze_duplicate_booking_id", "bronze_invalid_booking_status", "bronze_rating_out_of_range",
               "bronze_negative_value_or_distance", "bronze_flag_status_consistency",
               "bronze_reason_flag_consistency", "completed_rides_missing_payment_method"]


def test_clean_sample_passes_rule_checks(spark, records):
    res = bronze_results(spark, records)
    failed = [k for k in CLEAN_RULES if res[k].status != "PASS"]
    assert failed == []


@pytest.mark.parametrize("field,value,rule", [
    ("Booking Value", "-50", "bronze_negative_value_or_distance"),
    ("Driver Ratings", "7", "bronze_rating_out_of_range"),
    ("Booking Status", "Teleported", "bronze_invalid_booking_status"),
    ("Cancelled Rides by Driver", "1", "bronze_flag_status_consistency"),
    ("Payment Method", None, "completed_rides_missing_payment_method"),
    ("Booking ID", None, "bronze_key_completeness"),
])
def test_corruption_is_detected(spark, records, field, value, rule):
    first_with_status(records, "Completed")[field] = value
    res = bronze_results(spark, records)
    assert res[rule].status == "FAIL"
    assert res[rule].failing_count >= 1


def test_failed_check_includes_sample_records(spark, records):
    first_with_status(records, "Completed")["Booking Value"] = "-50"
    res = bronze_results(spark, records)
    assert len(res["bronze_negative_value_or_distance"].sample_failing_records) >= 1


def test_duplicate_spike_is_hard_failure(spark, records):
    records += [dict(r) for r in records[:10]]          # ~6% duplicated rows
    res = bronze_results(spark, records)
    assert res["bronze_duplicate_booking_id"].status == "FAIL" and res["bronze_duplicate_booking_id"].hard


def test_null_spike_is_hard_failure(spark, records):
    for r in [x for x in records if x["Booking Status"] == "Completed"][:20]:
        r["Driver Ratings"] = r["Customer Rating"] = r["Booking Value"] = None
    res = bronze_results(spark, records)
    assert res["bronze_unexpected_null_spike"].status == "FAIL"


def test_silver_checks_pass_on_pipeline_output(spark, records):
    b = records_to_bronze(spark, records)
    silver, quarantine = etl.to_silver(b, "test-batch")
    res = {r.rule: r for r in qc.silver_checks(spark, silver, quarantine, b, "test-batch", *RANGE)}
    for rule in ["silver_key_completeness", "silver_unique_booking_id", "silver_validity",
                 "silver_consistency", "silver_completed_outcome", "bronze_silver_reconciliation"]:
        assert res[rule].status == "PASS", rule


def test_silver_check_catches_bad_silver(spark, records):
    b = records_to_bronze(spark, records)
    silver, quarantine = etl.to_silver(b, "test-batch")
    broken = silver.withColumn("booking_value", F.lit(-1.0))
    res = {r.rule: r for r in qc.silver_checks(spark, broken, quarantine, b, "test-batch", *RANGE)}
    assert res["silver_validity"].status == "FAIL"


def test_summary_marks_hard_failures():
    results = [qc.CheckResult("a", "bronze", "x", "PASS", True, 0, 0),
               qc.CheckResult("b", "bronze", "x", "FAIL", False, 1, 0),
               qc.CheckResult("c", "bronze", "x", "FAIL", True, 1, 0)]
    s = qc.summarize(results)
    assert s["status"] == "FAIL" and s["hard_failed"] == ["c"] and s["soft_failed"] == ["b"]
    assert qc.summarize(results[:2])["status"] == "PASS_WITH_WARNINGS"
