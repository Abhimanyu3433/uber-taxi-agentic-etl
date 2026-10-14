"""Unit tests for the Gold table builders."""

import pytest
from pyspark.sql import functions as F

from conftest import records_to_bronze
from src import etl_pipeline as etl
from src import gold_analytics as gold


@pytest.fixture
def silver(spark, records):
    s, _ = etl.to_silver(records_to_bronze(spark, records), "test-batch")
    return s.cache()


def test_daily_totals_reconcile(silver):
    daily = gold.build_daily_bookings(silver)
    assert daily.agg(F.sum("total_bookings")).first()[0] == silver.count()
    status_sum = daily.select((F.col("completed") + F.col("cancelled_by_customer") + F.col("cancelled_by_driver")
                               + F.col("no_driver_found") + F.col("incomplete")).alias("s"),
                              "total_bookings").filter("s != total_bookings").count()
    assert status_sum == 0


def test_revenue_counts_completed_only(silver):
    daily = gold.build_daily_bookings(silver)
    expected = silver.filter("is_completed").agg(F.sum("booking_value")).first()[0]
    assert abs(daily.agg(F.sum("revenue")).first()[0] - expected) < 0.01


def test_completion_rate_between_0_and_1(silver):
    daily = gold.build_daily_bookings(silver)
    assert daily.filter(~F.col("completion_rate").between(0, 1)).count() == 0


def test_cancellation_shares_sum_to_one(silver):
    c = gold.build_cancellations_by_reason(silver)
    sums = c.groupBy("cancelled_by").agg(F.sum("pct_of_party_cancellations").alias("s")).collect()
    assert all(abs(r["s"] - 1) < 0.01 for r in sums)


def test_all_builders_return_rows(silver):
    for name, (builder, _, _) in gold.BUILDERS.items():
        assert builder(silver).count() > 0, name


def test_top_locations_ranked_and_capped(silver):
    t = gold.build_top_locations(silver)
    assert t.groupBy("location_type").agg(F.max("rank").alias("m")).filter(f"m > {gold.TOP_N_LOCATIONS}").count() == 0
