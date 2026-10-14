"""
gold_analytics.py - Silver -> Gold business tables.

Gold tables (one Delta table per question the business asks):
  daily_bookings              bookings, completion / cancellation rate, revenue per day
  cancellations_by_reason     cancellation reasons, customer vs driver
  revenue_by_vehicle_payment  revenue by vehicle type and payment method
  peak_hour_demand            demand by hour of day and day of week
  top_locations               top pickup and drop locations
  tat_by_vehicle              average VTAT and CTAT by vehicle type
  ratings_by_vehicle          average driver and customer ratings by vehicle type
  incomplete_by_reason        incomplete rides by reason

Revenue = Booking Value of Completed rides (incomplete rides carry a value but
were not completed, so they are reported separately and excluded from revenue).
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import config

TOP_N_LOCATIONS = 20


def _pct(numerator, denominator):
    return F.round(numerator / F.when(denominator == 0, None).otherwise(denominator), 4)


# ---------------------------------------------------------------------------
# Table builders (pure functions: DataFrame in, DataFrame out -> easy to unit test)
# ---------------------------------------------------------------------------
def build_daily_bookings(silver: DataFrame) -> DataFrame:
    s = F.col("booking_status")
    return (silver.groupBy("booking_date")
            .agg(F.count("*").alias("total_bookings"),
                 F.sum((s == "Completed").cast("int")).alias("completed"),
                 F.sum((s == "Cancelled by Customer").cast("int")).alias("cancelled_by_customer"),
                 F.sum((s == "Cancelled by Driver").cast("int")).alias("cancelled_by_driver"),
                 F.sum((s == "No Driver Found").cast("int")).alias("no_driver_found"),
                 F.sum((s == "Incomplete").cast("int")).alias("incomplete"),
                 F.round(F.sum(F.when(s == "Completed", F.col("booking_value"))), 2).alias("revenue"),
                 F.round(F.avg(F.when(s == "Completed", F.col("booking_value"))), 2).alias("avg_booking_value"),
                 F.round(F.avg(F.when(s == "Completed", F.col("ride_distance"))), 2).alias("avg_ride_distance"))
            .withColumn("completion_rate", _pct(F.col("completed"), F.col("total_bookings")))
            .withColumn("cancellation_rate",
                        _pct(F.col("cancelled_by_customer") + F.col("cancelled_by_driver"),
                             F.col("total_bookings")))
            .withColumn("booking_month", F.date_format("booking_date", "yyyy-MM"))
            .orderBy("booking_date"))


def build_cancellations_by_reason(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("cancelled_by")
    return (silver.filter(F.col("is_cancelled"))
            .groupBy("cancelled_by", "cancellation_reason")
            .agg(F.count("*").alias("cancellations"))
            .withColumn("pct_of_party_cancellations",
                        _pct(F.col("cancellations"), F.sum("cancellations").over(w)))
            .orderBy("cancelled_by", F.desc("cancellations")))


def build_revenue_by_vehicle_payment(silver: DataFrame) -> DataFrame:
    return (silver.filter(F.col("is_completed"))
            .groupBy("vehicle_type", "payment_method")
            .agg(F.count("*").alias("completed_rides"),
                 F.round(F.sum("booking_value"), 2).alias("revenue"),
                 F.round(F.avg("booking_value"), 2).alias("avg_booking_value"),
                 F.round(F.sum("ride_distance"), 2).alias("total_distance"))
            .withColumn("revenue_per_km", F.round(F.col("revenue") / F.col("total_distance"), 2))
            .orderBy(F.desc("revenue")))


def build_peak_hour_demand(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("booking_hour", "day_of_week")
            .agg(F.count("*").alias("bookings"),
                 F.sum(F.col("is_completed").cast("int")).alias("completed"),
                 F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"))
            .withColumn("completion_rate", _pct(F.col("completed"), F.col("bookings")))
            .orderBy("day_of_week", "booking_hour"))


def build_top_locations(silver: DataFrame) -> DataFrame:
    def ranked(col_name, kind):
        agg = (silver.groupBy(F.col(col_name).alias("location"))
               .agg(F.count("*").alias("bookings"),
                    F.sum(F.col("is_completed").cast("int")).alias("completed"),
                    F.round(F.sum(F.when(F.col("is_completed"), F.col("booking_value"))), 2).alias("revenue")))
        w = Window.orderBy(F.desc("bookings"), "location")
        return (agg.withColumn("rank", F.row_number().over(w))
                .filter(F.col("rank") <= TOP_N_LOCATIONS)
                .withColumn("location_type", F.lit(kind)))

    cols = ["location_type", "rank", "location", "bookings", "completed", "revenue"]
    return ranked("pickup_location", "pickup").select(cols).unionByName(
        ranked("drop_location", "drop").select(cols))


def build_tat_by_vehicle(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("vehicle_type")
            .agg(F.count("*").alias("bookings"),
                 F.round(F.avg("avg_vtat"), 2).alias("avg_vtat_min"),
                 F.round(F.expr("percentile_approx(avg_vtat, 0.5)"), 2).alias("median_vtat_min"),
                 F.round(F.avg("avg_ctat"), 2).alias("avg_ctat_min"),
                 F.round(F.expr("percentile_approx(avg_ctat, 0.5)"), 2).alias("median_ctat_min"))
            .orderBy("vehicle_type"))


def build_ratings_by_vehicle(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("vehicle_type")
            .agg(F.sum(F.col("driver_rating").isNotNull().cast("int")).alias("rated_rides"),
                 F.round(F.avg("driver_rating"), 3).alias("avg_driver_rating"),
                 F.round(F.avg("customer_rating"), 3).alias("avg_customer_rating"))
            .orderBy("vehicle_type"))


def build_incomplete_by_reason(silver: DataFrame) -> DataFrame:
    total = Window.partitionBy()
    return (silver.filter(F.col("booking_status") == "Incomplete")
            .groupBy("incomplete_reason", "vehicle_type")
            .agg(F.count("*").alias("incomplete_rides"))
            .withColumn("pct_of_incomplete", _pct(F.col("incomplete_rides"),
                                                  F.sum("incomplete_rides").over(total)))
            .orderBy(F.desc("incomplete_rides")))


BUILDERS = {
    "daily_bookings": (build_daily_bookings, ["booking_month"], ["booking_date"]),
    "cancellations_by_reason": (build_cancellations_by_reason, None, None),
    "revenue_by_vehicle_payment": (build_revenue_by_vehicle_payment, None, None),
    "peak_hour_demand": (build_peak_hour_demand, None, None),
    "top_locations": (build_top_locations, None, None),
    "tat_by_vehicle": (build_tat_by_vehicle, None, None),
    "ratings_by_vehicle": (build_ratings_by_vehicle, None, None),
    "incomplete_by_reason": (build_incomplete_by_reason, None, None),
}


def optimize(spark: SparkSession, path: str, zorder_cols=None) -> str:
    """OPTIMIZE compacts small files; Z-ORDER co-locates rows by the given columns."""
    sql = f"OPTIMIZE delta.`{path}`"
    if zorder_cols:
        sql += f" ZORDER BY ({', '.join(zorder_cols)})"
    try:
        spark.sql(sql)
        return "ok"
    except Exception as e:  # optimisation is a nice-to-have; never fail the pipeline for it
        return f"skipped: {str(e)[:150]}"


def run_gold(spark: SparkSession, batch_id: str = None, tables: list = None) -> dict:
    """Rebuild the Gold tables from Silver (always a full rebuild: Gold is small and derived)."""
    from src.etl_pipeline import new_batch_id, write_load_log

    batch_id = batch_id or new_batch_id()
    started = datetime.now(timezone.utc)
    log = {"batch_id": batch_id, "layer": "gold", "mode": "full", "started_at": started}
    summary = {}
    try:
        silver = spark.read.format("delta").load(config.SILVER_PATH).cache()
        silver_rows = silver.count()
        for name in (tables or list(BUILDERS)):
            builder, partition_by, zorder = BUILDERS[name]
            df = builder(silver).withColumn("gold_batch_id", F.lit(batch_id))
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(config.GOLD_TABLES[name])
            summary[name] = {"rows": spark.read.format("delta").load(config.GOLD_TABLES[name]).count(),
                             "optimize": optimize(spark, config.GOLD_TABLES[name], zorder)}
        # Silver is queried by date and vehicle type far more than anything else
        summary["_silver_optimize"] = optimize(spark, config.SILVER_PATH, ["booking_date", "vehicle_type"])
        silver.unpersist()
        log.update(rows_read=silver_rows, rows_written=sum(v["rows"] for k, v in summary.items()
                                                           if isinstance(v, dict)),
                   status="SUCCESS", message=f"{len(summary) - 1} gold tables built")
        log["tables"] = summary
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


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    config.configure_adls(spark)
    print(run_gold(spark))
