from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_daily_trip_metrics(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df
        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime")
        )
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("passenger_count").alias("total_passengers"),
            F.sum("trip_distance").alias("total_distance"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tips"),
            F.sum("tolls_amount").alias("total_tolls"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_trip_amount")
        )
) 