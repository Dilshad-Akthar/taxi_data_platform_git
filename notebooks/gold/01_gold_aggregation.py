# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.table("taxi.silver.yellow_tripdata")

display(silver_df.limit(10))

# COMMAND ----------

from pathlib import Path
import sys

src_path = Path("/Workspace/taxi_data_platform/src")

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# COMMAND ----------


from transformations.gold_transformation import transform_daily_trip_metrics

gold_daily = transform_daily_trip_metrics(silver_df)

gold_daily = gold_daily.orderBy("pickup_date")

display(gold_daily)

gold_daily.printSchema()

# COMMAND ----------

display(
    gold_daily.select(
        F.count("*").alias("days"),
        F.min("pickup_date").alias("min_date"),
        F.max("pickup_date").alias("max_date"),
        F.sum("total_trips").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue")
    )
)

# COMMAND ----------

gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("taxi.gold.daily_trip_metrics")

# COMMAND ----------

display(
    spark.table("taxi.gold.daily_trip_metrics")
    .orderBy("pickup_date")
    .limit(10)
)

# COMMAND ----------

gold_df = spark.table("taxi.gold.daily_trip_metrics")

display(
    gold_df.select(
        F.count("*").alias("days"),
        F.sum("total_trips").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.min("pickup_date").alias("min_date"),
        F.max("pickup_date").alias("max_date")
    )
)

# COMMAND ----------

display(
    gold_df.select(
        F.sum(F.when(F.col("total_trips") <= 0, 1).otherwise(0)).alias("invalid_trip_days"),
        F.sum(F.when(F.col("total_revenue") < 0, 1).otherwise(0)).alias("negative_revenue_days"),
        F.sum(F.when(F.col("total_distance") < 0, 1).otherwise(0)).alias("negative_distance_days"),
        F.sum(F.when(F.col("avg_trip_distance") < 0, 1).otherwise(0)).alias("negative_avg_distance"),
        F.sum(F.when(F.col("avg_trip_amount") < 0, 1).otherwise(0)).alias("negative_avg_amount")
    )
)

# COMMAND ----------

bronze_count = spark.table("taxi.bronze.yellow_tripdata").count()
silver_count = spark.table("taxi.silver.yellow_tripdata").count()

gold_trips = (
    gold_df
    .agg(F.sum("total_trips").alias("total_trips"))
    .first()["total_trips"]
)

print("=== TAXI DATA PLATFORM VALIDATION ===")

print(f"Bronze rows: {bronze_count:,}")
print(f"Silver rows: {silver_count:,}")
print(f"Gold trips : {gold_trips:,}")

assert bronze_count >= silver_count
assert gold_trips == silver_count

print("Bronze → Silver: PASS")
print("Silver → Gold reconciliation: PASS")
print("Gold: PASS")
print("=== PROJECT VALIDATION COMPLETE ===")

# COMMAND ----------

print("=== TAXI DATA PLATFORM VALIDATION ===")

bronze = spark.table("taxi.bronze.yellow_tripdata")
silver = spark.table("taxi.silver.yellow_tripdata")
gold = spark.table("taxi.gold.daily_trip_metrics")

bronze_count = bronze.count()
silver_count = silver.count()
gold_count = gold.agg(
    F.sum("total_trips").alias("total_trips")
).first()["total_trips"]

print(f"Bronze rows: {bronze_count:,}")
print(f"Silver rows: {silver_count:,}")
print(f"Gold trips : {gold_count:,}")

assert bronze_count == 47_248_845
assert silver_count == 47_222_631
assert gold_count == silver_count

print("Bronze: PASS")
print("Silver: PASS")
print("Gold: PASS")
print("Silver → Gold reconciliation: PASS")
print("=== PROJECT VALIDATION COMPLETE ===")

# COMMAND ----------

import os

for root, dirs, files in os.walk("/Workspace/taxi_data_platform"):
    level = root.replace("/Workspace/taxi_data_platform", "").count(os.sep)
    if level <= 3:
        print(root)
        for file in files:
            print("   ", file)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS gold_count
# MAGIC FROM taxi.gold.daily_trip_metrics;