# Databricks notebook source
from pathlib import Path

project_root = Path("/Workspace/taxi_data_platform")
src_path = project_root / "src"

print(src_path)

# COMMAND ----------

import sys

src_path = "/Workspace/taxi_data_platform/src"

if src_path not in sys.path:
    sys.path.insert(0, src_path)

from transformations.silver_transformation import transform_yellow_tripdata

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("taxi.bronze.yellow_tripdata")

display(
    bronze_df.select(
        F.count("*").alias("total_rows"),
        F.count("VendorID").alias("vendor_id_present"),
        F.count("tpep_pickup_datetime").alias("pickup_datetime_present"),
        F.count("tpep_dropoff_datetime").alias("dropoff_datetime_present"),
        F.count("passenger_count").alias("passenger_count_present"),
        F.count("trip_distance").alias("trip_distance_present"),
        F.count("fare_amount").alias("fare_amount_present"),
        F.count("total_amount").alias("total_amount_present")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("taxi.bronze.yellow_tripdata")

display(
    bronze_df.select(
        F.sum(
            F.when(F.col("passenger_count") <= 0, 1).otherwise(0)
        ).alias("invalid_passenger_count"),
        F.sum(
            F.when(F.col("trip_distance") < 0, 1).otherwise(0)
        ).alias("negative_trip_distance"),
        F.sum(
            F.when(F.col("fare_amount") < 0, 1).otherwise(0)
        ).alias("negative_fare_amount"),
        F.sum(
            F.when(F.col("total_amount") < 0, 1).otherwise(0)
        ).alias("negative_total_amount"),
        F.sum(
            F.when(
                F.col("tpep_dropoff_datetime")
                < F.col("tpep_pickup_datetime"),
                1
            ).otherwise(0)
        ).alias("invalid_datetime_order")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

duplicate_groups = (
    bronze_df
    .groupBy(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "RateCodeID",
        "store_and_fwd_flag",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount"
    )
    .count()
    .filter(F.col("count") > 1)
)

display(
    duplicate_groups.agg(
        F.count("*").alias("duplicate_groups"),
        F.sum("count").alias("rows_in_duplicate_groups")
    )
)

# COMMAND ----------

display(
    duplicate_groups
    .orderBy(F.desc("count"))
)


# COMMAND ----------

display(
    bronze_df
    .groupBy("_source_file_name")
    .count()
    .orderBy("_source_file_name")
)


# COMMAND ----------

from transformations.silver_transformation import transform_yellow_tripdata
bronze_df = spark.table("taxi.bronze.yellow_tripdata")

silver_df = transform_yellow_tripdata(bronze_df)

display(silver_df.limit(10))

# COMMAND ----------

print("Silver row count:", silver_df.count())

silver_df.printSchema()

display(silver_df.limit(10))

# COMMAND ----------

print("Bronze rows :", bronze_df.count())
print("Silver rows :", silver_df.count())
print("Rows removed:", bronze_df.count() - silver_df.count())

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("taxi.silver.yellow_tripdata")

# COMMAND ----------

display(
    spark.table("taxi.silver.yellow_tripdata").select(
        F.count("*").alias("total_rows")
    )
)

# COMMAND ----------

display(
    spark.table("taxi.silver.yellow_tripdata").select(
        F.count("*").alias("total_rows"),
        F.count("passenger_count").alias("passenger_count_present"),
        F.count("trip_distance").alias("trip_distance_present"),
        F.count("fare_amount").alias("fare_amount_present"),
        F.count("total_amount").alias("total_amount_present"),
        F.sum(F.when(F.col("passenger_count") <= 0, 1).otherwise(0)).alias("invalid_passenger_count"),
        F.sum(F.when(F.col("trip_distance") < 0, 1).otherwise(0)).alias("negative_trip_distance"),
        F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("negative_fare_amount"),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("negative_total_amount"),
        F.sum(
            F.when(
                F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
                1
            ).otherwise(0)
        ).alias("invalid_datetime_order")
    )
)

# COMMAND ----------

from pathlib import Path
import sys

src_path = Path("/Workspace/taxi_data_platform/src")

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from transformations.silver_transformation import transform_yellow_tripdata

from datetime import datetime

test_data = [
    # 1. VALID ROW → should be kept
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),

    # 2. passenger_count = 0 → should be removed
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        0,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),

    # 3. Negative trip_distance → should be removed
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        -1.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),

    # 4. Negative fare_amount → should be removed
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        -20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),

    # 5. Negative total_amount → should be removed
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        -25.0,
    ),

    # 6. Dropoff before pickup → should be removed
    (
        1,
        datetime(2026, 1, 1, 11, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),

    # 7. Exact duplicate of valid row → one copy should be removed
    (
        1,
        datetime(2026, 1, 1, 10, 0, 0),
        datetime(2026, 1, 1, 10, 30, 0),
        2,
        5.0,
        -73.99,
        40.75,
        1,
        "N",
        -73.98,
        40.76,
        1,
        20.0,
        0.0,
        1.5,
        3.0,
        0.0,
        0.3,
        25.0,
    ),
]

test_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_longitude",
    "pickup_latitude",
    "RateCodeID",
    "store_and_fwd_flag",
    "dropoff_longitude",
    "dropoff_latitude",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]

test_df = spark.createDataFrame(test_data, test_columns)

result_df = transform_yellow_tripdata(test_df)

print("Input rows:", test_df.count())
print("Rows after transformation:", result_df.count())

display(result_df)

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("taxi.bronze.yellow_tripdata")

checks = bronze.select(
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0))
        .alias("null_passenger_count"),

    F.sum(F.when(F.col("passenger_count") <= 0, 1).otherwise(0))
        .alias("invalid_passenger_count"),

    F.sum(F.when(F.col("trip_distance").isNull(), 1).otherwise(0))
        .alias("null_trip_distance"),

    F.sum(F.when(F.col("trip_distance") < 0, 1).otherwise(0))
        .alias("negative_trip_distance"),

    F.sum(F.when(F.col("fare_amount").isNull(), 1).otherwise(0))
        .alias("null_fare_amount"),

    F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0))
        .alias("negative_fare_amount"),

    F.sum(F.when(F.col("total_amount").isNull(), 1).otherwise(0))
        .alias("null_total_amount"),

    F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0))
        .alias("negative_total_amount"),

    F.sum(
        F.when(
            (F.col("tpep_pickup_datetime").isNotNull()) &
            (F.col("tpep_dropoff_datetime").isNotNull()) &
            (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")),
            1
        ).otherwise(0)
    ).alias("invalid_datetime_order")
)

display(checks)

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("taxi.bronze.yellow_tripdata")

rejected = bronze.filter(
    (F.col("passenger_count") <= 0) |
    (F.col("trip_distance") < 0) |
    (F.col("fare_amount") < 0) |
    (F.col("total_amount") < 0) |
    (
        F.col("tpep_dropoff_datetime")
        < F.col("tpep_pickup_datetime")
    )
)

print("Rows rejected by Silver quality filters:", rejected.count())

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("taxi.bronze.yellow_tripdata")

filtered = (
    bronze
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("total_amount") >= 0)
    .filter(
        F.col("tpep_dropoff_datetime")
        >= F.col("tpep_pickup_datetime")
    )
)

duplicate_groups = (
    filtered
    .groupBy(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "RateCodeID",
        "store_and_fwd_flag",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_stats = duplicate_groups.agg(
    F.count("*").alias("duplicate_groups"),
    F.sum("count").alias("rows_in_duplicate_groups"),
    F.sum(F.col("count") - 1).alias("duplicate_rows_removed")
)

display(duplicate_stats)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_count
# MAGIC FROM taxi.silver.yellow_tripdata;