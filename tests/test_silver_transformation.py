# Databricks notebook source
from pathlib import Path
import sys
from datetime import datetime

project_root = Path("/Workspace/taxi_data_platform")
src_path = project_root / "src"

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from transformations.silver_transformation import transform_yellow_tripdata

# COMMAND ----------

from datetime import datetime

from transformations.silver_transformation import transform_yellow_tripdata


def test_silver_removes_invalid_rows(spark):
    test_data = [
        # Valid
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            1,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
        ),

        # Invalid passenger_count
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            0,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
        ),

        # Negative trip_distance
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            1,
            -1.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
        ),

        # Negative fare_amount
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            1,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            -5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
        ),

        # Negative total_amount
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            1,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            -20.0,
        ),

        # Invalid datetime order
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 9, 30),
            1,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
        ),

        # Duplicate of valid row
        (
            1,
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 10, 30),
            1,
            10.0,
            0.0,
            0.0,
            1,
            "N",
            0.0,
            0.0,
            1,
            5.0,
            0.0,
            0.5,
            1.0,
            0.0,
            0.3,
            20.0,
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

    assert result_df.count() == 1

    result = result_df.collect()[0]

    assert result["passenger_count"] == 1
    assert result["trip_distance"] == 10.0
    assert result["fare_amount"] == 5.0
    assert result["total_amount"] == 20.0

# COMMAND ----------

