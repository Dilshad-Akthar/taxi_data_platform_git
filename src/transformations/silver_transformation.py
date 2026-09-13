from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_yellow_tripdata(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(
            F.col("tpep_dropoff_datetime")
            >= F.col("tpep_pickup_datetime")
        )
        .dropDuplicates([
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
        ])
    )