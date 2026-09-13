from pyspark.sql import DataFrame, SparkSession


class BronzeWriter:
    """Writes streaming DataFrames to Bronze Delta tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(
        self,
        dataframe: DataFrame,
        target_table: str,
        checkpoint_path: str,
    ):
        return (
            dataframe.writeStream
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_path)
            .toTable(target_table)
        )