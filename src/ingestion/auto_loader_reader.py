from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


class AutoLoaderReader:
    """Reads source files incrementally using Databricks Auto Loader."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(
        self,
        source_path: str,
        file_format: str,
        schema: object,
        schema_location: str,
        header: bool = True,
        delimiter: str = ",",
    ) -> DataFrame:

        source_df = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", schema_location)
            .option("header", str(header).lower())
            .option("delimiter", delimiter)
            .schema(schema)
            .load(source_path)
        )

        return source_df.select(
            "*",
            current_timestamp().alias("_ingested_at"),
            col("_metadata.file_path").alias("_source_file_path"),
            col("_metadata.file_name").alias("_source_file_name"),
            col("_metadata.file_modification_time").alias(
                "_source_file_modification_time"
            ),
        )