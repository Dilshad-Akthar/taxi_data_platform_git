# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath("../.."))

from src.config.config_loader import ConfigLoader


CONFIG_PATH = "../../config/datasets/yellow_tripdata.yml"

config_loader = ConfigLoader()
config = config_loader.load(CONFIG_PATH)

config

# COMMAND ----------

from src.config.config_validator import ConfigValidator


validator = ConfigValidator()
validator.validate(config)

print("Configuration validation passed.")

# COMMAND ----------

from src.schema.schema_registry import SchemaRegistry


schema_registry = SchemaRegistry()
schema = schema_registry.get_schema(config["schema"]["name"])

schema

# COMMAND ----------

from src.ingestion.auto_loader_reader import AutoLoaderReader

pipeline_state_path = "/Volumes/taxi/source/pipeline_state/yellow_tripdata"
schema_location = f"{pipeline_state_path}/schema"

reader = AutoLoaderReader(spark)

stream_df = reader.read(
    source_path=config["source"]["path"],
    file_format=config["source"]["format"],
    schema=schema,
    schema_location=schema_location,
    header=config["source"]["header"],
    delimiter=config["source"]["delimiter"],
)

stream_df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

stream_df = (
    stream_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file_path", col("_metadata.file_path"))
    .withColumn("_source_file_name", col("_metadata.file_name"))
    .withColumn(
        "_source_file_modification_time",
        col("_metadata.file_modification_time")
    )
)

stream_df.printSchema()

# COMMAND ----------

from src.ingestion.bronze_writer import BronzeWriter


target_table = (
    f"{config['target']['catalog']}."
    f"{config['target']['schema']}."
    f"{config['target']['table']}"
)

checkpoint_path = f"{pipeline_state_path}/checkpoint"

writer = BronzeWriter(spark)

query = writer.write(
    dataframe=stream_df,
    target_table=target_table,
    checkpoint_path=checkpoint_path,
)

query.awaitTermination()

# COMMAND ----------

result = spark.sql("""
SELECT
    COUNT(*) AS row_count,
    COUNT(_ingested_at) AS ingested_at_count,
    COUNT(_source_file_path) AS source_path_count,
    COUNT(_source_file_name) AS source_name_count,
    COUNT(_source_file_modification_time) AS source_modification_time_count
FROM taxi.bronze.yellow_tripdata
""")

result.show()

print("=== METADATA SAMPLE ===")

spark.sql("""
SELECT
    _ingested_at,
    _source_file_name,
    _source_file_modification_time
FROM taxi.bronze.yellow_tripdata
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
DESCRIBE taxi.bronze.yellow_tripdata
""").show(30, truncate=False)