from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import validate_schema
from helpers import *
from metadata import *
from quality.quality import run_quality_checks
from metadata import (
    write_run_metadata,
)
import gc
from bronze_job import BRONZE_LAYER_DATASET_LOCATION, read_bronze

from helpers import *


# the location prefix of silver layer dataset in the object storage
SILVER_LAYER_DATASET_LOCATION = "s3a://delta-data/silver/silver_"


def silver_job(
    spark,
    config,
    run_context,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
    write_metadata=True,
):
    transformed_data, quality_summary = read_and_transform_bronze(
        spark,
        config,
        bronze_location,
    )
    processed_rows = quality_summary["processed_rows"]
    rejected_rows = quality_summary["rejected_rows"]

    write_silver(transformed_data, config, silver_location)

    if write_metadata:
        write_run_metadata(
            spark,
            run_context,
            config["dataset"],
            config["schema_version"],
            # [],
            processed_rows,
            rejected_rows,
        )

    # free up unused memory
    del transformed_data
    gc.collect()


def read_and_transform_bronze(
    spark,
    config,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
):
    bronze_data = read_bronze(spark, config["dataset"], bronze_location)
    validate_schema(bronze_data, config.get("schema"))
    cleaned_data, quality_summary = run_quality_checks(
        bronze_data,
        config.get("quality_checks", [])
    )
    transformed_data = apply_operations(cleaned_data, config.get("operations", []))

    # free up unused memory
    del bronze_data
    del cleaned_data
    return transformed_data, quality_summary


def write_silver(
    df,
    config,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    target = f"{silver_location}{config['dataset']}"
    if "partitions" in config and config["partitions"]:
        (
            df.write.format("delta")
            .partitionBy(config["partitions"])
            .mode("overwrite")
            .option("overwriteSchema", "true") # maybe not needed?
            .save(target)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true") # maybe not needed?
            .save(target)
        )

    print(f"Written silver Delta table to: {target}")


def read_silver(
    spark,
    dataset,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    return spark.read.format("delta").load(f"{silver_location}{dataset}")
