from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from operations import apply_operations
from schema_validations import validate_schema
from helpers import *
from quality.quality import run_quality_checks, get_checks_that_should_be_rerun_after_append
from metadata import record_execution, start_execution, get_last_silver_run_from_metadata, rm_bronze_metadata
import gc
from bronze_job import BRONZE_LAYER_DATASET_LOCATION, read_bronze

from helpers import *

# the location prefix of silver layer dataset in the object storage
SILVER_LAYER_DATASET_LOCATION = "s3a://delta-data/silver/silver_"

def silver_job(
    spark,
    config,
    run_context,
    mode="overwrite",
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
    write_metadata=True,
    incremental=False,
):
    execution = start_execution(
        run_context,
        stage="silver",
        dataset=config["dataset"],
        schema_version=config["schema_version"],
    )
    transformed_data, quality_summary = read_and_transform_bronze(
        spark,
        config,
        bronze_location,
        incremental=incremental,
    )
    processed_rows = quality_summary["processed_rows"]
    rejected_rows = quality_summary["rejected_rows"]
    validation_failure_count = (
        quality_summary["validation_failure_count"]
    )

    if incremental:
        checks = get_checks_that_should_be_rerun_after_append(config.get("quality_checks", []))

        if checks:
            existing_silver = read_silver(spark, config["dataset"])
            combined = existing_silver.unionByName(transformed_data)

            transformed_data, quality_summary = run_quality_checks(
                combined,
                config.get("quality_checks", [])
            )

            processed_rows += quality_summary["processed_rows"]
            rejected_rows += quality_summary["rejected_rows"]
            validation_failure_count += quality_summary["validation_failure_count"]


    # re-write the whole silver layer with the combined, cleaned dataset
    write_silver(transformed_data, config, silver_location, mode="overwrite")

    if write_metadata:
        record_execution(
            spark,
            execution,
            processed_records = processed_rows + rejected_rows,
            inserted_records = processed_rows,
            rejected_records = rejected_rows,
            validation_failure_count=validation_failure_count,
        )

    del transformed_data
    gc.collect()

def read_and_transform_bronze(
    spark,
    config,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    incremental=False, # True if we want to transform only new data
):
    dataset = config["dataset"]
    if incremental:
        watermark = get_last_silver_run_from_metadata(spark, dataset)
        # we will read the bronze layer and filter it based on the 'ingested_at' column.
        # since spark is lazy, reading the whole table and then doing .filter() is the exact
        # same as just reading only what we need from the delta table
        bronze_data = read_bronze(spark, dataset, remove_metadata=False, bronze_location=bronze_location)
        if watermark is not None:
            print(f"Incremental silver job for {dataset}: filtering bronze data ingested at >= {watermark}")
            bronze_data = bronze_data.filter(F.col("ingested_at") >= F.lit(watermark))
        else:
            print(f"Incremental silver job for {dataset}: no previous silver run found, processing all bronze data.")
        bronze_data = rm_bronze_metadata(bronze_data)
    else:
        bronze_data = read_bronze(spark, dataset, remove_metadata=True, bronze_location=bronze_location)

    validate_schema(bronze_data, config.get("schema"))
    cleaned_data, quality_summary = run_quality_checks(
        bronze_data,
        config.get("quality_checks", [])
    )
    transformed_data = apply_operations(cleaned_data, config.get("operations", []))

    del bronze_data
    del cleaned_data
    return transformed_data, quality_summary

def write_silver(
    df,
    config,
    mode="overwrite",
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    target = f"{silver_location}{config['dataset']}"
    if "partitions" in config and config["partitions"]:
        (
            df.write.format("delta")
            .partitionBy(config["partitions"])
            # .mode("overwrite")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(target)
        )
    else:
        (
            df.write.format("delta")
            # .mode("overwrite")
            .mode(mode)
            .option("overwriteSchema", "true")
            .save(target)
        )

    print(f"Written silver Delta table to: {target}")

def read_silver(
    spark,
    dataset,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    return spark.read.format("delta").load(f"{silver_location}{dataset}")
