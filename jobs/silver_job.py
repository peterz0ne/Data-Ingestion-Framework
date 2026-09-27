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

            # before unionizing with the existing silver, we mark the data to new and old
            # in order to append only the new data at the end, instead of overwriting the whole layer

            # 1. tag both dataframes before unioning
            existing_silver = read_silver(spark, config["dataset"]).withColumn("_is_new_batch", F.lit(False))
            new_data_marked = transformed_data.withColumn("_is_new_batch", F.lit(True))

            combined = existing_silver.unionByName(new_data_marked, allowMissingColumns=True)

            # 2. run checks on the entire dataset (dropping duplicates, bad rows, etc. in memory)
            checked_combined, quality_summary = run_quality_checks(
                combined,
                checks
            )

            # 3. extract only the new rows that survived the checks
            transformed_data = (
                checked_combined
                .filter(F.col("_is_new_batch") == True)
                .drop("_is_new_batch")
            ) # Clean up the marker

            # existing_silver = read_silver(spark, config["dataset"])
            # combined = existing_silver.unionByName(transformed_data, allowMissingColumns=True)

            # transformed_data, quality_summary = run_quality_checks(
            #     combined,
            #     checks
            # )

            processed_rows += quality_summary["processed_rows"]
            rejected_rows += quality_summary["rejected_rows"]
            validation_failure_count += quality_summary["validation_failure_count"]


        # re-write the whole silver layer with the combined, cleaned dataset
        transformed_data = transformed_data.withColumn("execution_id",F.lit(execution["execution_id"]))
        write_silver(transformed_data, config, mode="append", silver_location=silver_location)
    else:
        transformed_data = transformed_data.withColumn("execution_id",F.lit(execution["execution_id"]))
        write_silver(transformed_data, config, mode="overwrite", silver_location=silver_location)

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
    # schema_version="1.0",
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    incremental=False, # True if we want to transform only new data
):
    dataset = config["dataset"]
    if incremental:
        watermark = get_last_silver_run_from_metadata(spark, dataset)
        # we will read the bronze layer and filter it based on the 'ingested_at' column.
        # since spark is lazy, reading the whole table and then doing .filter() is the exact
        # same as just reading only what we need from the delta table
        bronze_data = read_bronze(spark, dataset, schema_version=config["schema_version"], remove_metadata=False, bronze_location=bronze_location)
        if watermark is not None:
            print(f"Incremental silver job for {dataset}: filtering bronze data ingested at >= {watermark}")
            bronze_data = bronze_data.filter(F.col("ingested_at") >= F.lit(watermark))
        else:
            print(f"Incremental silver job for {dataset}: no previous silver run found, processing all bronze data.")
        bronze_data = rm_bronze_metadata(bronze_data)
    else:
        bronze_data = read_bronze(spark, dataset, schema_version=config["schema_version"], remove_metadata=True, bronze_location=bronze_location)

    # we need to read only the expected columns, in case the read_bronze call
    # returned data with multiple schema versions, and the latest version
    # drops columns, making the merged result (of both schema versions)
    # still have the dropped column, filled with null values (because the old
    # version still had the column, and thus the merged result kept it)
    expected_columns = list(config["schema"].keys())
    bronze_data = bronze_data.select(*expected_columns)

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
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target)
        )
    else:
        (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target)
        )

    print(f"Written silver Delta table to: {target}")

def read_silver(
    spark,
    dataset,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    return spark.read.format("delta").load(f"{silver_location}{dataset}")
