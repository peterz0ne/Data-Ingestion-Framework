from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import get_schema_validation_errors
from helpers import *
from quality.quality import run_quality_checks
from metadata import record_execution, start_execution
import gc
from bronze_job import BRONZE_LAYER_DATASET_LOCATION, read_bronze
from quality.rejected_records import write_rejected_records

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
    execution = start_execution(
        run_context,
        stage="silver",
        dataset=config["dataset"],
        schema_version=config["schema_version"],
    )
    transformed_data, rejected_data, quality_summary = read_and_transform_bronze(
        spark,
        config,
        bronze_location,
        silver_location
    )
    processed_rows = quality_summary["processed_rows"]
    rejected_rows = quality_summary["rejected_rows"]
    validation_failure_count = (
        quality_summary["validation_failure_count"]
    )

    write_rejected_records(
        rejected_data,
        config["dataset"],
        execution,
    )

    write_silver(transformed_data, config, silver_location)

    if write_metadata:
        record_execution(
            spark,
            execution,
            processed_records = processed_rows + rejected_rows,
            inserted_records = processed_rows,
            rejected_records = rejected_rows,
            validation_failure_count=validation_failure_count,
        )

    del rejected_data
    del transformed_data
    gc.collect()

def read_and_transform_bronze(
    spark,
    config,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    bronze_data = read_bronze(spark, config["dataset"], bronze_location)
    schema_errors = get_schema_validation_errors(bronze_data, config["schema"])

    quality_checks = config.get("quality_checks", [])
    reference_datasets = load_reference_datasets(spark, quality_checks, silver_location)
    valid_data, rejected_data, quality_summary = run_quality_checks(bronze_data, quality_checks, reference_datasets, schema_errors)

    transformed_data = apply_operations(valid_data, config.get("operations", []))

    del bronze_data
    del valid_data

    return transformed_data, rejected_data, quality_summary

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
            .option("overwriteSchema", "true")
            .save(target)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
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

def load_reference_datasets(
    spark,
    quality_checks,
    silver_location,
):
    reference_datasets = {}

    for check in quality_checks:
        if check["action"] != "validate_reference_exists":
            continue

        reference_dataset = check["parameters"]["reference_dataset"]

        if reference_dataset not in reference_datasets:
            reference_datasets[reference_dataset] = read_silver(
                spark,
                reference_dataset,
                silver_location,
            )

    return reference_datasets