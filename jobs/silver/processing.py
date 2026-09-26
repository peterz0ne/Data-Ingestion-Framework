from pyarrow import dataset
from pyspark.sql import functions as F
from bronze_job import BRONZE_LAYER_DATASET_LOCATION, read_bronze
from metadata import get_last_silver_run_from_metadata, rm_bronze_metadata
from operations import apply_operations
from quality.quality import run_quality_checks
from schema_validations import get_schema_validation_errors
from .storage import SILVER_LAYER_DATASET_LOCATION, load_reference_datasets

def read_and_transform_bronze(
    spark,
    config,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
    incremental=False,
):
    bronze_data = read_bronze_for_silver(spark, config, bronze_location, incremental)

    # Validate the incoming Bronze batch before selecting active-schema columns.
    schema_errors = get_schema_validation_errors(bronze_data, config["schema"])
    if schema_errors:
        return reject_schema_invalid_batch(bronze_data, schema_errors)

    return validate_and_transform_schema_valid_data(
        spark,
        bronze_data,
        config,
        silver_location,
    )

def read_bronze_for_silver(spark, config, bronze_location, incremental):
    dataset = config["dataset"]
    schema_version = config["schema_version"]
    
    if not incremental:
        return read_bronze(spark, dataset, schema_version=schema_version, remove_metadata=True, bronze_location=bronze_location)

    watermark = get_last_silver_run_from_metadata(spark, dataset, schema_version)
    # we will read the bronze layer and filter it based on the 'ingested_at' column.
    # since spark is lazy, reading the whole table and then doing .filter() is the exact
    # same as just reading only what we need from the delta table
    bronze_data = read_bronze(spark, dataset, schema_version=schema_version, remove_metadata=False, bronze_location=bronze_location)

    if watermark is None:
        print(f"Incremental silver job for {dataset}: no previous silver run found, processing all bronze data.")
    else:
        print(f"Incremental silver job for {dataset}: filtering bronze data ingested at >= {watermark}")
        bronze_data = bronze_data.filter(F.col("ingested_at") >= F.lit(watermark))

    return rm_bronze_metadata(bronze_data)

def reject_schema_invalid_batch(bronze_data, schema_errors):
    _, rejected_data, quality_summary = run_quality_checks(bronze_data, quality_checks=[], schema_errors=schema_errors)
    return None, rejected_data, quality_summary

def validate_and_transform_schema_valid_data(spark, bronze_data, config, silver_location):
    expected_columns = list(config["schema"].keys())
    bronze_data = bronze_data.select(*expected_columns)

    quality_checks = config.get("quality_checks", [])
    reference_datasets = load_reference_datasets(spark, quality_checks, silver_location)
    valid_data, rejected_data, quality_summary = run_quality_checks(bronze_data, quality_checks, reference_datasets)

    transformed_data = apply_operations(valid_data,config.get("operations", []))

    return transformed_data, rejected_data, quality_summary
