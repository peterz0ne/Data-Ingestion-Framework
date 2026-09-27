import gc
from bronze_job import BRONZE_LAYER_DATASET_LOCATION
from metadata import record_execution, start_execution
from quality.rejected_records import write_rejected_records
from silver.incremental import validate_incremental_batch_against_silver
from silver.processing import read_and_transform_bronze
from silver.storage import SILVER_LAYER_DATASET_LOCATION, read_silver, write_silver
from pyspark.sql import functions as F


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

    transformed_data, rejected_data, quality_summary = read_and_transform_bronze(
        spark,
        config,
        bronze_location,
        silver_location,
        incremental=incremental,
    )

    if incremental:
        transformed_data, rejected_data, quality_summary = validate_incremental_batch_against_silver(
            spark,
            transformed_data,
            rejected_data,
            quality_summary,
            config,
            silver_location,
        )
    transformed_data = transformed_data.withColumn("execution_id",F.lit(execution["execution_id"]))
    rejected_data = rejected_data.withColumn("execution_id",F.lit(execution["execution_id"]))
    write_rejected_records(rejected_data, config["dataset"], execution)

    # Write only the new valid rows incrementally. Rebuild Silver for full runs.
    if transformed_data is not None:
        write_silver(
            transformed_data,
            config,
            mode="append" if incremental else "overwrite",
            silver_location=silver_location
        )

    if write_metadata:
        record_execution(
            spark,
            execution,
            processed_records = quality_summary["processed_rows"] + quality_summary["rejected_rows"],
            inserted_records = quality_summary["processed_rows"],
            rejected_records = quality_summary["rejected_rows"],
            validation_failure_count=quality_summary["validation_failure_count"],
        )

    del rejected_data
    if transformed_data is not None:
        del transformed_data

    gc.collect()
