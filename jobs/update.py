'''
Job to insert new data into the bronze/silver/gold layers
'''

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from metadata import (
    create_run_context,
    get_last_ingestion_run_from_metadata,
    rm_bronze_metadata
)
from schema_validations import validate_schema
from quality.quality import run_quality_checks
from operations import apply_operations
from helpers import *
from analytics.data_products import materialize_all_data_products
from gold_job import gold_job
from silver_job import silver_job, write_silver
from bronze_job import bronze_job, read_bronze
from weather_conditions_job import load_coco_codes


def main():
    spark = create_spark("datasets")
    run_context = create_run_context()

    # update this to retrieve the new csvs
    config_paths = get_config_paths()

    try:
        for config_path in config_paths:
            config = load_config(config_path)
            dataset = config["dataset"]
            print(f"Loading dataset: {dataset}")
            bronze_job(spark, config, run_context, mode="append")

        for config_path in config_paths:
            config = load_config(config_path)
            dataset = config["dataset"]

            # retrieve the latest ingestion starting time
            watermark = get_last_ingestion_run_from_metadata(spark)
            # 2. Only bronze rows loaded since then
            bronze = read_bronze(spark, dataset, remove_metadata=False)
            if watermark is None:
                new_rows = bronze
                print("no watermark")
            else:
                new_rows = bronze.filter(F.col("ingested_at") >= F.lit(watermark))
                print(f"NEW ROWS: {new_rows.count()}")

            if len(new_rows.head(1)) == 0:
                print("No new bronze rows, silver is up to date.")
                return

            # make sure to remove the metadata to not get 'invalid schema' errors
            new_rows = rm_bronze_metadata(new_rows)

            # clean the data here for now in order to avoid
            # calling silver_job() that reads the whole bronze layer
            # (we want to do stuff only to the new bronze data)

            # validate/clean/transform new data
            validate_schema(new_rows, config.get("schema"))
            cleaned_data, quality_summary = run_quality_checks(
                new_rows,
                config.get("quality_checks", [])
            )
            transformed_data = apply_operations(cleaned_data, config.get("operations", []))

            write_silver(transformed_data, config, mode="append")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
