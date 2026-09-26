'''
Job to insert new data into the bronze/silver/gold layers
'''

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from metadata import (
    create_run_context,
    # rm_bronze_metadata
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

            silver_job(spark, config, run_context, incremental=True)


    finally:
        spark.stop()

if __name__ == "__main__":
    main()
