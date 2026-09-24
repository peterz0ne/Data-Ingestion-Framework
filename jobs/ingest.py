import json
import sys

from pyspark.sql import SparkSession
from metadata import (
    create_run_context,
)
from helpers import *
from analytics.data_products import materialize_all_data_products
from gold_job import gold_job
from silver_job import silver_job
from bronze_job import bronze_job
from weather_conditions_job import load_coco_codes


def main():

    spark = create_spark("datasets")
    run_context = create_run_context()
    config_paths = get_config_paths()

    try:
        for config_path in config_paths:
            config = load_config(config_path)
            dataset = config["dataset"]
            print(f"Loading dataset: {dataset}")
            bronze_job(spark, config, run_context)

        for config_path in config_paths:
            config = load_config(config_path)
            silver_job(spark, config, run_context)
        load_coco_codes(spark)
        gold_job(spark, run_context=run_context)
        materialize_all_data_products(spark, run_context)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
