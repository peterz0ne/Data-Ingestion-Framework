from pyspark import find_spark_home
import json
import sys

from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import validate_schema
from quality import run_quality_checks
from metadata import (
    create_run_context,
    write_run_metadata,
)
from helpers import *
from gold_job import gold_job, write_gold
from silver_job import silver_job,write_silver
from bronze_job import bronze_job, write_bronze


def main():


    spark = create_spark("datasets")
    run_context = create_run_context()
    config_paths=get_config_paths()
        

    try:
        for config_path in config_paths:

            config = load_config(config_path)
            dataset = config["dataset"]
            print(f"Loading dataset: {dataset}")

            bronze_job(spark, config,run_context)
        for config_path in config_paths:
            silver_job(spark,config,run_context)



        gold_job(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()