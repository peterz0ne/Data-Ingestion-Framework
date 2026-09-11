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
            

            bronze_df = bronze_job(spark, config,run_context)
            write_bronze(bronze_df, dataset)
        for config_path in config_paths:
            df, quality_summary = silver_job(spark,config)

            processed_rows = quality_summary["processed_rows"]
            rejected_rows = quality_summary["rejected_rows"]

            write_silver(df, dataset,config)


        df_gold = gold_job(spark,
                 taxi_trips_table="silver/silver_taxi_trips",
                 taxi_zones_table="silver/silver_taxi_zones",
                 air_quality_table="silver/silver_air_quality",
                 weather_table="silver/silver_weather"
                )

        write_gold(df_gold,"integrated_taxi_trips","pickup_datetime")


        write_run_metadata(
            spark,
            run_context,
            dataset,
            schema_version,
            source_files,
            processed_rows,
            rejected_rows,
        )


        df_gold.printSchema()
        df_gold.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()