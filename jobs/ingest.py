from pyspark import find_spark_home
import json
import sys

from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import validate_schema
from quality import run_quality_checks
from metadata import (
    add_bronze_metadata,
    create_run_context,
    write_run_metadata,
)
from helpers import *
from gold_job import gold_job


def main():
    config_path = get_config_path()
    config = load_config(config_path)

    schema_version = config["schema_version"]
    dataset = config["dataset"]
    source = config["source"]

    spark = create_spark(dataset)
    run_context = create_run_context()

    try:
        print(f"Loading dataset: {dataset}")

        raw_df = read_source(spark, source)
        source_files = sorted(raw_df.inputFiles())

        print("Source files:")
        for path in source_files:
            print(f"  {path}")

        bronze_df = add_bronze_metadata(raw_df, run_context)
        write_bronze(bronze_df, dataset)

        validate_schema(raw_df, config.get("schema"))

        df = apply_operations(raw_df, config.get("operations", []))

        df, quality_summary = run_quality_checks(
            df,
            config.get("quality_checks", []),
        )

        processed_rows = quality_summary["processed_rows"]
        rejected_rows = quality_summary["rejected_rows"]

        write_silver(df, dataset)
        df_gold = gold_job(spark,
                 taxi_trips_location="s3a://delta-data/silver/silver_taxi_trips",
                 taxi_zones_location="s3a://delta-data/silver/silver_taxi_zones",
                 air_quality_location="s3a://delta-data/silver/silver_air_quality",
                 weather_location="s3a://delta-data/silver/silver_weather"
                )

        write_gold(df_gold,"integrated_taxi_trips")


        write_run_metadata(
            spark,
            run_context,
            dataset,
            schema_version,
            source_files,
            processed_rows,
            rejected_rows,
        )


        df.printSchema()
        df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()