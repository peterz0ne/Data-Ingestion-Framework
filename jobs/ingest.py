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

def get_config_path():
    if len(sys.argv) != 2:
        print("Usage: ingest.py <configuration-path>")
        sys.exit(1)

    return sys.argv[1]


def load_config(config_path):
    with open(config_path) as file:
        return json.load(file)

def create_spark(dataset):
    spark = SparkSession.builder.appName(f"Read-{dataset}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_source(spark, source):
    return (
        spark.read
        .format(source["format"])
        .options(**source["options"])
        .load(source["paths"])
    )


# def write_bronze(df, dataset):
#     target = f"s3a://delta-data/bronze/bronze_{dataset}"
#     df.write.format("delta").mode("overwrite").save(target)
#     print(f"Written bronze Delta table to: {target}")
def write_bronze(df, dataset):
    target = f"s3a://delta-data/bronze/bronze_{dataset}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .option("delta.columnMapping.mode", "name")
        .save(target)
    )
    print(f"Written bronze Delta table to: {target}")


def write_silver(df, dataset):
    target = f"s3a://delta-data/silver/silver_{dataset}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"Written silver Delta table to: {target}")


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