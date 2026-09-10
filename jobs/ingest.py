import json
import sys

from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import validate_schema
from quality import run_quality_checks

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


def write_bronze(df, dataset):
    target = f"s3a://delta-data/bronze/bronze_{dataset}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"Written bronze Delta table to: {target}")


def write_silver(df, dataset):
    target = f"s3a://delta-data/silver/silver_{dataset}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"Written silver Delta table to: {target}")


def main():
    config_path = get_config_path()
    config = load_config(config_path)

    dataset = config["dataset"]
    source = config["source"]

    spark = create_spark(dataset)

    try:
        print(f"Loading dataset: {dataset}")
        df = read_source(spark, source)
        print("Source files:")
        for path in sorted(df.inputFiles()):
            print(f"  {path}")

        # write to bronze layer before doing any transformations or validations
        write_bronze(df, dataset)

        # validate schema and apply operations to transform the data
        validate_schema(df, config.get("schema"))
        df = apply_operations(df, config.get("operations", []))

        # write to silver layer after transformations and validations
        write_silver(df, dataset)

        # later, do some enrichment and aggregation and write to the gold layer

        df = run_quality_checks(df, config.get("quality_checks", {}))

        df.printSchema()
        df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()