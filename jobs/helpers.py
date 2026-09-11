from pyspark import find_spark_home
import json
import sys

from pyspark.sql import SparkSession


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
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .option("delta.columnMapping.mode", "name")
        .option("overwriteSchema", "true")
        .save(target)
    )
    print(f"Written bronze Delta table to: {target}")


def write_silver(df, dataset):
    target = f"s3a://delta-data/silver/silver_{dataset}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"Written silver Delta table to: {target}")

def write_gold(df, dataset):
    target = f"s3a://delta-data/silver/gold_{dataset}"
    df.write.format("delta").mode("overwrite").save(target)
    print(f"Written gold Delta table to: {target}")
