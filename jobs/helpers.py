import json
import sys

from pyspark.sql import SparkSession

def get_config_path():
    if len(sys.argv) != 2:
        print("Usage: ingest.py <configuration-path>")
        sys.exit(1)

    return sys.argv[1]

def get_config_paths():
    return [
        "/opt/spark/configs/taxi_zones.json",
        "/opt/spark/configs/taxi_trips.json",
        "/opt/spark/configs/air_quality.json",
        "/opt/spark/configs/weather.json",
    ]

def get_update_config_paths():
    return [
        "/opt/spark/configs/taxi_trips_v2.json",
        "/opt/spark/configs/air_quality_v2.json",
        "/opt/spark/configs/weather_v2.json",
    ]

def load_config(config_path):
    with open(config_path) as file:
        return json.load(file)

def get_feat_config_path():
    return "/opt/spark/jobs/predictions/configs/ml_tip_prediction.json"

def create_spark(dataset):
    spark = (
        SparkSession.builder
        .appName(f"Read-{dataset}")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.maxPlanStringLength", "8192")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_source(spark, source):
    return (
        spark.read
        .format(source["format"])
        .options(**source["options"])
        .load(source["paths"])
    )
