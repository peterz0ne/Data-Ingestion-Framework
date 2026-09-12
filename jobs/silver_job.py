from pyspark.sql import SparkSession
from operations import apply_operations
from schema_validations import validate_schema
from helpers import *
from metadata import *
from quality import run_quality_checks
from metadata import (
    add_bronze_metadata,
    create_run_context,
    write_run_metadata,
)
import gc
from bronze_job import read_bronze

from helpers import *

def silver_transformations(spark,config):
    df = read_bronze(spark,config["dataset"])
    validate_schema(df, config.get("schema"))
    df = apply_operations(df, config.get("operations", []))
    return run_quality_checks(
        df,
        config.get("quality_checks", []),
    )

def write_silver(df, config):
    target = f"s3a://delta-data/silver/silver_{config['dataset']}"
    if("partitions" in config.keys() and config["partitions"] != []):
        df.write.format("delta").partitionBy(config["partitions"]).mode("overwrite").option("overwriteSchema", "true").save(target)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

    print(f"Written silver Delta table to: {target}")

def read_silver(spark,dataset):
    return spark.read.format("delta").load(f"s3a://delta-data/silver/silver_{dataset}")


def silver_job(spark,config,run_context):
    df, quality_summary = silver_transformations(spark,config)
    processed_rows = quality_summary["processed_rows"]
    rejected_rows = quality_summary["rejected_rows"]

    write_silver(df,config)

    write_run_metadata(
                spark,
                run_context,
                config["dataset"],
                config["schema_version"],
                [],
                processed_rows,
                rejected_rows,
            )
    del df
    gc.collect()