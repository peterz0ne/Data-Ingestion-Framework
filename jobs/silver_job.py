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

def silver_job(spark,config):
    df= read_source(spark,config["source"])
    validate_schema(df, config.get("schema"))
    df = apply_operations(df, config.get("operations", []))
    return run_quality_checks(
        df,
        config.get("quality_checks", []),
    )

def write_silver(df, dataset, config):
    target = f"s3a://delta-data/silver/silver_{dataset}"
    if("partitions" in config.keys()):
        df.write.format("delta").partitionBy(config["partitions"]).mode("overwrite").option("overwriteSchema", "true").save(target)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

    print(f"Written silver Delta table to: {target}")