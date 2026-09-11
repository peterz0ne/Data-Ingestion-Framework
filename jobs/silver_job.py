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

from helpers import *

def silver_transformations(spark,config):
    df= read_source(spark,config["source"])
    validate_schema(df, config.get("schema"))
    df = apply_operations(df, config.get("operations", []))
    return run_quality_checks(
        df,
        config.get("quality_checks", []),
    )

def write_silver(df, config):
    target = f"s3a://delta-data/silver/silver_{config['dataset']}"
    if("partitions" in config.keys()):
        df.write.format("delta").partitionBy(config["partitions"]).mode("overwrite").option("overwriteSchema", "true").save(target)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

    print(f"Written silver Delta table to: {target}")

def silver_job(spark,config,run_context):
    df, quality_summary = silver_transformations(spark,config)
    processed_rows = quality_summary["processed_rows"]
    rejected_rows = quality_summary["rejected_rows"]

    write_silver(df,config)

    raw_df = read_source(spark, config["source"])
    source_files = sorted(raw_df.inputFiles())
    write_run_metadata(
                spark,
                run_context,
                config["dataset"],
                config["schema_version"],
                source_files,
                processed_rows,
                rejected_rows,
            )
    del df
    del raw_df
    gc.collect()