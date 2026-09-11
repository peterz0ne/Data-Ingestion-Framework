from helpers import *
from metadata import *
import gc


def bronze_transformation(spark,config,run_context):
    raw_df = read_source(spark, config["source"])
    source_files = sorted(raw_df.inputFiles())
    print("Source files:")
    for path in source_files:
        print(f"  {path}")
    df = add_bronze_metadata(raw_df, run_context)
    return df

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

def bronze_job(spark,config,run_context):
    df = bronze_transformation(spark,config,run_context)
    write_bronze(df, config["dataset"])
    del df
    gc.collect()
