from helpers import *
from metadata import *
import gc


def read_landing_and_add_metadata(spark,config,run_context):
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

def read_bronze(spark,dataset):
    df= spark.read.format("delta").load(f"s3a://delta-data/bronze/bronze_{dataset}")
    return df.drop("source_file_path", "ingested_at","ingestion_run_id")
    


def bronze_job(spark,config,run_context):
    df = read_landing_and_add_metadata(spark,config,run_context)
    write_bronze(df, config["dataset"])
    del df
    gc.collect()
