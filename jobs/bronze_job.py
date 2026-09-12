from helpers import *
from metadata import *
import gc

# the location prefix of bronze layer dataset in the object storage
BRONZE_LAYER_DATASET_LOCATION = "s3a://delta-data/bronze/bronze_"


def bronze_job(
    spark,
    config,
    run_context,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
):
    df = read_landing_and_add_metadata(spark, config, run_context)
    write_bronze(df, config["dataset"], bronze_location)

    # free up unused memory
    del df
    gc.collect()


def read_landing_and_add_metadata(spark, config, run_context):
    raw_df = read_source(spark, config["source"])
    source_files = sorted(raw_df.inputFiles())
    print("Source files:")
    for path in source_files:
        print(f"  {path}")
    df = add_bronze_metadata(raw_df, run_context)
    return df


def write_bronze(
    df,
    dataset,
    bronze_location=BRONZE_LAYER_DATASET_LOCATION,
):
    target = f"{bronze_location}{dataset}"
    (
        df.write
        .format("delta")
        .mode("overwrite") # maybe use only append?
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .option("delta.columnMapping.mode", "name")
        .option("overwriteSchema", "true") # maybe not needed?
        .save(target)
    )
    print(f"Written bronze Delta table to: {target}")


def read_bronze(spark, dataset, bronze_location=BRONZE_LAYER_DATASET_LOCATION):
    df = spark.read.format("delta").load(f"{bronze_location}{dataset}")
    # make sure to not return any metadata when the bronze data are are requested
    # from another job (eg silver layer transformation)
    return rm_bronze_metadata(df)
