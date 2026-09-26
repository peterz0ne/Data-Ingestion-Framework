from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PIPELINE_EXECUTIONS_PATH = (
    "s3a://delta-data/metadata/pipeline_executions"
)

PIPELINE_EXECUTIONS_SCHEMA = StructType(
    [
        StructField("pipeline_run_id", StringType(), nullable=False),
        StructField("execution_id", StringType(), nullable=False),
        StructField("stage", StringType(), nullable=False),
        StructField("dataset", StringType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("finished_at", TimestampType(), nullable=False),
        StructField("duration_seconds", DoubleType(), nullable=False),
        StructField("processed_records", LongType(), nullable=True),
        StructField("inserted_records", LongType(), nullable=True),
        StructField("rejected_records", LongType(), nullable=True),
        StructField("validation_failure_count", LongType(), nullable=True),
    ]
)

def create_run_context():
    return {
        "run_id": str(uuid4()),
        "started_at": datetime.now(timezone.utc),
        "started_timer": perf_counter(),
    }

def start_execution(run_context, stage, dataset, schema_version):
    return {
        "pipeline_run_id": run_context["run_id"],
        "execution_id": str(uuid4()),
        "stage": stage,
        "dataset": dataset,
        "schema_version": schema_version,
        "started_at": datetime.now(timezone.utc),
        "started_timer": perf_counter(),
    }

def record_execution(
    spark,
    execution,
    processed_records=None,
    inserted_records=None,
    rejected_records=None,
    validation_failure_count=None,
):
    finished_at = datetime.now(timezone.utc)
    duration_seconds = round(
        perf_counter() - execution["started_timer"],
        3,
    )

    row = [
        (
            execution["pipeline_run_id"],
            execution["execution_id"],
            execution["stage"],
            execution["dataset"],
            execution["schema_version"],
            execution["started_at"],
            finished_at,
            duration_seconds,
            processed_records,
            inserted_records,
            rejected_records,
            validation_failure_count,
        )
    ]

    (
        spark.createDataFrame(row, PIPELINE_EXECUTIONS_SCHEMA)
        .write.format("delta")
        .mode("append")
        .save(PIPELINE_EXECUTIONS_PATH)
    )

def add_bronze_metadata(df, run_context, config):
    return (
        df.withColumn("source_file_path", F.input_file_name())
        .withColumn("ingested_at", F.lit(run_context["started_at"]))
        .withColumn("schema_version", F.lit(config["schema_version"]))
        .withColumn("ingestion_run_id", F.lit(run_context["run_id"]))
    )

def rm_bronze_metadata(df):
    return df.drop("source_file_path", "ingested_at", "schema_version", "ingestion_run_id")


# retrieve and return the latest finished_at or started_at execution for the silver stage and dataset from pipeline_executions
def get_last_silver_run_from_metadata(spark, dataset, metadata_location=PIPELINE_EXECUTIONS_PATH):
    try:
        df = spark.read.format("delta").load(metadata_location)
        row = df.filter((F.col("stage") == "silver") & (F.col("dataset") == dataset)) \
                .agg(F.max("started_at").alias("max_started")) \
                .first()
        if row:
            return row["max_started"]
    except Exception as e:
        print(f"Could not retrieve silver metadata watermark: {e}")
    return None
