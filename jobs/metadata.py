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

def add_bronze_metadata(df, run_context):
    return (
        df.withColumn("source_file_path", F.input_file_name())
        .withColumn("ingested_at", F.lit(run_context["started_at"]))
        .withColumn("ingestion_run_id", F.lit(run_context["run_id"]))
    )

def rm_bronze_metadata(df):
    return df.drop("source_file_path", "ingested_at", "ingestion_run_id")
