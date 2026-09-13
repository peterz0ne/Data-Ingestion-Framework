from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from pyspark.sql import functions as F

def create_run_context():
    return {
        "run_id": str(uuid4()),
        "started_at": datetime.now(timezone.utc),
        "started_timer": perf_counter(),
    }

def add_bronze_metadata(df, run_context):
    return (
        df.withColumn("source_file_path", F.input_file_name())
        .withColumn("ingested_at", F.lit(run_context["started_at"]))
        .withColumn("ingestion_run_id", F.lit(run_context["run_id"]))
    )

def rm_bronze_metadata(df):
    return df.drop("source_file_path", "ingested_at", "ingestion_run_id")

def write_run_metadata(
    spark,
    run_context,
    dataset,
    schema_version,
    processed_rows,
    rejected_rows,
):
    finished_at = datetime.now(timezone.utc)
    duration_seconds = round(
        perf_counter() - run_context["started_timer"],
        3,
    )

    metadata_df = spark.createDataFrame(
        [
            (
                run_context["run_id"],
                dataset,
                schema_version,
                processed_rows,
                rejected_rows,
                run_context["started_at"],
                finished_at,
                duration_seconds,
                "SUCCESS",
            )
        ],
        [
            "run_id",
            "dataset",
            "schema_version",
            "processed_rows",
            "rejected_rows",
            "started_at",
            "finished_at",
            "duration_seconds",
            "status",
        ],
    )

    (
        metadata_df.write
        .format("delta")
        .mode("append")
        .save("s3a://delta-data/metadata/ingestion_runs")
    )