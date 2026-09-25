from pyspark.sql import functions as F

REJECTED_RECORDS_PATH = "s3a://delta-data/metadata/rejected_records"

def build_rejected_records(rejected_df, dataset, execution):
    if "rejection_reasons" not in rejected_df.columns:
        raise ValueError(
            "rejected_df must contain a 'rejection_reasons' column."
        )

    source_columns = rejected_df.drop("rejection_reasons").columns
    raw_record = F.to_json(F.struct(*source_columns))

    return (
        rejected_df
        .withColumn("raw_record", raw_record)
        .select(
            F.lit(execution["pipeline_run_id"]).alias("pipeline_run_id"),
            F.lit(execution["execution_id"]).alias("execution_id"),
            F.lit(execution["stage"]).alias("stage"),
            F.lit(dataset).alias("dataset"),
            F.lit(execution["schema_version"]).alias("schema_version"),
            F.current_timestamp().alias("rejected_at"),
            F.col("rejection_reasons"),
            F.col("raw_record"),
        )
    )

def write_rejected_records(
    rejected_df,
    dataset,
    execution,
    path=REJECTED_RECORDS_PATH,
):
    rejected_records = build_rejected_records(rejected_df, dataset, execution)

    (
        rejected_records.write
        .format("delta")
        .mode("append")
        .save(path)
    )
