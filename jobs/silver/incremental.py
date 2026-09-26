from pyspark.sql import functions as F
from quality.quality import build_quality_summary, run_quality_checks
from .storage import read_silver


def validate_incremental_batch_against_silver(spark, transformed_data, rejected_data, quality_summary, config, silver_location):
    
    # No valid new rows remain after the batch-level checks.
    if transformed_data is None:
        return transformed_data, rejected_data, quality_summary

    # Revalidate new rows against existing Silver data.
    transformed_data, cross_batch_rejected_data, cross_batch_summary, = run_cross_batch_quality_checks(
        spark,
        transformed_data,
        config,
        silver_location,
    )

    if cross_batch_rejected_data is None or cross_batch_summary is None:
        return transformed_data, rejected_data, quality_summary

    # Rejections from both batch-level and cross-batch checks.
    rejected_data = rejected_data.unionByName(cross_batch_rejected_data, allowMissingColumns=True)

    # Reclassify newly rejected rows in the final metrics.
    updated_quality_summary = dict(quality_summary)
    updated_quality_summary["processed_rows"] -= cross_batch_summary["rejected_rows"]
    updated_quality_summary["rejected_rows"] += cross_batch_summary["rejected_rows"]
    updated_quality_summary["validation_failure_count"] += cross_batch_summary["validation_failure_count"]

    return transformed_data, rejected_data, updated_quality_summary

def run_cross_batch_quality_checks(spark, transformed_data, config, silver_location):
    cross_batch_checks = config.get("cross_batch_quality_checks", [],)
    if not cross_batch_checks:
        return transformed_data, None, None

    # Mark row origin so only valid or rejected new rows are returned.
    existing_silver = read_silver(spark, config["dataset"], silver_location).withColumn("_is_new_batch", F.lit(False))
    new_data_marked = transformed_data.withColumn("_is_new_batch", F.lit(True))
    combined_data = existing_silver.unionByName(new_data_marked, allowMissingColumns=True)

    valid_data, rejected_data, _ = run_quality_checks(combined_data, cross_batch_checks)

    valid_new_data = (
        valid_data
        .filter(F.col("_is_new_batch") == F.lit(True))
        .drop("_is_new_batch")
    )

    rejected_new_data = (
        rejected_data
        .filter(F.col("_is_new_batch") == F.lit(True))
        .drop("_is_new_batch")
    )

    cross_batch_summary = build_quality_summary(rejected_new_data)

    return valid_new_data, rejected_new_data, cross_batch_summary
