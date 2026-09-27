from pyspark.sql import functions as F
from .quality_validators import (
    validate_allowed_values,
    validate_duplicates,
    validate_non_negative_numbers,
    validate_null_values,
    validate_reference_exists,
    validate_primary_key,
    validate_numeric_range,
)
from .validation_results import (
    finalize_rejection_reasons,
    initialize_rejection_reasons,
    split_valid_and_rejected_records,
)

VALIDATOR_REGISTRY = {
    "validate_primary_key": validate_primary_key,
    "validate_allowed_values": validate_allowed_values,
    "validate_non_negative_numbers": validate_non_negative_numbers,
    "validate_null_values": validate_null_values,
    "validate_duplicates": validate_duplicates,
    "validate_reference_exists": validate_reference_exists,
    "validate_numeric_range": validate_numeric_range,
}

def build_quality_summary(df):
    statistics = (
        df
        .agg(
            F.count("*").alias("total_records"),
            F.coalesce(
                F.sum(
                    F.when(
                        F.size("rejection_reasons") > 0,
                        F.lit(1),
                    ).otherwise(F.lit(0))
                ),
                F.lit(0),
            ).alias("rejected_records"),
            F.coalesce(
                F.sum(F.size("rejection_reasons")),
                F.lit(0),
            ).alias("validation_failure_count"),
        )
        .first()
    )

    rejected_records = statistics["rejected_records"]
    return {
        "processed_rows": statistics["total_records"] - rejected_records,
        "rejected_rows": rejected_records,
        "validation_failure_count": statistics["validation_failure_count"],
    }

def run_quality_checks(df, quality_checks, reference_datasets=None, schema_errors=()):
    reference_datasets = reference_datasets or {}
    df = initialize_rejection_reasons(df, schema_errors)
    validation_context = { "reference_datasets": reference_datasets, }

    if not schema_errors:
        for check in quality_checks:
            action = check["action"]
            parameters = check["parameters"]

            validator = VALIDATOR_REGISTRY.get(action)

            if validator is None:
                raise ValueError(f"Unknown quality check: {action}")

            df = validator(df, parameters, validation_context)

    df = finalize_rejection_reasons(df)

    quality_summary = build_quality_summary(df)

    valid_df, rejected_df = split_valid_and_rejected_records(df)

    return valid_df, rejected_df, quality_summary
