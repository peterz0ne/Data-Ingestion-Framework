import sys
from .quality_batching import (
    apply_batched_row_checks,
    batchable_rules_for_check,
)
from .quality_validators import (
    validate_allowed_values,
    validate_duplicates,
    validate_non_negative_numbers,
    validate_null_values,
    validate_primary_key,
)

def run_quality_checks(df, quality_checks):
    rows_before_checks = df.count()
    validation_metrics = {
        "validation_failure_count": 0,
    }
    pending_rules = []

    for check in quality_checks:
        batchable_rules = batchable_rules_for_check(df, check)

        if batchable_rules is not None:
            pending_rules.extend(batchable_rules)
            continue

        # Apply all preceding warn/drop row checks in one batch before
        # handling a validation that must remain on the legacy path.
        if pending_rules:
            df = apply_batched_row_checks(df, pending_rules, validation_metrics)
            pending_rules = []

        action = check["action"]
        parameters = check["parameters"]

        if action == "validate_primary_key":
            df = validate_primary_key(df, parameters, validation_metrics)
        elif action == "validate_allowed_values":
            df = validate_allowed_values(df, parameters, validation_metrics)
        elif action == "validate_non_negative_numbers":
            df = validate_non_negative_numbers(df, parameters, validation_metrics)
        elif action == "validate_null_values":
            df = validate_null_values(df, parameters, validation_metrics)
        elif action == "validate_duplicates":
            df = validate_duplicates(df, parameters, validation_metrics)
        else:
            print(f"Unknown quality check: {action}")
            sys.exit(1)

    # Apply a final batch when the configuration ends with batchable checks.
    if pending_rules:
        df = apply_batched_row_checks(df, pending_rules, validation_metrics)

    processed_rows = df.count()
    rejected_rows = rows_before_checks - processed_rows

    return df, {
        "processed_rows": processed_rows,
        "rejected_rows": rejected_rows,
        "validation_failure_count": (
            validation_metrics["validation_failure_count"]
        ),
    }





