import sys

from pyspark.sql import functions as F


def check_primary_key(df, key_columns):
    total_rows = df.count()

    rows_with_keys = df.dropna(subset=key_columns).count()
    missing_key_rows = total_rows - rows_with_keys

    duplicate_key_groups = (
         df.dropna(subset=key_columns)
         .groupBy(*key_columns)
         .count()
         .filter(F.col("count") > 1)
         .count()
    )

    if missing_key_rows > 0 or duplicate_key_groups > 0:
        print("Primary key check failed.")
        print(f"Rows with missing key values: {missing_key_rows}")
        print(f"Duplicate key groups: {duplicate_key_groups}")
        sys.exit(1)

    print(f"Primary key check passed: {', '.join(key_columns)}")


def run_quality_checks(df, quality_checks):
    # key_columns = quality_checks.get("primary_key")

    for check in quality_checks.get("quality_checks", []):
        pass
    # handle_validation()

    # if key_columns:
    #     check_primary_key(df, key_columns)

    return df

def handle_validation_failure(
    df,
    column,
    invalid_condition,
    parameters,
    message,
):
    failure = parameters.get("on_failure", {"action": "fail"})
    if isinstance(failure, str):
        failure = {"action": failure}

    action = failure.get("action", "fail")

    if action == "remove_rows":
        print(f"{message} Removing {invalid_count} invalid rows.")
        return df.filter(~invalid_condition)

    if action == "fill":
        if "value" in failure:
            replacement = F.lit(failure["value"])
        elif failure.get("method") in {"mean", "median", "mode"}:
            method = failure["method"]
            valid_df = df.filter(~invalid_condition)
            if method == "mean":
                replacement = F.lit(valid_df.select(F.avg(column)).first()[0])
            elif method == "median":
                replacement = F.lit(valid_df.approxQuantile(column, [0.5], 0.0)[0])
            else:
                replacement = F.lit(
                    valid_df.groupBy(column)
                    .count()
                    .orderBy(F.desc("count"), F.asc(column))
                    .first()[0]
                )
        else:
            print("Fill failure action requires 'value' or method 'mean', 'median', or 'mode'.")
            sys.exit(1)

        print(f"{message} Filling {invalid_count} invalid rows.")
        return df.withColumn(column, F.when(invalid_condition, replacement).otherwise(F.col(column)))

    # place more failure actions here as needed

    # if no invalid rows, return the original DataFrame
    invalid_count = df.filter(invalid_condition).count()
    if invalid_count == 0:
        return df

    # here and onwards, we have invalid rows
    if action == "fail":
        print(message)
        sys.exit(1)

    # no valid action found, exit with error
    print(f"Error in quality checks: unknown failure action '{action}'.")
    sys.exit(1)


def validate_allowed_values(df, parameters):
    column = parameters["column"]
    allowed_values = parameters["allowed_values"]
    invalid_condition = F.col(column).isNull() | ~F.col(column).isin(allowed_values)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains values outside the allowed set: {allowed_values}",
    )


def validate_positive_numbers(df, parameters):
    column = parameters["column"]
    invalid_condition = F.col(column).isNull() | (F.col(column) < 0)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains non-positive or null values.",
    )
