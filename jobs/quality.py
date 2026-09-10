import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def missing_key_condition(key_columns):
    condition = F.lit(False)
    for column in key_columns:
        condition = condition | F.col(column).isNull()
    return condition

def add_duplicate_key_count(df, key_columns):
    return df.withColumn(
        "_duplicate_key_count",
        F.count("*").over(Window.partitionBy(*key_columns)),
    )

def validate_primary_key(df, parameters):
    key_columns = parameters["columns"]

    df = handle_validation_failure(
        df,
        None,
        missing_key_condition(key_columns),
        parameters,
        f"Primary key contains missing values: {', '.join(key_columns)}.",
    )

    df = add_duplicate_key_count(df, key_columns)

    df = handle_validation_failure(
        df,
        None,
        F.col("_duplicate_key_count") > 1,
        parameters,
        f"Primary key contains duplicate values: {', '.join(key_columns)}.",
    )

    return df.drop("_duplicate_key_count")

def run_quality_checks(df, quality_checks):
    for check in quality_checks:
        action = check["action"]
        parameters = check["parameters"]

        if action == "validate_primary_key":
            df = validate_primary_key(df, parameters)
        elif action == "validate_allowed_values":
            df = validate_allowed_values(df, parameters)
        elif action == "validate_non_negative_numbers":
            df = validate_non_negative_numbers(df, parameters)
        else:
            print(f"Unknown quality check: {action}")
            sys.exit(1)

    return df

def get_failure_action(parameters):
    failure = parameters.get("on_failure", {"action": "fail"})

    action = failure.get("action", "fail")

    if action not in {"fail", "drop", "fill"}:
        print(f"Unknown failure action: {action}")
        sys.exit(1)

    return action, failure

def get_fill_value(df, column, invalid_condition, failure):
    if "value" in failure:
        return F.lit(failure["value"])

    method = failure.get("method")
    valid_df = df.filter(~invalid_condition)

    if method == "mean":
        value = valid_df.select(F.avg(column)).first()[0]
    elif method == "median":
        values = valid_df.approxQuantile(column, [0.5], 0.0)
        value = values[0] if values else None
    elif method == "mode":
        row = (
            valid_df.groupBy(column)
            .count()
            .orderBy(F.desc("count"), F.asc(column))
            .first()
        )
        value = row[0] if row else None
    else:
        print("Fill requires either 'value' or method 'mean', 'median', or 'mode'.")
        sys.exit(1)

    if value is None:
        print(f"Cannot calculate a {method} replacement for '{column}'.")
        sys.exit(1)

    return F.lit(value)

def handle_validation_failure(
    df,
    column,
    invalid_condition,
    parameters,
    message,
):
    invalid_count = df.filter(invalid_condition).count()
    if invalid_count == 0:
        return df

    action, failure = get_failure_action(parameters)

    if action == "fail":
        print(f"{message} Invalid rows: {invalid_count}")
        sys.exit(1)

    if action == "drop":
        print(f"{message} Removing {invalid_count} invalid rows.")
        return df.filter(~invalid_condition)

    if action == "fill" and column is None:
        print("Primary-key validation cannot use the 'fill' action.")
        sys.exit(1)
    replacement = get_fill_value(df, column, invalid_condition, failure)
    print(f"{message} Filling {invalid_count} invalid rows.")

    return df.withColumn(
        column,
        F.when(invalid_condition, replacement).otherwise(F.col(column)),
    )


def validate_allowed_values(df, parameters):
    column = parameters["column"]
    allowed_values = parameters["allowed_values"]
    invalid_condition = ~F.col(column).isin(allowed_values)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains values outside the allowed set: {allowed_values}",
    )


def validate_non_negative_numbers(df, parameters):
    column = parameters["column"]
    invalid_condition = (F.col(column) < 0)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains negative or null values."
    )
