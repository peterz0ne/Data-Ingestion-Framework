from functools import reduce
from operator import and_

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .quality_failure_handling import (
    get_failure_action,
    handle_validation_failure,
)

def validate_allowed_values(df, parameters):
    column = parameters["column"]
    allowed_values = parameters["allowed_values"]
    invalid_condition = F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains values outside the allowed set: {allowed_values}",
    )


def validate_non_negative_numbers(df, parameters):
    column = parameters["column"]
    invalid_condition = F.col(column).isNotNull() & (F.col(column) < 0)

    return handle_validation_failure(
        df,
        column,
        invalid_condition,
        parameters,
        f"Validation failed: column '{column}' contains negative values."
    )


def validate_null_values(df, parameters):
    column = parameters["column"]
    null_values = parameters.get("null_values", [])
    columns = df.columns if column == "*" else [column]

    for column_name in columns:
        invalid_condition = F.col(column_name).isNull()
        if null_values:
            invalid_condition = invalid_condition | F.col(column_name).isin(null_values)

        df = handle_validation_failure(
            df,
            column_name,
            invalid_condition,
            parameters,
            f"Validation failed: column '{column_name}' contains null or NaN values.",
        )

    return df


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


def add_duplicate_count_validate_duplicates(
    df,
    key_columns,
    duplicate_column_name,
):
    counts = (
        df.groupBy(*key_columns)
        .agg(F.count(F.lit(1)).alias(duplicate_column_name))
    )

    join_condition = reduce(
        and_,
        [
            F.col(f"data.{column}").eqNullSafe(F.col(f"keys.{column}"))
            for column in key_columns
        ],
    )

    duplicate_window = Window.partitionBy(*key_columns).orderBy(
        F.monotonically_increasing_id()
    )

    return (
        df.alias("data")
        .join(counts.alias("keys"), join_condition, "left")
        .select(
            "data.*",
            F.col(f"keys.{duplicate_column_name}"),
        )
        .withColumn("_duplicate_number", F.row_number().over(duplicate_window))
    )

def validate_duplicates(df, parameters):
    columns = parameters["columns"]
    if columns == "*":
        columns = df.columns

    failure_action, _ = get_failure_action(parameters)

    if failure_action == "drop":
        print(f"Deduplicating records using columns: {columns}.")
        return df.dropDuplicates(columns)

    df = add_duplicate_count_validate_duplicates(
        df,
        columns,
        "_duplicate_count",
    )

    duplicate_condition = (
        (F.col("_duplicate_count") > 1)
        & reduce(
            and_,
            [F.col(column).isNotNull() for column in columns],
        )
    )

    df = handle_validation_failure(
        df,
        None,
        duplicate_condition,
        parameters,
        f"Validation failed: duplicate records found for columns: {columns}.",
        duplicate_condition & (F.col("_duplicate_number") > 1),
    )

    return df.drop("_duplicate_count", "_duplicate_number")

