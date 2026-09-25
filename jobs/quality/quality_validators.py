from functools import reduce
from operator import and_
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from .validation_results import add_rejection_reason

def validate_allowed_values(df, parameters, validation_context=None):
    column = parameters["column"]
    allowed_values = parameters["allowed_values"]
    invalid_condition = F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)

    return add_rejection_reason(
        df,
        invalid_condition,
        (
            f"Column '{column}' must contain one of: "
            f"{allowed_values}."
        ),
    )

def validate_non_negative_numbers(df, parameters, validation_context=None):
    columns = parameters["columns"]
    for column in columns:
        invalid_condition = F.col(column).isNotNull() & (F.col(column) < 0)

        df = add_rejection_reason(
            df,
            invalid_condition,
            f"Column '{column}' must be non-negative.",
        )

    return df

def validate_null_values(df, parameters, validation_context=None):
    null_values = parameters.get("null_values", [])
    columns = parameters["columns"]

    for column_name in columns:
        invalid_condition = F.col(column_name).isNull()
        if null_values:
            invalid_condition = invalid_condition | F.col(column_name).isin(null_values)

        df = add_rejection_reason(
            df,
            invalid_condition,
            f"Column '{column_name}' must not be missing.",
        )

    return df

def validate_reference_exists(df, parameters, validation_context=None):
    reference_dataset = parameters["reference_dataset"]
    reference_datasets = (validation_context or {}).get("reference_datasets", {})

    if reference_dataset not in reference_datasets:
        raise ValueError(
            f"Reference dataset '{reference_dataset}' is not available."
        )

    column = parameters["column"]
    reference_column = parameters["reference_column"]
    reference_df = reference_datasets[reference_dataset]

    reference_values = (
        reference_df
        .select(
            F.col(reference_column).alias("_reference_value")
        )
        .filter(F.col(reference_column).isNotNull())
        .dropDuplicates()
    )

    df = df.join(
        reference_values,
        F.col(column) == F.col("_reference_value"),
        "left",
    )

    df = add_rejection_reason(
        df,
        F.col(column).isNotNull()
        & F.col("_reference_value").isNull(),
        (
            f"Column '{column}' references a missing record in "
            f"'{reference_dataset}'."
        ),
    )

    return df.drop("_reference_value")

def missing_key_condition(key_columns):
    condition = F.lit(False)
    for column in key_columns:
        condition = condition | F.col(column).isNull()
    return condition

def validate_primary_key(df, parameters, validation_context=None):
    key_columns = parameters["columns"]

    df = add_rejection_reason(
        df,
        missing_key_condition(key_columns),
        (
            "Primary key contains missing values: "
            f"{', '.join(key_columns)}."
        ),
    )

    duplicate_window = (
        Window
        .partitionBy(*key_columns)
        .orderBy(F.monotonically_increasing_id())
    )
    df = df.withColumn(
        "_duplicate_number",
        F.row_number().over(duplicate_window),
    )

    all_key_values_present = reduce(
        and_,
        [F.col(column).isNotNull() for column in key_columns],
    )

    df = add_rejection_reason(
        df,
        all_key_values_present
        & (F.col("_duplicate_number") > 1),
        f"Duplicate primary key: {', '.join(key_columns)}.",
    )

    return df.drop("_duplicate_number")

def validate_duplicates(df, parameters, validation_context=None):
    columns = parameters["columns"]

    if columns == "*":
        columns = df.drop("rejection_reasons").columns

    duplicate_window = (
        Window
        .partitionBy(*columns)
        .orderBy(F.monotonically_increasing_id())
    )

    df = df.withColumn(
        "_duplicate_number",
        F.row_number().over(duplicate_window),
    )

    df = add_rejection_reason(
        df,
        F.col("_duplicate_number") > 1,
        f"Duplicate record based on: {', '.join(columns)}.",
    )

    return df.drop("_duplicate_number")

def validate_numeric_range(
    df,
    parameters,
    validation_context=None,
):
    column = parameters["column"]
    minimum = parameters["minimum"]
    maximum = parameters["maximum"]

    invalid_condition = (
        F.col(column).isNotNull()
        & (
            (F.col(column) < minimum) | (F.col(column) > maximum)
        )
    )

    return add_rejection_reason(
        df,
        invalid_condition,
        (
            f"Column '{column}' must be between "
            f"{minimum} and {maximum}."
        ),
    )