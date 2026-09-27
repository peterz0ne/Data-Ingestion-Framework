from pyspark.sql import functions as F

def get_schema_validation_errors(df, expected_schema):
    actual_schema = dict(df.dtypes)
    errors = []

    # Collect errors for required columns that are missing or a wrong type.
    for column, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column)

        if actual_type is None:
            errors.append(f"Required column '{column}' is missing.")
        elif actual_type != expected_type:
            errors.append(
                (
                    f"Column '{column}' has type '{actual_type}', "
                    f"expected '{expected_type}'."
                )
            )

    # Add errors for unexpected columns (column from active schema not part of expected_schema) that contain at least one value.
    errors.extend(
        get_populated_unexpected_column_errors(
            df,
            expected_schema,
        )
    )

    return errors

def get_populated_unexpected_column_errors(df, expected_schema):
    unexpected_columns = [
        column
        for column in df.columns
        if column not in expected_schema
    ]

    if not unexpected_columns:
        return []

    column_counts = (
        df
        .agg(
            *[
                F.count(F.col(column)).alias(column)
                for column in unexpected_columns
            ]
        )
        .first()
    )

    return [
        f"Unsupported new column '{column}'."
        for column in unexpected_columns
        if column_counts[column] > 0
    ]
