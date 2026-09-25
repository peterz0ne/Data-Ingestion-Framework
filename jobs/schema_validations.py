def get_schema_validation_errors(df, expected_schema):
    actual_schema = dict(df.dtypes)
    errors = []

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

    for column in actual_schema:
        if column not in expected_schema:
            errors.append(f"Unsupported new column '{column}'.")

    return errors