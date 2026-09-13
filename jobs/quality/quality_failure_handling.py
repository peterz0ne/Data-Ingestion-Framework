import sys
from pyspark.sql import functions as F

def get_failure_action(parameters):
    failure = parameters.get("on_failure", {"action": "fail"})
    action = failure.get("action", "fail")

    if action not in {"fail", "drop", "fill", "warn"}:
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
    drop_condition=None,
):
    invalid_count = df.filter(invalid_condition).count()
    if invalid_count == 0:
        return df

    action, failure = get_failure_action(parameters)

    if action == "warn":
        print(f"[WARN]: {message} Invalid rows: {invalid_count} (since action='warn' was specified, ignore this)")
        return df # dont fail

    if action == "fail":
        print(f"{message} Invalid rows: {invalid_count}")
        sys.exit(1)

    if action == "drop":
        drop_condition = drop_condition if drop_condition is not None else invalid_condition
        print(f"{message} Removing {invalid_count} invalid rows.")
        return df.filter(~drop_condition)

    if action == "fill" and column is None:
        print("Primary-key validation cannot use the 'fill' action.")
        sys.exit(1)
    replacement = get_fill_value(df, column, invalid_condition, failure)
    print(f"{message} Filling {invalid_count} invalid rows.")

    return df.withColumn(
        column,
        F.when(invalid_condition, replacement).otherwise(F.col(column)),
    )