from pyspark.sql import functions as F

from .quality_failure_handling import get_failure_action

def make_batch_rule(column, condition, failure_action, message):
    return {
        "column": column,
        "condition": condition,
        "failure_action": failure_action,
        "message": message,
    }

def batchable_rules_for_check(df, check):
    check_action = check["action"]
    parameters = check["parameters"]
    failure_action, _ = get_failure_action(parameters)

    if failure_action not in {"warn", "drop"}:
        return None

    if check_action == "validate_allowed_values":
        column = parameters["column"]
        allowed_values = parameters["allowed_values"]

        return [
            make_batch_rule(
                column,
                allowed_values_condition(parameters),
                failure_action,
                (
                    f"Validation failed: column '{column}' contains values "
                    f"outside the allowed set: {allowed_values}"
                ),
            )
        ]

    if check_action == "validate_non_negative_numbers":
        column = parameters["column"]

        return [
            make_batch_rule(
                column,
                non_negative_condition(parameters),
                failure_action,
                f"Validation failed: column '{column}' contains negative values.",
            )
        ]

    if check_action == "validate_null_values":
        return [
            make_batch_rule(
                column,
                condition,
                failure_action,
                f"Validation failed: column '{column}' contains null or NaN values.",
            )
            for column, condition in null_value_conditions(df, parameters)
        ]

    return None

def allowed_values_condition(parameters):
    column = parameters["column"]
    allowed_values = parameters["allowed_values"]

    return F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)

def non_negative_condition(parameters):
    column = parameters["column"]

    return F.col(column).isNotNull() & (F.col(column) < 0)


def null_value_conditions(df, parameters):
    column = parameters["column"]
    null_values = parameters.get("null_values", [])
    columns = df.columns if column == "*" else [column]

    rules = []

    for column_name in columns:
        condition = F.col(column_name).isNull()

        if null_values:
            condition = condition | F.col(column_name).isin(null_values)

        rules.append((column_name, condition))

    return rules

def count_batch_rule_violations(df, rules):
    aggregations = [
        F.count("*").alias("_total_rows"),
    ]

    for index, rule in enumerate(rules):
        stats_name = f"_invalid_{index}"
        rule["stats_name"] = stats_name

        aggregations.append(
            F.count(
                F.when(rule["condition"], F.lit(1))
            ).alias(stats_name)
        )

    return df.agg(*aggregations).first().asDict()

def apply_batched_row_checks(df, rules):
    stats = count_batch_rule_violations(df, rules)
    combined_drop_condition = None

    for rule in rules:
        invalid_count = stats[rule["stats_name"]]

        if invalid_count == 0:
            continue

        if rule["failure_action"] == "warn":
            print(
                f"[WARN]: {rule['message']} "
                f"Invalid rows: {invalid_count} "
                "(since action='warn' was specified, ignore this)"
            )

        elif rule["failure_action"] == "drop":
            print(f"{rule['message']} Removing {invalid_count} invalid rows.")

            if combined_drop_condition is None:
                combined_drop_condition = rule["condition"]
            else:
                combined_drop_condition = (
                    combined_drop_condition | rule["condition"]
                )

    if combined_drop_condition is not None:
        return df.filter(~combined_drop_condition)

    return df