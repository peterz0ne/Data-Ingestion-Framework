from pyspark.sql import functions as F

REJECTION_REASON_COLUMN_PREFIX = "_rejection_reason_"

def initialize_rejection_reasons(df, initial_rejection_reasons=()):
    return df.withColumn(
        "rejection_reasons",
        F.array(
            *[
                F.lit(reason)
                for reason in initial_rejection_reasons
            ]
        ).cast("array<string>"),
    )

def get_rejection_reason_columns(df):
    return [
        column
        for column in df.columns
        if column.startswith(REJECTION_REASON_COLUMN_PREFIX)
    ]

def add_rejection_reason(df, invalid_condition, reason):
    reason_columns = get_rejection_reason_columns(df)
    reason_column = (
        f"{REJECTION_REASON_COLUMN_PREFIX}{len(reason_columns)}"
    )

    return df.withColumn(
        reason_column,
        F.when(invalid_condition, F.lit(reason)),
    )

def finalize_rejection_reasons(df):
    reason_columns = get_rejection_reason_columns(df)

    if not reason_columns:
        return df

    new_reasons = F.filter(
        F.array(*[F.col(column) for column in reason_columns]),
        lambda reason: reason.isNotNull(),
    )

    return (
        df
        .withColumn(
            "rejection_reasons",
            F.concat(F.col("rejection_reasons"), new_reasons),
        )
        .drop(*reason_columns)
    )

def split_valid_and_rejected_records(df):
    rejected_df = df.filter(F.size("rejection_reasons") > 0)

    valid_df = (
        df
        .filter(F.size("rejection_reasons") == 0)
        .drop("rejection_reasons")
    )

    return valid_df, rejected_df
