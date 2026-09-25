from pyspark.sql import functions as F

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

def add_rejection_reason(df, invalid_condition, reason):
    return df.withColumn(
        "rejection_reasons",
        F.when(
            invalid_condition,
            F.concat(
                F.col("rejection_reasons"),
                F.array(F.lit(reason)),
            ),
        ).otherwise(F.col("rejection_reasons")),
    )

def split_valid_and_rejected_records(df):
    rejected_df = df.filter(F.size("rejection_reasons") > 0)

    valid_df = (
        df
        .filter(F.size("rejection_reasons") == 0)
        .drop("rejection_reasons")
    )

    return valid_df, rejected_df
