import sys

from pyspark.sql import functions as F


def check_primary_key(df, key_columns):
    total_rows = df.count()

    rows_with_keys = df.dropna(subset=key_columns).count()
    missing_key_rows = total_rows - rows_with_keys

    duplicate_key_groups = (
        df.groupBy(*key_columns)
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
    key_columns = quality_checks.get("primary_key")

    if key_columns:
        check_primary_key(df, key_columns)

    return df