from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from quality.quality import run_quality_checks


spark = (
    SparkSession.builder
    .appName("quality-test")
    .master("local[2]")
    .getOrCreate()
)

try:
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("vendor_id", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("note", StringType(), True),
    ])

    df = spark.createDataFrame(
        [
            (1, 1, 12.0, "valid"),
            (2, 2, -1.0, "negative fare"),
            (3, 9, 10.0, "invalid vendor"),
            (4, 9, -2.0, "fails both drop rules"),
            (5, None, 5.0, "null vendor: warning only"),
            (6, 1, 20.0, "duplicate"),
            (6, 1, 20.0, "duplicate"),
            (7, None, 7.0, "null duplicate"),
            (7, None, 7.0, "null duplicate"),
        ],
        schema,
    )

    checks = [
        {
            "action": "validate_duplicates",
            "parameters": {
                "columns": "*",
                "on_failure": {"action": "drop"},
            },
        },
        {
            "action": "validate_allowed_values",
            "parameters": {
                "column": "vendor_id",
                "allowed_values": [1, 2],
                "on_failure": {"action": "drop"},
            },
        },
        {
            "action": "validate_non_negative_numbers",
            "parameters": {
                "column": "fare_amount",
                "on_failure": {"action": "drop"},
            },
        },
        {
            "action": "validate_null_values",
            "parameters": {
                "column": "*",
                "on_failure": {"action": "warn"},
            },
        },
    ]

    cleaned_df, summary = run_quality_checks(df, checks)

    remaining_ids = [
        row.id
        for row in cleaned_df.orderBy("id").select("id").collect()
    ]

    assert remaining_ids == [1, 5, 6, 7], remaining_ids
    assert summary == {
        "processed_rows": 4,
        "rejected_rows": 5,
    }, summary

    print("Quality batch test passed.")
    print(summary)

finally:
    spark.stop()