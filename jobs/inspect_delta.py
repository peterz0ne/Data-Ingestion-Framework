import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark():
    spark = SparkSession.builder.appName("Inspect-Delta").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def show_delta_table(spark, title, path):
    print(f"\n{'=' * 60}")
    print(title)
    print(path)
    print("=" * 60)

    df = spark.read.format("delta").load(path)

    df.printSchema()
    print(f"Rows: {df.count()}")
    df.show(5, truncate=False)


def main():
    if len(sys.argv) != 2:
        print("Usage: inspect_delta.py <dataset>")
        sys.exit(1)

    dataset = sys.argv[1]

    bronze_path = f"s3a://delta-data/bronze/bronze_{dataset}"
    metadata_path = "s3a://delta-data/metadata/ingestion_runs"

    spark = create_spark()

    try:
        show_delta_table(
            spark,
            f"Bronze table: {dataset}",
            bronze_path,
        )

        metadata_df = (
            spark.read
            .format("delta")
            .load(metadata_path)
            .filter(F.col("dataset") == dataset)
            .orderBy(F.desc("finished_at"))
        )

        print(f"\n{'=' * 60}")
        print(f"Ingestion runs: {dataset}")
        print("=" * 60)

        metadata_df.printSchema()
        metadata_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()