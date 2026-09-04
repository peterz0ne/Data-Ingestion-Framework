from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def main():

    builder = (
        SparkSession.builder
        .appName("DeltaLakeTest")

        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )

        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000"
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            "minio"
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "minio123456"
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("\n")
    print("=" * 60)
    print("Spark version:", spark.version)
    print("=" * 60)
    print("\n")

    data = [
        (1, "Alice", 30),
        (2, "Bob", 35),
        (3, "Charlie", 40),
        (4, "David", 28),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name", "age"]
    )

    print("Input DataFrame:")
    df.show()

    delta_path = "s3a://delta-data/users"

    print("Writing Delta table:")
    print(delta_path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(delta_path)
    )

    print("\nDelta table written successfully.")

    print("\nReading Delta table:")

    result = (
        spark.read
        .format("delta")
        .load(delta_path)
    )

    result.show()

    print("\nDelta table history:")

    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(
        spark,
        delta_path
    )

    delta_table.history().show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
