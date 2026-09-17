from pyspark.sql import SparkSession

DEFAULT_SILVER_TABLE_LOCATION = "s3a://delta-data/silver/silver_"
TAXI_TRIPS_SOURCE_VIEW = "source_taxi_trips"
AIR_QUALITY_SOURCE_VIEW = "source_air_quality"


def register_broadcast_join_sources(
    spark: SparkSession,
    silver_location: str = DEFAULT_SILVER_TABLE_LOCATION,
) -> None:
    (
        spark.read.format("delta")
        .load(f"{silver_location}taxi_trips")
        .createOrReplaceTempView(TAXI_TRIPS_SOURCE_VIEW)
    )
    (
        spark.read.format("delta")
        .load(f"{silver_location}air_quality")
        .createOrReplaceTempView(AIR_QUALITY_SOURCE_VIEW)
    )
