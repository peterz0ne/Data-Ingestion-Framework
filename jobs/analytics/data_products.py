from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.runner import (
    DEFAULT_INTEGRATED_TABLE_PATH,
    register_integrated_taxi_trips,
)

ANALYTICAL_PRODUCT_LOCATION = "s3a://delta-data/analytics/"
ANALYTICAL_PRODUCT_METADATA_PATH = (
    "s3a://delta-data/metadata/analytical_product_refreshes"
)

DATA_PRODUCTS = (
    {
        "name": "monthly_zone_demand",
        "query_source": "analytics.dataframe_queries.query_01_monthly_zone_demand",
        "schema_version": "1.0",
    },
    {
        "name": "average_trip_distance_by_weather",
        "query_source": "query_02_average_trip_distance_by_weather.sql",
        "schema_version": "1.0",
    },
    {
        "name": "air_quality_vs_demand",
        "query_source": "query_03_air_quality_vs_demand.sql",
        "schema_version": "1.0",
    },
    {
        "name": "zone_weather_demand_variation",
        "query_source": "query_04_zone_weather_demand_variation.sql",
        "schema_version": "1.0",
    },
)

_QUERY_SQL_DIRECTORY = Path(__file__).parent / "sql_queries"

def load_product_sql(spec) -> str:
    if not spec["query_source"].endswith(".sql"):
        raise ValueError(f"{spec['name']} is not an SQL product.")
    return (_QUERY_SQL_DIRECTORY / spec["query_source"]).read_text(encoding="utf-8")

def build_product_dataframe(
    spark: SparkSession,
    spec,
) -> DataFrame:
    if spec["query_source"].endswith(".sql"):
        return spark.sql(load_product_sql(spec))

    module = import_module(spec["query_source"])
    return module.build_query(spark)

def _metadata_table_exists(spark: SparkSession) -> bool:
    return DeltaTable.isDeltaTable(spark, ANALYTICAL_PRODUCT_METADATA_PATH)

def _first_product_creation_time(
    spark: SparkSession,
    product_name: str,
) -> datetime | None:
    row = (
        spark.read.format("delta")
        .load(ANALYTICAL_PRODUCT_METADATA_PATH)
        .where(F.col("product_name") == product_name)
        .agg(F.min("created_at").alias("created_at"))
        .first()
    )
    if row is None:
        return None

    return row["created_at"]

def _existing_created_at(
    spark: SparkSession,
    product_name: str,
    fallback: datetime,
) -> datetime:
    if not _metadata_table_exists(spark):
        return fallback

    created_at = _first_product_creation_time(spark, product_name)
    if created_at is None:
        return fallback
    return created_at

def _add_product_metadata(
    dataframe: DataFrame,
    spec,
    created_at: datetime,
    refreshed_at: datetime,
    refresh_run_id: str,
) -> DataFrame:
    return (
        dataframe.withColumn("product_name", F.lit(spec["name"]))
        .withColumn("source_path", F.lit(DEFAULT_INTEGRATED_TABLE_PATH))
        .withColumn("created_at", F.lit(created_at))
        .withColumn("refreshed_at", F.lit(refreshed_at))
        .withColumn("refresh_run_id", F.lit(refresh_run_id))
        .withColumn("schema_version", F.lit(spec["schema_version"]))
    )

def _write_refresh_metadata(
    spark: SparkSession,
    spec,
    created_at: datetime,
    refreshed_at: datetime,
) -> None:
    metadata = spark.createDataFrame(
        [
            (
                spec["name"],
                created_at,
                refreshed_at,
            )
        ],
        [
            "product_name",
            "created_at",
            "refreshed_at",
        ],
    )
    (
        metadata.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(ANALYTICAL_PRODUCT_METADATA_PATH)
    )

def materialize_product(
    spark: SparkSession,
    spec,
    refresh_run_id: str,
) -> None:
    refreshed_at = datetime.now(timezone.utc)
    created_at = _existing_created_at(spark, spec["name"], refreshed_at)
    name = spec["name"]
    product_path = f"{ANALYTICAL_PRODUCT_LOCATION.rstrip('/')}/{name}"

    query_result = build_product_dataframe(spark, spec)

    product = _add_product_metadata(
        query_result,
        spec,
        created_at,
        refreshed_at,
        refresh_run_id,
    )

    (
        product.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(product_path)
    )

    _write_refresh_metadata(
        spark,
        spec,
        created_at,
        refreshed_at,
    )

def materialize_all_data_products(
    spark: SparkSession,
) -> None:
    print("registering taxi trips table...")
    register_integrated_taxi_trips(spark)
    print("registered taxi trips table")
    refresh_run_id = str(uuid4())

    for spec in DATA_PRODUCTS:
        name = spec["name"]
        print(f"Materializing data product: {name}")
        materialize_product(spark, spec, refresh_run_id)
        print(f"Written data product: {name}")
