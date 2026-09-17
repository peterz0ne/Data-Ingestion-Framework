from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

DEFAULT_INTEGRATED_TABLE_PATH = (
    "s3a://delta-data/gold/gold_integrated_taxi_trips"
)
INTEGRATED_TABLE_VIEW = "integrated_taxi_trips"

_QUERY_DIRECTORY = Path(__file__).parent / "queries"
_QUERY_FILES = {
    "query_03_air_quality_vs_demand": "query_03_air_quality_vs_demand.sql",
    "query_03_air_quality_vs_demand_february_2024": (
        "query_03_air_quality_vs_demand_february_2024.sql"
    ),
    "query_05_peak_travel_hours": "query_05_peak_travel_hours.sql",
    "query_05_peak_travel_hours_february_2024": (
        "query_05_peak_travel_hours_february_2024.sql"
    ),
    "broadcast_join_normal": "broadcast_join_normal.sql",
    "broadcast_join_forced": "broadcast_join_forced.sql",
}

def available_queries() -> tuple[str, ...]:
    return tuple(_QUERY_FILES)


def load_query_sql(query_name: str) -> str:
    try:
        filename = _QUERY_FILES[query_name]
    except KeyError as error:
        choices = ", ".join(available_queries())
        raise ValueError(
            f"Unknown query '{query_name}'. Available queries: {choices}."
        ) from error

    return (_QUERY_DIRECTORY / filename).read_text(encoding="utf-8")


def register_integrated_taxi_trips(
    spark: SparkSession,
    table_path: str = DEFAULT_INTEGRATED_TABLE_PATH,
) -> DataFrame:
    table = spark.read.format("delta").load(table_path)
    table.createOrReplaceTempView(INTEGRATED_TABLE_VIEW)
    return table


def execute_query(
    spark: SparkSession,
    query_name: str,
    table_path: str = DEFAULT_INTEGRATED_TABLE_PATH,
) -> DataFrame:
    register_integrated_taxi_trips(spark, table_path)
    return execute_registered_query(spark, query_name)


def execute_registered_query(
    spark: SparkSession,
    query_name: str,
) -> DataFrame:
    return spark.sql(load_query_sql(query_name))
