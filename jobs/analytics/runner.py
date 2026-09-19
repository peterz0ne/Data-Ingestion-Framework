from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules

from pyspark.sql import DataFrame, SparkSession

DEFAULT_INTEGRATED_TABLE_PATH = (
    "s3a://delta-data/gold/gold_integrated_taxi_trips"
)
INTEGRATED_TABLE_VIEW = "integrated_taxi_trips"

_SQL_QUERY_DIRECTORY = Path(__file__).parent / "sql_queries"
_DATAFRAME_QUERY_DIRECTORY = Path(__file__).parent / "dataframe_queries"
_DATAFRAME_QUERY_PACKAGE = "analytics.dataframe_queries"

def sql_query_names() -> tuple[str, ...]:
    return tuple(
        sorted(query_file.stem for query_file in _SQL_QUERY_DIRECTORY.glob("*.sql"))
    )

def dataframe_query_names() -> tuple[str, ...]:
    if not _DATAFRAME_QUERY_DIRECTORY.is_dir():
        return ()

    return tuple(
        sorted(
            module.name
            for module in iter_modules([str(_DATAFRAME_QUERY_DIRECTORY)])
            if not module.name.startswith("_")
        )
    )

def available_queries() -> tuple[str, ...]:
    return sql_query_names() + dataframe_query_names()


def load_query_sql(query_name: str) -> str:
    if query_name not in sql_query_names():
        choices = ", ".join(available_queries())
        raise ValueError(
            f"Unknown query '{query_name}'. Available queries: {choices}."
        )

    return (_SQL_QUERY_DIRECTORY / f"{query_name}.sql").read_text(encoding="utf-8")


# Load a DataFrame API query module.
# It must define build_query(spark).
def _load_dataframe_query(query_name: str):
    module = import_module(f"{_DATAFRAME_QUERY_PACKAGE}.{query_name}")
    build_query = getattr(module, "build_query", None)
    if not callable(build_query):
        raise ValueError(
            f"DataFrame query '{query_name}' must define build_query(spark)."
        )
    return build_query


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
    if query_name in sql_query_names():
        return spark.sql(load_query_sql(query_name))

    if query_name in dataframe_query_names():
        return _load_dataframe_query(query_name)(spark)

    return spark.sql(load_query_sql(query_name))
