from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
from helpers import load_config, read_source
import re

REQUIRED_RAW_DATASETS = (
    "taxi_trips",
    "weather",
    "air_quality",
    "taxi_zones",
)

TAXI_COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "Airport_fee": "airport_fee",
}

TAXI_ZONE_COLUMN_RENAMES = {
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
}

WEATHER_UNUSED_COLUMNS = (
    "snwd",
    "snwd_source",
    "wpgt",
    "wpgt_source",
)

AIR_QUALITY_COLUMN_RENAMES = {
    "Date Local": "local_date",
    "Time Local": "local_time",
    "Date GMT": "gmt_date",
    "Time GMT": "gmt_time",
    "Date of Last Change": "last_change_date",
}

AIR_QUALITY_COLUMN_TYPES = {
    "latitude": "double",
    "longitude": "double",
    "sample_measurement": "double",
    "mdl": "double",
    "local_date": "date",
    "gmt_date": "date",
    "last_change_date": "date",
}

def project_root() -> Path:
    return Path(__file__).resolve().parents[3]

def load_raw_sources(spark: SparkSession, raw_source_configs: dict[str, str]) -> dict[str, DataFrame]:
    missing_datasets = set(REQUIRED_RAW_DATASETS) - set(raw_source_configs)
    if missing_datasets:
        missing = ", ".join(sorted(missing_datasets))
        raise ValueError(f"Missing raw-source configurations: {missing}.")

    raw_sources = {}

    for dataset_name in REQUIRED_RAW_DATASETS:
        config_path = project_root() / raw_source_configs[dataset_name]
        source_config = load_config(config_path)["source"]
        raw_sources[dataset_name] = read_source(spark, source_config)

    return raw_sources

def require_columns(
    dataframe: DataFrame,
    required_columns: tuple[str, ...],
    dataset_name: str,
) -> None:
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"{dataset_name} is missing required columns: {missing}.")


def prepare_raw_taxi_trips(taxi_trips: DataFrame) -> DataFrame:
    require_columns(taxi_trips, tuple(TAXI_COLUMN_RENAMES), "Raw taxi trips")

    prepared_trips = taxi_trips

    for source_column, target_column in TAXI_COLUMN_RENAMES.items():
        prepared_trips = prepared_trips.withColumnRenamed(source_column, target_column)

    return (
        prepared_trips
        .withColumn(
            "pickup_datetime",
            F.to_utc_timestamp(
                F.col("pickup_datetime"),
                "America/New_York",
            ),
        )
        .withColumn(
            "dropoff_datetime",
            F.to_utc_timestamp(
                F.col("dropoff_datetime"),
                "America/New_York",
            ),
        )
    )

def prepare_raw_taxi_zones(taxi_zones: DataFrame) -> DataFrame:
    require_columns(
        taxi_zones,
        (*TAXI_ZONE_COLUMN_RENAMES, "service_zone"),
        "Raw taxi zones",
    )

    prepared_zones = taxi_zones

    for source_column, target_column in TAXI_ZONE_COLUMN_RENAMES.items():
        prepared_zones = prepared_zones.withColumnRenamed(
            source_column,
            target_column,
        )

    return prepared_zones

def prepare_raw_weather(weather: DataFrame) -> DataFrame:
    return (
        weather
        .drop(*WEATHER_UNUSED_COLUMNS)
        .withColumn(
            "observation_datetime",
            F.make_timestamp(
                F.col("year"),
                F.col("month"),
                F.col("day"),
                F.col("hour"),
                F.lit(0),
                F.lit(0),
            ),
        )
    )

def standardize_column_name(column_name: str) -> str:
    standardized_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name)
    return standardized_name.strip("_").lower()


def prepare_raw_air_quality(air_quality: DataFrame) -> DataFrame:
    prepared_air_quality = air_quality.drop("Uncertainty")

    for source_column, target_column in AIR_QUALITY_COLUMN_RENAMES.items():
        prepared_air_quality = prepared_air_quality.withColumnRenamed(
            source_column,
            target_column,
        )

    for column_name in prepared_air_quality.columns:
        standardized_name = standardize_column_name(column_name)
        prepared_air_quality = prepared_air_quality.withColumnRenamed(
            column_name,
            standardized_name,
        )

    prepared_air_quality = prepared_air_quality.withColumn(
        "observation_datetime",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.col("gmt_date"),
                F.col("gmt_time"),
            ),
            "yyyy-MM-dd HH:mm",
        ),
    )

    prepared_air_quality = prepared_air_quality.withColumns(
        {
            column_name: F.col(column_name).cast(data_type)
            for column_name, data_type in AIR_QUALITY_COLUMN_TYPES.items()
        }
    )

    return prepared_air_quality