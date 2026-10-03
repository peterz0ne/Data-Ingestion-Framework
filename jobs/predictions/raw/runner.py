from pyspark.sql import DataFrame, SparkSession, functions as F
from helpers import load_config
from predictions.raw.operations import (
    load_raw_sources,
    prepare_raw_air_quality,
    prepare_raw_taxi_trips,
    prepare_raw_taxi_zones,
    prepare_raw_weather,
    project_root,
)
from predictions.raw.validation import (
    validate_raw_air_quality,
    validate_raw_schema,
    validate_raw_taxi_trips,
    validate_raw_taxi_zones,
    validate_raw_weather,
)
from predictions.raw.integration import (
    join_taxi_trips_with_air_quality,
    join_taxi_trips_with_weather,
    join_taxi_trips_with_zones,
)
from predictions.generate_training_set import (
    build_training_set,
    validate_percentages,
)

def load_and_prepare_raw_sources(
    spark: SparkSession,
    raw_source_configs: dict[str, str],
) -> dict[str, DataFrame]:
    print("[raw-training] 2.1/4 Loading raw source files")
    raw_sources = load_raw_sources(spark, raw_source_configs)

    print("[raw-training] 2.2/4 Validating source schemas")
    dataset_configs = {
        dataset_name: load_config(project_root() / config_path)
        for dataset_name, config_path in raw_source_configs.items()
    }

    schema_valid_sources = {
        dataset_name: validate_raw_schema(
            raw_sources[dataset_name],
            dataset_configs[dataset_name]["schema"],
            dataset_name,
        )
        for dataset_name in raw_sources
    }

    print("[raw-training] 2.3/4 Validating Taxi Zones")
    valid_taxi_zones = validate_raw_taxi_zones(
        schema_valid_sources["taxi_zones"],
    )

    print("[raw-training] 2.4/4 Validating Taxi Trips")
    valid_taxi_trips = validate_raw_taxi_trips(
        schema_valid_sources["taxi_trips"],
        valid_taxi_zones,
    )

    print("[raw-training] 2.5/4 Validating Weather")
    valid_weather = validate_raw_weather(
        schema_valid_sources["weather"],
    )

    print("[raw-training] 2.6/4 Validating Air Quality")
    valid_air_quality = validate_raw_air_quality(
        schema_valid_sources["air_quality"],
    )

    print("[raw-training] 2.7/4 Applying source-specific transformations")
    return {
        "taxi_trips": prepare_raw_taxi_trips(valid_taxi_trips),
        "taxi_zones": prepare_raw_taxi_zones(valid_taxi_zones),
        "weather": prepare_raw_weather(valid_weather),
        "air_quality": prepare_raw_air_quality(valid_air_quality),
    }

def build_raw_integrated_dataset(
    prepared_sources: dict[str, DataFrame],
) -> DataFrame:
    taxi_trips_with_zones = join_taxi_trips_with_zones(
        prepared_sources["taxi_trips"],
        prepared_sources["taxi_zones"],
    )

    taxi_trips_with_weather = join_taxi_trips_with_weather(
        taxi_trips_with_zones,
        prepared_sources["weather"],
    )

    return join_taxi_trips_with_air_quality(
        taxi_trips_with_weather,
        prepared_sources["air_quality"],
    )

def build_raw_training_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    print("[raw-training] 1/4 Validating split configuration")
    dataset_config = config["dataset"]
    feature_config = config["features"]

    validate_percentages(
        dataset_config["train_percentage"],
        dataset_config["validation_percentage"],
        dataset_config["test_percentage"],
    )

    print("[raw-training] 2/4 Loading, schema-validating, quality-validating, and preparing raw sources")
    prepared_sources = load_and_prepare_raw_sources(
        spark,
        config["comparison"]["raw_source_configs"],
    )

    print("[raw-training] 3/4 Integrating Taxi, Zones, Weather, and Air Quality")
    integrated_data = build_raw_integrated_dataset(prepared_sources)

    print("[raw-training] 4/4 Creating temporary trip ID and assigning train/validation/test splits")
    integrated_data = integrated_data.withColumn(
        dataset_config["id_column"],
        F.monotonically_increasing_id() + F.lit(1),
    )

    feature_columns = tuple(
        feature_config["numeric_columns"]
        + feature_config["categorical_columns"]
        + feature_config["datetime_columns"]
    )

    return build_training_set(
        integrated_data=integrated_data,
        target_column=config["problem"]["target_column"],
        feature_columns=feature_columns,
        id_column=dataset_config["id_column"],
        split_column=dataset_config["split_column"],
        train_percentage=dataset_config["train_percentage"],
        validation_percentage=dataset_config["validation_percentage"],
        seed=dataset_config["split_seed"],
    )