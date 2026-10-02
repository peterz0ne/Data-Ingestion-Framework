from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

TAXI_REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
)

TAXI_ALLOWED_VALUES = {
    "VendorID": (1, 2, 6, 7),
    "RatecodeID": (1, 2, 3, 4, 5, 6, 99),
    "store_and_fwd_flag": ("Y", "N"),
    "payment_type": (0, 1, 2, 3, 4, 5, 6),
}

TAXI_NON_NEGATIVE_COLUMNS = (
    "passenger_count",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "trip_distance",
)

WEATHER_PRIMARY_KEY = (
    "year",
    "month",
    "day",
    "hour",
)

WEATHER_ALLOWED_VALUES = {
    "month": tuple(range(1, 13)),
    "day": tuple(range(1, 32)),
    "hour": tuple(range(24)),
    "coco": tuple(range(1, 28)),
}

WEATHER_NON_NEGATIVE_COLUMNS = (
    "year",
    "prcp",
    "wspd",
    "pres",
)

WEATHER_NUMERIC_RANGES = {
    "rhum": (0, 100),
    "wdir": (0, 360),
    "cldc": (0, 8),
}

AIR_QUALITY_REQUIRED_COLUMNS = (
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
)

def validate_raw_taxi_zones(taxi_zones: DataFrame) -> DataFrame:
    duplicate_window = (
        Window
        .partitionBy("LocationID")
        .orderBy(F.monotonically_increasing_id())
    )

    return (
        taxi_zones
        .withColumn(
            "_location_id_duplicate_number",
            F.row_number().over(duplicate_window),
        )
        .filter(
            F.col("LocationID").isNotNull()
            & F.col("Zone").isNotNull()
            & ~F.col("Zone").isin("N/A")
            & (F.col("_location_id_duplicate_number") == 1)
        )
        .drop("_location_id_duplicate_number")
    )

def validate_raw_taxi_trips(
    taxi_trips: DataFrame,
    valid_taxi_zones: DataFrame,
) -> DataFrame:
    pickup_zone_ids = valid_taxi_zones.select(
        F.col("LocationID").alias("pickup_location_id"),
    ).dropDuplicates()

    dropoff_zone_ids = valid_taxi_zones.select(
        F.col("LocationID").alias("dropoff_location_id"),
    ).dropDuplicates()

    unique_trips = taxi_trips.dropDuplicates()

    valid_trips = (
        unique_trips
        .join(
            pickup_zone_ids,
            unique_trips["PULocationID"]
            == pickup_zone_ids["pickup_location_id"],
            "left_semi",
        )
        .join(
            dropoff_zone_ids,
            unique_trips["DOLocationID"]
            == dropoff_zone_ids["dropoff_location_id"],
            "left_semi",
        )
    )

    valid_conditions = [
        *[
            F.col(column_name).isNotNull()
            for column_name in TAXI_REQUIRED_COLUMNS
        ],
        *[
            F.col(column_name).isNull()
            | F.col(column_name).isin(*allowed_values)
            for column_name, allowed_values in TAXI_ALLOWED_VALUES.items()
        ],
        *[
            F.col(column_name).isNull()
            | (F.col(column_name) >= 0)
            for column_name in TAXI_NON_NEGATIVE_COLUMNS
        ],
    ]

    valid_condition = F.lit(True)
    for condition in valid_conditions:
        valid_condition = valid_condition & condition

    return valid_trips.filter(valid_condition)

def validate_raw_weather(weather: DataFrame) -> DataFrame:
    primary_key_window = (
        Window
        .partitionBy(*WEATHER_PRIMARY_KEY)
        .orderBy(F.monotonically_increasing_id())
    )

    validated_weather = weather.withColumn(
        "_weather_duplicate_number",
        F.row_number().over(primary_key_window),
    )

    valid_conditions = [
        F.col("_weather_duplicate_number") == 1,
        *[
            F.col(column_name).isNotNull()
            for column_name in WEATHER_PRIMARY_KEY
        ],
        *[
            F.col(column_name).isNull()
            | F.col(column_name).isin(*allowed_values)
            for column_name, allowed_values in WEATHER_ALLOWED_VALUES.items()
        ],
        *[
            F.col(column_name).isNull()
            | (F.col(column_name) >= 0)
            for column_name in WEATHER_NON_NEGATIVE_COLUMNS
        ],
        *[
            F.col(column_name).isNull()
            | F.col(column_name).between(minimum, maximum)
            for column_name, (minimum, maximum)
            in WEATHER_NUMERIC_RANGES.items()
        ],
    ]

    valid_condition = F.lit(True)
    for condition in valid_conditions:
        valid_condition = valid_condition & condition

    return (
        validated_weather
        .filter(valid_condition)
        .drop("_weather_duplicate_number")
    )

def validate_raw_air_quality(air_quality: DataFrame) -> DataFrame:
    valid_condition = F.lit(True)

    for column_name in AIR_QUALITY_REQUIRED_COLUMNS:
        valid_condition = valid_condition & F.col(column_name).isNotNull()

    return (
        air_quality
        .dropDuplicates()
        .filter(valid_condition)
    )

def validate_raw_schema(
    dataframe: DataFrame,
    expected_schema: dict[str, str],
    dataset_name: str,
) -> DataFrame:
    actual_schema = dict(dataframe.dtypes)
    errors = []

    for column_name, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column_name)

        if actual_type is None:
            errors.append(f"Missing required column: {column_name}.")
        elif actual_type != expected_type:
            errors.append(
                f"Column '{column_name}' has type "
                f"'{actual_type}', expected '{expected_type}'."
            )

    unexpected_columns = [
        column_name
        for column_name in dataframe.columns
        if column_name not in expected_schema
    ]

    if unexpected_columns:
        populated_columns = dataframe.agg(
            *[
                F.count(F.col(column_name)).alias(column_name)
                for column_name in unexpected_columns
            ]
        ).first()

        if populated_columns is None:
            raise RuntimeError(
                f"Could not inspect unexpected columns for {dataset_name}."
            )

        for column_name in unexpected_columns:
            if populated_columns[column_name] > 0:
                errors.append(
                    f"Unsupported populated column: {column_name}."
                )

    if errors:
        joined_errors = " ".join(errors)
        raise ValueError(
            f"{dataset_name} failed schema validation: {joined_errors}"
        )

    return dataframe