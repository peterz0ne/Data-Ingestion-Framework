from pyspark.sql import DataFrame, functions as F

WEATHER_FEATURE_COLUMNS = (
    "temp",
    "rhum",
    "prcp",
    "wdir",
    "wspd",
    "pres",
    "cldc",
    "coco",
)

def weather_features_with_suffix(
    weather: DataFrame,
    suffix: str,
) -> DataFrame:
    return weather.select(
        *[
            F.col(column_name).alias(f"{column_name}_{suffix}")
            for column_name in WEATHER_FEATURE_COLUMNS
        ],
        F.col("observation_datetime").alias(
            f"observation_datetime_{suffix}",
        ),
    )

def join_taxi_trips_with_zones(taxi_trips: DataFrame, taxi_zones: DataFrame) -> DataFrame:
    dropoff_zones = taxi_zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone_name"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    pickup_zones = taxi_zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone_name"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    return (
        taxi_trips
        .join(
            dropoff_zones,
            taxi_trips["dropoff_zone_id"]
            == dropoff_zones["dropoff_location_id"],
            "left",
        )
        .join(
            pickup_zones,
            taxi_trips["pickup_zone_id"]
            == pickup_zones["pickup_location_id"],
            "left",
        )
        .drop(
            "pickup_zone_id",
            "dropoff_zone_id",
            "pickup_location_id",
            "dropoff_location_id",
        )
    )

def join_taxi_trips_with_weather(
    taxi_trips: DataFrame,
    weather: DataFrame,
) -> DataFrame:
    pickup_weather = weather_features_with_suffix(weather, "pu")
    dropoff_weather = weather_features_with_suffix(weather, "do")

    trips_with_hours = (
        taxi_trips
        .withColumn(
            "pickup_datetime_hour",
            F.date_trunc("hour", F.col("pickup_datetime")),
        )
        .withColumn(
            "dropoff_datetime_hour",
            F.date_trunc("hour", F.col("dropoff_datetime")),
        )
    )

    return (
        trips_with_hours
        .join(
            pickup_weather,
            trips_with_hours["pickup_datetime_hour"]
            == pickup_weather["observation_datetime_pu"],
            "left",
        )
        .join(
            dropoff_weather,
            trips_with_hours["dropoff_datetime_hour"]
            == dropoff_weather["observation_datetime_do"],
            "left",
        )
        .drop(
            "observation_datetime_pu",
            "observation_datetime_do",
        )
    )

def aggregate_new_york_air_quality(
    air_quality: DataFrame,
) -> DataFrame:
    return (
        air_quality
        .filter(F.col("state_name") == "New York")
        .groupBy("observation_datetime")
        .agg(
            F.avg("sample_measurement").alias("average_value"),
        )
    )

def join_taxi_trips_with_air_quality(
    taxi_trips: DataFrame,
    air_quality: DataFrame,
) -> DataFrame:
    hourly_air_quality = aggregate_new_york_air_quality(air_quality)

    pickup_air_quality = hourly_air_quality.select(
        F.col("observation_datetime").alias("observation_datetime_pu"),
        F.col("average_value").alias("average_value_pu"),
    )

    dropoff_air_quality = hourly_air_quality.select(
        F.col("observation_datetime").alias("observation_datetime_do"),
        F.col("average_value").alias("average_value_do"),
    )

    return (
        taxi_trips
        .join(
            pickup_air_quality,
            taxi_trips["pickup_datetime_hour"]
            == pickup_air_quality["observation_datetime_pu"],
            "left",
        )
        .join(
            dropoff_air_quality,
            taxi_trips["dropoff_datetime_hour"]
            == dropoff_air_quality["observation_datetime_do"],
            "left",
        )
        .drop(
            "observation_datetime_pu",
            "observation_datetime_do",
        )
    )