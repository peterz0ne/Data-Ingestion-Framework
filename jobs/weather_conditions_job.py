from pyspark.sql.types import StructType, StructField, IntegerType, StringType
def load_coco_codes(spark):
    weather_data = [
        (1, "Clear"),
        (2, "Fair"),
        (3, "Cloudy"),
        (4, "Overcast"),
        (5, "Fog"),
        (6, "Freezing Fog"),
        (7, "Light Rain"),
        (8, "Rain"),
        (9, "Heavy Rain"),
        (10, "Freezing Rain"),
        (11, "Heavy Freezing Rain"),
        (12, "Sleet"),
        (13, "Heavy Sleet"),
        (14, "Light Snowfall"),
        (15, "Snowfall"),
        (16, "Heavy Snowfall"),
        (17, "Rain Shower"),
        (18, "Heavy Rain Shower"),
        (19, "Sleet Shower"),
        (20, "Heavy Sleet Shower"),
        (21, "Snow Shower"),
        (22, "Heavy Snow Shower"),
        (23, "Lightning"),
        (24, "Hail"),
        (25, "Thunderstorm"),
        (26, "Heavy Thunderstorm"),
        (27, "Storm")
    ]


    schema = StructType([
        StructField("coco_code", IntegerType(), False),
        StructField("weather_condition", StringType(), False)
    ])

    df = spark.createDataFrame(weather_data, schema)


    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://delta-data/silver/silver_weather_conditions")
