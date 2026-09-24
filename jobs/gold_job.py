from pyspark.sql import functions as F
from pyspark.sql.functions import col
from silver_job import read_silver
from metadata import record_execution, start_execution
import gc

# the location prefix of gold layer dataset in the object storage
GOLD_LAYER_DATASET_LOCATION = "s3a://delta-data/gold/gold_"

def gold_job(
    spark,
    run_context,
    dataset="integrated_taxi_trips",
    partitions="pickup_month",
    gold_location=GOLD_LAYER_DATASET_LOCATION,
    silver_location=None,
    schema_version="1.0",
    write_metadata=True,
):
    execution = start_execution(
        run_context,
        stage="gold",
        dataset=dataset,
        schema_version=schema_version,
    )

    df = gold_transformation(spark, silver_location=silver_location)
    output_records = df.count()
    
    write_gold(df, dataset, partitions, gold_location)

    if write_metadata:
        record_execution(
            spark,
            execution,
            processed_records=output_records,
            inserted_records=output_records,
            rejected_records=0,
            validation_failure_count=0,
        )

    del df
    gc.collect()


def gold_transformation(
        spark,
        taxi_trips_table="taxi_trips",
        taxi_zones_table="taxi_zones",
        air_quality_table="air_quality",
        weather_table="weather",
        silver_location=None,
    ):
    read_options = {} if silver_location is None else {"silver_location": silver_location}
    taxi_trips = read_silver(spark, taxi_trips_table, **read_options)
    taxi_zones = read_silver(spark, taxi_zones_table, **read_options)
    air_quality = read_silver(spark, air_quality_table, **read_options)
    weather = read_silver(spark, weather_table, **read_options)

    zones_drop = taxi_zones.withColumnsRenamed({
        "location_id": "dropoff_loc_id",
        "borough":"dropoff_borough",
        "service_zone": "dropoff_service_zone",
        "zone": "dropoff_zone_name"
    })
    zones_pick = taxi_zones.withColumnsRenamed({
        "location_id": "pickup_loc_id",
        "borough":"pickup_borough",
        "zone": "pickup_zone_name",
        "service_zone": "pickup_service_zone"
    })

    df = taxi_trips
    print("joining taxi data with zones")
    df = df.join(zones_drop, df["dropoff_zone_id"] == zones_drop["dropoff_loc_id"], "left")
    df = df.join(zones_pick, df["pickup_zone_id"] == zones_pick["pickup_loc_id"], "left")
    df = df.drop("dropoff_zone_id", "pickup_zone_id","pickup_loc_id","dropoff_loc_id")

    weather_pu = weather.select(
        col("temp").alias("temp_pu"),
        col("rhum").alias("rhum_pu"),
        col("prcp").alias("prcp_pu"),
        col("wdir").alias("wdir_pu"),
        col("wspd").alias("wspd_pu"),
        col("pres").alias("pres_pu"),
        col("cldc").alias("cldc_pu"),
        col("observation_datetime").alias("observation_datetime_pu"),
        col("coco").alias("coco_pu")
    )


    weather_do = weather.select(
        col("temp").alias("temp_do"),
        col("rhum").alias("rhum_do"),
        col("prcp").alias("prcp_do"),
        col("wdir").alias("wdir_do"),
        col("wspd").alias("wspd_do"),
        col("pres").alias("pres_do"),
        col("cldc").alias("cldc_do"),
        col("observation_datetime").alias("observation_datetime_do"),
        col("coco").alias("coco_do")
    )

    df = df.withColumn(
        "do_datetime_hour",
        F.date_trunc("hour", F.col("dropoff_datetime"))
    )
    df = df.withColumn(
        "pu_datetime_hour",
        F.date_trunc("hour", F.col("pickup_datetime"))
    )
    print("joining taxi data with weather")

    df = df.join(
        weather_do,
        df["do_datetime_hour"] == weather_do["observation_datetime_do"],
        "left"
    )

    df = df.join(
        weather_pu,
        df["pu_datetime_hour"] == weather_pu["observation_datetime_pu"],
        "left"
    )

    df = df.drop("observation_datetime_pu", "observation_datetime_do")

    print("joining taxi data with air quality")

    # we care only about NY for this analysis
    df_aq = air_quality.filter(air_quality.state_name == "New York")
    df_aq = df_aq.drop("date_local", "time_local", "time_gmt", "date_gmt", "site_num", "state_name", "Latitude", "Longitude", "parameter_code", "state_code", "county_code", "POC", "Datum", "county_name","method_code","date_of_last_change","observation_id")

    df_aq = df_aq.groupBy("observation_datetime").agg(
        F.avg("sample_measurement").alias("average_value"),
        F.avg("MDL").alias("MDL"),
        F.concat_ws(", ", F.collect_list("Qualifier")).alias("Qualifiers"),
        F.max("parameter_name").alias("parameter_name"),
        F.max("units_of_measure").alias("units_of_measure"),
        F.max("method_name").alias("method_name"),
        F.max("method_type").alias("method_type")
    )

    aq_pu = df_aq.select(
        col("average_value").alias("average_value_pu"),
        col("MDL").alias("MDL_pu"),
        col("observation_datetime").alias("observation_datetime_pu"),
        col("Qualifiers").alias("Qualifiers_pu"),
        col("parameter_name").alias("parameter_name_pu"),
        col("units_of_measure").alias("units_of_measure_pu"),
        col("method_name").alias("method_name_pu"),
        col("method_type").alias("method_type_pu")
    )
    aq_do = df_aq.select(
        col("average_value").alias("average_value_do"),
        col("MDL").alias("MDL_do"),
        col("observation_datetime").alias("observation_datetime_do"),
        col("Qualifiers").alias("Qualifiers_do"),
        col("parameter_name").alias("parameter_name_do"),
        col("units_of_measure").alias("units_of_measure_do"),
        col("method_name").alias("method_name_do"),
        col("method_type").alias("method_type_do")
    )
    print("joining taxi data with air quality")

    df = df.join(
        aq_pu,
        df["pu_datetime_hour"] == aq_pu["observation_datetime_pu"],
        "left"
    )

    df = df.join(
        aq_do,
        df["do_datetime_hour"] == aq_do["observation_datetime_do"],
        "left"
    )

    df = df.drop("observation_datetime_pu", "observation_datetime_do")

    df = df.withColumn(
        "date",
        F.to_date("pickup_datetime")
    )

    return df


def write_gold(
    df,
    dataset,
    partitions,
    gold_location=GOLD_LAYER_DATASET_LOCATION,
):
    print("writing gold data")
    target = f"{gold_location}{dataset}"
    df.write.format("delta").option("overwriteSchema", "true").partitionBy(partitions).mode("overwrite").save(target)
    print(f"Written gold Delta table to: {target}")
