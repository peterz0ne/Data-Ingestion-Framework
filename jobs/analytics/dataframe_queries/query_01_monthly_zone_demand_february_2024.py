from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.runner import INTEGRATED_TABLE_VIEW

def build_query(spark: SparkSession) -> DataFrame:
    return (
        spark.table(INTEGRATED_TABLE_VIEW)
        .withColumn("pickup_month", F.substring("pickup_date", 1, 7))
        .filter("pickup_date >= '2024-02-01' AND pickup_date < '2024-03-01'")
        .groupBy("pickup_zone_name", "pickup_month")
        .count()
        .orderBy("pickup_zone_name", "pickup_month")
    )
