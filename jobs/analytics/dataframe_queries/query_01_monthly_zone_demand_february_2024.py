from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.runner import INTEGRATED_TABLE_VIEW

def build_query(spark: SparkSession,table:str=INTEGRATED_TABLE_VIEW) -> DataFrame:
    return (
        spark.table(table)
        .filter("pickup_month = '2024-02'")
        .groupBy("pickup_zone_name", "pickup_month")
        .count()
        .orderBy("pickup_zone_name", "pickup_month")
    )
