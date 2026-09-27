from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.runner import INTEGRATED_TABLE_VIEW

def build_query(spark: SparkSession,table:str=INTEGRATED_TABLE_VIEW) -> DataFrame:
    return (
        spark.table(table)
        .withColumn("pickup_month", F.substring("pickup_date", 1, 7))
        .groupBy("pickup_month")
        .count()
        .orderBy("pickup_month")
    )
