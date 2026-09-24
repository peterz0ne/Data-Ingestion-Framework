from analytics.data_products import materialize_all_data_products
from helpers import create_spark
from metadata import create_run_context

def main() -> None:
    spark = create_spark("analytical-data-products")
    run_context = create_run_context()
    try:
        materialize_all_data_products(spark, run_context)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
