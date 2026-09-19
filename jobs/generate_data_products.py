from analytics.data_products import materialize_all_data_products
from helpers import create_spark

def main() -> None:
    spark = create_spark("analytical-data-products")
    try:
        materialize_all_data_products(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
