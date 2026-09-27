from metadata import create_run_context
from helpers import create_spark, get_update_config_paths, load_config
from silver_job import silver_job
from bronze_job import bronze_job
from analytics.data_products import materialize_new_data_products
from gold_job import gold_job

def main():
    spark = create_spark("datasets")
    run_context = create_run_context()

    # Process only the incremental update files.
    config_paths = get_update_config_paths()
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS pipeline_execution_dependency (
            execution_id STRING,
            related_execution_id STRING
        )
        USING DELTA
        LOCATION 's3a://delta-data/metadata/pipeline_execution_dependency'
    """)

    try:
        for config_path in config_paths:
            config = load_config(config_path)
            dataset = config["dataset"]
            print(f"Loading dataset: {dataset}")
            bronze_job(spark, config, run_context, mode="append")

        for config_path in config_paths:
            config = load_config(config_path)

            silver_job(spark, config, run_context, incremental=True)

        gold_job(spark, run_context=run_context, incremental=True)
        materialize_new_data_products(spark, run_context)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
