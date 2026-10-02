from pathlib import Path

from helpers import create_spark, load_config
from predictions.raw.runner import build_raw_training_dataset

TRAINING_SET_PATH_PREFIX = "s3a://delta-data/predictions/"
CONFIG_PATH = Path(__file__).parent / "predictions" / "configs" / "ml_tip_prediction.json"

def write_training_set(training_data, output_table_name, partitions) -> None:
    output_path = f"{TRAINING_SET_PATH_PREFIX}{output_table_name}"
    (
        training_data.write.format("delta")
        .mode("overwrite")
        .partitionBy(*partitions)
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    print(f"Wrote training set to {output_path}")


def main() -> None:
    config = load_config(CONFIG_PATH)
    spark = create_spark("raw-taxi-tip-training-set")

    try:
        training_data = build_raw_training_dataset(spark, config)

        write_training_set(
            training_data,
            config["comparison"]["raw_output_table"],
            config["dataset"].get("partitions", []),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()