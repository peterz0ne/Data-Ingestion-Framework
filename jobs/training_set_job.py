from pathlib import Path

from helpers import create_spark, load_config
from predictions.generate_training_set import (
    # DEFAULT_TRAINING_SET_PATH,
    generate_training_set,
)


DEFAULT_TRAINING_PATH_PREFIX = "s3a://delta-data/predictions/"
CONFIG_PATH = Path(__file__).parent / "predictions" / "configs" / "ml_tip_prediction.json"


def write_training_set(training_data, output_table_name, partitions) -> None:
    output_path = f"{DEFAULT_TRAINING_PATH_PREFIX}{output_table_name}"
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
    spark = create_spark("taxi-fare-training-set")
    try:
        training_data = generate_training_set(
            spark,
            target_column=config["problem"]["target_column"],
            feature_columns=tuple(config["features"]["numeric_columns"] + config["features"]["categorical_columns"] + config["features"]["datetime_columns"]),
            id_column=config["dataset"]["id_column"],
            split_column=config["dataset"]["split_column"],
            train_percentage=config["dataset"]["train_percentage"],
            validation_percentage=config["dataset"]["validation_percentage"],
            test_percentage=config["dataset"]["test_percentage"],
            seed=config["dataset"]["split_seed"],
        )
        write_training_set(training_data, config["dataset"]["output_table"], config["dataset"].get("partitions", []))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
