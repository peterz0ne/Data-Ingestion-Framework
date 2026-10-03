from helpers import create_spark, get_feat_config_path, load_config
from predictions.runner import (
    load_dataset_split,
    load_training_dataset,
    predict_tip_amounts,
)

def main() -> None:
    config = load_config(get_feat_config_path())
    spark = create_spark("tip-prediction-inference")

    try:
        dataset = load_training_dataset(spark, config)
        new_trips = load_dataset_split(dataset, config, "test")

        predictions = predict_tip_amounts(new_trips, config)

        predictions.select(
            "trip_id",
            "tip_amount",
            "prediction",
        ).show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()