from pyspark.sql import DataFrame, SparkSession, functions as F
from helpers import create_spark, get_feat_config_path, load_config
from predictions.generate_training_set import generate_training_set
from copy import deepcopy
from time import perf_counter
from predictions.runner import (
    evaluate_model_on_split,
    prepare_and_fit_tip_prediction_model,
)
from predictions.raw.runner import build_raw_training_dataset

def get_feature_columns(config: dict) -> tuple[str, ...]:
    feature_config = config["features"]

    return tuple(
        feature_config["numeric_columns"]
        + feature_config["categorical_columns"]
        + feature_config["datetime_columns"]
    )

def load_platform_baseline_training_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    dataset_config = config["dataset"]

    return generate_training_set(
        spark=spark,
        target_column=config["problem"]["target_column"],
        feature_columns=get_feature_columns(config),
        id_column=dataset_config["id_column"],
        split_column=dataset_config["split_column"],
        train_percentage=dataset_config["train_percentage"],
        validation_percentage=dataset_config["validation_percentage"],
        test_percentage=dataset_config["test_percentage"],
        seed=dataset_config["split_seed"],
    ).filter(
        F.col("pickup_datetime")
        < F.lit(config["comparison"]["baseline_end"])
    )

def run_training_experiment(
    workflow_name: str,
    dataset_builder,
    spark: SparkSession,
    config: dict,
) -> dict[str, object]:
    workflow_config = deepcopy(config)

    start_time = perf_counter()

    dataset = dataset_builder(spark, workflow_config)
    fitted_pipeline, prepared_dataset = prepare_and_fit_tip_prediction_model(dataset, workflow_config)

    training_seconds = perf_counter() - start_time

    test_metrics = evaluate_model_on_split(
        fitted_pipeline,
        prepared_dataset,
        "test",
        workflow_config,
    )

    return {
        "workflow": workflow_name,
        "training_seconds": training_seconds,
        "test_mae": test_metrics["mae"],
        "test_rmse": test_metrics["rmse"],
    }

def print_experiment_result(result: dict[str, object]) -> None:
    print(f"\n{result['workflow']}")
    print(f"End-to-end training time through model fit: {result['training_seconds']:.2f} seconds")
    print(f"Test MAE: {result['test_mae']:.4f}")
    print(f"Test RMSE: {result['test_rmse']:.4f}")


def main() -> None:
    config = load_config(get_feat_config_path())
    spark = create_spark("compare-raw-and-platform-training", executor_memory="3g")

    try:
        raw_result = run_training_experiment(
            "Raw workflow",
            build_raw_training_dataset,
            spark,
            config,
        )
        print_experiment_result(raw_result)

        platform_result = run_training_experiment(
            "Platform workflow",
            load_platform_baseline_training_dataset,
            spark,
            config,
        )
        print_experiment_result(platform_result)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()