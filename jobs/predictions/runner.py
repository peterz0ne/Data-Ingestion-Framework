import json
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from predictions.evaluation import calculate_regression_metric
from predictions.training import (
    build_regressor,
    build_training_pipeline,
    fit_training_pipeline,
    generate_predictions,
    save_fitted_pipeline,
    select_dataset_split,
)
from pyspark.ml import Pipeline, PipelineModel

TRAINING_SET_PATH_PREFIX = "s3a://delta-data/predictions/"
DEFAULT_CONFIG_PATH = (
    Path(__file__).parent
    / "configs"
    / "ml_tip_prediction.json"
)
REQUIRED_SPLITS = {"train", "validation", "test"}

def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    with config_path.open(encoding="utf-8") as config_file:
        return json.load(config_file)

def load_training_dataset(spark: SparkSession, config: dict) -> DataFrame:
    output_table = config["dataset"]["output_table"]
    output_path = f"{TRAINING_SET_PATH_PREFIX}{output_table}"

    return spark.read.format("delta").load(output_path)

def load_dataset_split(
    dataset: DataFrame,
    config: dict,
    split_name: str,
) -> DataFrame:
    return select_dataset_split(
        dataset,
        config["dataset"]["split_column"],
        split_name,
    )

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("credit_card_tip_prediction")
        .getOrCreate()
    )

def validate_dataset_splits(
    dataset: DataFrame,
    config: dict,
) -> None:
    split_column = config["dataset"]["split_column"]

    available_splits = {
        row[split_column]
        for row in dataset.select(split_column).distinct().collect()
    }
    missing_splits = REQUIRED_SPLITS - available_splits

    if missing_splits:
        raise ValueError(
            f"Training dataset is missing splits: {', '.join(sorted(missing_splits))}."
        )

def prepare_training_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    dataset = load_training_dataset(spark, config)

    validate_dataset_splits(dataset, config)

    return dataset

def evaluate_predictions(
    predictions: DataFrame,
    config: dict,
) -> dict[str, float]:
    model_config = config["model"]

    return {
        "mae": calculate_regression_metric(
            predictions,
            model_config["label_column"],
            model_config["prediction_column"],
            "mae",
        ),
        "rmse": calculate_regression_metric(
            predictions,
            model_config["label_column"],
            model_config["prediction_column"],
            "rmse",
        ),
    }

def fit_tip_prediction_model(
    dataset: DataFrame,
    feature_pipeline: Pipeline,
    config: dict,
) -> PipelineModel:
    training_data = load_dataset_split(dataset, config, "train")

    regressor = build_regressor(config)
    training_pipeline = build_training_pipeline(feature_pipeline, regressor)

    return fit_training_pipeline(training_data, training_pipeline)

def evaluate_model_on_split(
    fitted_pipeline: PipelineModel,
    dataset: DataFrame,
    split_name: str,
    config: dict,
) -> dict[str, float]:
    split_data = load_dataset_split(dataset, config, split_name)
    predictions = generate_predictions(fitted_pipeline, split_data)

    return evaluate_predictions(predictions, config)

def save_tip_prediction_model(
    fitted_pipeline: PipelineModel,
    config: dict,
) -> None:
    save_fitted_pipeline(
        fitted_pipeline,
        config["model"]["output_path"],
    )

def run_tip_prediction_training(
    spark: SparkSession,
    feature_pipeline: Pipeline,
    config: dict,
) -> dict[str, dict[str, float]]:
    dataset = prepare_training_dataset(spark, config)

    fitted_pipeline = fit_tip_prediction_model(
        dataset,
        feature_pipeline,
        config,
    )

    validation_metrics = evaluate_model_on_split(
        fitted_pipeline,
        dataset,
        "validation",
        config,
    )
    test_metrics = evaluate_model_on_split(
        fitted_pipeline,
        dataset,
        "test",
        config,
    )

    save_tip_prediction_model(fitted_pipeline, config)

    return {
        "validation": validation_metrics,
        "test": test_metrics,
    }