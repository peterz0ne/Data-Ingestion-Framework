from predictions.feature_extractor_pipeline import prepare_inference_dataframe
from predictions.training import load_fitted_pipeline
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
from pyspark.ml import PipelineModel
from predictions.feature_extractor_pipeline import (
    build_feature_pipeline,
    prepare_training_dataframe,
)

TRAINING_SET_PATH_PREFIX = "s3a://delta-data/predictions/"

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
    config: dict,
) -> PipelineModel:
    training_data = load_dataset_split(dataset, config, "train")

    print("[model-training] Building feature pipeline")
    feature_pipeline = build_feature_pipeline(config)

    print("[model-training] Fitting feature pipeline and Random Forest")
    regressor = build_regressor(config)
    training_pipeline = build_training_pipeline(feature_pipeline, regressor)

    fitted_pipeline = fit_training_pipeline(training_data, training_pipeline)

    print("[model-training] Model fitting complete")
    return fitted_pipeline

def evaluate_model_on_split(
    fitted_pipeline: PipelineModel,
    dataset: DataFrame,
    split_name: str,
    config: dict,
) -> dict[str, float]:
    print(f"[model-evaluation] Evaluating {split_name} split")
    split_data = load_dataset_split(dataset, config, split_name)
    predictions = generate_predictions(fitted_pipeline, split_data)

    metrics = evaluate_predictions(predictions, config)

    print(f"[model-evaluation] {split_name} evaluation complete")
    return metrics

def predict_tip_amounts(
    new_trips: DataFrame,
    config: dict,
) -> DataFrame:
    fitted_pipeline = load_fitted_pipeline(config["model"]["output_path"])
    prepared_trips = prepare_inference_dataframe(new_trips, config)

    return fitted_pipeline.transform(prepared_trips)

def prepare_and_fit_tip_prediction_model(
    dataset: DataFrame,
    config: dict,
) -> tuple[PipelineModel, DataFrame]:
    prepared_dataset = prepare_training_dataframe(dataset, config)

    fitted_pipeline = fit_tip_prediction_model(prepared_dataset, config)

    return fitted_pipeline, prepared_dataset

def run_tip_prediction_training(
    spark: SparkSession,
    config: dict,
) -> dict[str, float]:
    dataset = load_training_dataset(spark, config)
    fitted_pipeline, prepared_dataset = prepare_and_fit_tip_prediction_model(dataset, config)

    test_metrics = evaluate_model_on_split(
        fitted_pipeline,
        prepared_dataset,
        "test",
        config,
    )

    save_fitted_pipeline(fitted_pipeline, config["model"]["output_path"])

    return test_metrics
