from typing import Literal

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame

RegressionMetric = Literal["mae", "rmse"]

def require_columns(dataframe: DataFrame, required_columns: list[str]) -> None:
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}."
        )

# Calculates a requested regression metric by comparing predicted tips with actual tip amounts.
# MAE -> average absolute prediction error (in dollars)
# RMSE -> square-root mean error that penalizes large mistakes more heavily (in dollars)
def calculate_regression_metric(
    predictions: DataFrame,
    label_column: str,
    prediction_column: str,
    metric_name: RegressionMetric,
) -> float:
    supported_metrics = {"mae", "rmse"}
    if metric_name not in supported_metrics:
        raise ValueError(
            f"Unsupported metric '{metric_name}'. "
            f"Choose one of: {', '.join(sorted(supported_metrics))}."
        )
    require_columns(predictions, [label_column, prediction_column])

    evaluator = RegressionEvaluator(
        labelCol=label_column,
        predictionCol=prediction_column,
        metricName=metric_name,
    )

    return evaluator.evaluate(predictions)

def select_prediction_output(
    predictions: DataFrame,
    id_column: str,
    target_column: str,
    prediction_column: str,
) -> DataFrame:
    require_columns(predictions, [id_column, target_column, prediction_column])

    return predictions.select(id_column, target_column, prediction_column)