from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame, functions as F
from pyspark.ml import Pipeline, PipelineModel

def build_regressor(config: dict) -> RandomForestRegressor:
    model_config = config["model"]

    return RandomForestRegressor(
        featuresCol=model_config["features_column"],
        labelCol=model_config["label_column"],
        predictionCol=model_config["prediction_column"],
        maxBins=model_config["max_bins"],
        seed=config["dataset"]["split_seed"],
    )

def select_dataset_split(
    dataset: DataFrame,
    split_column: str,
    split_name: str,
) -> DataFrame:
    return dataset.filter(F.col(split_column) == split_name)

def build_training_pipeline(
    feature_pipeline: Pipeline,
    regressor: RandomForestRegressor,
) -> Pipeline:
    return Pipeline(
        stages=[*feature_pipeline.getStages(), regressor],
    )

def fit_training_pipeline(
    training_data: DataFrame,
    pipeline: Pipeline,
) -> PipelineModel:
    return pipeline.fit(training_data)

def generate_predictions(
    fitted_pipeline: PipelineModel,
    dataset: DataFrame,
) -> DataFrame:
    return fitted_pipeline.transform(dataset)

def save_fitted_pipeline(
    fitted_pipeline: PipelineModel,
    output_path: str,
) -> None:
    fitted_pipeline.write().overwrite().save(output_path)

def load_fitted_pipeline(
    model_path: str,
) -> PipelineModel:
    return PipelineModel.load(model_path)