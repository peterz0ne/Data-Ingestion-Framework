import math

from pyspark.sql import DataFrame, SparkSession, functions as F

from analytics.runner import DEFAULT_INTEGRATED_TABLE_PATH


SPLIT_BUCKET_COUNT = 1_000_000


def assign_splits(
    dataframe: DataFrame,
    id_column: str,
    split_column: str,
    train_percentage: float,
    validation_percentage: float,
    seed: int,
) -> DataFrame:
    split_bucket = F.pmod(
        F.xxhash64(F.col(id_column), F.lit(seed)),
        F.lit(SPLIT_BUCKET_COUNT),
    )
    train_boundary = train_percentage * SPLIT_BUCKET_COUNT / 100
    validation_boundary = (
        train_percentage + validation_percentage
    ) * SPLIT_BUCKET_COUNT / 100

    return dataframe.withColumn(
        split_column,
        F.when(split_bucket < train_boundary, F.lit("train"))
        .when(split_bucket < validation_boundary, F.lit("validation"))
        .otherwise(F.lit("test")),
    )


def build_training_set(
    integrated_data: DataFrame,
    target_column: str,
    feature_columns: tuple[str, ...],
    id_column: str,
    split_column: str,
    train_percentage: float,
    validation_percentage: float,
    seed: int = 42,
) -> DataFrame:
    if target_column == id_column:
        raise ValueError("The target cannot be the same as the ID column.")
    if target_column in feature_columns:
        raise ValueError("The target column cannot also be a feature.")
    if not feature_columns:
        raise ValueError("At least one feature column must be provided.")
    if len(set(feature_columns)) != len(feature_columns):
        raise ValueError("Feature columns must be unique.")
    if id_column in feature_columns:
        raise ValueError(f"The {id_column} cannot also be a feature.")
    if id_column not in integrated_data.columns:
        raise ValueError(f"The integrated Gold table must contain {id_column} for stable splits.")

    data = integrated_data

    missing_columns = set((target_column, *feature_columns)) - set(data.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Requested training columns are missing from Gold: {missing}")

    selected_data = data.select(
        *[F.col(column) for column in feature_columns],
        F.col(target_column),
        F.col(id_column),
    )
    split_data = assign_splits(
        selected_data,
        id_column,
        split_column,
        train_percentage,
        validation_percentage,
        seed,
    )
    return split_data


def validate_percentages(
    train_percentage: float,
    validation_percentage: float,
    test_percentage: float,
) -> None:
    percentages = (train_percentage, validation_percentage, test_percentage)
    if any(not math.isfinite(value) or value < 0 for value in percentages):
        raise ValueError("Split percentages must be finite, non-negative numbers.")
    if not math.isclose(sum(percentages), 100, abs_tol=1e-9):
        raise ValueError("Train, validation, and test percentages must sum to 100.")


def generate_training_set(
    spark: SparkSession,
    target_column: str,
    feature_columns: tuple[str, ...],
    id_column: str,
    split_column: str,
    gold_path: str = DEFAULT_INTEGRATED_TABLE_PATH,
    train_percentage: float = 70,
    validation_percentage: float = 15,
    test_percentage: float = 15,
    seed: int = 42,
) -> DataFrame:
    validate_percentages(
        train_percentage,
        validation_percentage,
        test_percentage,
    )
    integrated_data = spark.read.format("delta").load(gold_path)
    training_data = build_training_set(
        integrated_data,
        target_column,
        feature_columns,
        id_column,
        split_column,
        train_percentage,
        validation_percentage,
        seed,
    )
    return training_data
