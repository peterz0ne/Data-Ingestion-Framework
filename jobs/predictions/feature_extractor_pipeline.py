from pyspark.ml.feature import (
    Imputer,
    StandardScaler,
    OneHotEncoder,
    VectorAssembler,
    StringIndexer
)
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
import sys
sys.path.append("/opt/spark/jobs")

from helpers import get_feat_config_path,load_config,create_spark

def datetime_transformations(df,columns):
    for col in columns:
         df = (
              df.withColumn(f"{col}_month_sin",F.sin(2 * F.pi() * F.month(F.col(col)) / 12))
                .withColumn(f"{col}_month_cos",F.cos(2 * F.pi() * F.month(F.col(col)) / 12))
                .withColumn(f"{col}_day_of_year_sin",F.sin(2 * F.pi() * F.dayofyear(F.col(col))/ 365))
                .withColumn(f"{col}_day_of_year_cos",F.cos(2 * F.pi() * F.dayofyear(F.col(col))/ 365))
                .withColumn(f"{col}_hour_sin",F.sin(2 * F.pi() * F.hour(F.col(col))/ 24))
                .withColumn(f"{col}_hour_cos",F.cos(2 * F.pi() * F.hour(F.col(col))/ 24))
                .withColumn(f"{col}_month",F.month(F.col(col)))
                .withColumn(f"{col}_day",F.day(F.col(col)))
                .withColumn(f"{col}_is_weekend",F.when(F.dayofweek(F.col(col)).isin(1, 7), 1).otherwise(0))
              )
    return df

def numerical_transformations(columns):

    imputer = Imputer(
        inputCols=columns,
        outputCols=columns,
        strategy="median"
    )

    numeric_vector = VectorAssembler(
        inputCols=columns,
        outputCol="__numeric_vector",
        handleInvalid="keep"
    )

    scaler = StandardScaler(
        inputCol="__numeric_vector",
        outputCol="__numeric_scaled",
        withMean=True,
        withStd=True
    )

    return [
        imputer,
        numeric_vector,
        scaler
    ]

def categorical_one_hot_transformations(columns):
    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_index",
            handleInvalid="keep"
        )
        for c in columns
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_index" for c in columns],
        outputCols=[f"{c}_encoded" for c in columns]
    )

    return indexers + [encoder]

def categorical_ordinal_transformations(columns):
    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_encoded",
            handleInvalid="keep"
        )
        for c in columns
    ]
    return indexers

def vector_encoder(columns, output_column):
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol=output_column,
        handleInvalid="keep"
    )
    return assembler

def apply_filters(df, filters):
    for filter_config in filters:
        column = F.col(filter_config["column"])
        filter_type = filter_config["filter_type"]

        if filter_type == "ge":
            df = df.filter(column >= filter_config["value"])
        elif filter_type == "eq":
            df = df.filter(column == filter_config["value"])
        elif filter_type == "le":
            df = df.filter(column <= filter_config["value"])
        elif filter_type == "ne":
            df = df.filter(column != filter_config["value"])
        elif filter_type == "nulls":
            df = df.filter(column.isNotNull())

    return df

def prepare_inference_dataframe(df, config):
    df = apply_filters(
        df,
        config["dataset"]["inference_filters"],
    )

    return datetime_transformations(
        df,
        config["features"]["datetime_columns"],
    )

def prepare_training_dataframe(df, config):
    df = prepare_inference_dataframe(df, config)

    return apply_filters(
        df,
        config["dataset"]["training_filters"],
    )

def read_dataset(spark,table):
    return spark.sql(f"select * from delta.`s3a://delta-data/predictions/{table}`")

def build_feature_pipeline(config: dict) -> Pipeline:
    features = config["features"]
    numeric_columns = features["numeric_columns"]
    datetime_columns = features["datetime_columns"]
    onehot_columns = features["onehot_columns"]
    ordinal_columns = features["ordinal_columns"]

    stages = [
        *numerical_transformations(numeric_columns),
        *categorical_one_hot_transformations(onehot_columns),
        *categorical_ordinal_transformations(ordinal_columns),
        vector_encoder(
            [f"{c}_month_sin" for c in datetime_columns]
            +[f"{c}_month_cos" for c in datetime_columns]
            +[f"{c}_day_of_year_sin" for c in datetime_columns]
            +[f"{c}_day_of_year_cos" for c in datetime_columns]
            +[f"{c}_hour_sin" for c in datetime_columns]
            +[f"{c}_hour_cos" for c in datetime_columns]
            +[f"{c}_is_weekend" for c in datetime_columns]
            + ["__numeric_scaled"]
            + [f"{c}_encoded" for c in ordinal_columns]
            + [f"{c}_encoded" for c in onehot_columns],
            config["model"]["features_column"],
        )
    ]

    return Pipeline(stages=stages)

def write_to_table(df,table_path):
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(f"s3a://delta-data/predictions/{table_path}")

def create_feature_dataset(export=False):
    spark = create_spark("predictions")
    config = load_config(get_feat_config_path())
    df = read_dataset(spark,config["dataset"]['output_table'])
    df = prepare_training_dataframe(df, config)

    # fit the pipeline on train data and transform the whole dataset (all splits) to avoid data leakage
    # we want to use train's statistics to transform the whole dataset
    df_train = df.filter(F.col(config['dataset']['split_column']) == "train")
    model = build_feature_pipeline(config).fit(df_train)

    df = model.transform(df)
    df = df.select(
        config["model"]["features_column"],
        config["problem"]["target_column"],
        config["dataset"]["split_column"],
    )

    if(export):
        write_to_table(df,config['dataset']['feature_table'])
    return df

if __name__ == "__main__":
    create_feature_dataset(True)
