from pyspark.ml.feature import (
    Imputer,
    StandardScaler,
    OneHotEncoder,
    VectorAssembler
)
from pyspark.sql import functions as F
from pyspark.ml import Pipeline


from helpers import get_feat_config_path,load_config,create_spark

def datetime_transformations(df,columns):
    for col in columns:
         df = (
              df.withColumn(f"{col}_month_sin",F.sin(2 * F.pi() * F.month(F.col(col)) / range))
                .withColumn(f"{col}_day_of_year_sin",F.sin(2 * F.pi() * F.dayofyear(F.col(col))/ range))
                .withColumn(f"{col}_hour_sin",F.sin(2 * F.pi() * F.hour(F.col(col))/ range))
                .withColumn(f"{col}_month",F.sin(2 * F.pi() * F.hour(F.col(col))/ range))
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
        outputCol="__numeric_vector"
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

def categorical_transformations(columns):
    encoder = OneHotEncoder(
        inputCols=columns,
        outputCols=[f"{c}_encoded" for c in columns]
    )
    return encoder#.fit(df).transform(df)

def vector_encoder(columns):
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol="features"
    )
    return assembler#.transform(df)

def split_dataset(df,train,val,test,seed):
    return df.randomSplit(
        [train, val, test],
        seed=seed
    )

# def transform_features(df,config):
#     df = datetime_transformations(df,config["datetime_columns"])  
#     df = numerical_transformations(df,config["numeric_columns"])  
#     df = categorical_transformations(df,config["categorical_columns"])  
#     df = vector_encoder(df,
#                         config["datetime_columns"]
#                         +config["numeric_columns"]
#                         +[f"{c}_encoded" for c in config["categorical_columns"]])
#     return df

def dataset_specific_transformation(df,config):
    df = df.filter(df.payment_type == config['problem']['payment_type_filter'])
    df = df.filter(F.col(config['problem']['target_column']).isNotNull() & F.col(config['problem']['target_column'])>0)
    return df.drop("payment_type","total_amount")
    
def read_dataset(spark,table):
    return spark.sql(f"select * from delta.`s3a://delta-data/gold/{table}`")



def build_feature_pipeline() -> Pipeline:
    config = load_config(get_feat_config_path())
    features = config["features"]

    numeric_columns = features["numeric_columns"]
    categorical_columns = features["categorical_columns"]
    datetime_columns = features["datetime_columns"]

    stages = [
        numerical_transformations(numeric_columns),
        categorical_transformations(categorical_columns),
        vector_encoder(
            [f"{c}_month_sin" for c in datetime_columns]
            +[f"{c}_day_of_year_sin" for c in datetime_columns]
            +[f"{c}_hour_sin" for c in datetime_columns]
            +[f"{c}_month" for c in datetime_columns]
            +[f"{c}_day" for c in datetime_columns]
            +[f"{c}_is_weekend" for c in datetime_columns]
            + ["__numeric_scaled"]
            + [f"{c}_encoded" for c in categorical_columns]
        )
    ]

    return Pipeline(stages=stages)


def build_and_exec_pipeline():
    spark = create_spark("predictions")
    config = load_config(get_feat_config_path())
    df = read_dataset(spark,config["dataset"]['table'])
    df = dataset_specific_transformation(df,config)
    df = build_feature_pipeline().fit(df).transform(df)
    df = df.select([
        c for c in df.columns
        if c == "features" or c==config['problem']['target_column']
    ])
    train,val,test = split_dataset(df,config['dataset']['train_percentage'],config['dataset']['validation_percentage'],config['dataset']['test_percentage'],config['dataset']['split_seed'])
    train = train.withColumn("dataset_split",F.lit("train"))
    val = val.withColumn("dataset_split",F.lit("val"))
    test = test.withColumn("dataset_split",F.lit("test"))
    return train,val,test
