from pyspark.ml.feature import (
    Imputer,
    StandardScaler,
    OneHotEncoder,
    VectorAssembler,
    FeatureHasher,
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

def vector_encoder(columns):
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol="features",
        handleInvalid="keep"
    )
    return assembler#.transform(df)

def split_dataset(df,train,val,test,seed):
    return df.randomSplit(
        [float(train), float(val), float(test)],
        seed=seed
    )


def dataset_specific_transformation(df,config):
    df = df.filter(df.payment_type == config['problem']['payment_type_filter'])
    target = F.col(config["problem"]["target_column"])
    df = df.filter(target.isNotNull() & (target >= 0))
    return df

def read_dataset(spark,table):
    return spark.sql(f"select * from delta.`s3a://delta-data/predictions/{table}`")



def build_feature_pipeline(config) -> Pipeline:
    features = config["features"]

    numeric_columns = features["numeric_columns"]
    ordinal_columns = features["ordinal_cols"]
    onehot_columns = features["onehot_cols"]
    datetime_columns = features["datetime_columns"]

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
            + [f"{c}_encoded" for c in onehot_columns]
        )
    ]

    return Pipeline(stages=stages)

def split_based_on_cardinality(df,config,limit=10):
    categorical_cols = config['features']['categorical_columns']
    cardinality = {
        c: df.select(c).distinct().count()
        for c in categorical_cols
    }
    onehot_cols = [
        c for c in categorical_cols
        if cardinality[c] <= limit
    ]

    ordinal_cols = [
        c for c in categorical_cols
        if cardinality[c] > limit
    ]
    config['features']['ordinal_cols']=ordinal_cols
    config['features']['onehot_cols']=onehot_cols
    return config


def update_config(config):
    for c in config['features']['datetime_columns']:
        config['features']['categorical_columns'].append(f"{c}_month")
        config['features']['categorical_columns'].append(f"{c}_day")
    return config

def write_to_table(df,table_path):
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(f"s3a://delta-data/predictions/{table_path}")


def create_feature_dataset(export=False):
    spark = create_spark("predictions")
    config = load_config(get_feat_config_path())
    df = read_dataset(spark,config["dataset"]['output_table'])
    df = dataset_specific_transformation(df,config)
    df = datetime_transformations(df,config["features"]["datetime_columns"])

    df_train = df.filter(F.col(config['dataset']['split_column']) == "train")
    config = update_config(config)
    config = split_based_on_cardinality(df,config)
    # fit the pipeline on train data and transform the whole dataset (all splits) to avoid data leakage
    # we want to use train's statistics to transform the whole dataset
    model = build_feature_pipeline(config).fit(df_train)

    df = model.transform(df)
    df = df.select([
        c for c in df.columns
        if c == "features" or c==config['problem']['target_column'] or c==config['dataset']['split_column']
    ])

    if(export):
        write_to_table(df,config['dataset']['feature_table'])
    return df


if __name__ == "__main__":
    create_feature_dataset(True)


# split is already done
# transformations, scaling should be done on each split instead of whole dataset
# delete payment type later