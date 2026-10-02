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
                .withColumn(f"{col}_day_of_year_sin",F.sin(2 * F.pi() * F.dayofyear(F.col(col))/ 365))
                .withColumn(f"{col}_hour_sin",F.sin(2 * F.pi() * F.hour(F.col(col))/ 24))
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
    # encoder = OneHotEncoder(
    #     inputCols=columns,
    #     outputCols=[f"{c}_encoded" for c in columns]
    # )
    # return encoder#.fit(df).transform(df)

def vector_encoder(columns):
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol="features"
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
    df = df.filter(target.isNotNull() & (target > 0))
    return df.drop("total_amount")
    
def read_dataset(spark,table):
    return spark.sql(f"select * from delta.`s3a://delta-data/predictions/{table}`")



def build_feature_pipeline(config) -> Pipeline:
    features = config["features"]

    numeric_columns = features["numeric_columns"]
    categorical_columns = features["categorical_columns"]
    datetime_columns = features["datetime_columns"]

    stages = [
        *numerical_transformations(numeric_columns),
        *categorical_transformations(categorical_columns),
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


def write_to_table(df,table_path):
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(f"s3a://delta-data/predictions/{table_path}")


def create_feature_dataset(export=False):
    spark = create_spark("predictions")
    config = load_config(get_feat_config_path())
    df = read_dataset(spark,config["dataset"]['output_table'])
    df = dataset_specific_transformation(df,config)
    df = datetime_transformations(df,config["features"]["datetime_columns"])


    df_train = df.filter(F.col(config['dataset']['split_column']) == "train")
    df_test = df.filter(F.col(config['dataset']['split_column'])  == "test")
    df_val = df.filter(F.col(config['dataset']['split_column'])  == "validation")

    df_train = build_feature_pipeline(config).fit(df_train).transform(df_train)
    df_test = build_feature_pipeline(config).fit(df_test).transform(df_test)
    df_val = build_feature_pipeline(config).fit(df_val).transform(df_val)

    df = df_train.union(df_val).union(df_test)

    df = df.select([
        c for c in df.columns
        if c == "features" or c==config['problem']['target_column'] or c==config['dataset']['split_column']
    ])
    # train,val,test = split_dataset(df,config['dataset']['train_percentage'],config['dataset']['validation_percentage'],config['dataset']['test_percentage'],config['dataset']['split_seed'])
    # train = train.withColumn("dataset_split",F.lit("train"))
    # val = val.withColumn("dataset_split",F.lit("val"))
    # test = test.withColumn("dataset_split",F.lit("test"))
    # df_union = train.union(val).union(test)
    # df_union.show()
    if(export):
        write_to_table(df,config['dataset']['feature_table'])
    return df


# if __name__ == "__main__":
#     create_feature_dataset(True)


# split is already done
# transformations, scaling should be done on each split instead of whole dataset
# delete payment type later