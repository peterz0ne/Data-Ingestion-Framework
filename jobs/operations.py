import sys
import re

from pyspark.sql import functions as F

def rename_columns(df, parameters):
    source_columns = parameters["source"]
    target_columns = parameters["target"]

    if len(source_columns) != len(target_columns):
        print("Rename operation has different numbers of source and target columns.")
        sys.exit(1)

    for source, target in zip(source_columns, target_columns):
        df = df.withColumnRenamed(source, target)

    return df

def standardize_column_names(df):
    for column in df.columns:
        standardized_name = re.sub(r"[^a-zA-Z0-9]+", "_", column)
        standardized_name = standardized_name.strip("_").lower()

        df = df.withColumnRenamed(column, standardized_name)

    return df

def convert_to_utc(df, parameters):
    columns = parameters["columns"]
    source_timezone = parameters["source_timezone"]

    for column in columns:
        df = df.withColumn(
            column,
            F.to_utc_timestamp(F.col(column), source_timezone)
        )

    return df

def create_hourly_datetime(df, parameters):
    return df.withColumn(
        parameters["target_column"],
        F.make_timestamp(
            F.col(parameters["year_column"]),
            F.col(parameters["month_column"]),
            F.col(parameters["day_column"]),
            F.col(parameters["hour_column"]),
            F.lit(0),
            F.lit(0)
        )
    )

def create_timestamp_from_columns(df, parameters):
    timestamp_text = F.concat_ws(
        " ",
        F.col(parameters["date_column"]),
        F.col(parameters["time_column"])
    )

    return df.withColumn(
        parameters["target_column"],
        F.to_timestamp(timestamp_text, parameters["format"])
    )

def cast_columns(df, parameters):
    for column, data_type in parameters["columns"].items():
        df = df.withColumn(column, F.col(column).cast(data_type))

    return df


def generate_primary_key(df, parameters):
    column_name = parameters["column"]
    strategy = parameters.get("strategy", "incremental").lower()
    start_value = parameters.get("start", 1)

    if strategy == "uuid":
        return df.withColumn(column_name, F.uuid())
    elif strategy == "incremental":
        return df.withColumn(
            column_name,
            F.monotonically_increasing_id() + F.lit(start_value)
        )
    else:
        print("Unknown primary key generation strategy.")
        sys.exit(1)

def apply_operations(df, operations):
    for operation in operations:
        action = operation["action"]
        parameters = operation["parameters"]

        if action == "rename_columns":
            df = rename_columns(df, parameters)
        elif action == "standardize_column_names":
            df = standardize_column_names(df)
        elif action == "convert_to_utc":
            df = convert_to_utc(df, parameters)
        elif action == "create_hourly_datetime":
            df = create_hourly_datetime(df, parameters)
        elif action == "create_timestamp_from_columns":
            df = create_timestamp_from_columns(df, parameters)
        elif action == "cast_columns":
            df = cast_columns(df, parameters)
        elif action == "generate_primary_key":
            df = generate_primary_key(df, parameters)
        else:
            print(f"Unknown operation: {action}")
            sys.exit(1)

    return df