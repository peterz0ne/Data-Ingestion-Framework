SILVER_LAYER_DATASET_LOCATION = "s3a://delta-data/silver/silver_"

def write_silver(
    df,
    config,
    mode="overwrite",
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    target = f"{silver_location}{config['dataset']}"
    if "partitions" in config and config["partitions"]:
        (
            df.write.format("delta")
            .partitionBy(config["partitions"])
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target)
        )
    else:
        (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target)
        )

    print(f"Written silver Delta table to: {target}")

def read_silver(
    spark,
    dataset,
    silver_location=SILVER_LAYER_DATASET_LOCATION,
):
    return spark.read.format("delta").load(f"{silver_location}{dataset}")

def load_reference_datasets(
    spark,
    quality_checks,
    silver_location,
):
    reference_datasets = {}

    for check in quality_checks:
        if check["action"] != "validate_reference_exists":
            continue

        reference_dataset = check["parameters"]["reference_dataset"]

        if reference_dataset not in reference_datasets:
            reference_datasets[reference_dataset] = read_silver(
                spark,
                reference_dataset,
                silver_location,
            )

    return reference_datasets
