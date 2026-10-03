## Prediction Training Set (Task1)

Generate a taxi trip prediction training dataset set from the integrated Gold Delta table:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  /opt/spark/jobs/training_set_job.py
```
* Note: Make sure the ingestion pipeline has first run in order for the gold table to be generated

The root-level job loads the JSON config and invokes the prediction worker. The target,
feature columns, and split settings are set in
[`configs/ml_tip_prediction.json`](configs/ml_tip_prediction.json). The supplied configuration
uses `tip_amount` as the target, trip_id as the primary key, and sets the train/validation/test percentages to 70/15/15 with seed `42`.
It writes the Delta table to
`s3a://delta-data/predictions/gold_ml_tip_training` (dataset.output_table key).

Set the target, features, split percentages, and seed in the JSON config. The split
percentages must sum to 100. Split assignment is stable for a given `trip_id` and seed;
observed row counts may vary slightly from the requested percentages.

## Feature engineering (Task 2)

After generating `gold_ml_tip_training`, run:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/predictions/feature_extractor_pipeline.py
```

The job reads `gold_ml_tip_training`, filters to credit-card trips with valid non-negative tips, fits feature preprocessing on the `train` split only, and transforms all dataset splits.

It writes the final feature dataset to:

```text
s3a://delta-data/predictions/feature_training
```

The output contains `features`, `tip_amount`, and `dataset_split`.

## Model training and evaluation (Task 3)

Run the complete ML pipeline with:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/train_tip_model_job.py
```

The job reads the Task 1 training dataset, applies the Task 2 feature-engineering pipeline, trains the regression model on the training split, reports test MAE and RMSE, and saves the fitted pipeline.

To retrain when new data is available, regenerate the Task 1 dataset and rerun this command. The stored model is replaced with the newly fitted pipeline.

### Reusing the saved model for inference

After training, the fitted pipeline can predict tips for new raw trip records without retraining:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/predict_tip_model_job.py
```

The demonstration job loads the saved pipeline, applies inference-safe feature preparation, and prints `trip_id`, actual `tip_amount`, and the `prediction` tip.