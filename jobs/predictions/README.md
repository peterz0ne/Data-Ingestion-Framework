# Machine-learning workflow

Start the services and run ingestion to create Gold, following [Getting started](../../README.portfolio.md#getting-started). Run commands from the repository root.

## Configuration

[ml_tip_prediction.json](configs/ml_tip_prediction.json) defines the target, features, filters, splits, and model output path. The example predicts credit-card taxi tips with a Random Forest and uses a 70/15/15 train/validation/test split.

## Generate training data

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/training_set_job.py
```

Writes `s3a://delta-data/predictions/gold_ml_tip_training` from Gold.

## Train and evaluate

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/train_tip_model_job.py
```

Fits preprocessing and the model on the training split, reports test MAE/RMSE, and saves the fitted pipeline at the configured path. To retrain, regenerate training data and rerun this command. The saved model is replaced.

## Predict with the saved model

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/predict_tip_model_job.py
```

The demonstration prints trip IDs, actual tips, and predicted tips.

## Optional: export features

Training prepares its own features. To export a separate feature dataset for inspection:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/predictions/feature_extractor_pipeline.py
```

Writes `s3a://delta-data/predictions/feature_training`. Explore it with the [feature inspection notebook](../../notebooks/predictions/inspect_tip_feature_training.ipynb).

## Compare raw and platform workflows

Compare training directly from baseline source files against training from prepared Gold data:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/compare_training_workflow_job.py
```

Timing includes dataset construction, preprocessing, and model fitting; test MAE/RMSE evaluation is excluded. The comparison uses trips before `2025-01-01` and does not overwrite the saved model.

To check dataset equivalence, generate training data as above, then generate the raw dataset:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/raw_training_set_job.py
```

Run the [raw-versus-platform validation notebook](../../notebooks/predictions/validate_raw_vs_platform.ipynb) to compare the baseline records.
