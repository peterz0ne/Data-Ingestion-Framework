# Prediction Training Set

Generate a taxi fare training set from the integrated Gold Delta table:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/generate_training_set.py
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