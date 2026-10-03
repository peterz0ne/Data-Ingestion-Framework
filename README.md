# Generic Urban Data Integration Platform

## Run

### Prerequisites
Move the initial csv/parquet files inside the following paths:
- /landing/air_quality 
- /landing/taxi 
- /landing/taxi_zone_lookup 
- /landing/weather 

### Running with Docker
To setup the environment install docker and run the following command
```sh
docker compose up --build -d
```
This will create the containers and start the right away.

* To run the pipeline execute the following command (this will generate the gold, silver and bronze tables, as well as the dataproducts):
```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  /opt/spark/jobs/ingest.py
```
* To run the benchmark [notebook](./notebooks/benchmark.ipynb), see the [Jupyter setup guide](./notebooks/setup/README.md).

* To run a specific analytical query, run the the specific notbook in `notebooks/run_queries`.

* To recreate the optimization results, run the notebooks in the `notebooks/optimizations` folder

* To measure the sizes of each table, run the notebook: `notebooks/storage_metrics/storage_overhead.ipynb`

# Incremental Updates and Schema Evolution

First run the complete baseline pipeline:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest.py
```

Before running the update pipeline, download `yellow_tripdata_2025-01.parquet` from the [NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it in `landing/taxi/`.

Then run the three notebooks in [data_generation](./data_generation/) to create the incremental input files:

- `data_generation/taxi_trips/generate_taxi_trips.ipynb`
- `data_generation/air_quality/air_quality.ipynb`
- `data_generation/weather/weather.ipynb`

They generate the extended Taxi Trips, Air Quality, and Weather files in their configured `landing/` paths.

Now run the update pipeline with:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/update.py
```

The update pipeline appends new Bronze records and processes only records ingested since the latest Silver watermark. Schema versions are configured in `configs/`; each Silver execution processes the matching Bronze `schema_version`. The supplied update configurations demonstrate schema evolution through the new `aqi` Air Quality field and `humidity` Weather field.

## Monitoring and Validation Reports

Pipeline execution metrics are stored in the Delta table at `metadata/pipeline_executions`. Run [inspect_pipeline_monitoring.ipynb](./notebooks/monitoring/inspect_pipeline_monitoring.ipynb) to inspect execution duration, processed and rejected records, validation failures, and dataset-level trends.

Invalid records are written to the Delta quarantine table at `metadata/rejected_records`. Run [inspect_rejected_records.ipynb](./notebooks/monitoring/inspect_rejected_records.ipynb) to inspect rejected source records and their rejection reasons. Quarantined records never reach Silver, Gold, or analytical products.

## Reproducing Evaluation Experiments

Run the notebooks in [notebooks/update_evaluation](./notebooks/update_evaluation).

# Machine-Learning Workflow

After starting the containers and running the ingestion pipeline to create the integrated Gold table, run the following workflow.

1. Generate the training dataset:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/training_set_job.py
```

2. Run feature engineering:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/predictions/feature_extractor_pipeline.py
```

3. Train and evaluate the tip-prediction model:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/train_tip_model_job.py
```

This reports test MAE and RMSE and saves the fitted model.

4. Reproduce the raw-versus-platform comparison:

```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/compare_training_workflow_job.py
```

5. Retrain after new data are available by rerunning the training-dataset generation command followed by the model-training command. This replaces the saved model with one fitted on the refreshed data.