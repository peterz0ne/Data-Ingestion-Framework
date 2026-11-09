# Incremental updates and schema evolution

Start the services and prepare baseline inputs as described in [Getting started](../README.portfolio.md#getting-started). Run shell commands from the repository root.

First run the complete baseline pipeline:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest.py
```

Before running the update pipeline, download `yellow_tripdata_2025-01.parquet` from the [NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it in `landing/taxi/`.

Then run the three notebooks in [data_generation](../data_generation/) to create the incremental input files (check [Setup](../notebooks/setup/README.md) instructions to connect your notebook to the jupyter server):

- [Taxi trips](../data_generation/taxi_trips/generate_taxi_trips.ipynb)
- [Air quality](../data_generation/air_quality/generate_air_quality_2025.ipynb)
- [Weather](../data_generation/weather/weather.ipynb)

They generate the extended Taxi Trips, Air Quality, and Weather files in their configured `landing/` paths.

Now run the update pipeline with:

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/update.py
```

- The update pipeline appends new Bronze records and processes only records ingested since the latest Silver watermark.
- Schema versions are configured in `configs/`.
- Each Silver execution processes the matching Bronze `schema_version`.
- The supplied update configurations demonstrate schema evolution through the new `aqi` Air Quality field and `humidity` Weather field.

## Inspect and evaluate updates

Use the [monitoring guide](../notebooks/monitoring/README.md) to inspect execution metrics and rejected records. Run the [update evaluation notebooks](../notebooks/update_evaluation/) to reproduce timing and overhead experiments; follow their individual prerequisites.
