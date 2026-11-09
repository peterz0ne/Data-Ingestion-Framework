# Configurable Data Ingestion Framework

## Overview

A configurable data-ingestion framework designed to reuse the same processing logic across different datasets. JSON configurations define input sources, schemas, validation rules, and transformations. Datasets that use the supported operations can be added mainly through configuration.

The project demonstrates this framework with NYC taxi trips, taxi zones, weather, and air-quality data:

- **Reusable ingestion:** load and validate different datasets into Bronze and Silver Delta tables using shared pipeline components.
- **Incremental updates and schema evolution:** process new Bronze records using Silver watermarks, validate against existing Silver data, and support explicitly configured schema changes.
- **Data integration:** combine the example datasets into enriched taxi-trip tables in Gold.
- **Analytics:** explore taxi demand and its relationship with weather and air quality.
- **Machine learning:** prepare features and train a Random Forest model to predict taxi tips.

> **Tech stack:** Python · Apache Spark · Delta Lake · MinIO · Docker Compose  
> **Course:** Data-Intensive Computing (ID2221), KTH

### Repository structure

| Directory | Purpose |
| --- | --- |
| [conf/](conf/) | Spark runtime settings and Delta Lake / MinIO connectivity. |
| [configs/](configs/) | Dataset schemas, sources, transformations, and validation rules. |
| [data_generation/](data_generation/) | Notebooks that prepare incremental input batches and schema-change examples. |
| [documents/](documents/) | Weekly instructions, implementation reports, and evaluation reports. |
| [jobs/](jobs/) | Spark pipeline, analytics, and ML code. |
| [landing/](landing/) | Local input data. Source files are excluded from Git. |
| [minio/](minio/) | Object-storage bucket initialization script. |
| [notebooks/](notebooks/README.md) | Queries, monitoring, benchmarks, and ML experiments. |

## Architecture

- Flow: `Source Data → Bronze → Silver → Gold → Analytics / ML`
- Spark processes the data, and Delta Lake stores the tables in MinIO through an S3-compatible interface.
- Docker Compose provides a local Spark master, two workers, MinIO, and JupyterLab.

| Layer | Purpose |
| --- | --- |
| Bronze | Store source records with ingestion and schema-version metadata. |
| Silver | Validate records and apply configured cleaning and transformations. |
| Gold | Enrich taxi trips with zone information, hourly weather, and aggregated air-quality observations. |
| Analytics / ML | Build analytical tables and prepare training data for tip prediction. |

>Note: Rejected records are stored in quarantine with rejection reasons.

## Getting started

### Prerequisites

- Docker with Docker Compose.
- Sufficient CPU, memory, and disk space for the [configured services](docker-compose.yaml) and datasets.
- Source datasets downloaded as described below.

### 1. Prepare the inputs

Download the prepared [assignment datasets](https://drive.google.com/drive/folders/1qjBtPVDepDE22j0axqrLVR0A2a969Qyy?usp=sharing), or consult the original sources below.

| Dataset | Format | Original source |
| --- | --- | --- |
| Taxi trips | Parquet | [NYC TLC](https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Weather | CSV | [NOAA Integrated Surface Database](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) |
| Air quality | CSV | [EPA AirData](https://aqs.epa.gov/aqsweb/airdata/download_files.html) |
| Taxi zone lookup | CSV | [NYC TLC lookup file](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) |

Extract any ZIP archives, then place the files directly in the folders below, relative to the repository root. The baseline pipeline expects this exact layout:

```text
landing/
├── taxi/
│   ├── yellow_tripdata_2024-01.parquet
│   ├── yellow_tripdata_2024-02.parquet
│   └── yellow_tripdata_2024-03.parquet
├── taxi_zone_lookup/
│   └── taxi_zone_lookup.csv
├── weather/
│   └── weather.csv
└── air_quality/
    └── hourly_88101_2024.csv
```

- Use the January–March 2024 **Yellow Taxi** files for taxi trips.
- Place extracted CSV files directly in their matching folders, without an extra archive subfolder.
- Original-source downloads may need preparation to match the schemas in the [dataset configurations](configs/).
- Renaming a file alone does not make its contents compatible.

### 2. Start the services

Wait for the Spark workers and MinIO initialization to finish.

```sh
docker compose up --build -d
```

### 3. Run the pipeline

This creates the Bronze, Silver, and Gold tables and materializes the analytical products in MinIO's `delta-data` bucket.

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest.py
```

- Open the Spark UI at [localhost:8080](http://localhost:8080)
- Open the MinIO console at [localhost:9001](http://localhost:9001).
- Stop the services with `docker compose stop`.


## Further documentation

- [Notebook guide](notebooks/README.md) — queries, monitoring, benchmarks, and ML experiments.
- [Coursework instructions and weekly reports](documents/). Reports document the platform at each weekly submission. Some implementation details and file paths differ from the current version.
- [Incremental updates and ML workflow](README.md)
- [Dataset transformations and quality rules](configs/README.md)
- [Analytical data products](README.md#analytical-data-products) and [query notebooks](notebooks/run_queries/)
- [Jupyter setup](notebooks/setup/README.md)
- [Execution monitoring](notebooks/monitoring/inspect_pipeline_monitoring.ipynb) and [quarantine inspection](notebooks/monitoring/inspect_rejected_records.ipynb)
- [Benchmarking](notebooks/benchmarks/benchmark.ipynb), [query optimizations](notebooks/optimizations/), [storage overhead](notebooks/storage_metrics/storage_overhead.ipynb), and [update evaluation](notebooks/update_evaluation/)
