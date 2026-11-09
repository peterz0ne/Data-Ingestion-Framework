# Configurable Data Ingestion Framework

## Overview

A configurable data-ingestion framework designed to reuse the same processing logic across different datasets. JSON configurations define input sources, schemas, validation rules, and transformations. Datasets that use the supported operations can be added mainly through configuration.

The project demonstrates this framework with NYC taxi trips, taxi zones, weather, and air-quality data:

- **Reusable ingestion:** load and validate different datasets into Bronze and Silver Delta tables using shared pipeline components.
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
| Silver | Validate, clean, and standardize each dataset. |
| Gold | Enrich taxi trips with zone information and hourly weather and air-quality observations. |
| Analytics / ML | Produce demand-analysis tables and training data for tip prediction. |

Rejected records are stored in quarantine with rejection reasons. Execution metadata records processing metrics and dependencies between pipeline stages.

## Key features

- **Configuration-driven processing:** JSON files define sources, schemas, quality checks, operations, and partitions.
- **Validation and quarantine:** configurable checks separate valid and rejected records. Rejected records are stored with rejection reasons for inspection.
- **Incremental updates and schema evolution:** watermark-based Silver processing, checks against existing Silver data, and versioned dataset configurations.
- **Analytics and ML:** materialized demand and environmental analysis tables, plus feature engineering, model training, and evaluation for taxi-tip prediction.

## Getting started

You need Docker with Docker Compose and enough resources for the services in [docker-compose.yaml](docker-compose.yaml).

**1. Prepare the inputs.** Place the source files in these directories, matching the filenames and formats in each [dataset configuration](configs/):

```text
landing/
├── taxi/
├── taxi_zone_lookup/
├── weather/
└── air_quality/
```

The baseline taxi configuration expects January–March 2024 Parquet files. Input data must currently be acquired manually.

**2. Start the services.** Wait for the Spark workers and MinIO initialization to finish.

```sh
docker compose up --build -d
```

**3. Run the pipeline.** This creates the Bronze, Silver, and Gold tables and materializes the analytical products in MinIO's `delta-data` bucket.

```sh
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest.py
```

Open the Spark UI at [localhost:8080](http://localhost:8080) or the MinIO console at [localhost:9001](http://localhost:9001). Stop the services with `docker compose down`.

This is a local development setup. Incremental processing uses watermarks; exactly-once execution and recovery from partial failures have not been established.

## Further documentation

- [Notebook guide](notebooks/README.md) — queries, monitoring, benchmarks, and ML experiments.
- [Coursework instructions and weekly reports](documents/). Reports document the platform at each weekly submission; some implementation details and file paths differ from the current version.
- [Incremental updates and ML workflow](README.md)
- [Dataset transformations and quality rules](configs/README.md)
- [Analytical data products](README.md#analytical-data-products) and [query notebooks](notebooks/run_queries/)
- [Jupyter setup](notebooks/setup/README.md)
- [Execution monitoring](notebooks/monitoring/inspect_pipeline_monitoring.ipynb) and [quarantine inspection](notebooks/monitoring/inspect_rejected_records.ipynb)
- [Benchmarking](notebooks/benchmarks/benchmark.ipynb), [query optimizations](notebooks/optimizations/), [storage overhead](notebooks/storage_metrics/storage_overhead.ipynb), and [update evaluation](notebooks/update_evaluation/)
