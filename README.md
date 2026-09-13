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

To run the pipeline execute the following command:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  /opt/spark/jobs/ingest.py
```

To run the benchmark [notebook](./notebooks/benchmark.ipynb) check this [Readme](./notebooks/Readme.md).
