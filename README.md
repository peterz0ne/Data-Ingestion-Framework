# Generic Urban Data Integration Platform

## Documentation for used Data files (csv, parquet)
- Taxi Trips:
  - https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
  - Assumption local NYC time (Eastern Time, America/New_York)
- Weather: (Maybe these)
  - https://dev.meteostat.net/parameters
  - https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.pdf
  - Probably UTC timezone
- Air Quality: https://aqs.epa.gov/aqsweb/airdata/FileFormats.html#_format_5
- Taxi Zone Lookup:
  - Look [here](./data/taxi_zone_lookup/Readme.md)

## Published Analysis Example (might be useful)
- https://medium.com/@mozart38/where-do-people-go-in-nyc-the-recipe-of-an-analysis-a307499013a6



## Running with Docker

### macOS / Linux

**With live volume mounts (recommended during development so edits reflect immediately):**
```sh
docker compose run --rm --no-deps \
  -v "$PWD/jobs:/opt/spark/jobs:ro" \
  -v "$PWD/configs:/opt/spark/configs:ro" \
  -v "$PWD/data:/opt/spark/data:ro" \
  spark-master \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/spark/jobs/ingest.py \
  /opt/spark/configs/taxi_zones.json
```

**Using the built container files:**
```sh
docker compose up -d minio minio-init
docker compose run --rm spark-master \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/spark/jobs/ingest.py \
  /opt/spark/configs/taxi_zones.json
```

### Windows

```sh
docker compose up -d minio minio-init
* On windows run:
```
docker compose up -d minio minio-init
docker compose build --no-cache spark-master
docker compose run --rm spark-master \
  bash -lc "/opt/spark/bin/spark-submit --master local[2] /opt/spark/jobs/ingest.py /opt/spark/configs/taxi_trips.json"
```


## To run inspect_delta.py
```sh
docker compose run --rm --no-deps \
  -v "$PWD/jobs:/opt/spark/jobs:ro" \
  spark-master \
  /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/spark/jobs/inspect_delta.py \
  taxi_zones
```