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



* On windows run:
```
 docker compose up -d minio minio-init
docker compose run --rm spark-master \
  bash -lc "/opt/spark/bin/spark-submit --master local[2] /opt/spark/jobs/ingest.py /opt/spark/configs/taxi_zones.json"

```