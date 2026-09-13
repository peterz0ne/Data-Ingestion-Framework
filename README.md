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
  - Look [here](./landing/taxi_zone_lookup/Readme.md)

## Running with Docker
To setup the environment install docker and run the following command
```sh
docker compose up --build -d
```
This will create the containers and start the right away.

To run the pipeline execute the following command:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077  /opt/spark/jobs/ingest.py
```