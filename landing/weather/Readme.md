# Data Description

- https://dev.meteostat.net/parameters
- https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.pdf

## Primary entity
Hourly weather observation for a specific location

## Primary key
- year, month, day, hour

## Attributes for join
- year, month, day, hour: After transforming these fields to a date-hour format and also transforming the fields `tpep_pickup_datetime`, `tpep_dropoff_datetime` from the yellow tripdata parquets into date-hour format we can join them.

Timezone: UTC

## Temporal attributes
- year
- month
- day
- hour

## Categorical attributes
- temp_source
- rhum_source
- prcp_source
- snwd_source
- wdir_source
- wspd_source
- wpgt_source
- pres_source
- cldc_source
- coco_source
- cldc
- coco

## Growing attributes over time
As new weather data are inserted, the below attribute value ranges can expand:
- year
- temp
- prcp
- snwd
- wspd
- wpgt
- pres
