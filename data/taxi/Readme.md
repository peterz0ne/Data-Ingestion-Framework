# Data Description

https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## Primary entity
A Taxi ride

## Primary key
- vendor id
- timestamps (PU,DO)
- distance
- locations (PU,DO)
(or create a new one)

TODO: create a question on canvas

`- If we believe that a primary key cannot be guaranteed unique for a dataset, can we generate our own primary key?`


## Attributes for join
- tpep_pickup_datetime: After transforming the field to pickup date-hour it can join with the time fields `year,month,day,hour` from the weather.csv file and the fields `Date Local, Time Local` from the air_quality file.
- tpep_dropoff_datetime: After transforming the field to dropoff date-hour it can join with the time fields `year,month,day,hour` from the weather.csv file and the fields `Date Local, Time Local` from the air_quality file.
- PULocationID joins to `LocationID` on the taxi_zone_lookup table.
- DOLocationID joins to `LocationID` on the taxi_zone_lookup table.

Assumption: tpep_pickup_datetime and tpep_dropoff_datetime use local NYC time.

## Temporal attributes
- tpep_pickup_datetime
- tpep_dropoff_datetime

## Categorical attributes
- VendorID
- RatecodeID
- store_and_fwd_flag
- payment_type

The below attributes can technically be described as categorical but they are mainly foreign keys for taxi_zone_lookup.csv.
- PULocationID
- DOLocationID

## Growing attributes over time
As new trip data are inserted, the below attribute value ranges can expand:
- tpep_pickup_datetime
- tpep_dropoff_datetime
- passenger_count
- trip_distance
- fare_amount
- extra
- mta_tax
- tip_amount
- tolls_amount
- improvement_surcharge
- total_amount
- congestion_surcharge
- airport_fee