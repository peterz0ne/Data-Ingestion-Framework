## primary entity
Taxi ride

## primary key
- vendor id
- timestamps (PU,DO)
- distance
- locations (PU,DO) 

(or createa a new one)

## attributes for join
- PULocationID > taxi_zone_lookup[LocationID]
- DOLocationID > taxi_zone_lookup[LocationID]

## temporal attributes
- tpep_pickup_datetime
- tpep_dropoff_datetime

## categorical attributes
- VendorID
- RatecodeID
- store_and_fwd_flag
- PULocationID
- DOLocationID
- payment_type

## growing attributes over time
- tpep_pickup_datetime
- tpep_dropoff_datetime
- trip_distance
- passenger_count?
- fare_amount
- extra
- mta_tax
- tip_amount
- tolls_amount
- improvement_surcharge
- total_amount
- congestion_surcharge
- airport_fee
- cbd_congestion_fee