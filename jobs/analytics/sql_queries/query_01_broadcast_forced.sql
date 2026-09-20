SELECT /*+ BROADCAST(source_taxi_zones) */
    substring(pickup_date, 1, 7) as pickup_month,
    zone as pickup_zone_name,
    count(*) as demand
FROM source_taxi_trips
INNER JOIN source_taxi_zones
    ON source_taxi_trips.pickup_zone_id = source_taxi_zones.location_id
GROUP BY 1, 2
ORDER BY pickup_zone_name, pickup_month
