select
    avg(trip_distance) AS average_trip_distance,
    codes.weather_condition
from integrated_taxi_trips trips
join weather_conditions codes
    on trips.coco_pu=codes.coco_code
WHERE date >= DATE '2024-02-01'
    AND date < DATE '2024-03-01'
group by codes.weather_condition
order by 1
