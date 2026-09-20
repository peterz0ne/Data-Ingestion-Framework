select
    avg(trip_distance) AS average_trip_distance,
    codes.weather_condition
from integrated_taxi_trips trips
join weather_conditions codes
    on trips.coco_pu=codes.coco_code
WHERE pickup_month='2024-02'
group by codes.weather_condition
order by 1
