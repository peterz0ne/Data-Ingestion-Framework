select /*+ BROADCAST(air_quality) */
    avg(trip_distance),
    codes.weather_condition
from delta.`s3a://delta-data/gold/gold_integrated_taxi_trips` trips 
    join delta.`s3a://delta-data/silver/silver_weather_conditions` codes 
    on trips.coco_pu=codes.coco_code
WHERE date >= DATE '2024-02-01'
    AND date < DATE '2024-03-01'
group by codes.weather_condition
order by 1
