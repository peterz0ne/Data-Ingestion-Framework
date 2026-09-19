select /*+ BROADCAST(air_quality) */
    avg(trip_distance),
    codes.weather_condition
from delta.`s3a://delta-data/gold/gold_integrated_taxi_trips` trips 
    join delta.`s3a://delta-data/silver/silver_weather_conditions` codes 
    on trips.coco_pu=codes.coco_code
group by codes.weather_condition
    order by 1
