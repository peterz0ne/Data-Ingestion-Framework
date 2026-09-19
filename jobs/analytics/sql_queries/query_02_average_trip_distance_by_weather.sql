SELECT
    AVG(distance) AS average_trip_distance,
    weather
FROM (
    SELECT
        coco_do AS weather,
        trip_distance AS distance
    FROM integrated_taxi_trips

    UNION ALL

    SELECT
        coco_pu AS weather,
        trip_distance AS distance
    FROM integrated_taxi_trips
) AS weather_observations
GROUP BY weather
