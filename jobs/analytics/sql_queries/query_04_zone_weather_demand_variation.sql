WITH cd AS (
    SELECT
        SUM(cnt) AS cnt,
        coco,
        zone
    FROM (
        SELECT
            COUNT(*) AS cnt,
            coco_pu AS coco,
            pickup_zone_name AS zone
        FROM integrated_taxi_trips
        GROUP BY
            pickup_zone_name,
            coco_pu
    ) AS demand_by_zone_and_weather
    WHERE zone <> 'N/A'
      AND zone <> 'Outside of NYC'
    GROUP BY
        coco,
        zone
)
SELECT
    zone,
    (MAX(cnt) / AVG(cnt) - MIN(cnt) / AVG(cnt)) AS variation,
    MIN(cnt) AS minimum_trip_count,
    MIN(cnt) / AVG(cnt) AS minimum_relative_demand,
    MAX(cnt) / AVG(cnt) AS maximum_relative_demand,
    AVG(cnt) AS average_trip_count
FROM cd
GROUP BY zone
HAVING MIN(cnt) / AVG(cnt) > 0.01
ORDER BY variation DESC
