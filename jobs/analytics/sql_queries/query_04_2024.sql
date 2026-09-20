WITH cd AS (
    SELECT COUNT(coco_pu) AS cnt, coco_pu, pickup_zone_name as zone
    FROM integrated_taxi_trips
    where pickup_zone_name <> 'N/A' 
    and pickup_zone_name <> 'Outside of NYC'
    and pickup_month='2024-02'
    GROUP BY pickup_zone_name, coco_pu
)
select zone, variation from (
    SELECT
        zone,
        (MAX(cnt)/AVG(cnt) - MIN(cnt)/AVG(cnt)) AS variation,
        MIN(cnt),
        MIN(cnt)/AVG(cnt)
    FROM cd
    GROUP BY zone
    HAVING MIN(cnt)/AVG(cnt) > 0.01
) a
ORDER BY variation DESC, zone desc