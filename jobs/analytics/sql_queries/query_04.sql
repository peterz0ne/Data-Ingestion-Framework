WITH cd AS (
    SELECT SUM(cnt) AS cnt, coco, zone
    FROM (
        SELECT
            COUNT(*) AS cnt,
            coco_pu AS coco,
            pickup_zone_name AS zone
        FROM integrated_taxi_trips
        GROUP BY pickup_zone_name, coco_pu
    ) a
    where zone <> 'N/A' and zone <> 'Outside of NYC'
    GROUP BY coco, zone
)
select zone, variation from (
    SELECT
        zone,
        (MAX(cnt)/AVG(cnt) - MIN(cnt)/AVG(cnt)) AS variation,
        MIN(cnt)
        --MIN(cnt)/AVG(cnt),
        --MAX(cnt)/AVG(cnt),
        --AVG(cnt)
    FROM cd
    GROUP BY zone
    HAVING MIN(cnt)/AVG(cnt) > 0.01
) a
ORDER BY variation DESC, zone desc