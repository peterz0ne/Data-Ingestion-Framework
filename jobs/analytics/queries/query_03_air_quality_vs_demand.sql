-- Query 3: relationship between hourly pickup demand and pickup air quality.
-- Demand is the number of taxi trips beginning in an hour. Air quality is the
-- existing Gold-table hourly New York aggregate joined to the pickup hour.

--TODO: maybe instead of adding `observed_hours` and `demand_air_quality_correlation` in every 
--      row of the result, just do it if needed in a later step in a notebook or something?
WITH hourly_demand AS (
    SELECT
        pu_datetime_hour AS pickup_hour_utc,
        COUNT(*) AS trip_count,
        AVG(average_value_pu) AS avg_air_quality
    FROM integrated_taxi_trips
    WHERE average_value_pu IS NOT NULL
    GROUP BY pu_datetime_hour
),
relationship_summary AS (
    SELECT
        COUNT(*) AS observed_hours,
        corr(trip_count, avg_air_quality) AS demand_air_quality_correlation
    FROM hourly_demand
)
SELECT
    hourly_demand.pickup_hour_utc,
    hourly_demand.trip_count,
    hourly_demand.avg_air_quality,
    relationship_summary.observed_hours,
    relationship_summary.demand_air_quality_correlation
FROM hourly_demand
CROSS JOIN relationship_summary
ORDER BY hourly_demand.pickup_hour_utc
