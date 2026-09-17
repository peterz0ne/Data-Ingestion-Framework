WITH hourly_demand AS (
    SELECT
        pu_datetime_hour AS pickup_hour_utc,
        COUNT(*) AS trip_count,
        AVG(average_value_pu) AS avg_air_quality
    FROM integrated_taxi_trips
    WHERE date >= DATE '2024-02-01'
      AND date < DATE '2024-03-01'
      AND average_value_pu IS NOT NULL
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
