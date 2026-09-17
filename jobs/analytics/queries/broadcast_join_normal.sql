WITH hourly_air_quality AS (
    SELECT
        observation_datetime AS pickup_hour_utc,
        AVG(sample_measurement) AS avg_air_quality
    FROM source_air_quality
    WHERE state_name = 'New York'
    GROUP BY observation_datetime
),
joined_trips AS (
    SELECT
        date_trunc('hour', taxi.pickup_datetime) AS pickup_hour_utc,
        air_quality.avg_air_quality
    FROM source_taxi_trips AS taxi
    INNER JOIN hourly_air_quality AS air_quality
        ON date_trunc('hour', taxi.pickup_datetime)
            = air_quality.pickup_hour_utc
),
hourly_demand AS (
    SELECT
        pickup_hour_utc,
        COUNT(*) AS trip_count,
        MAX(avg_air_quality) AS avg_air_quality
    FROM joined_trips
    GROUP BY pickup_hour_utc
)
SELECT
    COUNT(*) AS observed_hours,
    corr(trip_count, avg_air_quality) AS demand_air_quality_correlation
FROM hourly_demand
