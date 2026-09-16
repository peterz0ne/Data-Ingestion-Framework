-- Query 5: peak pickup hours for each day of the week.
-- Taxi timestamps are stored in UTC. The reportable peak hour is converted to
-- America/New_York so it represents the local time experienced by riders.

--TODO: do we agree with this time change to NYC time?
WITH local_pickups AS (
    SELECT
        from_utc_timestamp(
            pickup_datetime,
            'America/New_York'
        ) AS pickup_datetime_local
    FROM integrated_taxi_trips
),
hourly_demand AS (
    SELECT
        dayofweek(pickup_datetime_local) AS weekday_number,
        date_format(pickup_datetime_local, 'EEEE') AS weekday,
        hour(pickup_datetime_local) AS pickup_hour_local,
        COUNT(*) AS trip_count
    FROM local_pickups
    GROUP BY
        dayofweek(pickup_datetime_local),
        date_format(pickup_datetime_local, 'EEEE'),
        hour(pickup_datetime_local)
),
ranked_hours AS (
    SELECT
        weekday_number,
        weekday,
        pickup_hour_local,
        trip_count,
        DENSE_RANK() OVER (
            PARTITION BY weekday_number
            ORDER BY trip_count DESC
        ) AS demand_rank
    FROM hourly_demand
)
SELECT
    weekday_number,
    weekday,
    pickup_hour_local,
    trip_count
FROM ranked_hours
WHERE demand_rank = 1
ORDER BY weekday_number, pickup_hour_local
