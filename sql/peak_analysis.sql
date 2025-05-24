-- Identify peak hours across all transport modes
WITH hourly_stats AS (
    SELECT 
        HOUR(timestamp) as hour_of_day,
        day_of_week,
        COUNT(*) as journey_count,
        SUM(passenger_count) as total_passengers
    FROM tfl_journeys
    WHERE year = 2024  -- Replace with your analysis year
    GROUP BY HOUR(timestamp), day_of_week
),
daily_stats AS (
    SELECT 
        hour_of_day,
        day_of_week,
        journey_count,
        total_passengers,
        AVG(journey_count) OVER (
            PARTITION BY day_of_week 
            ORDER BY hour_of_day 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ) as moving_avg_journeys
    FROM hourly_stats
)
SELECT 
    hour_of_day,
    day_of_week,
    journey_count,
    total_passengers,
    moving_avg_journeys,
    CASE 
        WHEN journey_count > (moving_avg_journeys * 1.5) THEN 'Peak'
        WHEN journey_count < (moving_avg_journeys * 0.5) THEN 'Quiet'
        ELSE 'Normal'
    END as period_type
FROM daily_stats
ORDER BY day_of_week, hour_of_day;

-- Find unusually quiet periods in Underground stations
SELECT 
    station_start,
    DATE(timestamp) as date,
    HOUR(timestamp) as hour,
    COUNT(*) as journey_count,
    AVG(passenger_count) as avg_passengers
FROM tfl_journeys
WHERE 
    transport_mode = 'Underground'
    AND year = 2024  -- Replace with your analysis year
GROUP BY 
    station_start,
    DATE(timestamp),
    HOUR(timestamp)
HAVING journey_count < (
    SELECT AVG(journey_count) * 0.3
    FROM (
        SELECT 
            station_start as station,
            DATE(timestamp) as date,
            HOUR(timestamp) as hour,
            COUNT(*) as journey_count
        FROM tfl_journeys
        WHERE transport_mode = 'Underground'
        GROUP BY station_start, DATE(timestamp), HOUR(timestamp)
    )
)
ORDER BY journey_count; 