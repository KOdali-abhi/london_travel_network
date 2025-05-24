-- Query to analyze popular transport methods by time of day
WITH hourly_transport AS (
    SELECT 
        transport_mode,
        HOUR(timestamp) as hour_of_day,
        COUNT(*) as journey_count,
        AVG(passenger_count) as avg_passengers
    FROM tfl_journeys
    WHERE year = 2024  -- Replace with your analysis year
    GROUP BY transport_mode, HOUR(timestamp)
)
SELECT 
    transport_mode,
    hour_of_day,
    journey_count,
    avg_passengers,
    RANK() OVER (PARTITION BY hour_of_day ORDER BY journey_count DESC) as popularity_rank
FROM hourly_transport
ORDER BY hour_of_day, popularity_rank;

-- Query to find busiest transport methods by day of week
SELECT 
    transport_mode,
    day_of_week,
    COUNT(*) as total_journeys,
    SUM(passenger_count) as total_passengers,
    AVG(fare_amount) as avg_fare
FROM tfl_journeys
WHERE year = 2024  -- Replace with your analysis year
GROUP BY transport_mode, day_of_week
ORDER BY day_of_week, total_journeys DESC; 