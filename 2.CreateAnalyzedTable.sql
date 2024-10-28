;WITH DistanceCalculation AS (
    SELECT 
        route_id,
        recorded_at,
        distance,
        -- Calculate distance increment directly using LAG
        CASE 
            WHEN LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) IS NULL 
                 OR distance >= LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) 
                THEN distance - COALESCE(LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at), 0)
            ELSE 0  -- Reset incremental distance at detected reset points
        END AS distance_increment,
        -- Calculate time interval in seconds directly using LAG
        DATEDIFF(SECOND, LAG(recorded_at) OVER (PARTITION BY route_id ORDER BY recorded_at), recorded_at) AS time_interval_seconds
    FROM 
        navigation_records
),
AvgIncrements AS (
    SELECT 
        route_id,
        AVG(distance_increment) AS avg_increment,
        AVG(time_interval_seconds) AS avg_time_interval
    FROM 
        DistanceCalculation
    WHERE 
        distance_increment > 0 OR time_interval_seconds > 0  -- Ignore zero increments
    GROUP BY 
        route_id
),
CumulativeDistance AS (
    SELECT 
        route_id,
        recorded_at,
        -- Calculate cumulative distance
        SUM(distance_increment) OVER (PARTITION BY route_id ORDER BY recorded_at) AS cumulative_distance,
        distance_increment,
        time_interval_seconds
    FROM 
        DistanceCalculation
),
CumulativeAnomaly AS (
    SELECT 
        CumulativeDistance.route_id,
        CumulativeDistance.recorded_at,
        distance_increment,
        cumulative_distance,
        time_interval_seconds,
        CASE 
            WHEN distance_increment > 3 * AvgIncrements.avg_increment THEN 2  -- abnormal large increase
            WHEN distance_increment = 0 THEN 1  -- No change in cumulative distance
            ELSE 0  -- Normal increment
        END AS cumulative_anomaly,
        -- Define time anomaly based on average time interval
        CASE 
            WHEN time_interval_seconds > 3 * AvgIncrements.avg_time_interval THEN 1  -- abnormal time interval
            ELSE 0  -- Normal time interval
        END AS time_anomaly
    FROM 
        CumulativeDistance
    JOIN 
        AvgIncrements ON CumulativeDistance.route_id = AvgIncrements.route_id
)
-- Use SELECT INTO to create the new table
SELECT 
    route_id,
    recorded_at,
    cumulative_distance,
    distance_increment,
    time_interval_seconds,
    cumulative_anomaly,
    time_anomaly
INTO analyzed_nav_records -- Creates the new table
FROM 
    CumulativeAnomaly
ORDER BY 
    route_id, recorded_at;


/*

In this query I wanted to create a new table to work on easily on Power BI


*/
