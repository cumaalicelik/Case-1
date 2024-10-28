WITH DistanceCalculation AS (
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
            WHEN distance_increment = 0 THEN 1  -- No change in cumulative distance (remained same, there is a problem)
            WHEN cumulative_distance < LAG(cumulative_distance) OVER (PARTITION BY CumulativeDistance.route_id ORDER BY recorded_at) THEN 3  -- checking if there is lower value than the previous cumulative distance
            ELSE 0  -- Normal increment within average bounds
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
SELECT 
    route_id,
    recorded_at,
    cumulative_distance,
	distance_increment,
    time_interval_seconds,
    cumulative_anomaly,
    time_anomaly
FROM 
    CumulativeAnomaly
	
ORDER BY 
    route_id, recorded_at;



/*

***Analysis Explanation

1.Distance & Time Calculation:
-Calculates incremental distance (distance_increment) between consecutive records. Resets are handled by setting distance_increment to 0 when the distance decreases.
-Calculates time intervals (time_interval_seconds) between records.

2.Average Calculations:
-Computes average distance_increment and time_interval_seconds for each route_id as baselines for anomaly detection.

3.Cumulative Distance:
-Tracks cumulative distance for each route, summing distance_increment while accounting for resets.

4.Anomaly Detection:
-Distance Anomalies (cumulative_anomaly): Flags large increases (2), no movement (1), or drops in cumulative distance (3).
-Time Anomalies (time_anomaly): Flags unusually large time gaps as 1.

*** 2nd row which instantly increases to 347244.40625 seems an anomaly too, but I would have asked this to the stakeholder.
There might be some explanations yet it seems to  be a problem here!

*/