
WITH DistanceResets AS (
    SELECT 
        route_id,
        recorded_at,
        distance,
        LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) AS previous_distance
    FROM navigation_records
),
CumulativeSegments AS (
    SELECT
        route_id,
        recorded_at,
        distance,
        -- Adjusting segment calculation for resets
        CASE 
            WHEN previous_distance IS NULL OR distance >= previous_distance 
                THEN distance - COALESCE(previous_distance, 0)
            ELSE 0  -- Reset segment to zero if a reset (drop in distance) is detected (although none found, I just kept it as it was)
        END AS segment_distance
    FROM DistanceResets
),
TotalDistanceAndDuration AS (
    SELECT
        route_id,
        ROUND(SUM(segment_distance), 4) AS total_distance,
        -- Calculate total duration in HH:MM:SS format
        CONCAT(
            CAST((DATEDIFF(second, MIN(recorded_at), MAX(recorded_at)) / 3600) AS VARCHAR(10)), ':',
            CAST(((DATEDIFF(second, MIN(recorded_at), MAX(recorded_at)) % 3600) / 60) AS VARCHAR(10)), ':',
            CAST((DATEDIFF(second, MIN(recorded_at), MAX(recorded_at)) % 60) AS VARCHAR(10))
        ) AS duration
    FROM CumulativeSegments
    GROUP BY route_id
)
SELECT * FROM TotalDistanceAndDuration;

/*
Explanation of what I did with this query:

***Handling Resets: 

First, I used a LAG function to get the previous distance reading for each route_id.
This allows me to spot potential resets in the distance values (like when a reading suddenly drops). 
Although no resets were detected here, I kept the logic to set segment_distance to zero if a reset is found, just in case.
***Calculating Segments: 

Then, I calculated segment_distance by taking the difference between consecutive distances. 
If there’s no reset, it’s simply the difference from the previous value. 
This step effectively creates smaller segments of distance traveled between each record, allowing for a more precise accumulation.
***Summing Total Distance and Formatting Duration: 

Finally, I grouped the data by route_id to get the total distance by summing up segment_distance for each route. 
For duration, I used DATEDIFF to calculate the total time span between the first and last records of each route, formatting it as HH:MM:SS for easy readability.

*/