SELECT 
    route_id,
    AVG(time_interval_seconds) AS avg_time_interval,
    SUM(CASE WHEN cumulative_anomaly = 0 THEN 1 ELSE 0 END) AS normal_0,
    SUM(CASE WHEN cumulative_anomaly = 1 THEN 1 ELSE 0 END) AS cumulative_anomaly_1,
    SUM(CASE WHEN cumulative_anomaly = 2 THEN 1 ELSE 0 END) AS cumulative_anomaly_2,
	SUM(CASE WHEN time_anomaly = 0 THEN 1 ELSE 0 END) AS normal_time,
    SUM(CASE WHEN time_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_time,
	COUNT(*) AS total
FROM 
    analyzed_nav_records
GROUP BY 
    route_id
ORDER BY 
    route_id;

/*

In this query, I wanted to get data ready for power bi report
In Power BI Viz Report I have shown below findings;
Total Record, Total Anomalies, Anomaly Rate, avg time interval
I thought big increases as anomaly too in my opinion and also shown it comparing to the total record (Anomaly2)
Also compared Anomaly1 (no increase) to the total records.
In above SQL query I have also shown anomaly time (if the avg time gap with previous record is 3 times higher than
the avg I see it as anomaly time and summed it under anomaly_time otherwise below notmal_time

*/
