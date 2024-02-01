SELECT 
    DATE(ad_data.time) AS "Date",
    EXTRACT(HOUR FROM ad_data.time) AS "Hour",
    SUM(ad_data.estimatedbackfillrevenue) AS "Total Backfill Revenue"
FROM 
    ad_data
GROUP BY 
    DATE(ad_data.time), EXTRACT(HOUR FROM ad_data.time)
ORDER BY 
    "Date", "Hour";