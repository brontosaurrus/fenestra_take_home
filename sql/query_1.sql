SELECT
    DATE(ad_data.time) AS day,
    DATE_PART('hour', ad_data.time) AS hour,
    COUNT(*) AS record_count
FROM
    ad_data
GROUP BY
    day, hour
ORDER BY
    day, hour;