SELECT
    ad_data.buyer,
    COUNT(*) AS record_count,
    SUM(ad_data.estimatedbackfillrevenue) AS total_revenue
FROM
    ad_data
GROUP BY
    ad_data.buyer;