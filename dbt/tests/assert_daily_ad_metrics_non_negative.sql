-- Custom test: Ensures aggregated ad metrics are always non-negative.

SELECT 
    dailyAdMetricKey,
    ad_date,
    total_impressions,
    unique_viewers,
    total_ad_duration_seconds,
    avg_ad_duration_seconds
FROM {{ ref('daily_ad_metrics') }}
WHERE total_impressions < 0
   OR unique_viewers < 0
   OR total_ad_duration_seconds < 0.0
   OR avg_ad_duration_seconds < 0.0

