-- Singular Test: Ensures derived ratio metrics in daily_ad_metrics remain strictly between 0.0 and 1.0.
-- Invariant: free_tier_ratio BETWEEN 0.0 AND 1.0

SELECT
    dailyAdMetricKey,
    free_tier_ratio
FROM {{ ref('daily_ad_metrics') }}
WHERE free_tier_ratio < 0.0
   OR free_tier_ratio > 1.0

