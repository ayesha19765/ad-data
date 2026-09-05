{{ config(
    materialized = 'table',
    partition_by = {
      "field": "ad_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["adType", "videoKey"]
) }}

-- Grain: One row per calendar date, adType, and video content key.

WITH ad_facts AS (
    SELECT 
        CAST(DATE(ts) AS DATE) AS ad_date,
        adType,
        video,
        videoKey,
        userKey,
        level,
        duration,
        adEventKey
    FROM {{ ref('fact_ad_events') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['ad_date', 'adType', 'videoKey']) }} AS dailyAdMetricKey,
    ad_date,
    adType,
    video,
    videoKey,
    COUNT(adEventKey) AS total_impressions,
    COUNT(DISTINCT userKey) AS unique_viewers,
    COALESCE(SUM(duration), 0.0) AS total_ad_duration_seconds,
    COALESCE(SAFE_DIVIDE(SUM(duration), COUNT(adEventKey)), 0.0) AS avg_ad_duration_seconds,
    COUNTIF(level = 'free') AS free_tier_impressions,
    COUNTIF(level = 'paid') AS paid_tier_impressions,
    COALESCE(SAFE_DIVIDE(COUNTIF(level = 'free'), COUNT(adEventKey)), 0.0) AS free_tier_ratio
FROM ad_facts
GROUP BY 
    ad_date,
    adType,
    video,
    videoKey

