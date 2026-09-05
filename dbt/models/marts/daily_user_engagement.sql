{{ config(
    materialized = 'table',
    partition_by = {
      "field": "activity_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["subscription_tier"]
) }}

-- Grain: One row per calendar date and user subscription tier.

WITH stream_facts AS (
    SELECT 
        CAST(DATE(ts) AS DATE) AS activity_date,
        COALESCE(level, 'NA') AS subscription_tier,
        userKey,
        duration,
        streamKey
    FROM {{ ref('fact_streams') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['activity_date', 'subscription_tier']) }} AS dailyEngagementKey,
    activity_date,
    subscription_tier,
    COUNT(streamKey) AS total_streams,
    COUNT(DISTINCT userKey) AS unique_streaming_users,
    COALESCE(SUM(duration), 0.0) AS total_watch_duration_seconds,
    COALESCE(SAFE_DIVIDE(SUM(duration), 3600.0), 0.0) AS total_watch_hours,
    COALESCE(SAFE_DIVIDE(SUM(duration), COUNT(streamKey)), 0.0) AS avg_stream_duration_seconds
FROM stream_facts
GROUP BY 
    activity_date,
    subscription_tier

