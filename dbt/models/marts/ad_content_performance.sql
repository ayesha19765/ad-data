{{ config(
    materialized = 'table',
    cluster_by = ["adType", "content_genre"]
) }}

-- Grain: One row per content video (videoKey) and ad format (adType).

WITH ad_facts AS (
    SELECT 
        videoKey,
        adType,
        userKey,
        level,
        duration,
        adEventKey
    FROM {{ ref('fact_ad_events') }}
),

content_dim AS (
    SELECT 
        movieKey,
        movieName AS content_title,
        genre AS content_genre,
        rating AS content_rating,
        year AS release_year
    FROM {{ ref('dim_movies') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['ad_facts.videoKey', 'ad_facts.adType']) }} AS adContentPerformanceKey,
    ad_facts.videoKey,
    ad_facts.adType,
    COALESCE(content_dim.content_title, 'Unknown Content') AS content_title,
    COALESCE(content_dim.content_genre, 'NA') AS content_genre,
    COALESCE(content_dim.content_rating, 0.0) AS content_rating,
    COALESCE(content_dim.release_year, 0) AS release_year,
    COUNT(ad_facts.adEventKey) AS total_impressions,
    COUNT(DISTINCT ad_facts.userKey) AS unique_viewers,
    COALESCE(SUM(ad_facts.duration), 0.0) AS total_ad_duration_seconds,
    COALESCE(SAFE_DIVIDE(SUM(ad_facts.duration), COUNT(ad_facts.adEventKey)), 0.0) AS avg_ad_duration_seconds,
    COUNTIF(ad_facts.level = 'free') AS free_tier_impressions,
    COUNTIF(ad_facts.level = 'paid') AS paid_tier_impressions,
    COALESCE(SAFE_DIVIDE(COUNTIF(ad_facts.level = 'free'), COUNT(ad_facts.adEventKey)), 0.0) AS free_tier_ratio
FROM ad_facts
LEFT JOIN content_dim 
    ON ad_facts.videoKey = content_dim.movieKey
GROUP BY 
    ad_facts.videoKey,
    ad_facts.adType,
    content_dim.content_title,
    content_dim.content_genre,
    content_dim.content_rating,
    content_dim.release_year

