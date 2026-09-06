{{ config(
    materialized = 'incremental',
    unique_key = 'adEventKey',
    incremental_strategy = 'merge',
    incremental_predicates = ["DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"],
    partition_by = {
      "field": "ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["userKey", "adType", "videoKey"]
) }}

WITH ad_events AS (
    SELECT 
        userId,
        adType,
        video,
        duration,
        level,
        auth,
        userAgent,
        city,
        state,
        lat,
        lon,
        ts
    FROM {{ ref('stg_ad_events') }}
    {% if is_incremental() %}
    -- Lookback window of 3 days to safely process late-arriving ad telemetry
    WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})
    {% endif %}
)

SELECT 
    {{ dbt_utils.surrogate_key(['ad_events.userId', 'ad_events.ts', 'ad_events.adType', 'ad_events.video']) }} AS adEventKey,
    COALESCE(dim_users.userKey, 'NA') AS userKey,
    COALESCE(dim_movies.movieKey, 'NA') AS videoKey,
    COALESCE(dim_datetime.dateKey, 0) AS dateKey,
    COALESCE(dim_location.locationKey, 'NA') AS locationKey,
    ad_events.adType AS adType,
    ad_events.video AS video,
    ad_events.duration AS duration,
    ad_events.level AS level,
    ad_events.auth AS auth,
    ad_events.userAgent AS userAgent,
    ad_events.ts AS ts
FROM ad_events
LEFT JOIN {{ ref('dim_users') }} AS dim_users
    ON ad_events.userId = dim_users.userId 
    AND CAST(ad_events.ts AS DATE) >= dim_users.rowActivationDate 
    AND CAST(ad_events.ts AS DATE) < dim_users.rowExpirationDate
LEFT JOIN {{ ref('dim_movies') }} AS dim_movies
    ON REPLACE(REPLACE(ad_events.video, '"', ''), '\\', '') = dim_movies.movieName
LEFT JOIN {{ ref('dim_location') }} AS dim_location
    ON ad_events.city = dim_location.city 
    AND ad_events.state = dim_location.stateCode 
    AND ad_events.lat = dim_location.latitude 
    AND ad_events.lon = dim_location.longitude
LEFT JOIN {{ ref('dim_datetime') }} AS dim_datetime
    ON dim_datetime.date = DATE_TRUNC(ad_events.ts, HOUR)
