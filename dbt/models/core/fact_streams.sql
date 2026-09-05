{{ config(
    materialized = 'incremental',
    unique_key = 'streamKey',
    incremental_strategy = 'merge',
    partition_by = {
      "field": "ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["userKey", "videoKey", "locationKey"]
) }}

WITH watch_events AS (
    SELECT * 
    FROM {{ ref('stg_watch_events') }}
    {% if is_incremental() %}
    -- Lookback window of 3 days to safely process late-arriving telemetry events
    WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})
    {% endif %}
)

SELECT 
    {{ dbt_utils.surrogate_key(['watch_events.userId', 'watch_events.ts', 'watch_events.video']) }} AS streamKey,
    COALESCE(dim_users.userKey, 'NA') AS userKey,
    COALESCE(dim_movies.movieKey, 'NA') AS videoKey,
    COALESCE(dim_datetime.dateKey, 0) AS dateKey,
    COALESCE(dim_location.locationKey, 'NA') AS locationKey,
    watch_events.duration AS duration,
    watch_events.level AS level,
    watch_events.ts AS ts
FROM watch_events
LEFT JOIN {{ ref('dim_users') }} AS dim_users
    ON watch_events.userId = dim_users.userId 
    AND CAST(watch_events.ts AS DATE) >= dim_users.rowActivationDate 
    AND CAST(watch_events.ts AS DATE) < dim_users.rowExpirationDate
LEFT JOIN {{ ref('dim_movies') }} AS dim_movies
    ON REPLACE(REPLACE(watch_events.video, '"', ''), '\\', '') = dim_movies.movieName
LEFT JOIN {{ ref('dim_location') }} AS dim_location
    ON watch_events.city = dim_location.city 
    AND watch_events.state = dim_location.stateCode 
    AND watch_events.lat = dim_location.latitude 
    AND watch_events.lon = dim_location.longitude
LEFT JOIN {{ ref('dim_datetime') }} AS dim_datetime
    ON dim_datetime.date = DATE_TRUNC(watch_events.ts, HOUR)
