{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'ad_events') }}
)

SELECT
    CAST(ts AS TIMESTAMP) AS ts,
    COALESCE(TRIM(adType), 'NA') AS adType,
    COALESCE(TRIM(video), 'NA') AS video,
    CAST(duration AS FLOAT64) AS duration,
    COALESCE(TRIM(auth), 'NA') AS auth,
    COALESCE(TRIM(level), 'NA') AS level,
    COALESCE(TRIM(city), 'NA') AS city,
    COALESCE(TRIM(state), 'NA') AS state,
    COALESCE(TRIM(userAgent), 'NA') AS userAgent,
    CAST(lon AS FLOAT64) AS lon,
    CAST(lat AS FLOAT64) AS lat,
    CAST(userId AS INT64) AS userId,
    COALESCE(TRIM(lastName), 'NA') AS lastName,
    COALESCE(TRIM(firstName), 'NA') AS firstName,
    COALESCE(TRIM(dateOfBirth), 'NA') AS dateOfBirth,
    COALESCE(TRIM(gender), 'NA') AS gender,
    CAST(registration AS INT64) AS registration
FROM source_data
WHERE ts IS NOT NULL

