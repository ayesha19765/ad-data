{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'page_view_events') }}
)

SELECT
    CAST(ts AS TIMESTAMP) AS ts,
    COALESCE(TRIM(page), 'NA') AS page,
    COALESCE(TRIM(auth), 'NA') AS auth,
    COALESCE(TRIM(method), 'NA') AS method,
    CAST(status AS INT64) AS status,
    COALESCE(TRIM(level), 'NA') AS level,
    COALESCE(TRIM(city), 'NA') AS city,
    COALESCE(TRIM(state), 'NA') AS state,
    COALESCE(TRIM(userAgent), 'NA') AS userAgent,
    CAST(lon AS FLOAT64) AS lon,
    CAST(lat AS FLOAT64) AS lat,
    CAST(userId AS INT64) AS userId,
    COALESCE(TRIM(lastName), 'NA') AS lastName,
    COALESCE(TRIM(firstName), 'NA') AS firstName,
    COALESCE(TRIM(gender), 'NA') AS gender,
    COALESCE(TRIM(dateOfBirth), 'NA') AS dateOfBirth,
    CAST(registration AS INT64) AS registration,
    COALESCE(TRIM(video), 'NA') AS video,
    COALESCE(TRIM(device), 'NA') AS device,
    COALESCE(TRIM(os), 'NA') AS os,
    CAST(duration AS FLOAT64) AS duration
FROM source_data
WHERE ts IS NOT NULL

