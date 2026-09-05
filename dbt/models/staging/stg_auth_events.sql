{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'auth_events') }}
)

SELECT
    CAST(ts AS TIMESTAMP) AS ts,
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
    COALESCE(TRIM(device), 'NA') AS device,
    COALESCE(TRIM(os), 'NA') AS os,
    CAST(registration AS INT64) AS registration,
    COALESCE(success, FALSE) AS success
FROM source_data
WHERE ts IS NOT NULL

