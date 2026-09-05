{{ config(
    materialized = 'table',
    cluster_by = ['stateCode', 'city']
) }}

SELECT {{ dbt_utils.surrogate_key(['latitude', 'longitude', 'city', 'stateName']) }} AS locationKey,
       *
FROM (
    SELECT 
        DISTINCT 
        watch_events.city,
        COALESCE(state_codes.stateCode, 'NA') AS stateCode,
        COALESCE(state_codes.stateName, 'NA') AS stateName,
        watch_events.lat AS latitude,
        watch_events.lon AS longitude
    FROM {{ ref('stg_watch_events') }} AS watch_events
    LEFT JOIN {{ ref('stg_state_codes') }} AS state_codes 
        ON watch_events.state = state_codes.stateCode

    UNION ALL

    SELECT 
        'NA' AS city,
        'NA' AS stateCode,
        'NA' AS stateName,
        0.0 AS latitude,
        0.0 AS longitude
)
