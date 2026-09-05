{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ ref('state_codes') }}
)

SELECT
    COALESCE(TRIM(stateCode), 'NA') AS stateCode,
    COALESCE(TRIM(stateName), 'NA') AS stateName
FROM source_data

