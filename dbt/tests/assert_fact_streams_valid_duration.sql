-- Singular Test: Ensures streaming fact duration is non-negative.
-- Invariant: duration >= 0.0

SELECT
    streamKey,
    duration
FROM {{ ref('fact_streams') }}
WHERE duration < 0.0

