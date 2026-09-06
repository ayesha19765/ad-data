-- Singular Test: Ensures fact_ad_events timestamps are not null and within plausible historical/current range.
-- Invariant: ts must be populated and not in the distant future.

SELECT
    adEventKey,
    ts
FROM {{ ref('fact_ad_events') }}
WHERE ts IS NULL
   OR ts > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

