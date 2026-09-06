-- Singular Test: Ensures dim_users validity date intervals are logically consistent.
-- Invariant: rowActivationDate must be less than or equal to rowExpirationDate.

SELECT
    userKey,
    userId,
    rowActivationDate,
    rowExpirationDate
FROM {{ ref('dim_users') }}
WHERE rowActivationDate > rowExpirationDate
   OR rowActivationDate IS NULL
   OR rowExpirationDate IS NULL

