# Subsystem Revision Guide: SCD Type 2 Implementation

## 1. Overview & Business Rationale

A **Slowly Changing Dimension Type 2 (SCD2)** preserves historical attribute values by creating a new record whenever a tracked attribute changes.

### Why SCD2 is Critical for Adaptive Ads:
In a hybrid ad-supported and ad-free subscription streaming model, users frequently transition between tiers:
- When user `usr_101` was on the **`Free`** tier in January, they saw ads (generating ad impressions).
- In February, `usr_101` upgraded to **`Premium`** (ad-free).
- If we used **SCD Type 1** (in-place overwrite), `usr_101` would show as `Premium` for all historical transactions, corrupting January's ad-attribution metrics.
- **SCD Type 2** ensures January transactions join to the `Free` state, and February transactions join to the `Premium` state.

---

## 2. Step-by-Step SQL Window Function Logic

The dimension [`dbt/models/core/dim_users.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_users.sql) implements SCD2 using pure SQL window functions without relying on stateful database snapshot engines:

```sql
WITH user_events AS (
    -- Step 1: Extract all user state change events
    SELECT
        user_id AS userId,
        subscription_tier AS subscriptionTier,
        country_code AS countryCode,
        event_timestamp AS eventTimestamp
    FROM {{ ref('stg_auth_events') }}
),

state_changes AS (
    -- Step 2: Compare current tier against previous tier using LAG
    SELECT
        userId,
        subscriptionTier,
        countryCode,
        eventTimestamp,
        CASE
            WHEN LAG(subscriptionTier) OVER (
                PARTITION BY userId ORDER BY eventTimestamp
            ) = subscriptionTier THEN 0
            ELSE 1
        END AS isNewState
    FROM user_events
),

sessionized_states AS (
    -- Step 3: Compute running sum to establish distinct state intervals
    SELECT
        userId,
        subscriptionTier,
        countryCode,
        eventTimestamp,
        SUM(isNewState) OVER (
            PARTITION BY userId ORDER BY eventTimestamp
        ) AS stateGroup
    FROM state_changes
),

aggregated_intervals AS (
    -- Step 4: Determine activation and expiration timestamps for each group
    SELECT
        userId,
        subscriptionTier,
        countryCode,
        MIN(eventTimestamp) AS rowActivationDate,
        LEAD(MIN(eventTimestamp)) OVER (
            PARTITION BY userId ORDER BY MIN(eventTimestamp)
        ) AS nextActivationDate
    FROM sessionized_states
    GROUP BY userId, subscriptionTier, countryCode, stateGroup
)

-- Step 5: Final output with surrogate keys and validity flags
SELECT
    {{ dbt_utils.generate_surrogate_key(['userId', 'subscriptionTier', 'rowActivationDate']) }} AS userKey,
    userId,
    subscriptionTier,
    countryCode,
    rowActivationDate,
    COALESCE(nextActivationDate, TIMESTAMP('9999-12-31 23:59:59')) AS rowExpirationDate,
    CASE WHEN nextActivationDate IS NULL THEN TRUE ELSE FALSE END AS isCurrent
FROM aggregated_intervals
```

---

## 3. Concrete User State Transition Example

| `userKey` | `userId` | `subscriptionTier` | `rowActivationDate` | `rowExpirationDate` | `isCurrent` |
| :--- | :--- | :--- | :--- | :--- | :---: |
| `a83b...` | `usr_101` | `Free` | `2026-01-01 00:00:00` | `2026-02-15 10:30:00` | `FALSE` |
| `f92c...` | `usr_101` | `Premium` | `2026-02-15 10:30:00` | `2026-06-01 00:00:00` | `FALSE` |
| `711e...` | `usr_101` | `Family_Premium` | `2026-06-01 00:00:00` | `9999-12-31 23:59:59` | `TRUE` |

---

## 4. SCD2 Invariants & Verification Tests

1. **Non-Overlapping Intervals**: For any user, time intervals are mutually exclusive.
2. **Valid Range Order**: `rowActivationDate <= rowExpirationDate` (enforced by `assert_dim_users_valid_date_ranges.sql`).
3. **Single Active Record**: Exactly one record per `userId` has `isCurrent = TRUE`.
4. **Deterministic Rebuild**: Rebuilding `dim_users` from raw auth events generates the identical SCD2 history without data loss.

