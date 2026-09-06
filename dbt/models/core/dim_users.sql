{{ config(
    materialized = 'table',
    cluster_by = ['userId']
) }}

-- SCD Type 2 user dimension: tracks user subscription levels (free vs paid) over time.

SELECT {{ dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey, *
SELECT 
    {{ dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey,
    userId,
    firstName,
    lastName,
    gender,
    dateOfBirth,
    level,
    registration,
    rowActivationDate,
    rowExpirationDate,
    currentRow
FROM
(
    SELECT 
        CAST(userId AS INT64) AS userId, 
        firstName, 
        lastName, 
        gender, 
        dateOfBirth, 
        level, 
        CAST(registration AS INT64) AS registration, 
        minDate AS rowActivationDate,
        LEAD(minDate, 1, DATE '9999-12-31') OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY grouped) AS rowExpirationDate,
        CASE WHEN RANK() OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY grouped DESC) = 1 THEN 1 ELSE 0 END AS currentRow
    FROM
    (
        SELECT 
            userId, 
            firstName, 
            lastName, 
            gender, 
            dateOfBirth, 
            registration, 
            level, 
            grouped, 
            CAST(MIN(date) AS DATE) AS minDate
        FROM
        (
            SELECT *, SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY date) AS grouped
            SELECT 
                userId,
                firstName,
                lastName,
                gender,
                dateOfBirth,
                registration,
                level,
                date,
                SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY date) AS grouped
            FROM
            (
                SELECT *, CASE WHEN LAG(level, 1, 'NA') OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY date) <> level THEN 1 ELSE 0 END AS lagged
                SELECT 
                    userId,
                    firstName,
                    lastName,
                    gender,
                    dateOfBirth,
                    registration,
                    level,
                    date,
                    CASE WHEN LAG(level, 1, 'NA') OVER(PARTITION BY userId, firstName, lastName, gender, dateOfBirth ORDER BY date) <> level THEN 1 ELSE 0 END AS lagged
                FROM
                (
                    SELECT DISTINCT 
                        userId,
                        firstName,
                        lastName,
                        gender,
                        dateOfBirth,
                        registration,
                        level,
                        ts AS date
                    FROM {{ ref('stg_watch_events') }}
                    WHERE userId != 0
                )
            )
        )
        GROUP BY userId, firstName, lastName, gender, dateOfBirth, registration, level, grouped
    )

    UNION ALL

    SELECT 
        {{ dbt_utils.surrogate_key(['userId', 'CAST(MIN(ts) AS DATE)', 'level']) }} AS userKey,
        CAST(userId AS INT64) AS userId, 
        firstName, 
        lastName, 
        gender, 
        dateOfBirth, 
        level, 
        CAST(registration AS INT64) AS registration, 
        CAST(MIN(ts) AS DATE) AS rowActivationDate, 
        DATE '9999-12-31' AS rowExpirationDate, 
        1 AS currentRow
    FROM {{ ref('stg_watch_events') }} 
    WHERE userId = 0
    GROUP BY userId, firstName, lastName, gender, dateOfBirth, level, registration
)
