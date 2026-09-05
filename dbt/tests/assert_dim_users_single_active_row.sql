-- Custom test: Ensures no userId has more than one active record (currentRow = 1) simultaneously in dim_users.

SELECT 
    userId,
    COUNTIF(currentRow = 1) AS active_rows_count
FROM {{ ref('dim_users') }}
WHERE userId != 0
GROUP BY userId
HAVING COUNTIF(currentRow = 1) > 1

