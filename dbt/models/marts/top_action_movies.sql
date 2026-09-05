{{ config(materialized='table') }}

-- Content catalog mart ranking top action movies by IMDb rating.

WITH src AS (
    SELECT
        COALESCE(movie_name, title) AS title,
        SAFE_CAST(rating AS FLOAT64) AS rating
    FROM {{ source('imdb', 'action') }}
)

SELECT 
    title,
    rating
FROM src
WHERE rating IS NOT NULL
ORDER BY rating DESC
LIMIT 100

