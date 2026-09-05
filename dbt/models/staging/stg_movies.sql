{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('staging', 'movies') }}
)

SELECT
    COALESCE(TRIM(movie_id), 'NNNNNNNNNNNNNNNNNNN') AS movieId,
    COALESCE(TRIM(movie_name), 'NA') AS movieName,
    COALESCE(CAST(year AS INT64), 0) AS year,
    COALESCE(TRIM(certificate), 'NA') AS certificate,
    COALESCE(TRIM(runtime), 'NA') AS runtime,
    COALESCE(TRIM(genre), 'NA') AS genre,
    COALESCE(CAST(rating AS FLOAT64), 0.0) AS rating,
    COALESCE(TRIM(description), 'NA') AS description,
    COALESCE(TRIM(director), 'NA') AS director,
    COALESCE(TRIM(director_id), 'NA') AS directorId,
    COALESCE(TRIM(star), 'NA') AS star,
    COALESCE(TRIM(star_id), 'NA') AS starId,
    COALESCE(CAST(votes AS INT64), 0) AS votes,
    COALESCE(CAST(`gross(in $)` AS FLOAT64), 0.0) AS gross
FROM source_data

