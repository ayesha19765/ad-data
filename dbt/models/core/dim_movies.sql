{{ config(
    materialized = 'table',
    cluster_by = ['movieId']
) }}

SELECT {{ dbt_utils.surrogate_key(['movieId']) }} AS movieKey,
       movieId,
       movieName,
       year,
       certificate,
       runtime,
       genre,
       rating,
       description,
       director,
       directorId,
       star,
       starId,
       votes,
       gross
FROM (
    SELECT 
        movieId,
        movieName,
        year,
        certificate,
        runtime,
        genre,
        rating,
        description,
        director,
        directorId,
        star,
        starId,
        votes,
        gross
    FROM {{ ref('stg_movies') }}

    UNION ALL

    SELECT 
        'NNNNNNNNNNNNNNNNNNN' AS movieId,
        'NA' AS movieName,
        0 AS year,
        'NA' AS certificate,
        'NA' AS runtime,
        'NA' AS genre,
        0.0 AS rating,
        'NA' AS description,
        'NA' AS director,
        'NA' AS directorId,
        'NA' AS star,
        'NA' AS starId,
        0 AS votes,
        0.0 AS gross
)
