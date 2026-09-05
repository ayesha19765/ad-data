{{ config(
    materialized = 'view'
) }}

-- Denormalized reporting view joining streaming facts with all core dimensions for BI analysis.

SELECT
    fact_streams.streamKey AS streamKey,
    fact_streams.userKey AS userKey,
    fact_streams.videoKey AS videoKey,
    fact_streams.dateKey AS dateKey,
    fact_streams.locationKey AS locationKey,
    fact_streams.duration AS streamDuration,
    fact_streams.ts AS timestamp,

    dim_users.firstName AS firstName,
    dim_users.lastName AS lastName,
    dim_users.gender AS gender,
    dim_users.level AS userLevel,
    dim_users.userId AS userId,
    dim_users.currentRow AS currentUserRow,

    dim_movies.runtime AS videoDuration,
    dim_movies.movieName AS videoName,
    dim_movies.genre AS videoGenre,
    dim_movies.rating AS videoRating,

    dim_location.city AS city,
    dim_location.stateName AS state,
    dim_location.latitude AS latitude,
    dim_location.longitude AS longitude,

    dim_datetime.date AS dateHour,
    dim_datetime.dayOfMonth AS dayOfMonth,
    dim_datetime.dayOfWeek AS dayOfWeek,
    dim_datetime.weekendFlag AS weekendFlag

FROM {{ ref('fact_streams') }} AS fact_streams
LEFT JOIN {{ ref('dim_users') }} AS dim_users 
    ON fact_streams.userKey = dim_users.userKey
LEFT JOIN {{ ref('dim_movies') }} AS dim_movies 
    ON fact_streams.videoKey = dim_movies.movieKey
LEFT JOIN {{ ref('dim_location') }} AS dim_location 
    ON fact_streams.locationKey = dim_location.locationKey
LEFT JOIN {{ ref('dim_datetime') }} AS dim_datetime 
    ON fact_streams.dateKey = dim_datetime.dateKey

