-- Ensure idempotency: delete existing partition records for this execution interval prior to loading
DELETE FROM {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }}
WHERE ts >= TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}')
  AND ts < TIMESTAMP_ADD(TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}'), INTERVAL 1 HOUR);

INSERT INTO {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }} (
    ts,
    adType,
    video,
    duration,
    auth,
    level,
    city,
    state,
    userAgent,
    lon,
    lat,
    userId,
    lastName,
    firstName,
    dateOfBirth,
    gender,
    registration
)
SELECT
    timestamp AS ts,
    COALESCE(adType, 'NA') AS adType,
    COALESCE(videoTitle, 'NA') AS video,
    COALESCE(duration, -1) AS duration,
    COALESCE(auth, 'NA') AS auth,
    COALESCE(level, 'NA') AS level,
    COALESCE(city, 'NA') AS city,
    COALESCE(state, 'NA') AS state,
    COALESCE(userAgent, 'NA') AS userAgent,
    COALESCE(lon, 0.0) AS lon,
    COALESCE(lat, 0.0) AS lat,
    COALESCE(userId, 0) AS userId,
    COALESCE(lastName, 'NA') AS lastName,
    COALESCE(firstName, 'NA') AS firstName,
    COALESCE(dateOfBirth, 'NA') AS dateOfBirth,
    COALESCE(gender, 'NA') AS gender,
    COALESCE(registration, 9999999999999) AS registration
FROM {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }}_{{ logical_date.strftime("%m%d%H") }};
