-- Ensure idempotency: delete existing partition records for this execution interval prior to loading
DELETE FROM {{ BIGQUERY_DATASET }}.{{ PAGE_VIEW_EVENTS_TABLE }}
WHERE ts >= TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}')
  AND ts < TIMESTAMP_ADD(TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}'), INTERVAL 1 HOUR);

INSERT INTO {{ BIGQUERY_DATASET }}.{{ PAGE_VIEW_EVENTS_TABLE }} (
    ts,
    page,
    auth,
    method,
    status,
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
    registration,
    video,
    device,
    os,
    duration
)
SELECT
    timestamp AS ts,
    COALESCE(page, 'NA') AS page,
    COALESCE(auth, 'NA') AS auth,
    COALESCE(method, 'NA') AS method,
    COALESCE(status, 0) AS status,
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
    COALESCE(registration, 9999999999999) AS registration,
    COALESCE(videoTitle, 'NA') AS video,
    COALESCE(deviceType, 'NA') AS device,
    COALESCE(deviceOs, 'NA') AS os,
    COALESCE(duration, -1) AS duration
FROM {{ BIGQUERY_DATASET }}.{{ PAGE_VIEW_EVENTS_TABLE }}_{{ logical_date.strftime("%m%d%H") }};
