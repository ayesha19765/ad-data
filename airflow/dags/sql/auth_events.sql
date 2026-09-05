-- Ensure idempotency: delete existing partition records for this execution interval prior to loading
DELETE FROM {{ BIGQUERY_DATASET }}.{{ AUTH_EVENTS_TABLE }}
WHERE ts >= TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}')
  AND ts < TIMESTAMP_ADD(TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}'), INTERVAL 1 HOUR);

INSERT INTO {{ BIGQUERY_DATASET }}.{{ AUTH_EVENTS_TABLE }} (
    ts,
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
    device,
    os,
    registration,
    success
)
SELECT
    timestamp AS ts,
    COALESCE(level, 'NA') AS level,
    COALESCE(city, 'NA') AS city,
    COALESCE(state, 'NA') AS state,
    COALESCE(userAgent, 'NA') AS userAgent,
    COALESCE(CAST(lon AS NUMERIC), 0.0) AS lon,
    COALESCE(CAST(lat AS NUMERIC), 0.0) AS lat,
    COALESCE(userId, 0) AS userId,
    COALESCE(lastName, 'NA') AS lastName,
    COALESCE(firstName, 'NA') AS firstName,
    COALESCE(dateOfBirth, 'NA') AS dateOfBirth,
    COALESCE(gender, 'NA') AS gender,
    COALESCE(deviceType, 'NA') AS device,
    COALESCE(deviceOs, 'NA') AS os,
    COALESCE(registration, 9999999999999) AS registration,
    COALESCE(success, FALSE) AS success
FROM {{ BIGQUERY_DATASET }}.{{ AUTH_EVENTS_TABLE }}_{{ logical_date.strftime("%m%d%H") }};
