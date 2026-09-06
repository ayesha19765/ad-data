# Duplicate Handling & Deduplication Strategy

## 1. Types of Duplicates in Data Platforms

In enterprise data pipelines, duplicates originate from two fundamentally distinct mechanisms:

```
                      ┌─────────────────────────────────────────┐
                      │            DUPLICATE EVENTS             │
                      └────────────────────┬────────────────────┘
                                           │
                ┌──────────────────────────┴──────────────────────────┐
                ▼                                                     ▼
    ┌───────────────────────┐                             ┌───────────────────────┐
    │  PIPELINE DUPLICATES  │                             │   SOURCE DUPLICATES   │
    ├───────────────────────┤                             ├───────────────────────┤
    │ • Airflow task retry  │                             │ • Client SDK retry    │
    │ • Pipeline backfill   │                             │ • Mobile network retry│
    │ • Network re-transmit │                             │ • Multi-tab telemetry │
    │ • Worker crash recov. │                             │ • Double ad click     │
    └───────────────────────┘                             └───────────────────────┘
```

---

## 2. Pipeline Duplicates Resolution: Partition-Scoped Idempotency

Pipeline duplicates occur when an orchestrator (Airflow) retries a failed task or executes a backfill over an already-processed time window.

### Mechanism:
Before inserting records from the external Parquet staging table into the target BigQuery staging table, the Airflow SQL script executes an atomic partition delete scoped strictly to the execution hour:

```sql
-- airflow/dags/sql/ad_events.sql
DELETE FROM {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }}
WHERE ts >= TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}')
  AND ts < TIMESTAMP_ADD(TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}'), INTERVAL 1 HOUR);

INSERT INTO {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }} (...)
SELECT ...
FROM {{ BIGQUERY_DATASET }}.{{ AD_EVENTS_TABLE }}_{{ logical_date.strftime("%m%d%H") }};
```

### Guarantees:
- Re-running an Airflow task 1 time or 100 times produces the exact same row count in staging.
- Never creates duplicate partitions or phantom rows.

---

## 3. Source Duplicates Resolution: Surrogate Keys & Merge Upserts

Source duplicates occur when the upstream producer or client browser emits multiple identical event records (e.g. user double-clicking, browser retry on poor connection).

### 1. Deterministic Surrogate Primary Keys
Because raw event streams may lack a globally unique backend-generated `uuid`, the core warehouse constructs deterministic surrogate primary keys from natural composite business keys using MD5 hashing:

- **Streams Fact**:
  `streamKey = MD5(CONCAT(COALESCE(userId, 0), '|', ts, '|', COALESCE(video, 'NA')))`
- **Ad Events Fact**:
  `adEventKey = MD5(CONCAT(COALESCE(userId, 0), '|', ts, '|', COALESCE(adType, 'NA'), '|', COALESCE(video, 'NA')))`

### 2. dbt Incremental Merge Strategy
Core fact tables are configured with `incremental_strategy = 'merge'` on `unique_key = 'streamKey'` / `'adEventKey'`:

```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'adEventKey',
    incremental_strategy = 'merge',
    incremental_predicates = ["DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"],
    partition_by = {"field": "ts", "data_type": "timestamp", "granularity": "day"},
    cluster_by = ["userKey", "adType", "videoKey"]
) }}
```

### 3. BigQuery MERGE Behavior:
- When a duplicate source record with an identical composite natural key is ingested, BigQuery matches the existing `adEventKey` in the target partition and performs an in-place `UPDATE` rather than an `INSERT`.
- If an exact duplicate appears within the same batch, staging views can apply window deduplication:
  ```sql
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY userId, ts, adType, video 
      ORDER BY ts DESC
  ) = 1
  ```

---

## 4. Quality Invariants & Automated Tests

All core facts and analytical marts enforce uniqueness through automated dbt schema tests:

```yaml
# dbt/models/core/schema.yml
- name: fact_ad_events
  columns:
    - name: adEventKey
      tests:
        - unique
        - not_null
```

Any duplicate record that slips past deduplication immediately causes `dbt test` to fail the build in CI and production.

