# Schema Evolution Strategy & Management

## 1. Overview
In digital event telemetry, upstream client applications (web, iOS, Android, Smart TV) evolve rapidly. New tracking properties are introduced, obsolete fields are deprecated, and data formats change.

This document outlines how the **Adaptive Ads** platform safely handles schema changes across ingestion, BigQuery staging, and dbt transformation layers.

---

## 2. Schema Change Categorization

| Change Category | Action | Compatibility | Platform Impact | Required Action |
| :--- | :--- | :--- | :--- | :--- |
| **Field Addition** | New optional column added to event payload | **Backward-Compatible** | None on existing models | Update `airflow/dags/schema.py`, add column to `stg_*` views |
| **Field Deprecation** | Existing column omitted from future events | **Backward-Compatible (with defaults)** | Downstream models receive `NULL` | Staging layer applies `COALESCE(column, 'NA')` default |
| **Type Widening** | E.g., `INT64` → `NUMERIC`, `FLOAT64` → `STRING` | **Conditionally Compatible** | May cause BigQuery load error | Update staging table schema DDL, adjust explicit casts in `stg_*` |
| **Field Renaming** | Column `videoTitle` renamed to `video_name` | **Breaking** | Downstream dbt model compilation error | Update `stg_*` view with alias: `COALESCE(video_name, videoTitle) AS video` |
| **Field Removal** | Active column dropped from upstream producer | **Breaking** | dbt models fail if column is referenced | Perform phased deprecation across warehouse models |

---

## 3. Ingestion Layer Handling (Parquet → BigQuery)

### BigQuery External Tables & Parquet Compatibility
1. **Column Resolution by Name**: Parquet files store column names in file metadata. BigQuery external tables match fields by name rather than ordinal position.
2. **Schema Update Options**: When appending new fields to existing BigQuery staging tables, BigQuery load jobs utilize:
   ```python
   # schema_update_options
   ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
   ```
3. **Explicit Staging Table DDL**: Target staging tables are initialized via `airflow/dags/task_templates.py` (`create_empty_table`) with schema fields defined in `airflow/dags/schema.py`.

---

## 4. dbt Transformation Layer Handling

### Controlled Schema Exposure via Staging Views
Staging models (`dbt/models/staging/stg_*.sql`) isolate downstream core models from upstream schema shifts by enforcing explicit column casts and defaults:

```sql
-- dbt/models/staging/stg_watch_events.sql
SELECT
    CAST(ts AS TIMESTAMP) AS ts,
    COALESCE(TRIM(video), 'NA') AS video,
    CAST(duration AS FLOAT64) AS duration,
    COALESCE(TRIM(level), 'NA') AS level,
    ...
FROM source_data
WHERE ts IS NOT NULL
```

### Benefits:
- **Resilience to Missing Fields**: If upstream temporarily sends `NULL` for `video`, downstream joins receive `'NA'` rather than breaking join semantics.
- **Explicit Projection**: Core facts and marts explicitly project columns from staging views, preventing accidental wide-column proliferation.

---

## 5. Schema Evolution Deployment Workflow

```
[ Upstream Producer PR ] ──► [ Run Schema Drift Check (`scripts/check_schema.py`) ]
                                        │
                                        ▼
                             [ Compatible Change? ]
                                  ├── Yes: Update `schema.py` & `stg_*.sql` ──► Deploy
                                  └── No (Breaking): Execute Phased Migration:
                                        1. Add alias in `stg_*.sql`
                                        2. Backfill historical records
                                        3. Deprecate legacy column
```

