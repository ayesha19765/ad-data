# Subsystem Revision Guide: Apache Airflow Orchestration

## 1. Core Architecture & DAG Structure

The pipeline is orchestrated by a single parameterized Airflow DAG located in [`airflow/dags/adaptive_ads_dag.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/adaptive_ads_dag.py).

```
                      [start_pipeline]
                             │
     ┌───────────────────────┼───────────────────────┬───────────────────────┐
     ▼                       ▼                       ▼                       ▼
[ingest_watch_events] [ingest_ad_events] [ingest_page_views] [ingest_auth_events]
 (TaskGroup: Delete   (TaskGroup: Delete  (TaskGroup: Delete  (TaskGroup: Delete
  & Load Staging)      & Load Staging)     & Load Staging)     & Load Staging)
     └───────────────────────┼───────────────────────┴───────────────────────┘
                             │
                             ▼
                    [trigger_dbt_run]
                     (BashOperator)
                             │
                             ▼
                    [trigger_dbt_test]
                     (BashOperator)
                             │
                             ▼
                       [end_pipeline]
```

---

## 2. Dynamic TaskGroup Generation & `EVENT_CONFIG`

Instead of copy-pasting DAG tasks for each event stream, the pipeline uses a centralized configuration dictionary defined in [`airflow/dags/event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py) and instantiated via [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py).

### Configuration Schema
Each event stream in `EVENT_CONFIG` defines:
- `event_name`: Canonical event identifier (`watch_events`, `ad_events`, etc.).
- `gcs_prefix`: Target GCS folder path (`raw/{event_name}/...`).
- `staging_table`: BigQuery destination table (`stg_{event_name}`).
- `partition_column`: Hourly timestamp column for partition filtering.
- `sql_template`: Dedicated SQL load template (`airflow/dags/sql/load_stg_{event_name}.sql`).

```python
# airflow/dags/task_templates.py
def create_event_ingestion_taskgroup(event_name: str, config: EventConfig) -> TaskGroup:
    with TaskGroup(group_id=f"ingest_{event_name}") as tg:
        delete_existing = BigQueryInsertJobOperator(
            task_id="delete_existing_partition",
            configuration={
                "query": {
                    "query": f"DELETE FROM `{config.staging_table}` WHERE {config.partition_column} = '{{{{ execution_date }}}}';",
                    "useLegacySql": False,
                }
            }
        )
        load_parquet = GCSToBigQueryOperator(
            task_id="load_parquet_to_staging",
            bucket="{{ var.value.gcs_bucket }}",
            source_objects=[f"{config.gcs_prefix}/{{{{ execution_date.strftime('%Y/%m/%d/%H') }}}}/*.parquet"],
            destination_project_dataset_table=config.staging_table,
            source_format="PARQUET",
            write_disposition="WRITE_APPEND"
        )
        delete_existing >> load_parquet
    return tg
```

---

## 3. Key Operational Parameters

| Setting | Value | Rationale |
| :--- | :--- | :--- |
| **`schedule_interval`** | `@hourly` (`0 * * * *`) | Meets 1-hour analytical data freshness SLA. |
| **`catchup`** | `False` | Prevents runaway DAG runs upon initialization; backfills run via [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py). |
| **`max_active_runs`** | `1` | Enforces serialized execution of dbt transformations to avoid table write locks. |
| **`retries`** | `2` | Handles transient GCP networking timeouts. |
| **`retry_delay`** | `timedelta(minutes=5)` | Allows upstream GCS syncs or BigQuery slot contention to clear. |
| **`execution_timeout`**| `timedelta(minutes=30)` | Automatically kills stuck tasks, freeing Airflow worker slots. |

---

## 4. Idempotency & Replay Guarantee

1. **Jinja Templating**: Tasks reference `{{ execution_date }}` and `{{ ds }}` for deterministic time slice targeting.
2. **Partition Isolation**: Every ingestion task purges only its specific execution hour partition before appending new Parquet data.
3. **Safe Re-runs**: Triggering a manual rerun for a past hour cleanly replaces only that hour's staging partition without touching adjacent data.

