# End-to-End Data Flow & Event Tracing

## 1. System Data Flow Architecture

The **Adaptive Ads** data platform processes four high-throughput streaming/batch telemetry event sources through an automated, idempotent ELT architecture on Google Cloud Platform:

```
+-----------------------------------------------------------------------------------+
| 1. INGESTION & STORAGE                                                            |
|                                                                                   |
|  Event Sources               Cloud Storage (GCS)             BigQuery Staging     |
|  +--------------------+      +-----------------------+       +------------------+ |
|  | watch_events       | ---> | gs://bucket/raw/      | ----> | stg_watch_events | |
|  | ad_events          |      |   {event}/YYYY/MM/DD/ | (URI) | stg_ad_events    | |
|  | page_view_events   |      |   HH/*.parquet        |       | stg_page_views   | |
|  | auth_events        |      +-----------------------+       | stg_auth_events  | |
|  +--------------------+                                      +------------------+ |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 2. ORCHESTRATION & TRANSFORMATION (Airflow + dbt)                                 |
|                                                                                   |
|  Airflow DAG (`adaptive_ads_dag.py`)                                              |
|  Parallel TaskGroups -> BigQuery Partition Deletion -> External Table Ingestion   |
|                                                                                   |
|  dbt Transformation Pipeline (`dbt run`):                                         |
|  - Staging Views (`stg_*.sql`): 1:1 Clean Schema Representation                   |
|  - Core Dimensions: `dim_users` (SCD Type 2), `dim_movies`, `dim_location`        |
|  - Core Facts (Incremental Merge): `fact_streams`, `fact_ad_events`               |
|  - Marts (Aggregations): `daily_ad_metrics`, `user_engagement_summary`, etc.      |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 3. ANALYTICS & BUSINESS INTELLIGENCE                                              |
|                                                                                   |
|  Looker Studio BI Dashboards & Ad-Hoc Analytics                                   |
|  - Monetization & Yield Analysis (eCPM, CTR, Revenue, Fill Rate)                  |
|  - Content & User Streaming Performance (Tier distribution, Watch completion)     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Step-by-Step Data Progression

| Step | Stage | Technology / Component | Operation | Data Representation |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Generation** | Telemetry Emitters / Edge Clients | App user clicks ad or streams movie | Raw JSON / Protobuf telemetry payloads |
| **2** | **Landing** | Google Cloud Storage (GCS) | Batched landing into partitioned paths | Snappy-compressed columnar Parquet files |
| **3** | **Orchestration** | Apache Airflow (`adaptive_ads_dag.py`) | Triggers hourly interval DAG via dynamic `EVENT_CONFIG` | Airflow execution context (`ds`, `ts_nodash`) |
| **4** | **Staging Ingestion** | BigQuery Engine (`task_templates.py`) | Atomically deletes target partition, inserts via URI | BigQuery raw staging tables (`raw_stg.*`) |
| **5** | **Dimensional Modeling** | dbt (`dim_users.sql`) | Window functions detect attribute changes and generate SCD2 ranges | Dimension tables (`core.dim_users`, etc.) |
| **6** | **Fact Processing** | dbt (`fact_ad_events.sql`) | Incremental `merge` with 3-day sliding lookback | Partitioned & clustered fact tables (`core.fact_*`) |
| **7** | **Marts Aggregation** | dbt (`daily_ad_metrics.sql`) | Rollups by date, campaign, and user subscription tier | Analytical marts (`marts.*`) |
| **8** | **BI Consumption** | Looker Studio / BI Engine | Queries pre-aggregated marts using BI Engine acceleration | Interactive KPI scorecards and trend visualizations |

---

## 3. "Follow One Event": Complete Code Trace

Let us trace a single **`watch_event`** (`eventType = "playback_complete"`, `userId = "usr_9482"`, `movieId = "mov_401"`, `device = "smart_tv"`) through every physical file in the repository.

### Step 1: Raw Parquet Landing
- **File Path**: `gs://adaptive-ads-telemetry-prod/raw/watch_events/2026/09/01/14/part-0001.parquet`
- **Schema**: Enforced by [`contracts/watch_events.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/watch_events.yml).
- **Payload**:
  ```json
  {
    "event_id": "evt_98319a28",
    "user_id": "usr_9482",
    "movie_id": "mov_401",
    "event_timestamp": "2026-09-01 14:22:10 UTC",
    "watch_duration_seconds": 3600,
    "playback_status": "completed",
    "device_type": "smart_tv",
    "country_code": "US"
  }
  ```

### Step 2: Airflow DAG Scheduling & TaskGroup Trigger
- **File**: [`airflow/dags/adaptive_ads_dag.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/adaptive_ads_dag.py)
- **Mechanism**: The DAG resolves the execution interval for `2026-09-01T14:00:00` and creates dynamic `TaskGroup("watch_events")` based on [`airflow/dags/event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py).
- **Code Execution**:
  ```python
  # airflow/dags/task_templates.py
  def create_event_ingestion_taskgroup(event_name: str, config: EventConfig) -> TaskGroup:
      with TaskGroup(group_id=f"ingest_{event_name}") as tg:
          delete_partition = BigQueryInsertJobOperator(...)
          load_stg = GCSToBigQueryOperator(...)
  ```

### Step 3: Atomic Partition Deletion & Staging Load
- **File**: [`airflow/dags/sql/load_stg_watch_events.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/sql/load_stg_watch_events.sql)
- **SQL Execution**:
  ```sql
  DELETE FROM `adaptive-ads.staging.stg_watch_events`
  WHERE partition_hour = TIMESTAMP('{{ execution_date }}');

  INSERT INTO `adaptive-ads.staging.stg_watch_events`
  SELECT * FROM external_table_gcs_uri;
  ```

### Step 4: Staging View Cleansing
- **File**: [`dbt/models/staging/stg_watch_events.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/staging/stg_watch_events.sql)
- **Transformation**: Casts datatypes, standardizes column naming, and validates timestamps.
  ```sql
  SELECT
      CAST(event_id AS STRING) AS eventId,
      CAST(user_id AS STRING) AS userId,
      CAST(movie_id AS STRING) AS movieId,
      TIMESTAMP(event_timestamp) AS eventTimestamp,
      CAST(watch_duration_seconds AS INT64) AS watchDurationSeconds,
      CAST(device_type AS STRING) AS deviceType
  FROM {{ source('staging', 'stg_watch_events') }}
  ```

### Step 5: SCD Type 2 Dimension Lookup
- **File**: [`dbt/models/core/dim_users.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_users.sql)
- **Logic**: Resolves whether user `usr_9482` was on the `Free` or `Premium` tier at `2026-09-01 14:22:10 UTC`.
- **Generated Record**: Returns surrogate key `userKey = MD5("usr_9482-Free-2026-08-01")`.

### Step 6: Fact Table Incremental Merge
- **File**: [`dbt/models/core/fact_streams.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_streams.sql)
- **Logic**: Incremental merge matches on `streamKey` (`MD5(userId || movieId || eventTimestamp)`).
- **Execution**:
  ```sql
  {{ config(
      materialized='incremental',
      unique_key='streamKey',
      incremental_strategy='merge',
      partition_by={'field': 'eventDate', 'data_type': 'date'}
  ) }}
  SELECT
      {{ dbt_utils.generate_surrogate_key(['w.userId', 'w.movieId', 'w.eventTimestamp']) }} AS streamKey,
      u.userKey,
      m.movieKey,
      w.watchDurationSeconds,
      DATE(w.eventTimestamp) AS eventDate
  FROM {{ ref('stg_watch_events') }} w
  LEFT JOIN {{ ref('dim_users') }} u
    ON w.userId = u.userId
   AND w.eventTimestamp BETWEEN u.rowActivationDate AND u.rowExpirationDate
  ```

### Step 7: Mart Rollup
- **File**: [`dbt/models/marts/user_engagement_summary.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/marts/user_engagement_summary.sql)
- **Logic**: Aggregates total watch hours and active stream counts partitioned by date and subscription tier for executive BI reporting.

