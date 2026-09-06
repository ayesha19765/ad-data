# Architecture Quick Reference & Cheat Sheet

## 1. Core Architecture Matrix

| Layer / Component | Technology | Primary Purpose | Key Configuration / Code File | Architectural Justification |
| :--- | :--- | :--- | :--- | :--- |
| **Telemetry Ingestion** | Google Cloud Storage (GCS) | Durable, low-cost raw object storage for columnar event dumps | Bucket path: `raw/{event}/YYYY/MM/DD/HH/` | High throughput, immutable raw event lake, decoupling ingestion from warehouse compute. |
| **Pipeline Orchestration** | Apache Airflow 2.8+ | Deterministic batch DAG scheduling, retries, and task dependency graph | [`airflow/dags/adaptive_ads_dag.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/adaptive_ads_dag.py) | Dynamic TaskGroup generation from [`event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py), partition-scoped idempotency. |
| **Staging Ingestion** | Google BigQuery | Ephemeral raw staging tables loaded atomically via URI | [`airflow/dags/sql/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/sql/) | Serverless scalable SQL engine, zero-management storage, native Parquet parsing. |
| **Transformation & Modeling** | dbt (data build tool) Core | SQL-first dimensional modeling, incremental merges, and schema testing | [`dbt/dbt_project.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/dbt_project.yml) | Version-controlled DAG modeling, built-in testing pyramid, automated lineage documentation. |
| **Slowly Changing Dimensions** | dbt / SQL Window Functions | SCD Type 2 historical state tracking for user subscription tiers | [`dbt/models/core/dim_users.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_users.sql) | Pure SQL `LAG`, `LEAD`, and `SUM(is_new_state)` over user events without brittle procedural code. |
| **Fact Tables** | dbt Incremental Models | High-throughput streaming facts with 3-day sliding lookback windows | [`dbt/models/core/fact_*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_ad_events.sql) | Incremental merge with `incremental_predicates` preventing full warehouse table rescans. |
| **Marts Layer** | BigQuery Clustered Tables | Pre-aggregated dimensional cubes optimized for BI query latency | [`dbt/models/marts/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/marts/) | Sub-second dashboard queries, eliminated repetitive runtime joins, cost minimization. |
| **BI & Visualization** | Looker Studio | Executive KPI dashboards, ad monetization analytics, audience metrics | [`docs/BI_DASHBOARD.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/BI_DASHBOARD.md) | Native BigQuery BI Engine integration, zero-infrastructure dashboard delivery. |
| **CI / CD Quality Gates** | GitHub Actions | Automated linting, static analysis, unit tests, and contract verification | [`.github/workflows/ci.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/.github/workflows/ci.yml) | Blocks broken SQL, schema drift, invalid Airflow configs, or secret leaks before merge. |

---

## 2. Telemetry Ingestion Streams

| Event Stream | Grain | Natural Key | Ingestion Schedule | Destination Staging Table | Core Model |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`watch_events`** | 1 record per movie playback chunk | `(userId, movieId, eventTimestamp)` | Hourly (`@hourly`) | `staging.stg_watch_events` | `core.fact_streams` |
| **`ad_events`** | 1 record per ad impression/click | `(adId, userId, eventTimestamp)` | Hourly (`@hourly`) | `staging.stg_ad_events` | `core.fact_ad_events` |
| **`page_view_events`**| 1 record per UI navigation | `(userId, pageUrl, eventTimestamp)` | Hourly (`@hourly`) | `staging.stg_page_views` | `marts.user_journey` |
| **`auth_events`** | 1 record per user login/tier change | `(userId, eventTimestamp)` | Hourly (`@hourly`) | `staging.stg_auth_events` | `core.dim_users` (SCD2) |

---

## 3. Storage & Processing Topology

```
Raw Telemetry (GCS)
   └── gs://adaptive-ads/raw/{event}/YYYY/MM/DD/HH/*.parquet
         │
         ▼ (Airflow Atomic Partition Insert)
BigQuery Staging Layer (`staging` dataset)
   └── `stg_watch_events`, `stg_ad_events`, `stg_page_views`, `stg_auth_events`
         │
         ▼ (dbt run - SCD2 & Incremental Merges)
BigQuery Core Warehouse Layer (`core` dataset)
   ├── Dimensions: `dim_users` (SCD2), `dim_movies`, `dim_location`
   └── Facts: `fact_streams`, `fact_ad_events` (Partitioned on `eventDate`, Clustered)
         │
         ▼ (dbt run - Analytical Mart Rollups)
BigQuery Analytical Marts Layer (`marts` dataset)
   ├── `daily_ad_metrics`
   ├── `user_engagement_summary`
   └── `campaign_performance_cube`
         │
         ▼ (Direct Query / BI Engine)
Looker Studio Dashboards
```

