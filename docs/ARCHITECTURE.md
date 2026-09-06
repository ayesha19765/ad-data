# System Architecture: Adaptive Ads Data Engineering Platform

## 1. Problem Statement & Business Context

In digital advertising and streaming media platforms, understanding ad performance, viewer engagement, and audience reach requires processing high-velocity event telemetry across multiple domains:
- **Video Playback Events** (`watch_events`): Track watch durations, content titles, and user playback sessions.
- **Advertising Impressions & Interactions** (`ad_events`): Track ad formats, exposures, and interaction completion.
- **User Navigation & Authentication** (`page_view_events`, `auth_events`): Track session health and subscriber tiers.

The **Adaptive Ads Data Engineering Platform** solves these challenges by providing an end-to-end, production-grade ELT data pipeline orchestrated with **Apache Airflow**, modeled with **dbt**, and queried on **Google BigQuery**.

---

## 2. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Data_Sources["1. Raw Event Telemetry (GCS Landing Zone)"]
        W_GCS["gs://.../watch_events/"]
        A_GCS["gs://.../ad_events/"]
        P_GCS["gs://.../page_view_events/"]
        AU_GCS["gs://.../auth_events/"]
        IMDB_GCS["IMDb Content Catalog Seeds"]
    end

    subgraph Airflow_Orchestration["2. Orchestration Layer (Apache Airflow)"]
        subgraph TG1["TaskGroup: ingest_watch_events"]
            W1["Create External Table"] --> W2["Ensure Staging Table"] --> W3["Idempotent Partition Load"] --> W4["Drop External Table"]
        end
        subgraph TG2["TaskGroup: ingest_ad_events"]
            A1["Create External Table"] --> A2["Ensure Staging Table"] --> A3["Idempotent Partition Load"] --> A4["Drop External Table"]
        end
        subgraph TG3["TaskGroup: ingest_page_view_events"]
            P1["Create External Table"] --> P2["Ensure Staging Table"] --> P3["Idempotent Partition Load"] --> P4["Drop External Table"]
        end
        subgraph TG4["TaskGroup: ingest_auth_events"]
            AU1["Create External Table"] --> AU2["Ensure Staging Table"] --> AU3["Idempotent Partition Load"] --> AU4["Drop External Table"]
        end
        
        DBT_SEED_TASK["dbt Seed Task (state_codes)"]
        DBT_RUN_TASK["dbt Run Task (Dims, Facts & Marts)"]
        DBT_TEST_TASK["dbt Test Task (Data Quality Gates)"]
    end

    subgraph BigQuery_Warehouse["3. BigQuery Dimensional Data Warehouse"]
        subgraph Staging_Layer["Staging Views (adaptive_ads_stg)"]
            STG_W["stg_watch_events"]
            STG_A["stg_ad_events"]
            STG_P["stg_page_view_events"]
            STG_AU["stg_auth_events"]
            STG_M["stg_movies"]
            STG_SC["stg_state_codes"]
        end

        subgraph Core_Layer["Core Star Schema (adaptive_ads_prod)"]
            DIM_U["dim_users (SCD Type 2)"]
            DIM_M["dim_movies (Content)"]
            DIM_L["dim_location (Geographic)"]
            DIM_D["dim_datetime (Time Series)"]
            FACT_S["fact_streams (Incremental / Partitioned)"]
            FACT_A["fact_ad_events (Incremental / Partitioned)"]
        end

        subgraph Marts_Layer["Analytical Marts & Reporting"]
            MART_AD["daily_ad_metrics"]
            MART_CONTENT["ad_content_performance"]
            MART_ENG["daily_user_engagement"]
            MART_WIDE["wide_streams (BI View)"]
        end
    end

    subgraph BI_Reporting["4. Analytics & BI Layer"]
        LOOKER["Looker Studio Executive Dashboard"]
    end

    %% Pipeline Connections
    W_GCS --> TG1
    A_GCS --> TG2
    P_GCS --> TG3
    AU_GCS --> TG4
    IMDB_GCS --> DBT_SEED_TASK

    TG1 & TG2 & TG3 & TG4 --> DBT_SEED_TASK --> DBT_RUN_TASK --> DBT_TEST_TASK

    TG1 --> STG_W
    TG2 --> STG_A
    TG3 --> STG_P
    TG4 --> STG_AU

    STG_W & STG_A & STG_M & STG_SC --> Core_Layer
    Core_Layer --> Marts_Layer
    Marts_Layer --> LOOKER
```

---

## 3. Storage & Data Modeling Architecture

The warehouse follows a 4-tier layer separation:
1. **Raw / Landing Zone**: Immutable hourly Parquet files stored in GCS (`gs://.../<event>/month=M/day=D/hour=H/*`).
2. **Staging Layer (`adaptive_ads_stg`)**: Lightly cleaned, casted, standardized views and partition tables populated by Airflow.
3. **Core Warehouse (`adaptive_ads_prod`)**: Kimball dimensional star schema:
   - Dimensions: `dim_users` (SCD Type 2), `dim_movies`, `dim_location`, `dim_datetime`.
   - Facts: `fact_streams`, `fact_ad_events` (incremental day-partitioned tables with multi-column clustering).
4. **Analytical Marts (`adaptive_ads_prod`)**: Aggregated, dashboard-ready summary tables and reporting views (`daily_ad_metrics`, `ad_content_performance`, `daily_user_engagement`, `wide_streams`).

---

## 4. Key Engineering Decisions & Trade-Offs

| Decision | Implemented Approach | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Orchestration Structure** | Airflow TaskGroups in unified hourly DAG | Multiple standalone DAGs per event | Synchronizes hourly batch execution, reduces scheduling overhead, and provides clear fan-out/fan-in observability. |
| **Ingestion Idempotency** | Partition-scoped `DELETE` before `INSERT` | Append-only raw loads | Prevents row duplication on task retries and backfills without needing expensive global table deduplication. |
| **Fact Materialization** | Incremental `merge` on surrogate keys with 3-day lookback | Full-table rebuilds (`table`) | Minimizes BigQuery slot consumption while automatically accommodating late-arriving events. |
| **Partitioning & Clustering** | Day-partitioned on `ts`, clustered on high-cardinality keys | Unpartitioned tables | Enables partition pruning and colocation, accelerating query performance and reducing scan costs. |

