# Phase 6 Performance Baseline & Scalability Audit

## Executive Summary
This document establishes the performance, throughput, and scalability baseline for the **Adaptive Ads** data engineering platform. It identifies architectural strengths, bottlenecks at scale, data ingestion and warehouse risks, and concrete optimization opportunities across the ELT lifecycle.

> [!NOTE]
> **Runtime Benchmark Status**: Runtime benchmark metrics (such as exact BigQuery bytes scanned and slot-ms execution latency) are unavailable locally because live BigQuery execution requires GCP credentials. The analysis below is based on static AST parsing, query plan evaluation, data warehouse modeling principles, and verified repository specifications.

---

## 1. Current Architecture Overview

```
GCS Raw Parquet Landing (Hourly Partitions)
       │
       ▼
Airflow TaskGroups (4 Parallel Streams)
       │
       ▼
BigQuery Staging Layer (`adaptive_ads_stg`)
  ├── Tables: `watch_events`, `ad_events`, `page_view_events`, `auth_events`
  └── Partitioned by: `ts` (HOUR granularity)
       │
       ▼
dbt Transformations (`adaptive_ads`)
  ├── Core Dimensions (Tables): `dim_users` (SCD2), `dim_movies`, `dim_location`, `dim_datetime`
  ├── Core Facts (Incremental Tables): `fact_streams`, `fact_ad_events` (Partitioned by DAY on `ts`, clustered)
  └── Analytical Marts (Tables/Views): `daily_ad_metrics`, `daily_user_engagement`, `ad_content_performance`, `wide_streams`
       │
       ▼
Looker Studio BI Dashboard Specification
```

---

## 2. Ingestion & Transformation Inventory

| Layer | Object / Model | Materialization | Partitioning | Clustering | Expected Grain |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Staging Ingestion** | `watch_events` | Table | `ts` (HOUR) | None | 1 row per watch telemetry event |
| **Staging Ingestion** | `ad_events` | Table | `ts` (HOUR) | None | 1 row per ad exposure/click |
| **Staging Ingestion** | `page_view_events` | Table | `ts` (HOUR) | None | 1 row per navigation event |
| **Staging Ingestion** | `auth_events` | Table | `ts` (HOUR) | None | 1 row per auth/login event |
| **dbt Staging** | `stg_*` (6 models) | View | None (inherits source) | None | 1:1 cleaned source mapping |
| **dbt Core** | `dim_users` | Table | None (dim) | `userId` | 1 row per user subscription validity window (SCD2) |
| **dbt Core** | `dim_movies` | Table | None (dim) | `movieId` | 1 row per IMDb movie title |
| **dbt Core** | `dim_location` | Table | None (dim) | `stateCode`, `city` | 1 row per geographic coordinate set |
| **dbt Core** | `dim_datetime` | Table | None (dim) | None | 1 row per calendar hour |
| **dbt Core** | `fact_streams` | Incremental | `ts` (DAY) | `userKey`, `videoKey`, `locationKey` | 1 row per completed stream event |
| **dbt Core** | `fact_ad_events` | Incremental | `ts` (DAY) | `userKey`, `adType`, `videoKey` | 1 row per ad interaction event |
| **dbt Marts** | `daily_ad_metrics` | Table | `ad_date` (DAY) | `adType`, `videoKey` | 1 row per calendar date, adType, videoKey |
| **dbt Marts** | `daily_user_engagement`| Table | `activity_date` (DAY)| `subscription_tier` | 1 row per calendar date, subscription tier |
| **dbt Marts** | `ad_content_performance`| Table | None (aggregated) | `adType`, `content_genre` | 1 row per videoKey and adType |
| **dbt Marts** | `wide_streams` | View | None (view on fact) | None | 1 row per stream event (reporting grain) |

---

## 3. Current Bottlenecks & Scaling Risks

### 1. Ingestion Bottlenecks
- **External Table Overhead**: Ingestion currently creates transient external tables pointing to GCS paths for each hourly batch. At 10x–100x partition volume, creating and deleting hundreds of external tables introduces API rate-limit bottlenecks on the Google Cloud BigQuery Control Plane.
- **Airflow Worker Saturation**: Airflow executes parallel TaskGroups using standard Celery or Local workers. Without connection pooling or TaskGroup concurrency caps, simultaneous ingestion of 20+ event streams could exhaust worker slots.

### 2. Warehouse & Transformation Bottlenecks
- **Surrogate Key Generation in BigQuery**: Models utilize `dbt_utils.surrogate_key` (MD5 hashing across concatenated strings). While deterministic and fast, hashing wide rows at 100M+ scale consumes significant BigQuery slot compute.
- **`dim_users` SCD Type 2 Full Scan**: `dim_users` scans the entire historical `stg_watch_events` staging view using window functions (`LAG`, `SUM(grouped)`, `LEAD`, `RANK`) to reconstruct subscription states. As staging tables grow to millions of rows, recomputing the entire SCD2 history on every hourly run becomes computationally prohibitive.
- **Unrestricted `SELECT *` Projections**: Intermediate CTEs in core facts and dimensions previously selected all columns from staging models rather than explicitly projecting required fields, causing unnecessary byte reads during join planning.

### 3. Query & Cost Risks
- **Late-Arriving Lookback Window Scan**: `fact_streams` and `fact_ad_events` use a 3-day sliding lookback window (`WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})`). While this ensures correctness for late telemetry, on large tables it forces BigQuery to scan the last 3 full partition days on every hourly execution.
- **Marts Materialization Strategy**: Marts like `daily_ad_metrics` are materialized as full tables rather than incremental partitions, recomputing historical aggregated metrics on every dbt run.

---

## 4. Optimization Opportunities Identified

1. **Explicit Column Projection (SELECT * Elimination)**:
   - Restrict column lists in core fact and dimension CTEs to only referenced fields.
2. **Partition Pruning Safeguards**:
   - Ensure all downstream BI and transformation queries apply strict partition filters on `ts` / `ad_date` / `activity_date`.
3. **Optimized Incremental Merge Predicates**:
   - Configure dbt incremental predicates to bound the BigQuery `MERGE` statement to only the active lookback partition range.
4. **Lightweight Operational & Schema Tooling**:
   - Deploy `scripts/check_schema.py` to detect schema drift before ingestion.
   - Deploy `scripts/backfill.py` to safely reprocess targeted partition ranges without triggering full-warehouse rebuilds.
5. **Architectural Scaling Trajectory (10x → 100x → 1000x)**:
   - Define exact scaling thresholds where micro-batching transitions into continuous streaming (Pub/Sub + Dataflow).

