# The Complete Project Story: Adaptive Ads

## 1. The Business Problem
In ad-supported digital streaming platforms (like Hulu, Spotify, YouTube), revenue depends on accurately measuring ad impressions, viewer engagement, and conversion rates across different subscription tiers (`free` vs `paid`).
However, streaming telemetry is challenging:
- **Massive Event Volume**: Millions of video playback and ad interaction events are generated every hour.
- **Independent Telemetry Streams**: Playback events (`watch_events`), ad impressions (`ad_events`), page navigation (`page_view_events`), and user logins (`auth_events`) arrive concurrently from web, mobile, and connected TV clients.
- **Late-Arriving Telemetry**: Mobile devices buffer events offline, resulting in events arriving hours or days late.
- **Subscription Drift**: Users constantly upgrade, downgrade, or cancel subscriptions, requiring historical attribution truth without corrupting past revenue metrics.

---

## 2. Engineering Requirements
1. **Decoupled Parallel Ingestion**: Ingest independent event streams in parallel so a failure in ad tracking does not block video streaming metrics.
2. **Strict Idempotency**: Ensure that retries, worker crashes, and backfills never create duplicate records.
3. **Historical Accuracy (SCD Type 2)**: Track user subscription tier transitions across time with non-overlapping validity ranges.
4. **Late-Arriving Reconciliation**: Reconcile delayed telemetry into historical warehouse partitions without scanning full historical tables.
5. **Cost-Efficient Warehouse Modeling**: Organize BigQuery storage with Day partitioning and high-selectivity clustering to keep query costs low.
6. **Executive BI Reporting**: Provide pre-aggregated analytical marts for sub-second Looker Studio dashboard queries.

---

## 3. End-to-End Architectural Narrative

```
[ Telemetry Producers ] ──► [ GCS Landing (Parquet) ] ──► [ Airflow (Parallel TaskGroups) ]
                                                                       │
                                                                       ▼
[ Looker Studio BI ] ◄── [ dbt Marts ] ◄── [ dbt Kimball Core ] ◄── [ BigQuery Staging ]
```

### Stage 1: Ingestion & Landing (GCS)
Event streams land in Google Cloud Storage as columnar Parquet files organized into hourly directory partitions: `gs://<bucket>/<stream>/month=M/day=D/hour=H/`.

### Stage 2: Orchestration (Apache Airflow 2.8.1)
The master DAG `adaptive_ads_dag` runs hourly at `05` minutes past the hour. Rather than hardcoding separate DAGs or monolithic tasks, a centralized configuration dictionary (`EVENT_CONFIG`) dynamically generates isolated, parallel **TaskGroups** for all four streams.

### Stage 3: Staging & Idempotent Ingestion (BigQuery)
Each TaskGroup spins up a transient external table pointing to GCS, executes a partition-scoped atomic `DELETE` + `INSERT` into `adaptive_ads_stg.<stream>`, and drops the external table. This guarantees exact-once state regardless of retries.

### Stage 4: Staging Views (`stg_*`)
dbt staging views standardize raw staging tables by casting datatypes, trimming whitespace, and applying `COALESCE(field, 'NA')` default fallbacks.

### Stage 5: Core Warehouse Transformations (Kimball Star Schema)
dbt transforms staging views into an enterprise dimensional warehouse (`adaptive_ads_prod`):
- **`dim_users` (SCD Type 2)**: Uses window functions (`LAG`, `SUM`, `LEAD`, `RANK`) over user event streams to track subscription transitions with `rowActivationDate`, `rowExpirationDate`, and `currentRow` flags.
- **`dim_movies`, `dim_location`, `dim_datetime`**: Conformed dimensions providing rich descriptive context.
- **`fact_streams` & `fact_ad_events`**: Incremental fact tables partitioned by `DAY` on `ts` and clustered on key slice dimensions. They use a **3-day sliding lookback window** and `incremental_predicates` to merge late-arriving events with zero full-table scans.

### Stage 6: Analytical Marts & Semantic Layer
Pre-aggregated dimensional marts (`daily_ad_metrics`, `ad_content_performance`, `daily_user_engagement`) aggregate millions of fact rows into daily and content-level performance metrics, computing rates safely via `COALESCE(SAFE_DIVIDE(...), 0.0)`.

### Stage 7: Analytics & BI Consumption (Looker Studio)
Executive dashboards query the pre-aggregated marts rather than raw fact tables, delivering sub-second widget render times at near-zero BigQuery scan cost.

### Stage 8: Quality, Governance & Disaster Recovery
The platform is protected by declarative data contracts (`contracts/*.yml`), 19 Python unit tests, 6 singular dbt business tests, 7-day BigQuery Time Travel recovery playbooks, and automated CI/CD quality gates in GitHub Actions.

