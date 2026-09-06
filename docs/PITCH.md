# Elevator Pitches & Technical Walkthroughs

Use these tailored walkthrough scripts during technical interviews based on the time allotted by the interviewer.

---

## 30-Second Version (Executive Elevator Pitch)
> *"I built Adaptive Ads, an end-to-end data engineering platform on Google Cloud that ingests, models, and analyzes high-velocity advertising and video streaming telemetry. Orchestrated by Apache Airflow with decoupled parallel TaskGroups, the pipeline loads raw Parquet files into BigQuery using partition-scoped idempotent replacements. It transforms raw telemetry into an enterprise Kimball star schema featuring SCD Type 2 user dimensions, incremental fact tables with 3-day late-arriving lookbacks, and Looker-ready analytical marts using dbt, backed by automated CI/CD validation and declarative data contracts."*

---

## 60-Second Version (Technical Summary)
> *"Adaptive Ads is a production-grade data platform designed to process four concurrent telemetry streams—video playback, ad impressions, page views, and user authentications.
> 
> To prevent stream coupling, I designed a dynamic Airflow orchestration layer driven by a centralized configuration registry that generates parallel TaskGroups. Ingestion into BigQuery staging tables uses an atomic partition-scoped delete-and-insert pattern, guaranteeing exact-once state on retries and backfills.
> 
> In the warehouse layer, dbt builds a Kimball dimensional model: an SCD Type 2 user dimension tracking subscriber transitions with zero-gap intervals, and incremental fact tables that leverage BigQuery Day-level partitioning, multi-column clustering, and sliding 3-day lookback windows for late-arriving mobile data.
> 
> Finally, pre-aggregated analytical marts feed Looker Studio dashboards, delivering sub-second reporting at minimal query scan cost."*

---

## 2-Minute Version (Architecture & Engineering Decisions)
> *"Adaptive Ads solves the challenge of analyzing advertising delivery, audience engagement, and content reach across streaming platforms with millions of events per hour.
> 
> **Architecture Overview**:
> 1. **Ingestion**: Columnar Parquet telemetry lands hourly in Cloud Storage. An Airflow master DAG uses dynamic TaskGroups to load all four streams in parallel. By executing partition-scoped atomic deletes prior to insert, we ensure strict idempotency without risk of duplicate records during retries or backfills.
> 2. **Transformation & Warehouse**: Using dbt inside BigQuery, raw data passes through staging views into a Kimball star schema. Our `dim_users` table implements SCD Type 2 logic using SQL window functions (`LAG`, `SUM`, `LEAD`, `RANK`) to capture subscription tier changes over time.
> 3. **Incremental Optimization**: Fact tables (`fact_streams`, `fact_ad_events`) are partitioned by Day and clustered on high-cardinality keys (`userKey`, `adType`, `videoKey`). We implement a 3-day sliding lookback window combined with `incremental_predicates` to reconcile late-arriving mobile telemetry without scanning historical warehouse partitions.
> 4. **Marts & BI**: We pre-aggregate metrics into daily and content-level marts, using `SAFE_DIVIDE` and `COALESCE` to eliminate divide-by-zero errors. Looker Studio connects directly to these marts.
> 5. **Quality & Reliability**: The pipeline is hardened with declarative YAML data contracts, 19 Python unit tests, singular dbt business assertions, and a 5-job GitHub Actions CI pipeline."*

---

## 5-Minute Version (Full Technical Walkthrough)
Use this structure when asked: *"Walk me through your data engineering project in detail."*
1. **Problem Context**: Streaming telemetry characteristics (high volume, decoupled streams, late-arriving mobile events, subscription tier drift).
2. **Ingestion & Dynamic Airflow Design**: Explain `EVENT_CONFIG` registry, TaskGroup factory, transient external table creation, and atomic partition replacement.
3. **Data Modeling Deep Dive**:
   - Staging views (`stg_*`) for type casting and null coalescence.
   - Kimball star schema: Conformed dimensions (`dim_movies`, `dim_location`, `dim_datetime`).
   - SCD Type 2 `dim_users`: Step through how `LAG` identifies tier changes, `SUM(lagged)` groups intervals, `LEAD` computes `rowExpirationDate`, and `RANK` flags `currentRow`.
4. **Incremental Optimization & BigQuery Pruning**: Explain Day-level partitioning, clustering rationale, 3-day lookback window (`WHERE ts >= MAX(ts) - 3 DAY`), and `incremental_predicates` bounding the `MERGE` target scan space.
5. **Analytical Marts & BI**: Explain `daily_ad_metrics` and `ad_content_performance`, derived rate calculations, and Looker Studio dashboard specification.
6. **Testing, Governance & Disaster Recovery**: Explain the 4-tier testing pyramid, declarative data contracts (`contracts/*.yml`), PII surrogate key isolation, BigQuery 7-day Time Travel, and the `scripts/backfill.py` utility.

---

## 10-Minute Deep Dive (Architecture Defense & Whiteboard)
Follow the [Architecture Cheat Sheet](ARCHITECTURE_CHEATSHEET.md) and [System Architecture](ARCHITECTURE.md) to whiteboard the complete data flow, defend technology trade-offs (Airflow vs Prefect, BigQuery vs Snowflake, Batch vs Streaming), and walk through the [Disaster Recovery Playbooks](DISASTER_RECOVERY.md).

