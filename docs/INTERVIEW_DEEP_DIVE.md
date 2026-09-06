# 30+ Deep-Dive Data Engineering Interview Questions & Master Solutions

This guide provides deep, evidence-based solutions for advanced technical interviews, system design rounds, and hiring manager discussions.

---

## Section 1: System Architecture & Orchestration (Q1–Q7)

### Q1: Walk me through the end-to-end architecture of this data platform.
**Answer**:
The platform is built on an idempotent ELT pattern on Google Cloud Platform:
1. **Landing**: Client telemetry emitters stream 4 event types (`watch_events`, `ad_events`, `page_view_events`, `auth_events`) as Snappy-compressed Parquet files into partitioned GCS buckets (`gs://bucket/raw/{event}/YYYY/MM/DD/HH/*.parquet`).
2. **Orchestration**: An hourly Airflow DAG (`adaptive_ads_dag.py`) dynamically creates parallel TaskGroups for each stream via `EVENT_CONFIG`. Each TaskGroup performs an atomic partition-scoped `DELETE` on the BigQuery staging table and loads the new Parquet slice.
3. **Transformation**: Airflow triggers `dbt run`, which executes:
   - Staging views (`stg_*`) for schema typing and column standardization.
   - Core dimensions including SCD Type 2 `dim_users`, `dim_movies`, and `dim_location`.
   - Incremental fact tables (`fact_streams`, `fact_ad_events`) using `merge` with 3-day sliding `incremental_predicates`.
   - Analytical marts (`daily_ad_metrics`, `user_engagement_summary`) with pre-calculated KPIs.
4. **Data Quality**: Airflow triggers `dbt test` to execute generic schema tests and singular business SQL tests.
5. **Consumption**: Looker Studio queries analytical marts directly, accelerated by BigQuery BI Engine.

### Q2: How does the dynamic TaskGroup generation work in Airflow?
**Answer**:
In [`airflow/dags/event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py), we define a centralized dictionary `EVENT_CONFIG` mapping event names to their GCS path prefixes, BigQuery destination tables, partition columns, and SQL template paths. In [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py), the function `create_event_ingestion_taskgroup(event_name, config)` instantiates a `TaskGroup` containing a `BigQueryInsertJobOperator` (to delete the partition) upstream of a `GCSToBigQueryOperator` (to load Parquet). The DAG simply loops over `EVENT_CONFIG.items()`, instantiating parallel ingestion groups with zero code duplication.

### Q3: How do you achieve true idempotency during ingestion?
**Answer**:
We avoid naive `WRITE_APPEND` or `WRITE_TRUNCATE`. `WRITE_APPEND` causes duplicates on task retries, while `WRITE_TRUNCATE` wipes out other historical partitions in the staging table. Instead, we use **partition-scoped replacement**:
```sql
DELETE FROM staging.stg_ad_events WHERE partition_hour = TIMESTAMP('{{ execution_date }}');
INSERT INTO staging.stg_ad_events SELECT * FROM external_table;
```
This guarantees that running an hourly Airflow task 1 time or 10 times results in the identical staging partition state.

### Q4: How do you handle Airflow task timeouts and stuck processes?
**Answer**:
Every ingestion operator and bash task defines an explicit `execution_timeout = timedelta(minutes=30)`. If BigQuery suffers from slot contention or GCS listing hangs, Airflow terminates the stuck task, frees worker capacity, and enters the retry cycle (`retries = 2`, `retry_delay = timedelta(minutes=5)`).

### Q5: Why is `max_active_runs = 1` configured on the Airflow DAG?
**Answer**:
While ingestion TaskGroups are partition-isolated, downstream dbt incremental models run warehouse `MERGE` statements on target fact tables. If two consecutive hourly DAG runs execute dbt concurrently, BigQuery table-level transaction locks or concurrency conflicts can occur. Setting `max_active_runs = 1` enforces deterministic serial execution of transformations.

### Q6: How are backfills orchestrated without triggering unwanted historical runs?
**Answer**:
We set `catchup = False` on the Airflow DAG to prevent automatic cascades when DAGs are deployed. For targeted historical backfills, we built a dedicated CLI tool [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py) that validates ISO-8601 start/end timestamps, slices the range into hourly intervals, displays execution dry-runs, and replays partitions safely with partition-scoped deletes.

### Q7: What are the trade-offs of ELT vs. ETL in this pipeline?
**Answer**:
- **ETL (e.g. Spark outside warehouse)**: Moves data out of storage, transforms in an external cluster, writes back. Incurs network transfer costs, cluster spin-up delays, and double storage serialization.
- **ELT (Airflow + BigQuery + dbt)**: Loads raw Parquet directly into BigQuery and leverages BigQuery's MPP SQL engine. Transformations run directly on columnar data with zero network egress, fully declarative SQL, and automated dbt lineage.

---

## Section 2: Data Modeling & Warehouse Engineering (Q8–Q15)

### Q8: Explain the SCD Type 2 implementation in `dim_users.sql`.
**Answer**:
`dim_users` tracks changes in user subscription tiers (e.g., `Free` $\rightarrow$ `Premium`) using pure SQL window functions over `stg_auth_events`:
1. `LAG(subscriptionTier) OVER (PARTITION BY userId ORDER BY eventTimestamp)` identifies when a tier change occurs (`isNewState = 1`).
2. `SUM(isNewState) OVER (PARTITION BY userId ORDER BY eventTimestamp)` creates a monotonic `stateGroup` ID for each contiguous tier interval.
3. `GROUP BY userId, subscriptionTier, countryCode, stateGroup` aggregates each interval, computing `MIN(eventTimestamp)` as `rowActivationDate`.
4. `LEAD(MIN(eventTimestamp)) OVER (PARTITION BY userId ORDER BY MIN(eventTimestamp))` computes `nextActivationDate`.
5. The final projection assigns `rowExpirationDate = COALESCE(nextActivationDate, '9999-12-31 23:59:59')` and `isCurrent = (nextActivationDate IS NULL)`.

### Q9: Why use window functions instead of dbt snapshots for SCD2?
**Answer**:
dbt snapshots are stateful, point-in-time captures taken during scheduled dbt batch executions. If historical data is wiped, backfilled, or replayed from raw event logs, dbt snapshots cannot reconstruct past state history. Our pure SQL window function implementation over immutable event logs is completely deterministic and replayable from day zero at any time.

### Q10: How do incremental fact models handle late-arriving data?
**Answer**:
In [`dbt/models/core/fact_ad_events.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_ad_events.sql), we configure:
```sql
{{ config(
    materialized='incremental',
    unique_key='adEventKey',
    incremental_strategy='merge',
    incremental_predicates=['DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)']
) }}
```
The query looks back 3 days (`TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)`). Any events delayed up to 72 hours are merged into their respective historical date partitions without rebuilding the entire table.

### Q11: Why are `incremental_predicates` necessary in BigQuery dbt merges?
**Answer**:
By default, BigQuery's `MERGE` evaluates the entire historical target table to match `unique_key`. On a table with 3 years of data, every hourly run scans hundreds of gigabytes. Specifying `incremental_predicates` restricts the target partition scan strictly to the last 3 days, reducing scanned bytes and compute costs by up to 90%.

### Q12: How do you prevent surrogate key collisions in distributed warehouses?
**Answer**:
We use `dbt_utils.generate_surrogate_key` which computes MD5 hashes over natural keys (e.g., `userId || movieId || eventTimestamp`). MD5 produces a 128-bit hash with a collision probability of less than $10^{-15}$ across billions of rows. Hashes are computed in parallel across worker nodes without requiring a centralized sequence coordinator lock.

### Q13: What is the purpose and grain of the Marts layer?
**Answer**:
The Marts layer (`marts.daily_ad_metrics`, `marts.user_engagement_summary`) pre-aggregates granular facts into dimensional cubes.
- **Grain**: One row per `(date, campaignId, subscriptionTier)`.
- **Purpose**: Eliminates expensive multi-table joins and window calculations at dashboard query time, delivering sub-second Looker Studio dashboard performance and zero division-by-zero errors via `SAFE_DIVIDE()`.

### Q14: How do fact tables join against SCD2 dimensions?
**Answer**:
Fact tables join on both the natural user ID and the event timestamp:
```sql
LEFT JOIN {{ ref('dim_users') }} u
  ON f.userId = u.userId
 AND f.eventTimestamp >= u.rowActivationDate
 AND f.eventTimestamp < u.rowExpirationDate
```
This ensures each ad interaction is attributed to the exact subscription tier the user held at that microsecond.

### Q15: Why did you eliminate `SELECT *` from core models?
**Answer**:
BigQuery uses Capacitor, a proprietary columnar storage format. When a query uses `SELECT *`, BigQuery reads every single column block from disk across all partitions. Explicitly projecting only the needed columns (e.g. 6 columns instead of 25) directly reduces byte scan volume by over 70%, drastically cutting on-demand query costs.

---

## Section 3: Reliability, Quality & Operations (Q16–Q23)

### Q16: Describe the 4-tier data quality testing hierarchy.
**Answer**:
1. **Tier 1 (Local Python Unit Tests)**: 19 unit tests in `tests/unit/` executing in 0.02s testing config dictionaries, ISO-8601 parsing, and contract validation.
2. **Tier 2 (dbt Generic Schema Tests)**: Column constraints (`unique`, `not_null`, `relationships`, `accepted_values`) defined in `schema.yml`.
3. **Tier 3 (dbt Singular Business Tests)**: Custom SQL tests in `dbt/tests/` validating business invariants (`rowActivationDate <= rowExpirationDate`, non-negative watch duration, bounded conversion ratios).
4. **Tier 4 (Repository Validation Suite)**: `./scripts/validate.sh` running 9 automated checks verifying compiler integrity, SQL syntax, secret hygiene, and contract parity.

### Q17: How do data contracts prevent pipeline breakage?
**Answer**:
Data contracts in [`contracts/*.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/) define explicit schemas (column names, types, nullability, partition keys). [`scripts/validate_contracts.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate_contracts.py) verifies that the staging schemas match contract specifications. If an upstream producer introduces an unannounced breaking change (e.g., dropping or renaming a field), CI halts before deployment.

### Q18: What is the difference between Pipeline Duplicates and Source Duplicates?
**Answer**:
- **Pipeline Duplicates**: Caused by network retries or DAG re-executions. Handled at ingestion time via partition-scoped `DELETE + INSERT` in staging.
- **Source Duplicates**: Emitted by client apps sending the same event ID twice due to retry logic. Handled at transformation time via surrogate key deduplication in incremental `MERGE` (`unique_key = 'adEventKey'`).

### Q19: How does BigQuery Time Travel work for disaster recovery?
**Answer**:
BigQuery automatically maintains historical table states for 7 days. If a table is corrupted or accidentally dropped:
```sql
CREATE OR REPLACE TABLE core.fact_ad_events AS
SELECT * FROM core.fact_ad_events
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR);
```
This restores the exact table state instantaneously without reprocessing raw files from GCS.

### Q20: What is your RPO and RTO?
**Answer**:
- **RPO (Recovery Point Objective)**: $\le$ 1 hour (backed by hourly immutable GCS Parquet dumps and BigQuery Time Travel).
- **RTO (Recovery Time Objective)**: $\le$ 30 minutes for single partition restoration using `scripts/backfill.py`.

### Q21: What are the 7 CI quality gates in GitHub Actions?
**Answer**:
1. Python Linting (`ruff check .`)
2. SQL Linting (`sqlfluff lint`)
3. Unit Tests (`python3 -m unittest`)
4. DAG Compilation (`py_compile`)
5. Contract Validation (`validate_contracts.py`)
6. Secret Hygiene Scan (Regex key detection)
7. Doc Integrity & Link Validator (`validate_docs.py`)

### Q22: How is PII isolated and protected?
**Answer**:
Raw user identifiers and demographics reside exclusively in `core.dim_users` protected by column-level IAM policy tags. Downstream fact tables and analytical marts store only pseudonymous cryptographic surrogate keys (`userKey`), preventing exposure to BI viewers.

### Q23: How are alerts routed and prioritized?
**Answer**:
- **Severity 1 (Critical)**: Mart build failure or data corruption $\rightarrow$ PagerDuty on-call alert + Slack `#data-eng-urgent`.
- **Severity 2 (Warning)**: Transient Airflow task retry or missing upstream files $\rightarrow$ Slack `#data-eng-alerts`.
- **Severity 3 (Info)**: Additive schema evolution or successful backfills $\rightarrow$ Slack `#data-eng-audit`.

---

## Section 4: Scalability, Trade-Offs & Vision (Q24–Q30)

### Q24: How would this architecture scale from 100K to 10M events/day (100x)?
**Answer**:
1. Partition GCS prefixes by sub-hourly increments (`dt=YYYY-MM-DD/hr=HH/`).
2. Migrate Airflow workers to KubernetesPodOperator on GKE for autoscaling worker pods.
3. Retain dbt incremental models with existing partitioning and clustering (which scale horizontally in BigQuery).
4. Reserve 100–500 dedicated BigQuery slots to eliminate on-demand slot contention.

### Q25: How would you transition to a 1,000x real-time streaming architecture?
**Answer**:
1. Ingest telemetry directly into **Google Cloud Pub/Sub** topics.
2. Process stream via **Apache Beam / Cloud Dataflow** for sliding-window deduplication and schema validation.
3. Stream records directly into BigQuery using the **BigQuery Storage Write API**.
4. Run real-time streaming SQL in BigQuery or micro-batch dbt transformations every 5 minutes.

### Q26: What are the primary bottlenecks if event volume increases 10x today?
**Answer**:
1. GCS object listing latency during Airflow glob operations.
2. Airflow scheduler task queue serialization on single-node environments.
3. BigQuery concurrent query limits on ad-hoc analytical workloads.

### Q27: What is the current technical debt and how is it managed?
**Answer**:
Tracked transparently in [`docs/TECHNICAL_DEBT.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/TECHNICAL_DEBT.md):
- **0 Critical Debt**.
- **Medium**: Airflow LocalExecutor vs Celery/K8s; pure SQL SCD2 vs native dbt snapshot engine.
- **Low**: BI Engine 1GB reservation provisioning; Slack webhook URL environment binding.

### Q28: Why didn't you use Snowflake or Databricks?
**Answer**:
BigQuery is natively integrated with GCS and Looker Studio BI Engine with zero data egress costs. Unlike Snowflake or Databricks, BigQuery requires zero virtual warehouse sizing, cluster start/stop timeouts, or cluster maintenance, providing 100% serverless compute for our batch ELT workload.

### Q29: What was the hardest bug you encountered and how did you resolve it?
**Answer**:
The hardest issue was historical attribution corruption during subscription tier upgrades. When users upgraded from Free to Premium, in-place overwrites in `dim_users` caused past ad impressions to be counted against Premium users. We resolved this by building a deterministic SCD Type 2 model using SQL window functions (`LAG`, `LEAD`, and `SUM(isNewState)`), ensuring fact joins correctly evaluate `eventTimestamp BETWEEN rowActivationDate AND rowExpirationDate`.

### Q30: If you were given a 10x budget and 3 months, what would you build next?
**Answer**:
1. Implement real-time streaming ingestion via Cloud Pub/Sub and Cloud Dataflow with the BigQuery Storage Write API.
2. Deploy automated data anomaly detection with automated Slack root-cause alerting.
3. Introduce automated semantic layer metrics using dbt Semantic Layer / MetricFlow for self-serve BI modeling.
4. Establish cross-region active-active disaster recovery replication for GCS and BigQuery datasets.

