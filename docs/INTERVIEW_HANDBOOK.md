# Data Engineering Interview Technical Handbook

This handbook is your comprehensive guide to explaining and defending every architectural and implementation decision in the **Adaptive Ads Data Engineering Platform** during technical interviews.

---

## 1. Quick Project Pitches

### 30-Second Elevator Pitch
> *"I built Adaptive Ads, an end-to-end data engineering platform that processes high-velocity ad and video stream telemetry into an enterprise Kimball data warehouse on Google BigQuery. Orchestrated by Apache Airflow with decoupled parallel TaskGroups and partition-scoped idempotent ingestion, the pipeline transforms raw telemetry into SCD Type 2 user dimensions, incremental partitioned facts, and Looker-ready analytical marts using dbt, with full CI/CD validation in GitHub Actions."*

### 2-Minute Project Overview
> *"Adaptive Ads solves the challenge of analyzing advertising delivery, audience engagement, and content reach across streaming platforms. 
> 
> The pipeline ingests four independent telemetry streams—watch events, ad impressions, page views, and authentications—from Cloud Storage into BigQuery. To prevent stream coupling, I refactored the Airflow orchestration into dynamic, parallel TaskGroups driven by a centralized configuration registry. Ingestion is made strictly idempotent using partition-scoped replacements, allowing safe retries and backfills without duplicate data.
> 
> For the warehouse layer, I designed a multi-tier dbt architecture spanning staging views, a Kimball dimensional core, and business marts. Key models include an SCD Type 2 user dimension tracking subscription tier changes over time and incremental fact tables that leverage BigQuery day-level partitioning, multi-column clustering, and 3-day lookback windows for late-arriving events. 
> 
> The repository is hardened with a two-tier GitHub Actions CI pipeline executing Ruff, SQLFluff, DAG parsing, and secret scans, backed by operational runbooks and Looker Studio BI specifications."*

---

## 2. 5-Minute Deep-Dive Architecture Walkthrough

```mermaid
flowchart LR
    A["Raw GCS Telemetry"] --> B["Airflow Parallel TaskGroups"]
    B --> C["BigQuery Staging (adaptive_ads_stg)"]
    C --> D["dbt Core Star Schema (adaptive_ads_prod)"]
    D --> E["dbt Analytical Marts"]
    E --> F["Looker Studio BI Dashboards"]
```

1. **Ingestion & Orchestration Layer**:
   - Hourly telemetry files arrive in GCS partitioned by `month=M/day=D/hour=H`.
   - `adaptive_ads_dag` executes hourly at `05` minutes past the hour. It dynamically spins up four isolated `TaskGroup` pipelines using `event_config.py`.
   - Each TaskGroup creates a transient external table, ensures the partitioned staging table exists, runs a partition-scoped `DELETE` + `INSERT` query, and drops the external table.
   - Decoupled parallel execution ensures that a failure in one stream (e.g. `ad_events`) does not block other streams from completing.
2. **Data Transformation & Modeling Layer**:
   - **Staging**: Views in `adaptive_ads_stg` cast datatypes, trim strings, and coalesce nulls.
   - **Core Warehouse**: Builds Kimball dimensions (`dim_users`, `dim_movies`, `dim_location`, `dim_datetime`) and incremental fact tables (`fact_streams`, `fact_ad_events`).
   - **SCD Type 2**: `dim_users` uses window functions (`LAG`, `SUM`, `LEAD`, `RANK`) to track subscriber membership changes (`free` vs `paid`) with non-overlapping validity dates.
   - **Incremental Facts**: `fact_streams` and `fact_ad_events` use BigQuery `merge` on surrogate keys with day-level partitioning and clustering, featuring a 3-day lookback window for late-arriving telemetry.
3. **Marts & Analytics Layer**:
   - Aggregated business summary tables (`daily_ad_metrics`, `ad_content_performance`, `daily_user_engagement`) and reporting views (`wide_streams`) optimized for Looker Studio dashboards.
4. **CI/CD & Operational Reliability**:
   - Multi-stage GitHub Actions CI runs Ruff, SQLFluff, `DagBag` parsing, and secret detection on every PR.
   - Standardized retries (`retries=2`, `retry_delay=3m`, `execution_timeout=15m`) and structured Python logging ensure high reliability.

---

## 3. Technology Choices & Design Trade-Offs

| Technology | Why Chosen for this Architecture | Alternatives Considered | Why Alternatives Were Not Chosen |
| :--- | :--- | :--- | :--- |
| **Apache Airflow** | Native DAG/TaskGroup dependency modeling, dynamic configuration generation, robust retry/backfill management. | Cron, Luigi, Prefect | Cron lacks dependency tracking and backfills; Luigi has outdated UI; Airflow is industry standard for scheduled ELT. |
| **Google BigQuery** | Serverless SQL data warehouse, automatic partition pruning, clustering acceleration, zero-management scaling. | PostgreSQL, Snowflake | Postgres cannot efficiently handle multi-terabyte analytical scans; BigQuery provides built-in serverless pricing and GCS integration. |
| **dbt (Data Build Tool)** | Modular SQL transformations, built-in testing, documentation, incremental materialization, lineage graphing. | Raw Python scripts, Stored Procedures | Stored procedures are hard to version-control and test; dbt provides software engineering best practices for SQL. |
| **Parquet** | Columnar binary format, high compression, schema preservation, efficient vectorized scans. | CSV, JSON | CSV and JSON are uncompressed, slow to parse, and lack strong data types. |

---

## 4. Deep-Dive Core Concepts

### 1. Ingestion Idempotency via Partition-Scoped Replacement
**The Problem**: If an hourly batch fails halfway or Airflow retries, a simple `INSERT INTO staging` appends duplicate records.  
**The Solution**: Every ingestion script executes an atomic partition delete on the execution window:
```sql
DELETE FROM adaptive_ads_stg.watch_events
WHERE ts >= TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}')
  AND ts < TIMESTAMP_ADD(TIMESTAMP('{{ logical_date.strftime("%Y-%m-%d %H:00:00+00") }}'), INTERVAL 1 HOUR);

INSERT INTO adaptive_ads_stg.watch_events (...)
SELECT ... FROM adaptive_ads_stg.watch_events_{{ logical_date.strftime("%m%d%H") }};
```
**Benefit**: Guarantees exact-once semantics on retries and backfills without requiring full-table deduplication.

### 2. SCD Type 2 Window Function Mechanics (`dim_users`)
Tracks historical user tier changes (`level`: `free` vs `paid`):
- `LAG(level, 1, 'NA') OVER (PARTITION BY userId ORDER BY ts)`: Flags when tier changes.
- `SUM(lagged) OVER (PARTITION BY userId ORDER BY ts)`: Assigns a unique integer group to each contiguous tier period.
- `MIN(ts)`: Calculates `rowActivationDate`.
- `LEAD(minDate, 1, DATE '9999-12-31') OVER (PARTITION BY userId ORDER BY grouped)`: Computes `rowExpirationDate`.
- `RANK() OVER (PARTITION BY userId ORDER BY grouped DESC) = 1`: Sets `currentRow = 1` for the active record.

### 3. Incremental Fact Processing with Late-Arriving Lookback
**The Problem**: Event timestamps may arrive delayed by hours or days due to network retries. Scanning all historical data is too slow and expensive.  
**The Solution**: Merge strategy with a 3-day lookback window:
```sql
{% if is_incremental() %}
WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
```
BigQuery scans only the last 3 days of events and executes partition-scoped upserts matching on `streamKey` / `adEventKey`.

---

## 5. 25+ Real Data Engineering Interview Questions & Answers

### Category A: Pipeline Architecture & System Design
1. **Q: How does data flow from raw telemetry to BI reporting in your project?**  
   *A:* Event telemetry lands in GCS as Parquet files. Airflow runs hourly, using TaskGroups to ingest files into BigQuery staging tables using partition-scoped replacements. dbt staging views standardize data, core models build SCD2 dimensions and incremental facts, and analytical marts aggregate metrics for Looker Studio.
2. **Q: Why did you choose an ELT pattern over an ETL pattern?**  
   *A:* ELT leverages BigQuery’s massively parallel processing (MPP) compute engine. Raw events are loaded directly into staging tables, allowing transformations and business logic to execute in SQL inside the data warehouse rather than overloading a transformation server.
3. **Q: What is the purpose of the staging layer in your dbt warehouse?**  
   *A:* Staging models act as a clean abstraction over raw tables: they rename columns, cast datatypes, trim strings, and coalesce nulls without introducing complex joins or business logic.

---

### Category B: Apache Airflow
4. **Q: Why did you use Airflow TaskGroups instead of creating 4 separate DAGs for each event stream?**  
   *A:* All 4 telemetry streams share the same hourly schedule (`5 * * * *`) and feed into the same downstream fact tables. TaskGroups provide decoupled parallel execution and visual clarity while avoiding complex cross-DAG sensor coordination.
5. **Q: How do you add a new event stream to the ingestion pipeline?**  
   *A:* Add the schema to `schema.py`, register the event in `event_config.py`, and add the SQL template in `airflow/dags/sql/`. The DAG dynamically constructs the new TaskGroup without code duplication.
6. **Q: What happens if the Airflow worker crashes during an ingestion task?**  
   *A:* The task times out via `execution_timeout=timedelta(minutes=15)` and Airflow triggers an automatic retry (`retries=2`). On retry, the pre-delete cleans any partial data before re-inserting.

---

### Category C: Google BigQuery & Performance
7. **Q: How did you configure partitioning and clustering, and why?**  
   *A:* Fact tables and daily marts are partitioned by Day on event timestamp (`ts` / `ad_date`) for query partition pruning. They are clustered by high-cardinality join/filter columns (`userKey`, `videoKey`, `adType`) to colocate blocks and accelerate scans.
8. **Q: How do you prevent divide-by-zero errors in analytical marts?**  
   *A:* All derived rate calculations (e.g. `avg_ad_duration_seconds`, `free_tier_ratio`) use `SAFE_DIVIDE(numerator, denominator)` which safely returns `NULL` (coalesced to `0.0`) instead of throwing runtime exceptions.

---

### Category D: dbt & Dimensional Modeling
9. **Q: How does `dim_users` handle historical tracking?**  
   *A:* It implements SCD Type 2 using window functions over timestamps to generate `rowActivationDate`, `rowExpirationDate`, and `currentRow` flags for each user subscription tier transition.
10. **Q: What is your surrogate key generation strategy?**  
    *A:* Surrogate keys are generated deterministically using `dbt_utils.surrogate_key` on unique business key components (e.g. `['userId', 'rowActivationDate', 'level']` for users, `['userId', 'ts', 'video']` for streams).
11. **Q: How do your fact tables handle late-arriving events?**  
    *A:* In incremental runs, the model filters `ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})`. The BigQuery `merge` strategy updates or inserts late records into historical partitions without scanning the entire warehouse.

---

### Category E: Data Quality, CI/CD & Operations
12. **Q: How do you prevent broken SQL from reaching production?**  
    *A:* GitHub Actions CI executes SQLFluff linting with BigQuery dialect and compiles dbt models on every pull request.
13. **Q: What singular business data quality tests did you implement?**  
    *A:* `assert_dim_users_single_active_row.sql` verifies that no user has multiple active records (`currentRow = 1`), and `assert_daily_ad_metrics_non_negative.sql` verifies metrics are non-negative.
14. **Q: How would you backfill one month of historical data?**  
    *A:* Clear the desired historical execution dates in Airflow Grid view. The idempotent partition-delete ensures safe reload without duplicating records.
15. **Q: How do you handle secrets and credentials safely?**  
    *A:* Zero credentials are committed to git; `.gitignore` excludes `.env`, `*.json`, `*.key`. Service account paths and project IDs are injected via environment variables.

