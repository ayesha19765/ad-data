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
    *A:* Use `scripts/backfill.py --start "YYYY-MM-01" --end "YYYY-MM-30"` or trigger an Airflow DAG backfill across the date range. Idempotent partition replacements prevent data duplication.
15. **Q: How do you handle secrets and credentials safely?**  
    *A:* Zero credentials are committed to git; `.gitignore` excludes `.env`, `*.json`, `*.key`. Service account paths and project IDs are injected via environment variables.

---

## 5. Advanced Scalability, Reliability & Architecture Deep-Dive Questions

### Scalability & Volume Scaling (10x → 100x → 1000x)
16. **Q: How does the pipeline behave if event volume increases 10x (100K events/hr)?**  
    *A:* At 10x scale, the batch architecture remains identical. Storage and scan volume scale linearly, but BigQuery cost remains bounded because `incremental_predicates` and partition pruning limit queries to active partitions. Multi-column clustering prevents full partition block scans.
17. **Q: What bottlenecks emerge at 100x volume (1M events/hr), and how do you resolve them?**  
    *A:* At 100x volume, creating hundreds of transient external tables per day causes BigQuery control-plane latency, and Airflow task queue contention increases. The solution is migrating from external tables to direct BigQuery batch loads / Storage Write API, scaling Composer worker pools with CeleryExecutor, and reserving BigQuery BI Engine memory for dashboard queries.
18. **Q: When would you transition from hourly batch ELT to real-time streaming (1,000x scale)?**  
    *A:* When business SLAs demand sub-minute telemetry availability (e.g. real-time fraud detection or real-time ad bidding pacing). Telemetry would publish to Google Cloud Pub/Sub, processed by Google Cloud Dataflow (Apache Beam) with watermarks and sliding window deduplication, writing directly into BigQuery via Storage Write API.
19. **Q: Why didn't you build the platform as real-time streaming from day one?**  
    *A:* Streaming introduces significant operational complexity (managing stream watermarks, out-of-order state windows, continuous cluster compute costs, and complex exactly-once semantics). Hourly micro-batching satisfies 99% of digital advertising reporting requirements at a fraction of the infrastructure cost.

### Warehouse Optimization & BigQuery Cost Engineering
20. **Q: How do you prevent BigQuery full-table scans during dbt incremental `merge` operations?**  
    *A:* We specify `incremental_predicates = ["DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"]` in `fact_streams` and `fact_ad_events`. This forces BigQuery's query planner to restrict the target partition search space strictly to the recent 7-day partition window during MERGE upserts.
21. **Q: Why did you choose DAY-level partitioning for facts but HOUR-level partitioning for staging?**  
    *A:* Staging tables receive hourly file batches, so `HOUR` partitioning isolates the exact batch for idempotent atomic replacement. Core facts span multi-year analytical horizons; `DAY` partitioning prevents hitting BigQuery's 4,000 partition limit per table while keeping partition scan sizes optimal.
22. **Q: Why cluster by `userKey`, `adType`, and `videoKey` on `fact_ad_events`?**  
    *A:* Clustering order reflects the highest-frequency join and filtering hierarchy: ad analysts slice metrics by user demographics (`userKey`), filter by ad placement format (`adType`), and evaluate content-level performance (`videoKey`). BigQuery uses this order to co-locate records within partition blocks and prune non-matching blocks.
23. **Q: Why did you eliminate unrestricted `SELECT *` from core warehouse models?**  
    *A:* In columnar warehouses like BigQuery, query cost and slot memory allocation are directly proportional to the number of columns read. Explicit column projection reduces I/O, optimizes memory pipelines during joins, and insulates models from unexpected upstream schema additions.

### Late-Arriving Data & Deduplication
24. **Q: How does the platform handle telemetry that arrives 6 hours late?**  
    *A:* The Airflow ingestion places the late batch into its true historical partition based on `ts`. During the hourly dbt run, the 3-day sliding lookback window (`ts >= MAX(ts) - INTERVAL 3 DAY`) picks up the late record, joins with historical SCD2 dimension states, and merges it into the fact partition without duplication.
25. **Q: What happens if an event arrives two weeks late?**  
    *A:* It is ingested into staging but falls outside the automated 3-day lookback window. It is reconciled during scheduled partition backfills via `scripts/backfill.py` or targeted dbt runs with backfill flags.
26. **Q: What is the difference between pipeline duplicates and source duplicates, and how does each get resolved?**  
    *A:* **Pipeline duplicates** (caused by Airflow retries or re-running failed tasks) are resolved via pre-insert partition deletes (`DELETE WHERE ts >= start AND ts < end`). **Source duplicates** (caused by client browser retries or double clicks) are resolved via deterministic surrogate primary keys (`streamKey`, `adEventKey`) and dbt `merge` upserts.

### Schema Evolution, Reliability & Operations
27. **Q: How does the pipeline handle upstream schema drift (e.g. new columns added)?**  
    *A:* Field additions are backward-compatible. BigQuery handles them via `ALLOW_FIELD_ADDITION`. Semantic staging views (`stg_*`) isolate downstream models, ensuring new fields do not break existing queries until deliberately mapped.
28. **Q: How does `scripts/check_schema.py` prevent ingestion failures?**  
    *A:* It compares target BigQuery table schemas against canonical definitions in `airflow/dags/schema.py` in CI, detecting added columns, removed columns, type mismatches, and nullability changes before deployment.
29. **Q: How would you safely backfill 30 days of data without exhausting BigQuery slot quotas?**  
    *A:* `scripts/backfill.py` slices the 30-day range into discrete hourly or daily intervals, executing them in controlled batches rather than launching 720 simultaneous unconstrained queries.
30. **Q: If `dim_users` grows to 50 million records, how do you scale the SCD Type 2 logic?**  
    *A:* Instead of a full-table scan with window functions on every run, transition `dim_users` to dbt snapshots (`dbt snapshot` using `check` or `timestamp` strategy), which only scans updated user state delta records rather than the full historical log.

---

## 6. Production Engineering, Governance & Disaster Recovery Questions

### Testing & Validation Pyramid
31. **Q: How would you test this data pipeline across the development lifecycle?**  
    *A:* We implement a 4-tier testing pyramid: Tier 1 (Fast Python unit tests in `tests/unit/` testing configs, dates, and schema drift logic in <1s); Tier 2 (Static analysis in CI using Ruff and SQLFluff); Tier 3 (dbt schema and singular business rule tests validating uniqueness, foreign keys, and SCD2 invariants); and Tier 4 (Staging integration tests in BigQuery).
32. **Q: What is your configuration testing strategy?**  
    *A:* `tests/unit/test_event_config.py` validates that every stream in `EVENT_CONFIG` contains all required metadata keys, that SQL template files physically exist, and that declared partition fields exist within the schema.
33. **Q: What singular dbt tests did you implement to protect business logic?**  
    *A:* We implemented `assert_dim_users_valid_date_ranges.sql` (ensures `rowActivationDate <= rowExpirationDate`), `assert_fact_streams_valid_duration.sql` (ensures non-negative duration), `assert_fact_ad_events_valid_timestamps.sql` (ensures timestamps are valid), and `assert_daily_ad_metrics_rates_bounded.sql` (ensures ratios are between 0.0 and 1.0).

### Data Contracts & Governance
34. **Q: What is a Data Contract in your platform and how is it enforced?**  
    *A:* Data contracts in `contracts/*.yml` define the formal interface between telemetry producers and the warehouse (dataset name, owner, partition strategy, column types, nullability, and quality expectations). `scripts/validate_contracts.py` validates compliance in CI before deployment.
35. **Q: What happens if an upstream producer adds a new column to an event?**  
    *A:* Field additions are backward-compatible. BigQuery supports `ALLOW_FIELD_ADDITION`. Semantic staging views (`stg_*`) isolate downstream models so nothing breaks until the contract is updated and the column is deliberately exposed.
36. **Q: What happens if an upstream producer changes a field data type?**  
    *A:* Breaking type changes are caught in CI by `scripts/validate_contracts.py` and `scripts/check_schema.py`. Staging views apply explicit casts to normalize types or fail fast during validation.
37. **Q: How do you handle user privacy and PII in the warehouse?**  
    *A:* Demographic attributes (`firstName`, `lastName`, `dateOfBirth`) are isolated exclusively in the SCD2 `dim_users` dimension table. Fact tables and marts reference only surrogate keys (`userKey`, `videoKey`, `locationKey`), ensuring analytical queries never expose direct PII.
38. **Q: What is your data retention lifecycle policy?**  
    *A:* Raw GCS files transition to Nearline at 30 days, Coldline at 90 days, and are deleted at 365 days. BigQuery staging and fact partitions expire automatically after 730 days (2 years).

### Disaster Recovery, RPO & RTO
39. **Q: What is your RPO (Recovery Point Objective) and RTO (Recovery Time Objective)?**  
    *A:* Proposed RPO is ≤ 1 hour (bounded by the hourly ingestion schedule). Proposed RTO is ≤ 30 minutes for single-stream partition recovery and ≤ 2 hours for a full-warehouse rebuild from raw GCS files.
40. **Q: How would you recover a corrupted staging partition?**  
    *A:* Airflow's partition-scoped replacement pattern allows executing an atomic `DELETE` + `INSERT` over the corrupted interval without affecting other partitions. Downstream dbt models are re-executed with `dbt run --select core marts`.
41. **Q: How would you restore a table accidentally deleted in BigQuery?**  
    *A:* Within 7 days, restore the table instantly using BigQuery Time Travel (`FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`). Beyond 7 days, re-run `scripts/backfill.py` from raw GCS Parquet archives.
42. **Q: What parts of the warehouse are rebuildable vs recoverable?**  
    *A:* All dbt dimensional models and marts are 100% **rebuildable** from raw staging tables. Staging tables and raw GCS Parquet files are **recoverable** via GCS 11-9s durability and BigQuery Time Travel.

### Deployment, Rollback & Operations
43. **Q: How do you separate development, testing, and production environments?**  
    *A:* Environments are isolated by GCP project and BigQuery dataset prefixes (`dev` for local Docker, `pr_<id>` for CI runners, and `prod` for Cloud Composer). Target configuration is driven by environment variables and dbt profiles.
44. **Q: How do you roll back a bad dbt deployment in production?**  
    *A:* Execute `git revert` on the offending commit. CI validates the revert and triggers a dbt run for the affected models to restore previous schema definitions.
45. **Q: How do you prevent secret leaks in a collaborative repository?**  
    *A:* Zero credentials or private keys are committed to git; `.gitignore` excludes `.env`, `*.json`, and `*.key`. CI executes an automated secret scanner on every pull request.
46. **Q: What technical debt remains in the platform?**  
    *A:* Documented in `docs/TECHNICAL_DEBT.md`: scaling `dim_users` from full window scans to dbt snapshot deltas at >50M rows, and transitioning from transient external tables to BigQuery Storage Write API at 100x scale.
47. **Q: What is the single strongest engineering decision in this platform?**  
    *A:* The decoupled parallel TaskGroup architecture driven by a centralized `EVENT_CONFIG` registry and partition-scoped atomic replacement pattern. It guarantees strict idempotency, isolates stream failures, and allows onboarding new event streams in minutes with zero DAG code duplication.
48. **Q: What would you change if this platform processed 50,000 events per second?**  
    *A:* Replace batch GCS landing with Google Cloud Pub/Sub, deploy Google Cloud Dataflow (Apache Beam) for streaming deduplication and windowed aggregations, and stream directly into BigQuery via the Storage Write API.

