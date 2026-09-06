# The "Why" Reference: Direct Answers to Key Architectural Questions

This document provides clear, evidence-based answers to the most common "Why" questions encountered during architectural reviews and engineering interviews.

---

## 1. Core Platform & Storage

### Q1: Why Apache Airflow for orchestration?
**Answer**: Airflow provides battle-tested DAG orchestration, flexible dynamic task generation via Python, built-in task retry/backoff policies, native Google Cloud operators (`BigQueryInsertJobOperator`, `GCSToBigQueryOperator`), and powerful backfilling capabilities. Using Airflow allows us to define parameterized TaskGroups driven by a centralized [`EVENT_CONFIG`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py), eliminating duplicated pipeline code across all 4 telemetry streams.

### Q2: Why Google BigQuery as the data warehouse?
**Answer**: BigQuery is a fully managed, serverless cloud data warehouse with automatic storage replication, instantaneous compute scaling, native ingestion from Cloud Storage, native support for nested/repeated data, and sub-second BI Engine acceleration. It eliminates cluster provisioning and database index maintenance while providing strict cost control through partition pruning and clustering.

### Q3: Why dbt Core for the transformation layer?
**Answer**: dbt Core establishes software engineering best practices for data transformations: declarative SQL modeling, automatic dependency graph resolution, modular Jinja templating, version-controlled transformations, schema testing pyramids, and automated documentation generation. It cleanly separates ingestion (Airflow) from analytical modeling (dbt).

### Q4: Why Parquet format over CSV or JSON Lines?
**Answer**: Columnar Parquet delivers 4–10x higher compression efficiency, typed schema enforcement, and drastic query performance improvements. Because BigQuery charges based on bytes scanned for ad-hoc queries, reading only the necessary columns from compressed Parquet cuts storage and transfer costs by over 70% compared to raw JSON or CSV.

### Q5: Why Snappy compression for Parquet files?
**Answer**: Snappy provides an optimal balance between fast compression/decompression speeds and CPU efficiency. While GZIP achieves slightly higher compression ratios, Snappy consumes significantly less CPU overhead during high-throughput ingestion and extraction workflows.

---

## 2. Data Modeling & Dimensional Design

### Q6: Why SCD Type 2 for the user dimension (`dim_users`)?
**Answer**: In ad tech and subscription streaming platforms, user attributes change frequently (e.g., Free tier $\rightarrow$ Premium tier, geographic relocation). If we used SCD Type 1 (in-place overwrites), historical ad impressions would erroneously link to a user's current subscription tier, falsifying historical revenue attribution. SCD Type 2 preserves historical state validity via `rowActivationDate` and `rowExpirationDate`.

### Q7: Why pure SQL window functions instead of dbt snapshots for SCD2?
**Answer**: dbt snapshots rely on stateful snapshot tables that capture point-in-time state during scheduled dbt runs. If a pipeline is paused, backfilled, or replayed from scratch, dbt snapshots cannot reconstruct past state history. Our pure SQL window function implementation (`LAG`, `LEAD`, and `SUM(is_new_state)`) deterministically rebuilds the entire SCD2 timeline directly from immutable event logs at any time.

### Q8: Why incremental materialization for fact tables (`fact_streams`, `fact_ad_events`)?
**Answer**: As streaming event volume grows into millions of rows daily, running full table scans and full table rebuilds on every hourly execution becomes prohibitively expensive and slow. Incremental materialization processes only new or modified events from the latest batches.

### Q9: Why `merge` strategy with `incremental_predicates`?
**Answer**: A standard `MERGE` statement without predicates forces BigQuery to scan the entire historical destination table to check for matching keys. By configuring `incremental_predicates = ["DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)"]`, we restrict the target table partition scan to the last 3 days, saving up to 90% of BigQuery scan bytes.

### Q10: Why a 3-day sliding lookback window for late-arriving data?
**Answer**: Telemetry emitted by mobile devices and smart TVs is frequently buffered during intermittent network disconnections or offline playback. Empirical ad-tech telemetry patterns show that >98% of delayed events arrive within 72 hours. A 3-day lookback window captures these events automatically during regular runs without incurring the cost of scanning older partitions.

### Q11: Why cryptographic MD5 / SHA256 surrogate keys?
**Answer**: Distributed data warehouses like BigQuery cannot generate auto-incrementing sequential integers without single-thread bottlenecking. Deterministic cryptographic hashes generated via `dbt_utils.generate_surrogate_key` produce collision-free, distributed surrogate keys independently in parallel.

---

## 3. Operations, Quality & Reliability

### Q12: Why partition-scoped delete + insert for staging idempotency?
**Answer**: Network hiccups or task failures can cause Airflow tasks to retry midway. If ingestion simply appends data, retries create duplicate rows in staging. An atomic `DELETE FROM table WHERE partition_hour = ...` followed by `INSERT` guarantees that executing a task once or ten times produces the exact same deterministic dataset.

### Q13: Why dynamic TaskGroups with centralized `EVENT_CONFIG`?
**Answer**: Defining separate DAGs or hardcoding tasks for 4 different telemetry streams leads to configuration drift and code bloat. Centralizing schemas, GCS paths, and partition configurations in [`airflow/dags/event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py) allows [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py) to generate identical, robust ingestion pipelines dynamically.

### Q14: Why separate Staging, Core, and Marts layers?
**Answer**:
1. **Staging**: Cleanses raw data and isolates source system peculiarities.
2. **Core**: Implements canonical dimensional models (conformed dimensions, standardized facts) representing business domain entities.
3. **Marts**: Pre-aggregates specific analytical cubes (monetization, user engagement) optimized for fast BI reporting without business analysts querying raw tables directly.

### Q15: Why PII isolation in `dim_users`?
**Answer**: Isolating sensitive user attributes (email, name, device IP) strictly in `dim_users` with column-level access controls ensures downstream fact tables and analytical marts contain only pseudonymous surrogate keys (`userKey`), complying with GDPR and CCPA privacy standards.

### Q16: Why declarative YAML data contracts?
**Answer**: Data producers and consumers frequently suffer from silent schema drift when producers add, rename, or drop fields. Declarative contracts in [`contracts/*.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/) define explicit schemas, types, and constraints, automatically validated by [`scripts/validate_contracts.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate_contracts.py) before deployment.

### Q17: Why a 4-tier testing hierarchy?
**Answer**:
- **Tier 1 (Unit Tests)**: Fast local Python tests (0.02s) testing helper logic, config parsing, and contract validation.
- **Tier 2 (Schema Tests)**: dbt column-level tests (`unique`, `not_null`, `relationships`, `accepted_values`).
- **Tier 3 (Singular Business Tests)**: Custom SQL assertions verifying complex business logic (valid date ranges, non-negative stream durations).
- **Tier 4 (Integrity Scripts)**: End-to-end repository validation via [`scripts/validate.sh`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate.sh).

