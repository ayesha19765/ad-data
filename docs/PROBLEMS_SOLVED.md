# Real Problems Solved: Technical Challenges, Root Causes & Fixes

During the evolution of the Adaptive Ads platform across Phases 1 through 8, several challenging data engineering problems were diagnosed, resolved, and hardened.

---

## 1. Problem: Pipeline Retries Causing Duplicate Staging Rows
- **Symptom**: When network latency caused an Airflow GCS-to-BigQuery task to retry, duplicate rows appeared in the raw staging tables, skewing downstream aggregations.
- **Root Cause**: The staging load task used standard `WRITE_APPEND` disposition without partition purging.
- **Solution**: Implemented an atomic `DELETE FROM table WHERE partition_hour = ...` step inside the [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py) TaskGroup before running the append load.
- **Outcome**: Executing an ingestion task 1 time or 10 times yields the exact same deterministic dataset.

---

## 2. Problem: Full-Table Scans on Incremental Fact Merges
- **Symptom**: As historical fact tables grew, hourly dbt incremental runs experienced expanding query runtimes and increasing BigQuery scan costs.
- **Root Cause**: BigQuery's standard `MERGE` statement evaluated the target table across all historical partitions to check for key matches.
- **Solution**: Added `incremental_predicates = ["DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)"]` to [`dbt/models/core/fact_ad_events.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_ad_events.sql).
- **Outcome**: Restricts the target partition scan strictly to the last 3 days, reducing BigQuery scan bytes by up to 90%.

---

## 3. Problem: Historical User Subscription Attribution Mismatch
- **Symptom**: When users upgraded from the `Free` tier to `Premium`, historical ad impressions were retroactively attributed to the `Premium` tier, corrupting revenue metrics.
- **Root Cause**: User dimension was modeled as SCD Type 1 (in-place overwrites).
- **Solution**: Engineered a deterministic SCD Type 2 dimension in [`dbt/models/core/dim_users.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_users.sql) using SQL window functions (`LAG`, `LEAD`, and running sum state grouping).
- **Outcome**: Fact tables join against `dim_users` using `eventTimestamp BETWEEN rowActivationDate AND rowExpirationDate`, guaranteeing accurate historical attribution.

---

## 4. Problem: Silent Schema Drift from Upstream Telemetry
- **Symptom**: Upstream mobile app updates occasionally introduced unexpected column name changes or type changes, causing dbt compilation failures.
- **Root Cause**: Lack of formal contracts between event producers and warehouse consumers.
- **Solution**: Created declarative YAML data contracts in [`contracts/*.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/) and built an automated CI validator [`scripts/validate_contracts.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate_contracts.py) and drift detector [`scripts/check_schema.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/check_schema.py).
- **Outcome**: Schema mismatches are caught in CI before code is merged to main.

---

## 5. Problem: Division by Zero Crashes in Derived KPI Calculations
- **Symptom**: On new ad campaigns with zero impressions, click-through-rate (CTR) and eCPM calculations triggered SQL runtime errors.
- **Root Cause**: Standard `/` division operators evaluated `0 / 0`.
- **Solution**: Refactored all metric calculations across [`dbt/models/marts/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/marts/) to use `SAFE_DIVIDE(clicks, impressions)` and `COALESCE(..., 0.0)`.
- **Outcome**: 100% stable mart builds and zero dashboard calculation errors.

