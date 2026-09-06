# What I Built: Code-to-Feature Mapping

This document provides a direct, verifiable mapping from every engineering feature to its exact implementation file and key code snippet in the repository.

---

## 1. Feature-to-Code Mapping Table

| Engineering Feature | Implementation File | Key Code Mechanism / Concept |
| :--- | :--- | :--- |
| **Dynamic Orchestration Engine** | [`airflow/dags/adaptive_ads_dag.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/adaptive_ads_dag.py) | Dynamic TaskGroup loops, parameterized Jinja templating |
| **Centralized Telemetry Registry**| [`airflow/dags/event_config.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/event_config.py) | `EVENT_CONFIG` dataclass dictionary defining all 4 event streams |
| **Reusable Ingestion TaskGroup** | [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py) | `create_event_ingestion_taskgroup()` with partition delete + insert |
| **Partition-Scoped SQL Ingestion** | [`airflow/dags/sql/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/sql/) | Atomic `DELETE FROM ... WHERE partition_hour = ...` + `INSERT` |
| **SCD Type 2 User Dimension** | [`dbt/models/core/dim_users.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_users.sql) | Pure SQL window functions: `LAG`, `SUM(isNewState)`, `LEAD` |
| **Incremental Fact Processing** | [`dbt/models/core/fact_ad_events.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_ad_events.sql) | `materialized='incremental'`, `merge`, `incremental_predicates` |
| **Content Dimension** | [`dbt/models/core/dim_movies.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_movies.sql) | Surrogate key generation, deduplication on `movieId` |
| **Geographic Dimension** | [`dbt/models/core/dim_location.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/dim_location.sql) | Surrogate key generation on `countryCode` and `city` |
| **Ad Monetization Mart** | [`dbt/models/marts/daily_ad_metrics.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/marts/daily_ad_metrics.sql) | Aggregated eCPM, CTR, impressions, revenue, `SAFE_DIVIDE()` |
| **Audience Engagement Mart** | [`dbt/models/marts/user_engagement_summary.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/marts/user_engagement_summary.sql) | Aggregated watch hours, completion rates by subscription tier |
| **Declarative Data Contracts** | [`contracts/*.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/) | Explicit schemas, datatypes, required fields, and partition keys |
| **Data Contract CLI Validator** | [`scripts/validate_contracts.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate_contracts.py) | Schema comparison between contract YAML and model registries |
| **Schema Drift Detector** | [`scripts/check_schema.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/check_schema.py) | Automated drift detection for added/removed/type-shifted columns |
| **Operational Backfill Tool** | [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py) | Slices ISO-8601 time windows into hourly idempotent intervals |
| **Python Unit Testing Suite** | [`tests/unit/*.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/tests/unit/) | 19 unit tests running in 0.022s testing configs and utilities |
| **Singular Business Tests** | [`dbt/tests/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/tests/) | SQL assertions verifying valid dates, positive metrics, ratio bounds |
| **Automated Local Validation** | [`scripts/validate.sh`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate.sh) | 9-step automated quality gate checking full repository health |
| **CI / CD Pipeline** | [`.github/workflows/ci.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/.github/workflows/ci.yml) | 7 GitHub Actions quality gates running on pull requests and pushes |

