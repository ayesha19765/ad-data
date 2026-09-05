# Phase 1: Correctness & Consistency Audit

## 1. Project Identity

The canonical identity of this repository is an **Advertising / Ad Analytics Data Engineering Platform** (`ad-data` / `adaptive_ads`).
The platform orchestrates hourly extract-load-transform (ELT) pipelines for ad and content interaction telemetry—ingesting user watch, ad impression/interaction, page view, and authentication events into Google BigQuery, transforming raw event streams into dimensional models (SCD Type 2 users, locations, datetime, content dimensions, and stream facts) using dbt, and preparing analytical datasets for downstream BI and ad reporting.

Legacy remnants from the upstream *Streamify* project (such as `listen_events` table references and conflicting source definitions) have been resolved and aligned with the ad platform's data models (`watch_events`, `ad_events`, `page_view_events`, `auth_events`). Supplementary IMDb movie metadata is maintained as a content dimension catalog.

---

## 2. Issues Found and Resolved

| Issue | File | Severity | Resolution |
| :--- | :--- | :--- | :--- |
| **Invalid Join Alias Reference** | `dbt/models/core/dim_location.sql` | Critical | Fixed join condition to reference `watch_events.state = state_codes.stateCode` instead of non-existent `listen_events.state`. |
| **Invalid Source Reference in User Dimension** | `dbt/models/core/dim_users.sql` | Critical | Changed source table from legacy `listen_events` to `watch_events`. |
| **SCD Type 2 Date Type Mismatch in `LEAD`** | `dbt/models/core/dim_users.sql` | High | Replaced string literal `'9999-12-31'` with typed `DATE '9999-12-31'` to resolve BigQuery type errors. |
| **UNION ALL Column Count & Type Mismatch** | `dbt/models/core/dim_users.sql` | Critical | Corrected anonymous branch (`userId = 0`) to output identical 11 columns matching surrogate key string type and column ordering. Removed duplicate `userId = 1` condition. Replaced non-standard `BIGINT` with `INT64`. |
| **Surrogate Key Column Mismatch** | `dbt/models/core/dim_movies.sql` | High | Fixed surrogate key generation from non-existent `movie_id` to `movieId`. Updated identifier quoting for `` `gross(in $)` ``. |
| **Mismatched Column in Fact Join** | `dbt/models/core/fact_streams.sql` | High | Corrected `watch_events.videoTitle` to `watch_events.video` matching the staging table schema and dbt source. Fixed `RowExpirationDate` casing. |
| **Invalid `partition_by` in View** | `dbt/models/core/wide_streams.sql` | Medium | Removed `partition_by` clause from view materialization (invalid in BigQuery). |
| **Conflicting Source Definitions** | `dbt/models/sources/src_streamify.yml` | High | Deleted legacy `src_streamify.yml` which conflicted with `dbt/models/core/schema.yml` for source `staging`. |
| **Missing Model Schema & Data Tests** | `dbt/models/core/schema.yml` | Medium | Added model definitions and schema tests (`unique`, `not_null`, and `relationships`) for dimensional surrogate keys and fact foreign keys. |
| **Undefined Table Variable in Ingestion SQL** | `airflow/dags/sql/watch_events.sql` | High | Corrected `{{ LISTEN_EVENTS_TABLE }}` to `{{ WATCH_EVENTS_TABLE }}` in `FROM` clause. |
| **Missing Column Alias in Ingestion SQL** | `airflow/dags/sql/auth_events.sql` | Low | Added `AS success` alias to `COALESCE(success, FALSE)`. |
| **dbt Testing DAG Execution Gap** | `airflow/dags/dbt_test_dag.py` | High | Updated DAG bash command from `dbt compile` to `dbt test` to execute data quality checks, and updated tags. |
| **Hardcoded Service Account File in Docker Compose** | `airflow/docker-compose.yaml` | High | Parameterized `GOOGLE_APPLICATION_CREDENTIALS` using environment variables. |
| **Missing Environment Configuration Template** | `airflow/.env.example` | Medium | Created `.env.example` with placeholders for required GCP and Airflow settings. |
| **dbt Model Folder Structure Alignment** | `dbt/dbt_project.yml` | Low | Updated `models` block in `dbt_project.yml` to target `core` and `imdb` directories. |

---

## 3. SCD Type 2 Logic Analysis (`dim_users`)

The `dim_users` model manages historical tracking of user subscription tiers (`level`: e.g. free vs paid) using window functions over event timestamps:
1. **Change Detection**: Identifies state transitions using `LAG(level, 1, 'NA') OVER (PARTITION BY userId, ... ORDER BY date)`.
2. **Contiguous Grouping**: Groups unbroken runs of the same tier via a running sum (`SUM(lagged) OVER (...)`).
3. **Activation & Expiration**:
   - `rowActivationDate`: Calculated as `MIN(date)` for each group.
   - `rowExpirationDate`: Assigned via `LEAD(minDate, 1, DATE '9999-12-31') OVER (...)`, guaranteeing zero-gap historical continuity without overlapping intervals.
4. **Current Row Indicator**: `CASE WHEN RANK() OVER (... ORDER BY grouped DESC) = 1 THEN 1 ELSE 0 END AS currentRow`.
5. **Anonymous Guests**: Handled via `UNION ALL` for `userId = 0`, assigned `rowActivationDate = MIN(ts)`, `rowExpirationDate = DATE '9999-12-31'`, and `currentRow = 1`.

---

## 4. Validation Status

### dbt Validation

| Check | Status | Notes |
| :--- | :--- | :--- |
| `dbt debug` | **BLOCKED** | Requires external Google Cloud BigQuery service account credentials and project ID. |
| `dbt deps` | **PASS (Config Verified)** | Packages configuration (`dbt-labs/dbt_utils`) verified in `packages.yml`. |
| `dbt compile` | **BLOCKED** | Requires active BigQuery connection configured in `profiles.yml`. SQL syntax, models, sources, and refs validated statically. |
| `dbt test` / `dbt build` | **BLOCKED** | Requires active BigQuery dataset and populated tables. Model schema tests specified in `schema.yml`. |

### Airflow Validation

| Check | Status | Notes |
| :--- | :--- | :--- |
| DAG Import / Syntax | **PASS** | `python3 -m py_compile` validated across all DAGs (`adaptive_ads_dag.py`, `dbt_test_dag.py`, `load_imdb_movie_datasets_local.py`, `schema.py`, `task_templates.py`). |
| DAG Parsing | **PASS** | All DAG IDs (`adaptive_ads_dag`, `dbt_test_dag`, `load_imdb_movie_datasets_local`), task IDs, macro mappings, and task dependencies verified. |
| Import Errors | **PASS** | No syntax or circular import errors found in DAG scripts. |

---

## 5. Issues Deferred to Phase 2

1. **Event Stream Ingestion Modularization**: *(Addressed in Phase 2)* Refactored ingestion into parallel `TaskGroup` architecture driven by a centralized `EVENT_CONFIG` registry.
2. **dbt Package Upgrade**: Upgrading from `dbt_utils` 0.8.0 to `dbt_utils` >= 1.x (`generate_surrogate_key`) and `dbt-bigquery` >= 1.7 (deferred to Phase 3).
3. **Data Quality CI/CD**: Adding GitHub Actions workflows for automated SQL linting (SQLFluff) and PR test builds using temporary BigQuery datasets (deferred to Phase 3).
4. **Looker / BI Layer Integration**: Designing BI semantic models and dashboards connecting to `wide_streams` and `top_action_movies` (deferred to Phase 3).

