# Adaptive Ads


**Enterprise Ad Analytics & Telemetry Data Engineering Platform**

[![CI Pipeline](https://github.com/ayesha19765/ad-data/actions/workflows/ci.yml/badge.svg)](https://github.com/ayesha19765/ad-data/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8+-orange.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.6+-FF694B.svg)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4.svg)](https://cloud.google.com/bigquery)

Adaptive Ads is a data engineering platform for processing advertising and video-streaming telemetry and turning raw events into analytics-ready datasets.

The project simulates a production data platform built around **Apache Airflow, dbt, Google BigQuery, and Google Cloud Storage**, with a focus on reliable ingestion, dimensional modeling, incremental processing, data quality, and operational recovery.

The main goal wasn't just to move data from A to B. It was to design a pipeline that can **retry safely, handle late-arriving events, preserve historical changes, detect bad data, and remain maintainable as the number of event streams grows.**

---

## Architecture

```mermaid
flowchart LR
    A["Raw Telemetry\nParquet / GCS"]
    B["Airflow\nOrchestration"]
    C["BigQuery\nStaging"]
    D["dbt\nTransformations"]
    E["Dimensional Warehouse"]
    F["Analytics Marts"]
    G["Looker Studio"]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
```

The pipeline processes four main event streams:

* Watch events
* Ad events
* Page-view events
* Authentication / subscription events

These events are cleaned and transformed into a Kimball-style dimensional warehouse before being exposed through analytical marts.

---

## What I Built

### Reliable ingestion with Airflow

The ingestion layer uses reusable, parameterized Airflow `TaskGroup`s instead of creating separate pipeline logic for every event type.

A centralized event configuration determines:

* Source event
* Destination table
* Partition column
* Load configuration
* Validation rules

This makes adding another event stream primarily a configuration change rather than a copy-paste exercise.

### Incremental warehouse processing

Large fact tables are not rebuilt from scratch on every run.

`fact_streams` and `fact_ad_events` use incremental processing with:

* BigQuery `MERGE`
* Day-level partitioning
* Multi-column clustering
* A 3-day lookback for late-arriving events
* Partition-aware merge predicates

This keeps processing focused on the data that can actually change.

### Historical user tracking

`dim_users` implements **Slowly Changing Dimension Type 2** logic.

Instead of overwriting a user's subscription tier, historical versions are retained:

```text
user_id | tier | valid_from | valid_to | is_current
----------------------------------------------------
101     | Free | Jan 01     | Mar 14   | false
101     | Paid | Mar 14     | NULL     | true
```

This allows historical analytics to answer questions such as:

> "How many watch hours came from Paid users at the time the event occurred?"

rather than only showing the user's current state.

### Data quality and contracts

The repository includes multiple layers of validation:

* Python unit tests
* dbt schema tests
* SQL business assertions
* Declarative data contracts
* Schema validation
* Security checks
* Documentation validation
* CI quality gates

The project currently has **19 Python unit tests** plus dbt and repository-level validation.

### Operational tooling

The project also includes tooling for situations that normally appear once a pipeline is in production.

Examples include:

* Partition-scoped backfills
* Dry-run backfill previews
* Schema drift detection
* Failure recovery procedures
* Data freshness monitoring
* SLO definitions
* Disaster recovery procedures

---

## Warehouse Design

| Layer   | Examples                                  | Purpose                          |
| ------- | ----------------------------------------- | -------------------------------- |
| Staging | `stg_watch_events`, `stg_ad_events`       | Clean and standardize raw events |
| Core    | `dim_users`, `dim_movies`, `dim_location` | Conformed dimensions             |
| Core    | `fact_streams`, `fact_ad_events`          | Incremental event facts          |
| Marts   | `daily_ad_metrics`                        | Daily advertising KPIs           |
| Marts   | `user_engagement_summary`                 | Audience engagement              |
| Marts   | `campaign_performance_cube`               | Campaign analysis                |

The resulting data model is designed for analytical workloads rather than operational transactions.

---

## Key Engineering Decisions

A major part of the project was understanding **why** a particular technology or design was appropriate.

Some of the important decisions were:

| Problem                | Decision                   |
| ---------------------- | -------------------------- |
| Workflow orchestration | Apache Airflow             |
| Transformations        | dbt                        |
| Analytical warehouse   | BigQuery                   |
| Raw event storage      | Parquet on GCS             |
| Warehouse model        | Kimball star schema        |
| Historical dimensions  | SCD Type 2                 |
| Large fact tables      | Incremental `MERGE`        |
| Late data              | 3-day lookback             |
| Query performance      | Partitioning + clustering  |
| Pipeline retries       | Idempotent partition loads |
| Data interfaces        | Declarative contracts      |
| Analytics layer        | Looker-ready marts         |

The project also documents alternatives that were considered, including Kafka, Spark, Snowflake, and other architectures.

---

## Reliability & Performance

Some of the production-oriented capabilities implemented in the project include:

* **Idempotent ingestion** using partition-scoped delete + insert
* **Late-arriving data handling** using a sliding lookback window
* **Incremental MERGE processing** for large fact tables
* **Partition pruning** to avoid unnecessary BigQuery scans
* **Clustering** for frequently filtered dimensions
* **Explicit column projections** instead of unrestricted `SELECT *`
* **Schema drift detection**
* **Automated data contracts**
* **CI validation before changes are merged**
* **RPO ≤ 1 hour**
* **RTO ≤ 30 minutes**

---

## Tech Stack

| Technology           | Role                             |
| -------------------- | -------------------------------- |
| Python               | Pipeline and operational tooling |
| Apache Airflow       | Orchestration                    |
| dbt Core             | SQL transformations and testing  |
| Google BigQuery      | Data warehouse                   |
| Google Cloud Storage | Raw data landing zone            |
| PostgreSQL / Redis   | Local Airflow infrastructure     |
| Docker Compose       | Local environment                |
| GitHub Actions       | CI/CD                            |
| Ruff / SQLFluff      | Code and SQL quality             |
| Looker Studio        | Analytics / dashboards           |

---

## Project Highlights

This project demonstrates experience with:

* Data pipeline architecture
* Workflow orchestration
* ETL / ELT design
* Dimensional modeling
* SCD Type 2
* Incremental processing
* BigQuery optimization
* Data quality engineering
* Data contracts
* CI/CD
* Backfills and recovery
* Observability
* Disaster recovery
* Production-oriented documentation

---

## Repository Structure

```text
ad-data/
│
├── airflow/              # Airflow DAGs and orchestration
├── dbt/                  # dbt project and warehouse models
├── contracts/            # Declarative data contracts
├── scripts/              # Backfills, validation, schema checks
├── tests/                # Automated tests
├── docker-compose.yml
└── README.md
```

---

## Contributors

* Ayesha
* Aarthi Honguthi
* Dhanalakshmi Dhanapal
