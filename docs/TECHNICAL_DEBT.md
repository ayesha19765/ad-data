# Technical Debt & Architecture Limitations Register

## 1. Executive Summary
A mature software and data engineering platform explicitly catalogs technical debt, architectural trade-offs, and intentional limitations. This document tracks technical debt across four priority tiers.

---

## 2. Technical Debt Register

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TECHNICAL DEBT INVENTORY                         │
└─────────────────────────────────────────────────────────────────────────┘
  [ TIER 1: CRITICAL ]           ──► None (All blocking issues resolved)
  [ TIER 2: MEDIUM PRIORITY ]    ──► SCD2 Snapshotting & Storage Write API
  [ TIER 3: LOW PRIORITY ]       ──► Local synthetic telemetry generator
  [ TIER 4: INTENTIONAL BOUNDS ] ──► Batch vs Streaming & Cloud Mocking
```

| ID | Category | Priority | Description | Remediation Plan |
| :--- | :--- | :--- | :--- | :--- |
| **DEBT-001** | Scalability | **Medium** | `dim_users` reconstructs SCD2 history via full staging scan window functions. At >50M users, compute cost will increase. | Migrate to dbt snapshots (`dbt snapshot`) using `check_cols` or `updated_at` strategy to process delta records only. |
| **DEBT-002** | Ingestion | **Medium** | Airflow ingestion creates transient BigQuery external tables for each hourly batch. At >100 streams, BigQuery control plane API rate limits may throttle table creation. | Transition from external tables to BigQuery Storage Write API / direct GCS load jobs. |
| **DEBT-003** | Testing | **Low** | Integration testing currently relies on cloud credentials or local static AST validation. | Implement DuckDB/SQLite local emulation layer for end-to-end dbt model execution without cloud connectivity. |
| **DEBT-004** | Operational | **Low** | Airflow task templates hardcode BigQuery table names in macros rather than fetching dynamically from dataset metadata. | Refactor `task_templates.py` to pass dataset references via Airflow connection objects. |

---

## 3. Intentional Architectural Limitations

1. **Hourly Micro-Batching vs. Real-Time Streaming**:
   - *Rationale*: Real-time streaming (Pub/Sub + Dataflow + Flink) introduces substantial compute cost, watermark tracking, and operational overhead. 99% of digital advertising analytics and reporting operate on hourly or daily reconciliation cadences.
2. **Local Sandbox Execution vs. Live Cloud Infrastructure**:
   - *Rationale*: The local repository is fully functional without paid Google Cloud billing. Cloud-dependent tasks (live BigQuery queries and Composer clusters) are clearly labeled as `BLOCKED` in local test reports rather than fabricated.

