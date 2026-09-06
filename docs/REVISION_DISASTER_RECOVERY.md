# Subsystem Revision Guide: Disaster Recovery & Business Continuity

## 1. Disaster Recovery Objectives (RPO & RTO)

| Metric | Target | Definition | Implementation Mechanism |
| :--- | :--- | :--- | :--- |
| **RPO (Recovery Point Objective)** | $\le$ 1 hour | Maximum acceptable data loss duration. | Hourly GCS Parquet immutable event logs + BigQuery 7-Day Time Travel. |
| **RTO (Recovery Time Objective)** | $\le$ 30 minutes | Maximum acceptable pipeline downtime. | Partition-scoped rebuilds via `scripts/backfill.py` & automated dbt builds. |

---

## 2. Warehouse Data Asset Taxonomy

```
+-----------------------------------------------------------------------------------+
| 1. REBUILDABLE ASSETS (Zero Backup Cost)                                          |
|    - dbt Models (`core.*`, `marts.*`, Staging Views)                              |
|    - Can be completely reconstructed from raw event logs via `dbt run --full-refresh`|
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
| 2. RECOVERABLE ASSETS (High Durability Storage)                                   |
|    - Raw GCS Parquet Files (Dual-Region / Multi-Region Cloud Storage 99.999999999%) |
|    - BigQuery Staging & Fact Tables (BigQuery 7-Day Snapshot Time Travel)          |
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
| 3. NON-RECOVERABLE / TRANSIENT ASSETS                                             |
|    - Airflow Task Instance Logs & Temp Scratch Files                              |
+-----------------------------------------------------------------------------------+
```

---

## 3. Disaster Recovery Decision Flowchart

```
                          [Disaster / Data Corruption Event]
                                          │
                                          ▼
                         [Is Raw GCS Data Intact?]
                                   /      \
                             YES  /        \ NO
                                 ▼          ▼
             [Is Corruption in dbt Model?] [GCS Multi-Region Restore]
                     /          \                   │
               YES  /            \ NO               ▼
                   ▼              ▼        [Replay Ingestion DAG]
        [Run `dbt run` for]   [Use BigQuery Time Travel]
        [Target Partition]    [Snapshot Restore (≤ 7 days)]
                   │                      │
                   └──────────┬───────────┘
                              ▼
                 [Run `scripts/validate.sh`]
                              ▼
                [Pipeline Restored to Green]
```

---

## 4. BigQuery Time Travel Recovery Example

If an operator accidentally deletes or overwrites `core.fact_ad_events`:

```sql
-- Step 1: Verify historical snapshot exists (1 hour ago)
SELECT COUNT(*)
FROM `adaptive-ads.core.fact_ad_events`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- Step 2: Restore table instantaneously without GCS reprocessing
CREATE OR REPLACE TABLE `adaptive-ads.core.fact_ad_events`
AS
SELECT *
FROM `adaptive-ads.core.fact_ad_events`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

