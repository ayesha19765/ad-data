# Subsystem Revision Guide: Ingestion Idempotency & Backfilling

## 1. The Idempotency Principle

In data engineering, an operation is **idempotent** if running it multiple times with the same input produces the exact same end state as running it once, without creating duplicates or corrupting data.

$$f(f(x)) = f(x)$$

---

## 2. Ingestion-Level Idempotency: Partition-Scoped Delete + Insert

### The Problem with Append-Only:
When an hourly Airflow task encounters a network hiccup during Parquet loading, Airflow retries the task. An append-only pipeline inserts the same Parquet records twice, creating duplicate records in BigQuery staging.

### The Solution:
Our pipeline enforces partition-scoped atomic replacement in [`airflow/dags/task_templates.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/task_templates.py) and [`airflow/dags/sql/*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/airflow/dags/sql/):

```sql
-- Step 1: Purge target execution partition
DELETE FROM `adaptive-ads.staging.stg_ad_events`
WHERE partition_hour = TIMESTAMP('{{ execution_date }}');

-- Step 2: Load clean partition from GCS
INSERT INTO `adaptive-ads.staging.stg_ad_events`
SELECT
    event_id,
    user_id,
    campaign_id,
    event_type,
    bid_amount_usd,
    event_timestamp,
    TIMESTAMP('{{ execution_date }}') AS partition_hour
FROM `adaptive-ads.staging.ext_ad_events_gcs`;
```

---

## 3. Transformation-Level Idempotency: Incremental Merges

At the warehouse modeling layer, fact tables ([`dbt/models/core/fact_*.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/models/core/fact_ad_events.sql)) use dbt's `merge` strategy keyed on cryptographic surrogate keys:

```sql
{{ config(
    materialized='incremental',
    unique_key='adEventKey',
    incremental_strategy='merge',
    incremental_predicates=['DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)']
) }}
```

- If an existing event is reprocessed, the `MERGE` updates or ignores the existing `adEventKey` rather than inserting a duplicate row.
- Surrogate keys are deterministically generated from natural attributes (`event_id` + `event_timestamp`).

---

## 4. Operational Backfilling Tool: `scripts/backfill.py`

When historical data needs to be reprocessed (e.g., late-arriving logs beyond 3 days or upstream schema backfills), we use the CLI backfill orchestrator [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py).

### Usage:
```bash
# Dry run validation of a 24-hour backfill window
python3 scripts/backfill.py \
  --start "2026-09-01T00:00:00" \
  --end "2026-09-02T00:00:00" \
  --event "ad_events" \
  --dry-run

# Force execution across all streams
python3 scripts/backfill.py \
  --start "2026-09-01T00:00:00" \
  --end "2026-09-02T00:00:00" \
  --force
```

### Safety Features:
- Validates strict ISO-8601 timestamps.
- Slices the time range into hourly intervals matching Airflow's execution partitions.
- Executes partition-scoped deletions before reloading to guarantee zero duplicate injection during backfills.

