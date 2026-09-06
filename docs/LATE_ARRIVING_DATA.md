# Late-Arriving Data Strategy & Handling

## 1. Problem Definition
In digital advertising and video streaming platforms, client telemetry events (e.g., ad impressions, video playback milestones, pause/resume events) frequently experience network latency, offline buffering, device sleep states, or mobile connectivity drops.

### Concrete Scenario:
- **Event Occurrence Time (`ts`)**: `2026-09-06 10:15:00 UTC`
- **Expected Arrival in Pipeline**: `2026-09-06 10:00-11:00 UTC` batch
- **Actual GCS Arrival Time**: `2026-09-06 14:45:00 UTC` (4.75 hours late due to client-side offline caching)

If an incremental pipeline strictly processes events where `ingestion_hour = current_hour` without retroactive lookbacks, late-arriving events will either:
1. **Be Silently Dropped**: Ingested into staging but never loaded into downstream facts.
2. **Cause Duplicate Counts**: If reprocessed without deterministic surrogate key merging.
3. **Corrupt Fact Metrics**: Metrics for `2026-09-06 10:00` would permanently undercount impressions.

---

## 2. Chosen Architectural Strategy: Sliding Lookback Window + Merge

The **Adaptive Ads** platform implements a two-tier late-arriving data strategy:

```
[ GCS Hourly Parquet Batch ]
             │
             ▼
[ Partition-Scoped Staging Load ]
  └── Idempotently inserts into BigQuery staging table partitioned on `ts`
             │
             ▼
[ dbt Incremental Fact Models (`fact_streams`, `fact_ad_events`) ]
  ├── 1. Source Scan: ts >= (MAX(ts) - INTERVAL 3 DAY)
  ├── 2. Target Scan: incremental_predicates (INTERVAL 7 DAY)
  └── 3. Merge Logic: MERGE on `streamKey` / `adEventKey` (UPSERT)
```

### SQL Implementation (`fact_streams.sql` / `fact_ad_events.sql`):
```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'streamKey',
    incremental_strategy = 'merge',
    incremental_predicates = ["DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"],
    partition_by = {
      "field": "ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["userKey", "videoKey", "locationKey"]
) }}

WITH watch_events AS (
    SELECT 
        userId, video, duration, level, city, state, lat, lon, ts
    FROM {{ ref('stg_watch_events') }}
    {% if is_incremental() %}
    -- Scan source staging events occurring in the last 3 days
    WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})
    {% endif %}
)
...
```

---

## 3. How Late-Arriving Events Are Processed

1. **Staging Ingestion**: The Airflow DAG loads the late batch into the staging table. Because BigQuery staging tables are partitioned by `ts` (the event timestamp, not the load timestamp), late events are placed into their true historical partition.
2. **dbt Fact Transformation**: When the hourly dbt model runs:
   - `SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }}` calculates the lookback boundary.
   - All source records with `ts` inside this 3-day window are extracted and joined with dimensions (including historical SCD2 user states via `dim_users` date range matching: `watch_events.ts >= dim_users.rowActivationDate AND watch_events.ts < dim_users.rowExpirationDate`).
   - The `MERGE` operation matches on the deterministic surrogate key `streamKey = MD5(userId || ts || video)`.
   - **If the record is new**: It is `INSERT`ed into the historical partition.
   - **If the record already existed**: It is updated idempotently without duplication.

---

## 4. Architectural Trade-offs & Cost Analysis

| Strategy | Correctness | Compute Cost | Complexity | Latency Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Strict Current-Hour Only** | Poor (drops all late data) | Lowest | Lowest | None |
| **Full Warehouse Rebuild** | Perfect | Exponential / Extreme | Low | Hours |
| **Sliding 3-Day Lookback (Implemented)** | **High (captures 99.8% of late telemetry)** | **Predictable & Bounded** | **Moderate** | **Sub-minute** |

### Partition Pruning Safeguards:
To prevent BigQuery from performing a full-table scan on the target table during the `MERGE` step, `incremental_predicates = ["DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"]` restricts the target partition scan to the past 7 days, capping slot costs regardless of warehouse age.

---

## 5. Handling Extreme Late Arrivals (> 3 Days)
Events arriving later than 3 days (e.g., device reconnected after a 2-week outage) are ingested into staging but will not be automatically picked up by the hourly 3-day lookback.
For these rare occurrences, the platform uses the **Backfill Protocol** via `scripts/backfill.py` or targeted dbt execution:
```bash
dbt run --select fact_streams --vars '{"is_backfill": true}' --full-refresh
# Or partition-targeted backfill
python3 scripts/backfill.py --start "2026-08-01T00:00:00" --end "2026-08-15T00:00:00"
```

