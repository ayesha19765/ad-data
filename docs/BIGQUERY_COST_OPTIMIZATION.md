# BigQuery Cost Optimization & Performance Engineering

## 1. Cost Drivers in BigQuery Analytics
In Google Cloud BigQuery, on-demand query pricing is billed directly by **bytes scanned** ($6.25 per TB). Inefficient query patterns, full-table scans, and uncontrolled BI dashboards can lead to runaway cloud expenditure.

The **Adaptive Ads** warehouse implements five architectural pillars to minimize compute and storage costs.

---

## 2. Five Architectural Cost Control Pillars

```
              ┌─────────────────────────────────────────────────────────┐
              │           BIGQUERY COST OPTIMIZATION PILLARS            │
              └────────────────────────────┬────────────────────────────┘
                                           │
  ┌───────────────────┬────────────────────┼────────────────────┬───────────────────┐
  ▼                   ▼                    ▼                    ▼                   ▼
┌──────────────┐    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    ┌──────────────┐
│ Partitioning │    │  Clustering  │     │ Incremental  │     │ Projection   │    │ Aggregated   │
│   Pruning    │    │ Block Filter │     │ Predicates   │     │  Narrowing   │    │ BI Marts     │
└──────────────┘    └──────────────┘     └──────────────┘     └──────────────┘    └──────────────┘
```

### Pillar 1: Strategic Partitioning
- **Staging Layer**: Partitioned on `ts` with `HOUR` granularity. Daily ingestion queries scan only 1/24th of the day's data during batch loads.
- **Core Fact Layer**: Partitioned on `ts` with `DAY` granularity. Downstream analytical queries targeting a 30-day window read only 30 partition blocks rather than entire multi-year tables.

### Pillar 2: High-Selectivity Multi-Column Clustering
- **`fact_ad_events`**: Clustered by `userKey`, `adType`, `videoKey`.
- **`fact_streams`**: Clustered by `userKey`, `videoKey`, `locationKey`.
- **`daily_ad_metrics`**: Clustered by `adType`, `videoKey`.
- **Mechanism**: BigQuery organizes partition blocks based on cluster keys. When a BI query filters on `adType = 'pre-roll'` and `videoKey = 'xyz'`, BigQuery skips non-matching storage blocks entirely (block-level pruning).

### Pillar 3: Incremental Merge with `incremental_predicates`
During dbt `merge` operations on incremental fact tables, BigQuery by default scans all target partitions unless restricted. We configure:
```sql
incremental_predicates = [
  "DBT_INTERNAL_DEST.ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"
]
```
This forces BigQuery's optimizer to restrict the target partition search space strictly to the recent 7-day lookback window, preventing scans on years of historical data.

### Pillar 4: Explicit Column Projection (SELECT * Elimination)
- In the core warehouse layer, intermediate CTEs and outer models explicitly declare needed columns.
- Selecting only 9 required columns instead of 20+ columns in fact transformations directly reduces query I/O and slot memory pressure.

### Pillar 5: BI Consumption via Aggregated Marts
- Looker Studio dashboards and executive queries connect directly to pre-aggregated marts (`daily_ad_metrics`, `daily_user_engagement`, `ad_content_performance`) rather than querying raw event tables.
- **Impact**: Queries scan kilobytes/megabytes of summary data instead of terabytes of raw event telemetry, resulting in sub-second dashboard rendering and near-zero BI query cost.

---

## 3. Query Anti-Patterns vs Optimized Patterns

| Anti-Pattern | Risk | Optimized Architectural Pattern |
| :--- | :--- | :--- |
| `SELECT * FROM fact_ad_events` in BI | Scans entire table (all columns) | Connect BI to `daily_ad_metrics` or project explicit columns |
| Unbounded `DATE(ts) >= '2026-01-01'` on timestamp partition | Function on column can prevent partition pruning in older SQL engines | `ts >= TIMESTAMP('2026-01-01 00:00:00 UTC')` |
| `a / b` calculation | Runtime division-by-zero crashes | `COALESCE(SAFE_DIVIDE(a, b), 0.0)` |
| Full historical rebuild on hourly run | Quadratic compute cost growth | Incremental `merge` with sliding 3-day lookback |

