# Subsystem Revision Guide: BigQuery Cost Optimization & Performance

## 1. Cost & Storage Optimization Mechanics

BigQuery on-demand pricing charges \$6.25 per TB of data scanned by queries, with 1 TB free per month. To guarantee minimal query costs and fast execution times, the warehouse implements three optimization pillars:

```
+-----------------------------------------------------------------------------------+
| 1. PARTITIONING (Day Granularity)                                                 |
|    - Prunes unqueried date partitions entirely from query execution scans.        |
|    - Configured on all fact tables: `partition_by = {'field': 'eventDate'}`       |
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
| 2. CLUSTERING (Multi-Column Sorting)                                              |
|    - Sorts data within each partition by high-cardinality filter dimensions.      |
|    - Configured on: `cluster_by = ['campaignId', 'adPlacement']`                  |
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
| 3. INCREMENTAL PREDICATES & PROJECTION PRUNING                                    |
|    - Limits MERGE scans to the last 3 days (`incremental_predicates`).             |
|    - Prohibits `SELECT *`; projects only required analytical columns.             |
+-----------------------------------------------------------------------------------+
```

---

## 2. Table-by-Table Optimization Configuration

| Table Name | Layer | Partition Key | Cluster Keys | Scan Reduction Impact |
| :--- | :--- | :--- | :--- | :--- |
| **`stg_watch_events`** | Staging | `partition_hour` (Timestamp) | `userId` | Up to 95% reduction on hourly Airflow loads |
| **`stg_ad_events`** | Staging | `partition_hour` (Timestamp) | `campaignId` | Up to 95% reduction on hourly Airflow loads |
| **`fact_streams`** | Core Fact | `eventDate` (Date) | `userId`, `movieId` | Up to 90% scan reduction during incremental dbt merges |
| **`fact_ad_events`** | Core Fact | `eventDate` (Date) | `campaignId`, `adPlacement` | Up to 90% scan reduction during incremental dbt merges |
| **`dim_users`** | Core Dimension| N/A (Dimension Table) | `userId` | Sub-second point lookups for SCD2 joins |
| **`daily_ad_metrics`** | Analytical Mart | `metricDate` (Date) | `campaignId`, `subscriptionTier` | Eliminates runtime joins for Looker Studio BI queries |

---

## 3. Query Optimization Best Practices Implemented

1. **Avoid `SELECT *`**:
   All core and marts models project explicit named columns. In columnar engines like BigQuery (Capacitor), projecting 5 columns instead of 20 reduces scanned bytes by 75%.
2. **Push Down Filters**:
   Where possible, filter predicates (`WHERE eventDate >= ...`) are applied in early CTEs rather than outer joins.
3. **Safe Mathematical Division**:
   Utilize `SAFE_DIVIDE(numerator, denominator)` to avoid query crashes on zero impressions/clicks.
4. **Partition Pruning Enforcement**:
   All fact models define `incremental_predicates` preventing full historical table scans during hourly `MERGE` operations.

