# Subsystem Revision Guide: dbt Transformation Layer

## 1. Overview & Project Layout

The transformation layer is managed by **dbt Core** under the [`dbt/`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/) directory. It models raw event telemetry into canonical dimensional entities and pre-aggregated analytical marts.

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/                 # 1:1 Cleaned Views of BigQuery Staging Tables
│   │   ├── schema.yml
│   │   ├── stg_watch_events.sql
│   │   ├── stg_ad_events.sql
│   │   ├── stg_page_views.sql
│   │   └── stg_auth_events.sql
│   ├── core/                    # Dimensions & Incremental Fact Tables
│   │   ├── schema.yml
│   │   ├── dim_users.sql        # SCD Type 2 User Dimension
│   │   ├── dim_movies.sql       # Conformed Content Dimension
│   │   ├── dim_location.sql     # Conformed Geographic Dimension
│   │   ├── fact_streams.sql     # Incremental Stream Playbacks Fact
│   │   └── fact_ad_events.sql   # Incremental Ad Impressions/Clicks Fact
│   └── marts/                   # Pre-Aggregated Analytical Data Marts
│       ├── schema.yml
│       ├── daily_ad_metrics.sql
│       ├── user_engagement_summary.sql
│       └── campaign_performance_cube.sql
└── tests/                       # Singular Custom Business Logic Tests
    ├── assert_dim_users_valid_date_ranges.sql
    ├── assert_fact_ad_events_valid_timestamps.sql
    ├── assert_fact_streams_valid_duration.sql
    └── assert_daily_ad_metrics_rates_bounded.sql
```

---

## 2. Model Layers & Materialization Strategy

| Layer | Materialization | Update Strategy | Grain | Key Performance Feature |
| :--- | :--- | :--- | :--- | :--- |
| **Staging (`stg_*`)** | `view` | Real-time query compilation | 1 raw telemetry record | Zero compute storage overhead; typed column casting. |
| **Dimensions (`dim_*`)** | `table` | Full rebuild / SCD2 timeline | 1 entity state version (`dim_users`) | Pre-computed surrogate keys, clustered on business keys. |
| **Facts (`fact_*`)** | `incremental` | `merge` with 3-day lookback | 1 event occurrence | Partitioned on `eventDate`, clustered on `(userId, campaignId)`. |
| **Marts (`marts_*`)** | `table` | Daily / Hourly refresh | Aggregated metrics by dimension | Clustered on query filter dimensions (`date`, `tier`). |

---

## 3. Incremental Merge Implementation

The fact models (`fact_streams.sql` and `fact_ad_events.sql`) utilize dbt's `incremental` materialization with optimized partition predicates:

```sql
{{ config(
    materialized='incremental',
    unique_key='adEventKey',
    incremental_strategy='merge',
    partition_by={
      'field': 'eventDate',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['campaignId', 'adPlacement'],
    incremental_predicates=[
      'DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)'
    ]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id', 'event_timestamp']) }} AS adEventKey,
    user_id AS userId,
    campaign_id AS campaignId,
    ad_placement AS adPlacement,
    event_type AS eventType,
    bid_amount_usd AS bidAmountUsd,
    event_timestamp AS eventTimestamp,
    DATE(event_timestamp) AS eventDate
FROM {{ ref('stg_ad_events') }}
{% if is_incremental() %}
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
{% endif %}
```

---

## 4. dbt Testing Hierarchy

1. **Schema Assertions** (`schema.yml`):
   - `unique` and `not_null` on primary/surrogate keys.
   - `relationships` foreign key integrity tests between facts and dimensions.
   - `accepted_values` on enum fields (`eventType`: `['impression', 'click', 'skip']`).
2. **Singular SQL Tests** (`dbt/tests/*.sql`):
   - Validates that `rowActivationDate <= rowExpirationDate` in `dim_users`.
   - Validates that conversion rates and CTR ratios remain strictly within $[0.0, 1.0]$.

