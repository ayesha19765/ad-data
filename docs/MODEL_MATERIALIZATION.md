# Model Materialization & Transformation Architecture

## Overview
This document catalogs the materialization strategy, data grain, update frequency, incremental configuration, and partition/clustering design for all 17 dbt models in the **Adaptive Ads** data warehouse.

---

## 1. Materialization Classification Matrix

| Layer | Model Name | Materialization | Primary Grain | Update Frequency | Incremental Strategy / Refresh | Partition Key | Cluster Keys |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Staging** | `stg_watch_events` | `view` | 1 row per watch event | Query-time | View on staging table | Inherited (`ts` HOUR) | None |
| **Staging** | `stg_ad_events` | `view` | 1 row per ad event | Query-time | View on staging table | Inherited (`ts` HOUR) | None |
| **Staging** | `stg_page_view_events` | `view` | 1 row per page view | Query-time | View on staging table | Inherited (`ts` HOUR) | None |
| **Staging** | `stg_auth_events` | `view` | 1 row per auth attempt | Query-time | View on staging table | Inherited (`ts` HOUR) | None |
| **Staging** | `stg_movies` | `view` | 1 row per IMDb title | Query-time | View on raw catalog | None | None |
| **Staging** | `stg_state_codes` | `view` | 1 row per US state | Query-time | View on seed | None | None |
| **Core** | `dim_users` | `table` | 1 row per user tier window (SCD2) | Hourly | Full table rebuild via window aggregations | None | `userId` |
| **Core** | `dim_movies` | `table` | 1 row per IMDb movie | Daily/On-demand | Replaced when catalog updates | None | `movieId` |
| **Core** | `dim_location` | `table` | 1 row per unique coordinate set | Hourly | Full rebuild from watch coordinates | None | `stateCode`, `city` |
| **Core** | `dim_datetime` | `table` | 1 row per calendar hour | Static / Seeded | Rebuilt across calendar horizon (2019–2025) | None | None |
| **Core** | `fact_streams` | `incremental` | 1 row per video playback | Hourly | `merge` with 3-day sliding lookback | `ts` (DAY) | `userKey`, `videoKey`, `locationKey` |
| **Core** | `fact_ad_events` | `incremental` | 1 row per ad impression/click | Hourly | `merge` with 3-day sliding lookback | `ts` (DAY) | `userKey`, `adType`, `videoKey` |
| **Marts** | `daily_ad_metrics` | `table` | 1 row per date, adType, videoKey | Hourly | Full aggregated table | `ad_date` (DAY) | `adType`, `videoKey` |
| **Marts** | `daily_user_engagement`| `table` | 1 row per date, tier | Hourly | Full aggregated table | `activity_date` (DAY)| `subscription_tier` |
| **Marts** | `ad_content_performance`| `table` | 1 row per videoKey, adType | Hourly | Aggregated content mart | None | `adType`, `content_genre` |
| **Marts** | `wide_streams` | `view` | 1 row per stream event | Query-time | Zero-storage denormalized view | Inherited (`ts` DAY) | Inherited from `fact_streams` |
| **Marts** | `top_action_movies` | `table` | 1 row per top action movie | Daily/On-demand | Materialized ranked table | None | None |

---

## 2. Rationale by Materialization Type

### Views (`staging/*`, `marts/wide_streams`)
- **Staging Views**: Act as lightweight semantic abstractions over raw BigQuery tables. They perform type casting, column trimming, and `COALESCE` null-handling without duplicating data on disk.
- **Wide Streams View**: Avoids redundant physical storage of a 25-column denormalized table. By remaining a `view` over `fact_streams` and dimension tables, BigQuery query pushdown evaluates only the columns requested by Looker Studio queries.

### Tables (`core/dim_*`, `marts/*`)
- **Dimension Tables**: Dimension entities are low-to-medium cardinality (thousands of rows for movies and locations). Materializing as `table` provides pre-computed surrogate keys and removes join overhead on downstream fact loads.
- **Aggregated Marts**: Analytical summary tables (`daily_ad_metrics`, `daily_user_engagement`, `ad_content_performance`) aggregate millions of fact rows into compact summaries. Pre-materializing them as tables ensures dashboard queries return in sub-second time without scanning raw facts.

### Incremental (`core/fact_streams`, `core/fact_ad_events`)
- **High-Volume Telemetry**: Stream and ad facts represent the largest data volume in the platform (append-only hourly event batches).
- **Merge Strategy**: Uses `incremental_strategy = 'merge'` on `unique_key = 'streamKey'` / `'adEventKey'`.
- **Partition Pruning**: Uses `incremental_predicates` to bound target partition scans to the active lookback window (`INTERVAL 7 DAY`), eliminating full-table scans during merge updates.
- **Late-Arriving Lookback**: Filters source data by `ts >= TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY)` to reconcile delayed mobile or web telemetry.

