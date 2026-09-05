# Phase 3: Data Warehouse Engineering & Analytics Layer

## 1. Warehouse Architecture

The Adaptive Ads Data Engineering Platform implements an enterprise-grade multi-layer data warehouse following Kimball dimensional modeling best practices on Google BigQuery:

```
Raw Staging (BigQuery Partitioned Tables)
                 ↓
dbt Staging Layer (views: stg_watch_events, stg_ad_events, stg_movies, etc.)
                 ↓
dbt Core Dimensional Warehouse (tables/incremental: dims & facts)
                 ↓
dbt Analytical Marts & Reporting Layer (tables/views: daily_ad_metrics, daily_user_engagement, wide_streams)
```

---

## 2. Model Inventory & Layer Responsibilities

| Layer | Model Name | Materialization | Grain | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Staging** | `stg_watch_events` | View | One row per watch telemetry event | Standardized, casted, null-coalesced video stream events. |
| **Staging** | `stg_ad_events` | View | One row per ad interaction event | Standardized, casted, null-coalesced ad impression events. |
| **Staging** | `stg_page_view_events` | View | One row per page view event | Standardized web/app navigation events. |
| **Staging** | `stg_auth_events` | View | One row per authentication event | Standardized user session/auth events. |
| **Staging** | `stg_movies` | View | One row per movie/video content item | Standardized content catalog metadata. |
| **Staging** | `stg_state_codes` | View | One row per US state | Standardized state code lookup reference. |
| **Core** | `dim_users` | Table (SCD Type 2) | One row per user per tier validity period | Tracks user tier transitions (free vs paid) with zero-gap intervals. |
| **Core** | `dim_movies` | Table | One row per content item + anonymous row | Enriched content dimension. |
| **Core** | `dim_location` | Table | One row per geographic city/state coordinate | Geographic location dimension. |
| **Core** | `dim_datetime` | Table | One row per calendar hour | Date and time dimension with calendar attributes. |
| **Core** | `fact_streams` | Incremental Table (Merge) | One row per video streaming event | Hourly stream facts partitioned by day and clustered. |
| **Core** | `fact_ad_events` | Incremental Table (Merge) | One row per ad impression / interaction | Ad interaction facts partitioned by day and clustered. |
| **Marts** | `daily_ad_metrics` | Table | One row per calendar date, adType, and video | Daily ad performance mart with safe derived rates. |
| **Marts** | `daily_user_engagement` | Table | One row per calendar date and subscription tier | Daily audience streaming engagement metrics. |
| **Marts** | `wide_streams` | View | One row per streaming event | Denormalized wide analytical reporting view for BI. |
| **Marts** | `top_action_movies` | Table | One row per top-ranked IMDb action movie | Content catalog analysis mart. |

---

## 3. Incremental Strategy & Late-Arriving Events

### Incremental Strategy (`fact_streams`, `fact_ad_events`)
- **Materialization**: `incremental`
- **Incremental Strategy**: `merge` with primary surrogate keys (`streamKey`, `adEventKey`).
- **Idempotency**: Using `unique_key` ensures that re-running pipelines never duplicates fact records.

### Late-Arriving Data Lookback Window
Telemetry streams can arrive delayed due to client offline queuing or network retries. During incremental runs, fact models inspect the high-water mark and query an overlapping 3-day lookback window:
```sql
{% if is_incremental() %}
WHERE ts >= (SELECT TIMESTAMP_SUB(MAX(ts), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
```
- **First Build**: Scans and builds all historical partitions.
- **Incremental Runs**: Scans only the last 3 days of events, updating or inserting records into the appropriate daily partitions via BigQuery `MERGE`.
- **Performance**: Eliminates expensive full-table historical scans while maintaining exact data completeness.

---

## 4. BigQuery Partitioning & Clustering

### Partitioning Specifications
| Model | Partition Field | Granularity | Rationale |
| :--- | :--- | :--- | :--- |
| `fact_streams` | `ts` | Day | Enables partition pruning on temporal event queries and aligns with incremental loads. |
| `fact_ad_events` | `ts` | Day | Enables partition pruning for ad interaction time-series analysis. |
| `daily_ad_metrics` | `ad_date` | Day | Aggregated daily mart partitioned for fast dashboard date-range filtering. |
| `daily_user_engagement` | `activity_date` | Day | Aggregated audience mart partitioned for trend analysis. |

### Clustering Specifications
| Model | Clustering Columns | Rationale |
| :--- | :--- | :--- |
| `fact_streams` | `["userKey", "videoKey", "locationKey"]` | Colocates stream facts by user and content keys for high-speed dimensional joins and filtering. |
| `fact_ad_events` | `["userKey", "adType", "videoKey"]` | Colocates ad facts by user, ad format (e.g. pre-roll/banner), and target video. |
| `daily_ad_metrics` | `["adType", "videoKey"]` | Optimizes slice-and-dice queries by ad format and video placement. |
| `daily_user_engagement` | `["subscription_tier"]` | Accelerates segmentation queries comparing free vs paid users. |
| `dim_users` | `["userId"]` | Accelerates surrogate key lookups and SCD2 historical reconstruction by user ID. |
| `dim_movies` | `["movieId"]` | Accelerates content key joins. |
| `dim_location` | `["stateCode", "city"]` | Accelerates regional filtering and geospatial grouping. |

---

## 5. SCD Type 2 Implementation (`dim_users`)

The `dim_users` dimension tracks changes to the user subscription tier (`level`: e.g. free vs paid) over time:
- **Natural Business Key**: `userId`
- **Surrogate Primary Key**: `userKey` generated via `dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level'])`.
- **Validity Intervals**:
  - `rowActivationDate`: `MIN(date)` of the subscription tier group.
  - `rowExpirationDate`: `LEAD(minDate, 1, DATE '9999-12-31') OVER (PARTITION BY userId... ORDER BY grouped)`.
- **Active Record Flag**: `currentRow = 1` for the latest record, `0` for historical records.
- **Uniqueness & Integrity**: Guaranteed non-overlapping validity ranges and tested for single active row per user ID.

---

## 6. Data Quality & Test Suite

### Generic Tests
1. **Uniqueness & Non-Null**:
   - Primary and surrogate keys in all staging, core, and marts models (`userKey`, `movieKey`, `locationKey`, `dateKey`, `streamKey`, `adEventKey`, `dailyAdMetricKey`, `dailyEngagementKey`).
2. **Referential Integrity (`relationships`)**:
   - `fact_streams.userKey` → `dim_users.userKey`
   - `fact_streams.videoKey` → `dim_movies.movieKey`
   - `fact_streams.locationKey` → `dim_location.locationKey`
   - `fact_streams.dateKey` → `dim_datetime.dateKey`
   - `fact_ad_events.userKey` → `dim_users.userKey`
   - `fact_ad_events.videoKey` → `dim_movies.movieKey`
   - `fact_ad_events.locationKey` → `dim_location.locationKey`
   - `fact_ad_events.dateKey` → `dim_datetime.dateKey`
3. **Domain & Enum Tests (`accepted_values`)**:
   - `dim_users.level`: `['free', 'paid', 'NA']`
   - `dim_users.currentRow`: `[0, 1]`
   - `dim_datetime.weekendFlag`: `[true, false]`

### Singular Business Quality Tests (`tests/`)
1. [`assert_dim_users_single_active_row.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/tests/assert_dim_users_single_active_row.sql): Asserts that no user ID has more than one active record (`currentRow = 1`).
2. [`assert_daily_ad_metrics_non_negative.sql`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/tests/assert_daily_ad_metrics_non_negative.sql): Asserts that aggregated metrics (`impressions`, `duration`, `unique_viewers`) are strictly non-negative.

---

## 7. Analytical Marts & Reusable Business Metrics

### 1. `daily_ad_metrics`
- **Purpose**: Tracks advertising delivery, engagement duration, and audience reach across ad formats and content placements.
- **Grain**: One row per `ad_date`, `adType`, and `videoKey`.
- **Metrics**:
  - `total_impressions`: `COUNT(adEventKey)`
  - `unique_viewers`: `COUNT(DISTINCT userKey)`
  - `total_ad_duration_seconds`: `SUM(duration)`
  - `avg_ad_duration_seconds`: `SAFE_DIVIDE(SUM(duration), COUNT(adEventKey))` (safe division against zero)
  - `free_tier_impressions`: `COUNTIF(level = 'free')`
  - `paid_tier_impressions`: `COUNTIF(level = 'paid')`
  - `free_tier_ratio`: `SAFE_DIVIDE(COUNTIF(level = 'free'), COUNT(adEventKey))`

### 2. `daily_user_engagement`
- **Purpose**: Measures audience platform usage and streaming duration segmented by membership tier.
- **Grain**: One row per `activity_date` and `subscription_tier`.
- **Metrics**:
  - `total_streams`: `COUNT(streamKey)`
  - `unique_streaming_users`: `COUNT(DISTINCT userKey)`
  - `total_watch_hours`: `SAFE_DIVIDE(SUM(duration), 3600.0)`
  - `avg_stream_duration_seconds`: `SAFE_DIVIDE(SUM(duration), COUNT(streamKey))`

### 3. `wide_streams`
- **Purpose**: Denormalized analytics view joining stream facts with all core dimensions for ad-hoc BI slicing in Looker / Tableau.

---

## 8. Query Optimization & Warehouse Performance

1. **Eliminated `SELECT *` in Downstream Joins**: Explicit column projections prevent unnecessary BigQuery slot utilization.
2. **Partition Pruning**: Date/timestamp partitioning ensures queries filter only relevant day partitions.
3. **Clustering Sort Acceleration**: Clustered columns colocated in storage blocks minimize bytes scanned during dimension joins and `GROUP BY` aggregations.
4. **Safe Division**: All ratios and averages utilize `SAFE_DIVIDE` to avoid runtime divide-by-zero exceptions on empty partitions.

---

## 9. Deferred Work (Phase 4+ Roadmap)

1. **Automated CI/CD**: GitHub Actions workflow for SQLFluff linting and PR integration testing.
2. **BI Dashboard Artifacts**: Looker Studio dashboard configurations connecting to `daily_ad_metrics` and `wide_streams`.
3. **dbt_utils Modernization**: Future upgrade to `dbt_utils` >= 1.x (`generate_surrogate_key`).

