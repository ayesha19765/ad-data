# BI Dashboard Specification: Looker Studio & Executive Analytics

This document specifies the Business Intelligence (BI) visualization layer for the **Adaptive Ads Data Engineering Platform**. It details KPI scorecards, time-series charts, dimension breakdowns, and filter controls designed for connection to Google Looker Studio or Tableau.

---

## 1. Executive Summary & KPI Scorecard

The top section of the dashboard delivers high-level operational visibility for advertising and audience engagement metrics:

```
┌─────────────────────────┬─────────────────────────┬─────────────────────────┬─────────────────────────┐
│ Total Ad Impressions    │ Total Ad Watch Duration │ Active Daily Viewers    │ Free Tier Viewer Ratio  │
│ 1,245,890               │ 34,512.4 hrs            │ 84,320                  │ 78.4%                   │
│ ▲ +12.4% vs prev week   │ ▲ +8.2% vs prev week    │ ▲ +5.1% vs prev week    │ ▼ -1.2% vs prev week    │
└─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

### Underlying Datasets & Measures
- **Dataset**: `adaptive_ads_prod.daily_ad_metrics`
- **KPI Metrics**:
  - `total_impressions`: `SUM(total_impressions)`
  - `total_ad_duration_hours`: `SUM(total_ad_duration_seconds) / 3600.0`
  - `unique_viewers`: `SUM(unique_viewers)`
  - `free_tier_ratio`: `SAFE_DIVIDE(SUM(free_tier_impressions), SUM(total_impressions))`

---

## 2. Temporal Engagement & Performance Trends

### Chart 1: Daily Ad Impressions & Duration Trend
- **Visual Type**: Dual-axis Time Series (Bar + Line)
- **X-Axis**: `ad_date` (Date)
- **Bar Metric (Left Y-Axis)**: `total_impressions` (SUM)
- **Line Metric (Right Y-Axis)**: `total_ad_duration_seconds` (SUM)
- **Breakdown Dimension**: `adType` (pre-roll, mid-roll, banner)

### Chart 2: Daily Streaming Volume by Subscription Tier
- **Visual Type**: 100% Stacked Area Chart
- **Dataset**: `adaptive_ads_prod.daily_user_engagement`
- **X-Axis**: `activity_date` (Date)
- **Y-Axis**: `total_streams` (SUM)
- **Series Dimension**: `subscription_tier` (`free` vs `paid`)

---

## 3. Content Placement & Ad Format Breakdown

### Table: Content Title Ad Exposure Matrix
- **Dataset**: `adaptive_ads_prod.ad_content_performance`
- **Sort**: `total_impressions` DESC

| Content Title | Genre | IMDb Rating | Ad Format (`adType`) | Impressions | Unique Viewers | Total Duration (s) | Avg Duration (s) | Free Tier % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| The Dark Knight | Action | 9.0 | mid-roll | 142,300 | 28,400 | 4,269,000 | 30.0s | 76.5% |
| Inception | Action | 8.8 | pre-roll | 118,200 | 31,200 | 1,773,000 | 15.0s | 82.1% |
| Interstellar | Sci-Fi | 8.7 | mid-roll | 98,450 | 19,800 | 2,953,500 | 30.0s | 74.2% |

---

## 4. Geospatial & Demographic Slicing

### Chart 3: Streaming Volume by US State
- **Visual Type**: Geo Map / Choropleth
- **Dataset**: `adaptive_ads_prod.wide_streams`
- **Location Field**: `state` (`dim_location.stateName`)
- **Metric**: `COUNT(streamKey)` (Streaming Sessions)
- **Tooltip**: `unique_users`, `avg_duration`

---

## 5. Global Interactive Filter Controls

| Filter Control | Source Field | Target Models | Behavior |
| :--- | :--- | :--- | :--- |
| **Date Range** | `ad_date` / `activity_date` | All Marts | Dynamic calendar window selector (Default: Last 30 Days). |
| **Ad Format** | `adType` | `daily_ad_metrics`, `ad_content_performance` | Multi-select dropdown (pre-roll, mid-roll, post-roll, banner). |
| **Content Genre** | `content_genre` | `ad_content_performance`, `wide_streams` | Multi-select dropdown (Action, Sci-Fi, Crime, Drama, etc.). |
| **Membership Tier** | `subscription_tier` / `level` | All Marts | Single-select radio (All, Free, Paid). |

---

## 6. Live Looker Studio Connection Guide

To connect Google Looker Studio to the warehouse:
1. Open [Looker Studio](https://lookerstudio.google.com/) -> **Create** -> **Data Source**.
2. Select the **BigQuery** connector.
3. Choose Project: `${GCP_PROJECT_ID}` -> Dataset: `adaptive_ads_prod`.
4. Select target table:
   - Primary KPI Dashboard: `daily_ad_metrics`
   - Content Performance: `ad_content_performance`
   - Audience Engagement: `daily_user_engagement`
5. Enable **Partitioning / Date Range Dimension** on `ad_date` / `activity_date` to leverage BigQuery partition pruning for low query cost.

