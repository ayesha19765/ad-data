# Subsystem Revision Guide: Dimensional Modeling & Star Schema

## 1. Dimensional Architecture Overview

The warehouse implements a **Star Schema** with conformed dimensions and transactional fact tables, designed to optimize ad monetization analytics and audience streaming performance.

```
                           +----------------------+
                           |   dim_location       |
                           +----------------------+
                           | locationKey (PK)     |
                           | countryCode          |
                           | city                 |
                           +----------------------+
                                      │
                                      ▼
+----------------------+   +----------------------+   +----------------------+
|   dim_users (SCD2)   |   |   fact_ad_events     |   |   dim_movies         |
+----------------------+   +----------------------+   +----------------------+
| userKey (PK)         |◄──┤ adEventKey (PK)      |──►| movieKey (PK)        |
| userId (NK)          |   | userKey (FK)         |   | movieId (NK)         |
| subscriptionTier     |   | campaignId           |   | title                |
| rowActivationDate    |   | locationKey (FK)     |   | genre                |
| rowExpirationDate    |   | bidAmountUsd         |   | releaseYear          |
| isCurrent            |   | eventTimestamp       |   +----------------------+
+----------------------+   | eventDate (Partition)|
                           +----------------------+
                                      ▲
                                      │
                           +----------------------+
                           |   fact_streams       |
                           +----------------------+
                           | streamKey (PK)       |
                           | userKey (FK)         |
                           | movieKey (FK)        |
                           | watchDurationSeconds |
                           | eventDate (Partition)|
                           +----------------------+
```

---

## 2. Model Grains & Key Definitions

| Model | Classification | Business Grain | Surrogate Key Formula | Natural Key(s) |
| :--- | :--- | :--- | :--- | :--- |
| **`dim_users`** | SCD Type 2 Dimension | 1 row per user state change | `MD5(userId || subscriptionTier || activationDate)` | `userId` |
| **`dim_movies`** | Conformed Dimension | 1 row per unique video content | `MD5(movieId)` | `movieId` |
| **`dim_location`**| Conformed Dimension | 1 row per unique geo-region | `MD5(countryCode || city)` | `countryCode`, `city` |
| **`fact_streams`** | Transactional Fact | 1 row per stream playback session | `MD5(userId || movieId || eventTimestamp)` | `(userId, movieId, eventTimestamp)` |
| **`fact_ad_events`**| Transactional Fact | 1 row per ad interaction | `MD5(eventId || eventTimestamp)` | `eventId` |
| **`daily_ad_metrics`**| Analytical Mart | 1 row per date, campaign, tier | `MD5(date || campaignId || subscriptionTier)` | `(date, campaignId, subscriptionTier)` |

---

## 3. Surrogate Key Strategy

### Why Hashes Over Sequences?
In BigQuery's distributed MPP architecture, auto-incrementing integer sequences (e.g., `SERIAL` or `IDENTITY`) cause severe serialization bottlenecks. We generate surrogate keys using cryptographic MD5 hashes (`dbt_utils.generate_surrogate_key`).

### Advantages:
1. **Fully Distributed**: Computed in parallel on worker nodes without coordination.
2. **Deterministic**: Generating a key for the same input values always yields the exact same hash across environments (`dev`, `test`, `prod`).
3. **Idempotent**: Re-running transformations produces identical primary keys.

---

## 4. Analytical Mart Aggregations

Marts pre-calculate complex, high-frequency analytical queries for downstream dashboards:

- **`daily_ad_metrics`**:
  - `total_impressions = COUNTIF(eventType = 'impression')`
  - `total_clicks = COUNTIF(eventType = 'click')`
  - `ctr = SAFE_DIVIDE(total_clicks, total_impressions)`
  - `ecpm = SAFE_DIVIDE(SUM(bidAmountUsd), total_impressions) * 1000`
  - `total_revenue = SUM(bidAmountUsd)`
- **Division by Zero Protection**: All derived ratios strictly utilize `SAFE_DIVIDE()` and `COALESCE()` to eliminate SQL runtime runtime exceptions.

