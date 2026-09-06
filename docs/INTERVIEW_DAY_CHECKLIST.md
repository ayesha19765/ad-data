# Interview Day Checklist & Whiteboard Guide

Follow this final countdown guide on the day of your technical interview.

---

## 1. The Final Countdown (15m, 10m, 5m)

### ⏱️ 15 Minutes Before:
- Open [`docs/ONE_PAGE_CHEATSHEET.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/ONE_PAGE_CHEATSHEET.md) on your screen.
- Verify key metrics: 4 telemetry streams, hourly schedule, 19 unit tests, 7 CI gates, 3-day lookback window.

### ⏱️ 10 Minutes Before:
- Review the **C-I-T-Q-C** framework in [`docs/ANSWER_FRAMEWORKS.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/ANSWER_FRAMEWORKS.md) for your introductory pitch.
- Mentally rehearse the SCD Type 2 `Free` $\rightarrow$ `Premium` attribution scenario.

### ⏱️ 5 Minutes Before:
- Take a deep breath. Remember: You built an idempotent, enterprise-grade ELT platform with robust testing and governance. You know every file and trade-off.

---

## 2. Whiteboard Architecture Diagram

If asked to draw the architecture on a physical whiteboard or virtual canvas (Miro/Excalidraw), draw this clean 5-box flow:

```
[1. Ingestion]           [2. Storage]               [3. Orchestration & Warehouse]           [4. Analytics]
Edge Clients /           Google Cloud               BigQuery (ELT + dbt)                    Looker Studio
Ad Servers               Storage (GCS)
+----------------+       +-------------------+      +--------------------------------+      +---------------+
| watch_events   | ----> | gs://bucket/raw/  | ---> | Staging (Partition Delete+Load)| ---> | Executive KPI |
| ad_events      |       |  {event}/YYYY/MM/ |      |   │                            |      | Dashboard     |
| page_views     |       |  DD/HH/*.parquet  |      |   ▼ (dbt run)                  |      | - eCPM, CTR   |
| auth_events    |       +-------------------+      | Core Dims (SCD2 dim_users)     |      | - Ad Revenue  |
+----------------+                                  | Core Facts (fact_ad_events)    |      | - Watch Hours |
                                                    |   │                            |      +---------------+
                                                    |   ▼                            |
                                                    | Marts (daily_ad_metrics)       |
                                                    +--------------------------------+
                                                                    │
                                                            [5. Quality & CI]
                                                            GitHub Actions (7 Gates)
                                                            19 Unit Tests + YAML Contracts
```

---

## 3. Whiteboard Talking Points
1. **Highlight the GCS path**: Shows intentional date/hour partitioning.
2. **Highlight the Airflow TaskGroup delete+insert**: Demonstrates idempotency.
3. **Highlight SCD2 in `dim_users`**: Demonstrates deep data modeling expertise.
4. **Highlight `incremental_predicates` in facts**: Demonstrates warehouse cost awareness.
5. **Highlight the Marts layer**: Explains why dashboards query marts instead of raw facts.

