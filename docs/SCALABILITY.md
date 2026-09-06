# Enterprise Scalability Architecture & Growth Trajectory

## 1. Executive Summary
This document defines the architectural growth trajectory for the **Adaptive Ads** platform across 1x (current baseline), 10x, 100x, and 1,000x scale. It provides concrete capacity formulas and outlines the exact conditions under which architectural components evolve.

```
       1x Baseline              10x Scale               100x Scale               1,000x Scale
  [ 10K events/hr ]       [ 100K events/hr ]        [ 1M events/hr ]         [ 10M+ events/hr ]
         │                       │                        │                          │
   Hourly Batch            Optimized Batch          Tuned Micro-Batch       Real-Time Streaming
(Airflow + BigQuery)    (Airflow + BigQuery)     (Composer 2 + BigQuery)   (Pub/Sub + Dataflow)
```

---

## 2. Capacity Model & Growth Formulas

### Mathematical Formulas:
$$\text{Daily Raw Data (MB)} = \frac{\text{Events}}{\text{hour}} \times \text{Avg Size (bytes)} \times \frac{24\text{ hours}}{10^6}$$
$$\text{Annual Warehouse Storage (GB)} = \frac{\text{Daily Raw Data (MB)} \times 365 \times \text{Warehouse Expansion Factor (1.4)}}{1,000}$$

### Worked Capacity Projections:

| Scale Tier | Events / Hour | Daily Events | Avg Event Size (Parquet) | Daily Storage (Raw) | Annual Storage (Raw + Core) | Recommended Pipeline Pattern |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1x (Current)** | 10,000 | 240,000 | ~350 bytes | ~84 MB | ~43 GB | Hourly Batch (Current Stack) |
| **10x** | 100,000 | 2,400,000 | ~350 bytes | ~840 MB | ~430 GB | Hourly Batch + Slot Reservations |
| **100x** | 1,000,000 | 24,000,000 | ~350 bytes | ~8.4 GB | ~4.3 TB | 15-min Micro-batch + BI Engine |
| **1,000x** | 10,000,000 | 240,000,000 | ~350 bytes | ~84 GB | ~43 TB | Streaming Ingestion (Pub/Sub + Dataflow) |

---

## 3. Scale-Tier Engineering Strategies

### Tier 1: Current Baseline (1x Scale)
- **Current Stack**: Airflow TaskGroups + GCS Parquet + BigQuery External Tables + dbt Incremental (`merge`).
- **Characteristics**: Low cost, simple orchestration, zero compute idling between hourly runs.

### Tier 2: 10x Scale (100K events/hour)
- **Identified Bottlenecks**: Increased BigQuery slot usage during concurrent TaskGroup loads.
- **Solutions Implemented in Phase 6**:
  1. `incremental_predicates` restricts `MERGE` partition scans to the active 7-day window.
  2. Multi-column clustering (`userKey`, `adType`, `videoKey`) prevents block scans.
  3. Staging and core queries eliminate `SELECT *`, reducing memory overhead.

### Tier 3: 100x Scale (1M events/hour)
- **Expected Bottlenecks**:
  - GCS file count per directory causes external table creation delays.
  - Airflow worker task queue latency during peak load hours.
- **Architectural Enhancements (Future Scale)**:
  1. **BigQuery Storage Write API**: Replace transient external table creation with direct BigQuery batch loads or the Storage Write API.
  2. **BigQuery BI Engine**: Allocate a 10 GB BI Engine reservation to accelerate Looker Studio dashboard queries directly in memory.
  3. **Airflow Celery / Kubernetes Executor**: Dynamically scale worker pods based on TaskGroup backlog.

### Tier 4: 1,000x Scale (10M+ events/hour — Continuous Streaming)
- **Trigger Condition**: When business requirements demand sub-minute telemetry latency or when hourly Parquet file counts exceed BigQuery batch load limits.
- **Future Architectural Migration**:
  - **Ingestion**: Client telemetry publishes directly to **Google Cloud Pub/Sub**.
  - **Stream Processing**: **Google Cloud Dataflow** (Apache Beam) executes windowed streaming deduplication, sessionization, and watermarking.
  - **Sink**: Dataflow writes directly to partitioned BigQuery staging and fact tables in real time via BigQuery Streaming Inserts / Storage Write API.
  - **Transformation**: dbt runs on a micro-batch cadence (or dbt Streaming) to maintain downstream analytics marts.

