# Subsystem Revision Guide: Scalability & Growth Trajectory

## 1. Current Architecture Baseline vs. Scaling Roadmap

The Adaptive Ads data platform is engineered with clean modular boundaries to scale seamlessly from batch processing up to enterprise streaming volumes:

| Metric / Dimension | Current Baseline | 10x Scale | 100x Scale | 1,000x Streaming Scale |
| :--- | :--- | :--- | :--- | :--- |
| **Event Volume** | ~100K events/day (~1–2 MB/hr) | ~1M events/day (~10–20 MB/hr) | ~10M events/day (~100–200 MB/hr) | ~100M–1B events/day (>10 GB/hr) |
| **Ingestion Engine** | Airflow batch GCS to BigQuery | Airflow batch GCS to BigQuery | Airflow batch with micro-partitions | Cloud Pub/Sub + Apache Beam / Dataflow |
| **Storage Format** | Snappy Parquet on GCS | Snappy Parquet on GCS | Partitioned Parquet on GCS | Avro / Protobuf $\rightarrow$ BigQuery Storage Write API |
| **BigQuery Ingestion** | Batch Load Jobs (Free tier) | Batch Load Jobs (Free tier) | BigQuery Storage Write API | BigQuery Streaming Inserts / Storage Write API |
| **Transformation** | dbt Core batch hourly | dbt Core batch hourly | dbt Incremental Micro-batches | Streaming SQL (Dataflow / BigQuery continuous) |
| **Cost Profile** | < \$10 / month | ~\$25–\$50 / month | ~\$150–\$300 / month | ~\$1,500–\$3,500 / month |

---

## 2. What Breaks First & Bottleneck Analysis

When scaling volume from current to 100x scale:

1. **Airflow Task Scheduling Latency (10x–50x)**:
   - *Symptom*: If event streams increase from 4 to 40, DAG scheduling overhead and worker task queuing expand run times.
   - *Fix*: Increase Celery/Kubernetes executor worker concurrency and consolidate fine-grained tasks.

2. **BigQuery Full-Table Scans (50x–100x)**:
   - *Symptom*: dbt models without partition filters trigger massive byte scans, increasing query latency and query costs.
   - *Fix*: Mandatory `incremental_predicates` and `cluster_by` enforced on all fact tables (already implemented in Phase 3/6).

3. **GCS Object Listing Bottlenecks (100x)**:
   - *Symptom*: Millions of small Parquet files in a single directory slow down GCS glob operations (`gs://.../*.parquet`).
   - *Fix*: Suffix hive-partitioned prefixes (`dt=YYYY-MM-DD/hr=HH/`) and batch write larger 128MB Parquet chunks.

---

## 3. Transition to 1,000x Streaming Architecture

If business requirements transition from hourly batch SLAs to sub-minute real-time ad bidding:

```
[Edge Clients / Ad Servers]
             │ (High-throughput telemetry)
             ▼
   [Cloud Pub/Sub Topics]
             │ (Real-time distributed queue)
             ▼
[Apache Beam / Cloud Dataflow Pipeline]
  - Stream deduplication (sliding watermarks)
  - Schema validation & dead-letter queue routing
             │
             ▼ (BigQuery Storage Write API)
   [BigQuery Streaming Tables]
             │
             ▼
   [Looker Studio Real-Time BI]
```

---

## 4. Current Technical Debt Register

As audited in [`docs/TECHNICAL_DEBT.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/TECHNICAL_DEBT.md):
- **Critical / High Debt**: **0 items**.
- **Medium Debt (2 items)**:
  1. *Airflow Celery Worker Scaling*: Currently uses LocalExecutor for single-node environments.
  2. *dbt Snapshot Migration*: SCD2 is implemented via pure SQL window functions; future migration to native dbt snapshots could simplify maintenance if real-time state storage is introduced.
- **Low Debt (2 items)**:
  1. *Looker Studio Direct Query Caching*: Production should leverage BI Engine 1GB memory reservation.
  2. *Alerting Webhook Integration*: Local environment logs alerts; production requires Slack/PagerDuty webhook URLs.

