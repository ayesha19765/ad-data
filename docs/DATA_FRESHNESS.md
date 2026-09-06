# Data Freshness & SLA Architecture

## 1. Data Freshness Targets by Layer

The **Adaptive Ads** platform operates on a coordinated hourly ELT schedule. The table below outlines the expected data freshness, acceptable latency thresholds, and target SLAs for each architectural layer.

```
[ Event Generation (T=0) ]
           │  (Batch buffer: 60 min)
           ▼
[ GCS Landing (T+5m) ]
           │  (Airflow Ingestion: 5-10 min)
           ▼
[ BigQuery Staging (T+15m) ]
           │  (dbt Core & Marts: 10-15 min)
           ▼
[ Core Warehouse & Marts (T+30m) ]
           │  (Looker Studio Cache: 60 min TTL)
           ▼
[ BI Dashboards & End-User Reports (T+90m max) ]
```

| Layer | Schedule / Trigger | Target Freshness | Warning Threshold (SLA Delay) | Critical Incident Threshold (P2) |
| :--- | :--- | :--- | :--- | :--- |
| **GCS Raw Landing** | Hourly batch emission | ≤ 65 minutes | > 75 minutes | > 120 minutes |
| **BigQuery Staging** | Airflow DAG (`5 * * * *`) | ≤ 75 minutes | > 90 minutes | > 120 minutes |
| **Core Facts & Dims**| Post-ingestion dbt run | ≤ 90 minutes | > 105 minutes | > 150 minutes |
| **Analytical Marts** | Unified dbt DAG execution | ≤ 90 minutes | > 105 minutes | > 150 minutes |
| **BI Dashboards** | Direct query / 1-hr cache | ≤ 120 minutes | > 180 minutes | > 240 minutes |

> [!NOTE]
> All freshness numbers above represent target engineering SLAs for the production environment.

---

## 2. Freshness Detection & Automated Monitoring

### 1. dbt Source Freshness Checks
dbt source freshness assertions monitor ingestion latency against GCS landing tables:
```yaml
# dbt/models/staging/schema.yml
sources:
  - name: staging
    freshness:
      warn_after: {count: 90, period: minute}
      error_after: {count: 150, period: minute}
    loaded_at_field: ts
    tables:
      - name: watch_events
      - name: ad_events
```

### 2. Airflow Task Execution & SLA Callbacks
- Airflow DAG `adaptive_ads_dag` defines `execution_timeout = timedelta(minutes=15)` per task and `retry_delay = timedelta(minutes=3)`.
- If an ingestion TaskGroup exceeds expected duration, the `sla_miss_callback` triggers an alert to the data platform on-call channel.

---

## 3. Incident Recovery Protocol

If an SLA breach occurs:
1. **Identify Bottleneck**: Check Airflow DAG run status to determine whether delay is in GCS file arrival, BigQuery ingestion, or dbt model compilation.
2. **Isolate Event Stream**: Since TaskGroups are independent, failure in `auth_events` does not invalidate `watch_events` partitions.
3. **Execute Catchup / Backfill**: Use `scripts/backfill.py` to trigger parallel re-ingestion of delayed hourly partitions once upstream connectivity is restored.

