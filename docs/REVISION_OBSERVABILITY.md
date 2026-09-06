# Subsystem Revision Guide: Observability, Alerts & SLOs

## 1. Observability Overview

Pipeline observability ensures that any ingestion degradation, transformation failure, schema drift, or data freshness violation is detected and routed to the engineering team before downstream BI consumers are impacted.

---

## 2. Service Level Objectives (SLOs) & Error Budgets

As documented in [`docs/SLO.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/SLO.md):

| Dimension | Target SLO | Evaluation Window | Error Budget Policy | Implemented vs. Proposed Status |
| :--- | :--- | :--- | :--- | :--- |
| **Data Freshness** | 99.0% of hourly runs complete within 45 min | 30-day rolling | If budget < 10%, freeze non-critical schema migrations | Proposed target (Verified locally via DAG configs) |
| **Pipeline Availability** | 99.5% successful daily DAG runs | 30-day rolling | Automatic PagerDuty alert on 2 consecutive hourly failures | Proposed target |
| **Data Quality Pass Rate** | 99.9% of dbt tests pass cleanly | Per run | Any Tier 2/3 test failure halts downstream mart deployment | Implemented & Verified in CI (`dbt test`) |
| **Disaster Recovery** | RPO $\le$ 1 hour, RTO $\le$ 30 min | Incident basis | Post-mortem root cause analysis within 24 hours | Implemented playbooks in [`DISASTER_RECOVERY.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/DISASTER_RECOVERY.md) |

---

## 3. Alerting & Routing Architecture

As detailed in [`docs/ALERTING.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/ALERTING.md):

```
+-----------------------------------------------------------------------------------+
| PIPELINE EVENT EMITTERS                                                           |
| Airflow DAG Failure | dbt Test Failure | Contract Drift | GCS Missing Files       |
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
| ALERT SEVERITY ROUTING                                                            |
|                                                                                   |
|  SEVERITY 1 (CRITICAL): Downstream BI Mart Build Blocked                         |
|  - Channel: PagerDuty On-Call + #data-eng-urgent Slack                            |
|                                                                                   |
|  SEVERITY 2 (WARNING): Transient Retry / Upstream GCS Lag                         |
|  - Channel: #data-eng-alerts Slack Channel                                        |
|                                                                                   |
|  SEVERITY 3 (INFO): Schema Additions / Successful Backfills                       |
|  - Channel: #data-eng-audit Log                                                   |
+-----------------------------------------------------------------------------------+
```

---

## 4. Operational Runbooks

When an alert triggers, on-call engineers consult [`docs/OPERATIONS_RUNBOOK.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/OPERATIONS_RUNBOOK.md) for standard diagnostic and remediation workflows:
1. Identify failing task ID and execution timestamp in Airflow UI.
2. Check task logs for specific BigQuery errors (quota exceeded, schema mismatch, or missing upstream GCS files).
3. If bad data arrived, quarantine the offending partition and trigger [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py).
4. Verify table health via [`scripts/validate.sh`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate.sh).

