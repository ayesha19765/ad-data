# Platform Component Ownership & RACI Matrix

## 1. Logical Component Ownership

This document defines the logical ownership, engineering boundaries, and operational accountability across all subsystems of the **Adaptive Ads** data platform.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     COMPONENT OWNERSHIP TOPOLOGY                        │
└─────────────────────────────────────────────────────────────────────────┘
  [ Airflow Orchestration ]   ──► Data Engineering
  [ GCS Raw Landing ]         ──► Data Engineering / Infrastructure
  [ BigQuery Staging ]        ──► Data Engineering
  [ dbt Core Warehouse ]      ──► Analytics Engineering / Data Engineering
  [ Analytical Marts ]        ──► Analytics Engineering
  [ Looker Studio BI ]        ──► Product Analytics & BI Team
  [ CI/CD & QA Tooling ]      ──► Platform Engineering
```

---

## 2. RACI Accountability Matrix

| Platform Layer / Asset | Responsible (R) | Accountable (A) | Consulted (C) | Informed (I) |
| :--- | :--- | :--- | :--- | :--- |
| **Telemetry Contracts (`contracts/`)** | Producer SDK Teams | Data Engineering | Analytics Engineering | BI Team |
| **Airflow DAGs & TaskGroups** | Data Engineering | Data Engineering Lead | Platform Eng | Analytics Eng |
| **GCS Telemetry Buckets** | Data Engineering | Infrastructure Team | Security Team | Compliance |
| **BigQuery Staging Tables** | Data Engineering | Data Engineering Lead | Analytics Eng | BI Team |
| **dbt Dimensions & Facts** | Analytics Engineering | Analytics Engineering | Data Engineering | Business Stakeholders |
| **Analytical Marts** | Analytics Engineering | BI Lead | Business Analysts | Executive Team |
| **Looker Studio Dashboards** | BI Analysts | Product Analytics Lead | Ad Operations | Marketing / Growth |
| **CI/CD Quality Gates** | Platform Engineering | Lead Data Engineer | DevOps | All Contributors |

