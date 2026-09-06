# Subsystem Revision Guide: Data Security & Governance

## 1. Security & Governance Principles

The Adaptive Ads platform implements enterprise data security and governance practices across four dimensions:
1. **Secret & Credential Hygiene**
2. **Role-Based Access Control (IAM Least Privilege)**
3. **PII Isolation & Anonymization**
4. **Data Retention & Lifecycle Management**

---

## 2. PII Isolation & Data Classification

Data is categorized into three tiers as detailed in [`docs/DATA_GOVERNANCE.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/DATA_GOVERNANCE.md):

```
+-----------------------------------------------------------------------------------+
| TIER 1: PUBLIC / AGGREGATE                                                        |
| Looker Studio Dashboards & Marts (`marts.daily_ad_metrics`)                        |
| - Aggregate counts, sums, ratios. Zero individual identifiers.                     |
+-----------------------------------------------------------------------------------+
                                         ▲
                                         │ (Aggregated & Joined via Hash Keys)
+-----------------------------------------------------------------------------------+
| TIER 2: INTERNAL / PSEUDONYMOUS                                                   |
| Fact Tables (`core.fact_streams`, `core.fact_ad_events`)                           |
| - Primary identifiers: Cryptographic surrogate keys (`userKey`, `streamKey`).     |
| - No raw emails, IP addresses, or personal names.                                 |
+-----------------------------------------------------------------------------------+
                                         ▲
                                         │ (Strict Column-Level Access Controls)
+-----------------------------------------------------------------------------------+
| TIER 3: SENSITIVE / PII                                                           |
| Dimension Table (`core.dim_users`)                                                |
| - Raw demographic & identity attributes isolated exclusively in SCD2 dimension.   |
| - Restricted access to authorized data stewards via BigQuery IAM policy tags.    |
+-----------------------------------------------------------------------------------+
```

---

## 3. IAM Least-Privilege Access Matrix

| Persona / Service Account | BigQuery Role | GCS Role | Description |
| :--- | :--- | :--- | :--- |
| **`sa-airflow-worker`** | `roles/bigquery.jobUser`, `roles/bigquery.dataEditor` (`staging`) | `roles/storage.objectViewer` (`raw/`) | Ingests raw Parquet into BigQuery staging dataset only. |
| **`sa-dbt-runner`** | `roles/bigquery.dataEditor` (`staging`, `core`, `marts`) | None | Reads staging, executes transformations, builds marts. |
| **`sa-looker-studio`** | `roles/bigquery.dataViewer` (`marts` only) | None | Read-only access to analytical marts layer; cannot access raw PII or facts. |
| **Data Engineers / Developers**| `roles/bigquery.admin` (`dev_*` datasets only) | `roles/storage.admin` (`dev-bucket`) | Sandbox development environments. |

---

## 4. Retention Policies & Secret Hygiene

- **Cloud Storage Lifecycle**: Raw telemetry Parquet files in `gs://.../raw/` transition to Coldline storage at 90 days and are purged at 365 days.
- **BigQuery Partition Expiration**: Raw staging tables expire partitions after 30 days; core fact tables retain partitions for 730 days (2 years).
- **Secret Hygiene Scanner**: Verified on every commit and CI run to guarantee zero plaintext API keys, service account JSON files, or `.env` configurations in version control.

