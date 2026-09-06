# Enterprise Data Governance & Privacy Architecture

## 1. Data Classification Matrix

The **Adaptive Ads** platform categorizes all data assets into three distinct governance sensitivity tiers:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      DATA CLASSIFICATION TIERS                          │
└─────────────────────────────────────────────────────────────────────────┘
  [ TIER 1: PUBLIC ]    ──► Non-sensitive reference seeds & IMDb catalog
  [ TIER 2: INTERNAL ]  ──► Aggregated KPI metrics & dimensional marts
  [ TIER 3: SENSITIVE ] ──► Raw telemetry streams & demographic user profiles
```

| Classification | Datasets / Models | Sensitivity | Access Scope | Retention Policy |
| :--- | :--- | :--- | :--- | :--- |
| **Tier 1: Public / Reference** | `stg_state_codes`, `dim_movies`, `stg_movies` | Low | Engineering, Analysts, Public Dashboards | Permanent / Upstream cadence |
| **Tier 2: Internal / Business**| `daily_ad_metrics`, `ad_content_performance`, `daily_user_engagement`, `wide_streams` | Medium | Authorized Business Analysts, Looker Studio | 730 Days in BigQuery |
| **Tier 3: Sensitive / PII** | `watch_events`, `ad_events`, `auth_events`, `page_view_events`, `dim_users` | High | Restricted Data Engineering & Compliance | GCS 365d / BQ 730d |

---

## 2. PII Considerations & Pseudonymization Architecture

### Identification vs Pseudonymity
- **Direct PII**: The raw event streams contain user demographic attributes (`firstName`, `lastName`, `dateOfBirth`, `gender`, geographic coordinates `lat`/`lon`).
- **Pseudonymous Business Keys**: The platform utilizes `userId` (integer) as an operational identifier.
- **Warehouse Privacy Protection (Surrogate Key Isolation)**:
  - Downstream analytical facts (`fact_streams`, `fact_ad_events`) and marts **do not expose raw personal names or dates of birth**.
  - All facts reference deterministic surrogate keys (`userKey`, `videoKey`, `locationKey`).
  - Demographic attributes are isolated exclusively inside the SCD Type 2 `dim_users` dimension table, enabling column-level security and restricted access.

---

## 3. Data Retention Lifecycle Policy

| Storage Tier | Data Layer | 0 – 30 Days | 30 – 90 Days | 90 – 365 Days | > 365 Days |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Google Cloud Storage** | Raw Parquet landing files | Standard Storage | Nearline Tier (50% cost) | Coldline Tier (80% cost) | Permanently Deleted |
| **BigQuery Staging** | `adaptive_ads_stg.*` | Active Partition | Active Partition | Long-Term Storage (50% cost)| Partition Expiration (730d) |
| **BigQuery Core & Marts**| `adaptive_ads_prod.*` | Active Partition | Active Partition | Long-Term Storage (50% cost)| Partition Expiration (730d) |

---

## 4. Role-Based Access Control (RBAC) & Least Privilege

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      LEAST-PRIVILEGE IAM ACCESS                         │
└─────────────────────────────────────────────────────────────────────────┘
  [ Airflow Worker SA ]     ──► GCS Read + BigQuery Staging Read/Write
  [ dbt Transformation SA ] ──► BigQuery Staging Read + Prod Read/Write
  [ Looker Studio SA ]      ──► BigQuery Prod Read-Only (Marts only)
  [ Developer / CI SA ]     ──► Ephemeral Branch Datasets Read/Write
```

### Identity and Permissions Specification:
1. **`sa-airflow-orchestrator`**:
   - `roles/storage.objectViewer` on `gs://<telemetry-bucket>`
   - `roles/bigquery.dataEditor` on `adaptive_ads_stg`
   - `roles/bigquery.jobUser`
2. **`sa-dbt-transformer`**:
   - `roles/bigquery.dataViewer` on `adaptive_ads_stg`
   - `roles/bigquery.dataEditor` on `adaptive_ads_prod`
   - `roles/bigquery.jobUser`
3. **`sa-looker-dashboard`**:
   - `roles/bigquery.dataViewer` restricted exclusively to `adaptive_ads_prod` marts tables
   - `roles/bigquery.jobUser` (no write or delete access)

