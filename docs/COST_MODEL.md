# Cloud Infrastructure Cost Model & Analysis

## 1. Cloud Cost Architecture Overview

The **Adaptive Ads** platform is engineered to minimize infrastructure and query operational expenses across Google Cloud. The cost model consists of four primary components:

```
┌────────────────────────────────────────────────────────────────────────┐
│                      GOOGLE CLOUD COST COMPONENTS                      │
└────────────────────────────────────────────────────────────────────────┘
  [ 1. GCS Storage ]          ──► Raw Parquet landing with automated tiering
  [ 2. BigQuery Storage ]     ──► Active vs Long-term partitioned storage
  [ 3. BigQuery Query Compute]──► On-demand compute optimized via pruning
  [ 4. Cloud Composer 2 ]     ──► Managed Airflow orchestration cluster
```

---

## 2. Component-by-Component Cost Breakdown (Hypothetical 1x Baseline)

> [!NOTE]
> All unit prices below reflect official Google Cloud public pricing (us-central1, as of 2026). The volume projections are hypothetical scenarios based on the platform's capacity formulas.

### 1. Google Cloud Storage (GCS)
- **Unit Pricing**: Standard: \$0.020/GB/mo | Nearline: \$0.010/GB/mo | Coldline: \$0.004/GB/mo.
- **Optimization Strategy**: Automated GCS bucket lifecycle rules transition raw Parquet files to Nearline at 30 days and Coldline at 90 days.
- **Estimated Baseline (1x Scale, ~2.5 GB raw/month)**: **<\$0.50 / month**.

### 2. BigQuery Storage
- **Unit Pricing**: Active Storage: \$0.020/GB/mo | Long-Term Storage (>90 days unmodified): \$0.010/GB/mo.
- **Optimization Strategy**: BigQuery partition-scoped table design ensures that partitions older than 90 days automatically transition to long-term pricing (50% discount).
- **Estimated Baseline (1x Scale, ~50 GB cumulative warehouse storage)**: **~\$0.75 / month**.

### 3. BigQuery Query Processing (On-Demand Compute)
- **Unit Pricing**: \$6.25 per TB scanned (first 1 TB/mo free).
- **Optimization Strategy**:
  - Partition pruning limits hourly fact updates to ~3 days of data instead of full historical scans.
  - Multi-column clustering skips irrelevant storage blocks.
  - Pre-aggregated marts serve Looker Studio dashboards, reducing daily BI scan volume by >90%.
- **Estimated Baseline (1x Scale, ~250 GB scanned/month across all hourly ELT runs)**: **\$0.00 / month** (within free tier).

### 4. Cloud Composer 2 (Managed Airflow)
- **Unit Pricing**: Composer 2 Small environment (~0.5 vCPU web server, 1 worker, 1 scheduler): **~\$280 – \$320 / month**.
- **Optimization Strategy**: Minimal environment sizing; DAG schedule (`5 * * * *`) batches work hourly rather than keeping dedicated compute continuously active.

---

## 3. Total Estimated Monthly Cost Summary

| Service Component | 1x Baseline (~10K events/hr) | 10x Scale (~100K events/hr) | 100x Scale (~1M events/hr) |
| :--- | :--- | :--- | :--- |
| **GCS Storage** | \$0.20 | \$2.00 | \$20.00 |
| **BigQuery Storage** | \$0.75 | \$7.50 | \$75.00 |
| **BigQuery Query Compute** | \$0.00 (Free Tier) | \$15.00 | \$120.00 |
| **Cloud Composer 2** | \$300.00 | \$300.00 | \$450.00 (Composer Med) |
| **Looker Studio BI** | \$0.00 (Standard) | \$0.00 | \$0.00 |
| **Total Estimated Cost** | **~\$301 / month** | **~\$325 / month** | **~\$665 / month** |

*Key Takeaway: The architecture demonstrates sub-linear cost growth ($2x cost for 10x data volume) due to partition pruning and clustered incremental processing.*

