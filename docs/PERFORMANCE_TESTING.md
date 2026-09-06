# Performance Testing & Benchmarking Strategy

## 1. Multi-Tier Performance Testing Framework

The **Adaptive Ads** platform employs a 4-tier testing hierarchy to prevent performance regressions and validate scalability before code reaches production.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PERFORMANCE TESTING HIERARCHY                        │
└─────────────────────────────────────────────────────────────────────────┘
  [ Tier 1: Local Pre-Commit ]   ──► Syntax, AST parsing, AST SELECT * scan
  [ Tier 2: CI Static Checks ]   ──► Linting, schema drift, dbt compile
  [ Tier 3: BigQuery Staging ]   ──► Bytes scanned, execution time, slot-ms
  [ Tier 4: Load & Stress ]      ──► 1x, 10x, 100x synthetic telemetry stress
```

---

## 2. Testing Tiers & Validation Scope

### Tier 1: Local Validation (`./scripts/validate.sh`)
- **Scope**: Developer workstation / local sandbox.
- **Checks**:
  - Python AST compilation (`py_compile`).
  - Airflow DAG & template mapping integrity.
  - SQL AST structure and non-emptiness.
  - dbt schema graph validation.
  - Credential hygiene.
  - Partition pruning safeguards (ensures `incremental_predicates` exists in all fact models).
  - Explicit projection validation (verifies no unprojected `SELECT *` in core models).
  - Operational script smoke tests (`check_schema.py`, `backfill.py --dry-run`).

### Tier 2: Continuous Integration (GitHub Actions)
- **Scope**: Automated PR validation.
- **Checks**:
  - Ruff code linting and formatting.
  - SQLFluff dialect validation.
  - Schema drift regression suite (`check_schema.py --strict`).

### Tier 3: BigQuery Cloud Integration (Staging Environment)
- **Scope**: Ephemeral PR dataset deployment in Google Cloud.
- **Key Metrics Tracked**:
  - **Total Bytes Scanned**: Evaluated via `total_bytes_billed` in `INFORMATION_SCHEMA.JOBS_BY_PROJECT`.
  - **Slot-Milliseconds**: Compute utilization per dbt model run.
  - **Partition Elimination Efficiency**: Percentage of partition blocks skipped during `WHERE` clauses.

---

## 3. Load Testing & Stress Methodology (1x → 10x → 100x)

> [!NOTE]
> The scenarios below outline the platform's load testing protocol for staging environments. Actual load test execution requires active Google Cloud billing and Composer compute quotas.

```
                                  LOAD TESTING PROTOCOL
┌──────────────┬────────────────────────┬───────────────────────────────────────────┐
│ Scale Factor │ Event Rate             │ Target Test Objective                     │
├──────────────┼────────────────────────┼───────────────────────────────────────────┤
│ 1x Baseline  │ 10,000 events/hour     │ Validate baseline latency (< 5 min run)   │
│ 10x Scale    │ 100,000 events/hour    │ Verify BigQuery merge slot saturation     │
│ 100x Scale   │ 1,000,000 events/hour  │ Evaluate GCS file count & Airflow workers │
└──────────────┴────────────────────────┴───────────────────────────────────────────┘
```

### 10x & 100x Stress Protocol:
1. **Synthetic Telemetry Ingestion**: Generate 1M Parquet records with skewed user and timestamp distributions across 24 hourly GCS partitions.
2. **Concurrent Airflow Execution**: Trigger all 4 TaskGroups simultaneously to measure BigQuery slot contention and external table API rate limits.
3. **Incremental dbt Rebuild**: Measure execution duration and slot memory consumption during the 3-day sliding lookback `MERGE` operation.
4. **Regression Thresholds**: Flag any PR that increases `total_bytes_billed` by > 15% for identical input data volume.

