# Disaster Recovery & Business Continuity Architecture

## 1. Recovery Objectives (RPO & RTO)

> [!NOTE]
> **Proposed Engineering Targets**: The recovery objectives below represent engineering architecture targets for cloud deployment scenarios.

```
┌────────────────────────────────────────────────────────────────────────┐
│                     RECOVERY SERVICE LEVEL TARGETS                     │
├───────────────────────────────────┬────────────────────────────────────┤
│ Recovery Point Objective (RPO)    │ ≤ 1 Hour (Single Ingestion Window) │
├───────────────────────────────────┼────────────────────────────────────┤
│ Recovery Time Objective (RTO)     │ ≤ 30 Minutes (Partition Recovery)  │
│                                   │ ≤ 2 Hours (Full Warehouse Rebuild) │
└───────────────────────────────────┴────────────────────────────────────┘
```

---

## 2. Data Durability & Backup Hierarchy

```
┌────────────────────────────────────────────────────────────────────────┐
│                        DATA RECOVERY TAXONOMY                          │
└────────────────────────────────────────────────────────────────────────┘
  [ 1. REBUILDABLE ASSETS ]  ──► All dbt models (dims, facts, marts, views)
  [ 2. RECOVERABLE ASSETS ]  ──► GCS raw Parquet files (365d durability)
                                 BigQuery table snapshot / Time Travel (7d)
  [ 3. NON-RECOVERABLE ]     ──► In-flight transient external tables
```

- **Rebuildable Assets**: All dimensional core models and marts can be 100% deterministically reproduced from raw staging tables and dbt seeds.
- **Recoverable Assets**: GCS bucket object versioning and 99.999999999% (11 9s) storage durability protect source Parquet files for 365 days. BigQuery Time Travel allows querying data up to 7 days prior (`FOR SYSTEM_TIME AS OF`).

---

## 3. Seven Failure Scenarios & Incident Playbooks

### Scenario 1: Airflow Orchestrator Outage (Composer Down)
- **Detection**: Heartbeat alert failure in Cloud Monitoring; SLA miss on hourly batch.
- **Containment**: Upstream telemetry queues buffer safely in Cloud Storage.
- **Recovery**: Restart Composer environment; once active, run `scripts/backfill.py --start <outage_start> --end <outage_end>` to catch up all missed intervals in parallel.
- **Validation**: Confirm `stg_*` table row counts match expected hourly averages.
- **Prevention**: Deploy Cloud Composer across multi-zone GKE clusters with automated worker node health checks.

### Scenario 2: BigQuery Regional Service Degradation
- **Detection**: Airflow task failures with `503 Service Unavailable` or slot quota errors.
- **Containment**: Airflow automatic task retries (`retries=2`, `retry_delay=3m`) hold pipeline execution without dropping raw data.
- **Recovery**: Once GCP restores BigQuery service, unpause DAG or trigger catchup runs.
- **Validation**: Run dbt schema tests (`dbt test --select core marts`).
- **Prevention**: Reserve BigQuery baseline slot capacity; enable multi-region failover dataset replication.

### Scenario 3: GCS Source Telemetry Unavailable / Delayed
- **Detection**: Airflow external table creation fails or returns 0 rows.
- **Containment**: Downstream dbt model execution halts gracefully due to task dependency gates.
- **Recovery**: Contact upstream streaming player team; once telemetry uploads resume, execute `scripts/backfill.py` for affected hours.
- **Validation**: Run `scripts/check_schema.py --strict` to verify payload integrity.
- **Prevention**: Configure dead-letter queuing on producer SDKs with client-side SQLite buffering.

### Scenario 4: Corrupted Staging Partition Loaded
- **Detection**: `dbt test` fails on downstream model; anomaly alert on negative duration or invalid enum.
- **Containment**: Flag downstream Looker Studio dashboard with maintenance banner.
- **Recovery**:
  1. Fix the upstream source file or filter corrupt rows in `airflow/dags/sql/`.
  2. Execute partition-scoped reload: the Airflow atomic `DELETE` + `INSERT` pattern cleans the corrupted partition and loads clean data.
  3. Re-run incremental facts with lookback reconciliation: `dbt run --select core marts`.
- **Validation**: Execute `dbt test --select core marts`.
- **Prevention**: Enforce data contracts in CI and run contract validator pre-ingestion.

### Scenario 5: Faulty dbt Model / SQL Deployment
- **Detection**: dbt build failure or data discrepancy in analytical marts.
- **Containment**: Production fact tables remain untouched if failure occurs before merge.
- **Recovery**: Revert Git commit on `main`; CI triggers automated dbt compile and deploy.
- **Validation**: Run `python3 -m unittest discover tests` and `./scripts/validate.sh`.
- **Prevention**: Mandatory CI approval gates running SQLFluff and dbt compilation on all PRs.

### Scenario 6: Accidental Table or Partition Deletion
- **Detection**: Missing dataset error in Airflow or BI queries.
- **Containment**: Isolate affected table reference in dbt DAG.
- **Recovery**:
  - *Within 7 Days*: Restore using BigQuery Time Travel:
    ```sql
    CREATE OR REPLACE TABLE adaptive_ads_stg.watch_events AS
    SELECT * FROM adaptive_ads_stg.watch_events
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
  - *Beyond 7 Days*: Re-run `scripts/backfill.py` from raw GCS Parquet archives.
- **Validation**: Run `dbt test`.
- **Prevention**: Apply BigQuery table deletion protection locks on production datasets.

### Scenario 7: Malformed Code Deployed to Main Branch
- **Detection**: GitHub Actions CI notification or immediate pipeline run failure.
- **Containment**: Airflow DAGs remain on last known good container image until deployment succeeds.
- **Recovery**: Execute standard Git rollback (`git revert HEAD && git push origin main`).
- **Validation**: Verify CI green status across all 5 verification jobs.
- **Prevention**: Enforce branch protection rules requiring passing CI and peer code review.

---

## 4. Disaster Recovery Operational Runbook

```bash
# Step 1: Identify affected partition interval
FAIL_START="2026-09-01T10:00:00"
FAIL_END="2026-09-01T14:00:00"

# Step 2: Validate raw source availability and contract integrity
python3 scripts/validate_contracts.py --strict
python3 scripts/check_schema.py --strict

# Step 3: Execute controlled partition-scoped backfill (Dry Run First)
python3 scripts/backfill.py --start "$FAIL_START" --end "$FAIL_END" --dry-run
python3 scripts/backfill.py --start "$FAIL_START" --end "$FAIL_END" --force

# Step 4: Reconcile downstream dbt core warehouse and marts
cd dbt && dbt run --select core marts --profiles-dir . --target prod

# Step 5: Execute full data quality test suite
dbt test --profiles-dir . --target prod

# Step 6: Verify platform integrity with developer validation suite
./scripts/validate.sh
```

