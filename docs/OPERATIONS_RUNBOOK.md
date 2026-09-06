# Operations Runbook: Adaptive Ads Data Pipeline

This runbook provides on-call data engineers and operators with triage procedures, failure diagnosis steps, backfill guidelines, and recovery playbooks for the **Adaptive Ads Data Engineering Platform**.

---

## 1. Pipeline Overview & Normal Execution

### Orchestration Cadence
- **DAG**: `adaptive_ads_dag` runs hourly at `05` minutes past the hour (`5 * * * *`).
- **Flow**:
  1. **Parallel Ingestion**: Four `TaskGroup` instances (`ingest_watch_events`, `ingest_ad_events`, `ingest_page_view_events`, `ingest_auth_events`) ingest the previous hour's GCS Parquet telemetry into BigQuery staging tables (`adaptive_ads_stg.<event_name>`).
  2. **dbt Seed**: `dbt_initiate` seeds reference lookup tables (`state_codes`).
  3. **dbt Transformations**: `dbt_adaptive_ads_run` incrementally builds dimensions (`dim_users`, `dim_movies`, etc.), facts (`fact_streams`, `fact_ad_events`), and analytical marts (`daily_ad_metrics`, `daily_user_engagement`).

---

## 2. Common Failures & Triage Procedures

### Issue 1: GCP Service Account / Authentication Failure
- **Symptom**: Airflow tasks fail immediately with `google.auth.exceptions.DefaultCredentialsError` or `401 Unauthorized`.
- **Root Cause**: Expired credentials, invalid JSON key path, or missing IAM permissions on BigQuery/GCS.
- **Triage**:
  1. Verify the service account key exists at the path specified by `${GOOGLE_APPLICATION_CREDENTIALS}`.
  2. Check that the service account has `BigQuery Admin` and `Storage Object Viewer` roles in GCP.
  3. Test credentials using `gcloud auth activate-service-account --key-file=<path>`.

### Issue 2: Missing GCS Parquet Source Data
- **Symptom**: `create_external_table` or ingestion query returns `0 records` or `Not Found: gs://<bucket>/<event>/month=...`.
- **Root Cause**: Upstream event collectors delayed or GCS partition path mismatch.
- **Triage**:
  1. Check GCS bucket path format: `gs://<bucket>/<event_name>/month=<M>/day=<D>/hour=<H>/*`.
  2. Inspect collector logs to determine if event publishing was delayed.
  3. The task will automatically retry up to 2 times with a 3-minute delay. If upstream recovers, ingestion resumes automatically.

### Issue 3: BigQuery Quota / Concurrency Limits
- **Symptom**: Tasks fail with `403 Quota Exceeded: Exceeded rate limits`.
- **Root Cause**: High concurrency or rapid backfills exceeding BigQuery API limits.
- **Triage**:
  1. Verify `max_active_runs=1` is configured on `adaptive_ads_dag`.
  2. If running backfills, avoid running more than 2 historical execution dates concurrently.

### Issue 4: dbt Data Quality Test Failure
- **Symptom**: `dbt_test_dag` or CI validation fails with test violation errors.
- **Root Cause**: Non-unique primary keys, null values in foreign keys, or unexpected status values.
- **Triage**:
  1. Inspect dbt error logs to identify the failing model and column.
  2. For `dim_users`: Run singular test `assert_dim_users_single_active_row.sql` to check for overlapping active rows.
  3. For `daily_ad_metrics`: Run `assert_daily_ad_metrics_non_negative.sql` to check for negative values.

---

## 3. Retry & Idempotency Strategy

- **Idempotent Ingestion**: Every hourly ingestion task executes an atomic partition delete before inserting:
  ```sql
  DELETE FROM adaptive_ads_stg.<table_name>
  WHERE ts >= TIMESTAMP('<execution_hour_start>')
    AND ts < TIMESTAMP_ADD(TIMESTAMP('<execution_hour_start>'), INTERVAL 1 HOUR);
  ```
- **Safe Retries**: Rerunning any failed Airflow task or clearing DAG runs in the Airflow UI will **never duplicate data** or leave corrupted states.
- **Transient Cleanup**: External tables are transient and cleaned up with `ignore_if_missing=True`.

---

## 4. Backfill Procedure

When historical data needs to be reprocessed (e.g. after an upstream collector outage):

> [!WARNING]
> Always verify that the target GCS files for the target historical range exist before initiating backfills.

### Steps:
1. Access the Airflow Webserver UI (`http://localhost:8080`).
2. Navigate to `adaptive_ads_dag` -> **Grid View**.
3. Select the desired historical execution interval and click **Clear** (with *Downstream* and *Recursive* enabled).
4. Monitor the task progress in Flower (`http://localhost:5555`) or Airflow Graph View.
5. Once staging ingestion completes, dbt will merge the historical partitions and update the downstream analytical marts.

---

## 5. Troubleshooting Checklist

- [ ] Are environment variables set in `airflow/.env` (`GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `GCP_GCS_BUCKET`)?
- [ ] Is Docker Compose healthy (`docker ps`)?
- [ ] Are Airflow Webserver and Worker services responsive?
- [ ] Has the local validation script passed (`./scripts/validate.sh`)?
- [ ] Are BigQuery table schemas matching `airflow/dags/schema.py`?

