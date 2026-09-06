# The Single-Screen Ultimate Cheat Sheet

```
+======================================================================================================================+
|                                    ADAPTIVE ADS DATA PLATFORM CHEAT SHEET                                            |
+======================================================================================================================+
| 1. CORE NUMBERS & METRICS                                                                                            |
| - 4 Telemetry Streams: `watch_events`, `ad_events`, `page_view_events`, `auth_events`                                |
| - Schedule & SLA: Hourly (`@hourly`), 45–60 min SLA, RPO <= 1h, RTO <= 30m                                           |
| - Quality Suite: 19 Python Unit Tests (0.02s), 4 Singular SQL Tests, 7 CI Quality Gates, 9-step `validate.sh`        |
| - Lookback Window: 3-Day Sliding Window for late-arriving telemetry via `incremental_predicates`                     |
+----------------------------------------------------------------------------------------------------------------------+
| 2. END-TO-END DATA FLOW                                                                                              |
| Raw Events -> GCS (Snappy Parquet) -> Airflow TaskGroup (Delete Partition + Insert) -> BigQuery Staging             |
|   -> dbt Staging Views -> dbt Core (SCD2 dim_users, fact_streams, fact_ad_events) -> dbt Marts -> Looker Studio     |
+----------------------------------------------------------------------------------------------------------------------+
| 3. KEY IMPLEMENTATIONS & FILE PATHS                                                                                  |
| - Airflow DAG: `airflow/dags/adaptive_ads_dag.py` (Dynamic TaskGroups over `EVENT_CONFIG` in `event_config.py`)      |
| - Staging Idempotency: `airflow/dags/sql/*.sql` (`DELETE FROM stg WHERE partition_hour = ts; INSERT INTO stg...`)   |
| - SCD Type 2: `dbt/models/core/dim_users.sql` (Pure SQL: `LAG(tier)`, `SUM(isNewState)`, `LEAD(date)`)              |
| - Incremental Fact: `dbt/models/core/fact_ad_events.sql` (`merge` strategy + 3-day `incremental_predicates`)         |
| - Marts: `dbt/models/marts/daily_ad_metrics.sql` (eCPM, CTR, impressions, revenue, `SAFE_DIVIDE()`)                 |
| - Contracts: `contracts/*.yml` validated by `scripts/validate_contracts.py` in CI                                   |
| - Backfill Tool: `scripts/backfill.py` (CLI hourly partition slicer and dry-run validator)                           |
+----------------------------------------------------------------------------------------------------------------------+
| 4. TOP 5 INTERVIEW DEFENSES & TRADE-OFFS                                                                             |
| 1. Why NOT Kafka/Flink? -> Hourly analytical SLA makes serverless BigQuery batching 10x cheaper with zero idle cost. |
| 2. Why Partition-Scoped Ingestion? -> Guarantees exact idempotency on Airflow task retries with zero staging dupes.  |
| 3. Why SQL Window Functions for SCD2? -> 100% deterministic and replayable from raw events (dbt snapshots are not). |
| 4. Why Incremental Predicates? -> Restricts destination scan to 3 days, saving 90% BigQuery scan bytes.              |
| 5. Why MD5 Surrogate Keys? -> Distributed, collision-free (<10^-15) key generation without coordinator locking.     |
+----------------------------------------------------------------------------------------------------------------------+
| 5. RECOVERY & TROUBLESHOOTING                                                                                        |
| - Accidental Drop -> BigQuery Time Travel: `CREATE TABLE ... FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(NOW(), 1 HOUR)`    |
| - Corrupted Data -> Quarantine Parquet in GCS -> Replay partition via `python3 scripts/backfill.py`                 |
| - Task Failure -> Airflow retries 2x (5m delay); stuck tasks killed at 30m timeout; partition purged before reload.  |
+======================================================================================================================+
```

