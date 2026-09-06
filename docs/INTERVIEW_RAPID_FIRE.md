# 75+ Rapid-Fire Data Engineering Interview Questions & Answers

Use this interactive flashcard document for rapid self-testing. Click on any question to expand the verified answer.

---

## Category 1: Architecture & Platform (Q1–Q10)

<details>
<summary><b>Q1: What is the core business problem this data platform solves?</b></summary>
It unifies high-throughput ad interactions and video streaming telemetry to deliver accurate, idempotent analytics on ad monetization (eCPM, CTR, yield) and audience subscriber engagement without corrupted attribution across tier changes.
</details>

<details>
<summary><b>Q2: What is the primary architecture pattern used?</b></summary>
Modern Cloud ELT (Extract $\rightarrow$ Load $\rightarrow$ Transform) on Google Cloud Platform using Airflow, GCS, BigQuery, and dbt.
</details>

<details>
<summary><b>Q3: What four telemetry streams are ingested?</b></summary>
<code>watch_events</code>, <code>ad_events</code>, <code>page_view_events</code>, and <code>auth_events</code>.
</details>

<details>
<summary><b>Q4: What is the pipeline's execution schedule and SLA?</b></summary>
Runs on an hourly schedule (<code>@hourly</code>) targeting a data freshness SLA of 45–60 minutes.
</details>

<details>
<summary><b>Q5: Where does raw telemetry land initially?</b></summary>
In Google Cloud Storage (GCS) partitioned as <code>gs://bucket/raw/{event}/YYYY/MM/DD/HH/*.parquet</code>.
</details>

<details>
<summary><b>Q6: What is the database modeling schema in BigQuery?</b></summary>
Star Schema consisting of conformed dimensions (<code>dim_users</code>, <code>dim_movies</code>, <code>dim_location</code>), incremental fact tables (<code>fact_streams</code>, <code>fact_ad_events</code>), and analytical marts (<code>daily_ad_metrics</code>).
</details>

<details>
<summary><b>Q7: What tool manages SQL transformations and lineage?</b></summary>
dbt Core (data build tool).
</details>

<details>
<summary><b>Q8: How are downstream business dashboards connected?</b></summary>
Looker Studio connects directly to BigQuery analytical marts accelerated by BigQuery BI Engine.
</details>

<details>
<summary><b>Q9: What CI/CD engine is used?</b></summary>
GitHub Actions, enforcing 7 automated quality gates on every pull request.
</details>

<details>
<summary><b>Q10: What is the license and environment structure?</b></summary>
Open source MIT license with three-tier environment separation (<code>dev</code>, <code>test/ci</code>, <code>prod</code>).
</details>

---

## Category 2: Ingestion & Storage (Q11–Q20)

<details>
<summary><b>Q11: Why is Parquet preferred over CSV or JSON?</b></summary>
Columnar storage, 4–10x Snappy compression, typed schema preservation, and dramatic BigQuery scan cost savings.
</details>

<details>
<summary><b>Q12: Why Snappy compression instead of GZIP?</b></summary>
Optimal balance between high compression ratio and fast CPU decompression throughput during high-frequency loads.
</details>

<details>
<summary><b>Q13: How is GCS storage organized?</b></summary>
Hive-style date/hour partitioning: <code>raw/{event_name}/YYYY/MM/DD/HH/part-*.parquet</code>.
</details>

<details>
<summary><b>Q14: How are raw files loaded into BigQuery staging?</b></summary>
Using <code>GCSToBigQueryOperator</code> reading Parquet files via URI pattern into staging tables.
</details>

<details>
<summary><b>Q15: What write disposition is used in BigQuery loading?</b></summary>
<code>WRITE_APPEND</code> preceded by an atomic partition-scoped <code>DELETE</code>.
</details>

<details>
<summary><b>Q16: How is staging data isolated from production marts?</b></summary>
Separated into dedicated BigQuery datasets: <code>staging</code>, <code>core</code>, and <code>marts</code>.
</details>

<details>
<summary><b>Q17: What is the retention policy on raw GCS files?</b></summary>
Transitions to Coldline at 90 days; deleted after 365 days.
</details>

<details>
<summary><b>Q18: What is the retention policy on staging tables?</b></summary>
30-day BigQuery partition expiration.
</details>

<details>
<summary><b>Q19: How are corrupt Parquet files prevented from entering the warehouse?</b></summary>
Declarative data contract validation in CI + BigQuery strict schema ingestion options.
</details>

<details>
<summary><b>Q20: How are late-arriving GCS files handled?</b></summary>
Via the 3-day sliding lookback window in incremental dbt merges and CLI backfill tooling.
</details>

---

## Category 3: Airflow Orchestration (Q21–Q30)

<details>
<summary><b>Q21: How are Airflow ingestion tasks generated dynamically?</b></summary>
Through parameterized TaskGroups iterating over the centralized <code>EVENT_CONFIG</code> dictionary.
</details>

<details>
<summary><b>Q22: Where is the central telemetry configuration defined?</b></summary>
In <code>airflow/dags/event_config.py</code>.
</details>

<details>
<summary><b>Q23: What retry policy is configured on Airflow tasks?</b></summary>
<code>retries = 2</code> with <code>retry_delay = timedelta(minutes=5)</code>.
</details>

<details>
<summary><b>Q24: What is the execution timeout for Airflow tasks?</b></summary>
<code>execution_timeout = timedelta(minutes=30)</code> to prevent hung worker processes.
</details>

<details>
<summary><b>Q25: Why is <code>catchup=False</code> set on the DAG?</b></summary>
To prevent unintended historical task cascades upon DAG initialization; backfills run explicitly via <code>scripts/backfill.py</code>.
</details>

<details>
<summary><b>Q26: Why is <code>max_active_runs=1</code> enforced?</b></summary>
To serialize dbt model builds and prevent table lock contention during incremental merges.
</details>

<details>
<summary><b>Q27: How does Airflow achieve staging idempotency?</b></summary>
By executing a partition-scoped <code>DELETE FROM staging_table WHERE partition_hour = execution_date</code> before inserting.
</details>

<details>
<summary><b>Q28: How does Airflow trigger dbt transformations?</b></summary>
Via <code>BashOperator</code> executing <code>dbt run</code> followed by <code>dbt test</code>.
</details>

<details>
<summary><b>Q29: How are environment secrets passed to Airflow?</b></summary>
Via environment variables and Google Secret Manager; zero plaintext credentials in git.
</details>

<details>
<summary><b>Q30: How do you trigger an ad-hoc backfill in Airflow?</b></summary>
Using the CLI utility <code>python3 scripts/backfill.py --start ... --end ...</code>.
</details>

---

## Category 4: BigQuery Performance & Cost (Q31–Q40)

<details>
<summary><b>Q31: How are BigQuery fact tables partitioned?</b></summary>
By day granularity on <code>eventDate</code> (<code>DATE(event_timestamp)</code>).
</details>

<details>
<summary><b>Q32: How are BigQuery fact tables clustered?</b></summary>
Clustered on high-cardinality query filter columns: <code>campaignId</code>, <code>adPlacement</code> (for ads) and <code>userId</code>, <code>movieId</code> (for streams).
</details>

<details>
<summary><b>Q33: What is partition pruning?</b></summary>
A BigQuery optimization where queries filtering on the partition column scan only the relevant date slices rather than the full table.
</details>

<details>
<summary><b>Q34: How does clustering improve query performance?</b></summary>
It physically collocates sorted data within partition blocks, skipping unneeded blocks during query execution.
</details>

<details>
<summary><b>Q35: What are <code>incremental_predicates</code> in dbt?</b></summary>
SQL filter clauses applied to the target table in a <code>MERGE</code> statement to restrict the partition scan range.
</details>

<details>
<summary><b>Q36: Why was <code>SELECT *</code> eliminated from core models?</b></summary>
To leverage BigQuery's columnar storage (Capacitor) and minimize byte scan costs.
</details>

<details>
<summary><b>Q37: What is BigQuery Time Travel?</b></summary>
A native feature allowing queries against historical table snapshots up to 7 days in the past (<code>FOR SYSTEM_TIME AS OF</code>).
</details>

<details>
<summary><b>Q38: How does Looker Studio query BigQuery efficiently?</b></summary>
By querying pre-aggregated analytical marts accelerated by BigQuery BI Engine in-memory caching.
</details>

<details>
<summary><b>Q39: What is the BigQuery on-demand query cost model?</b></summary>
\$6.25 per TB scanned, with 1 TB free per month.
</details>

<details>
<summary><b>Q40: How do we prevent division by zero in BigQuery SQL?</b></summary>
Using <code>SAFE_DIVIDE(numerator, denominator)</code> which returns <code>NULL</code> instead of raising a runtime error.
</details>

---

## Category 5: dbt & Transformations (Q41–Q50)

<details>
<summary><b>Q41: What materialization is used for staging models?</b></summary>
<code>view</code> (zero compute storage overhead, lightweight query compilation).
</details>

<details>
<summary><b>Q42: What materialization is used for fact tables?</b></summary>
<code>incremental</code> with the <code>merge</code> strategy.
</details>

<details>
<summary><b>Q43: What materialization is used for marts?</b></summary>
<code>table</code> (pre-computed aggregates for high-speed dashboard reads).
</details>

<details>
<summary><b>Q44: What macro generates surrogate keys in dbt?</b></summary>
<code>dbt_utils.generate_surrogate_key</code>, producing deterministic MD5 hashes.
</details>

<details>
<summary><b>Q45: How does <code>is_incremental()</code> work in dbt?</b></summary>
A Jinja macro that compiles filtering logic only during incremental runs, skipped during <code>--full-refresh</code>.
</details>

<details>
<summary><b>Q46: What is the purpose of <code>sources.yml</code>?</b></summary>
Declares upstream raw staging tables, enabling lineage tracking and freshness tests.
</details>

<details>
<summary><b>Q47: What generic schema tests are configured in dbt?</b></summary>
<code>unique</code>, <code>not_null</code>, <code>relationships</code>, and <code>accepted_values</code>.
</details>

<details>
<summary><b>Q48: What are singular tests in dbt?</b></summary>
Custom SQL queries in <code>dbt/tests/</code> that fail if they return any rows (e.g. invalid date ranges).
</details>

<details>
<summary><b>Q49: How do you run only modified models in dbt?</b></summary>
Using dbt state comparison: <code>dbt run --select state:modified+ --state path/to/artifacts</code>.
</details>

<details>
<summary><b>Q50: What dbt version and adapter are used?</b></summary>
dbt Core 1.7+ with <code>dbt-bigquery</code> adapter.
</details>

---

## Category 6: Dimensional Modeling & SCD2 (Q51–Q60)

<details>
<summary><b>Q51: What is a Slowly Changing Dimension Type 2 (SCD2)?</b></summary>
A dimensional modeling pattern that preserves full historical attribute history by creating new versioned records with activation/expiration timestamps.
</details>

<details>
<summary><b>Q52: Which dimension in this warehouse is SCD Type 2?</b></summary>
<code>dim_users</code>, tracking user subscription tier changes (<code>Free</code> $\rightarrow$ <code>Premium</code>).
</details>

<details>
<summary><b>Q53: How does SCD2 prevent revenue misattribution?</b></summary>
Historical ad impressions join against the user record active at the time of the event, correctly attributing ads to the Free tier even after the user upgrades to Premium.
</details>

<details>
<summary><b>Q54: What SQL window functions power the SCD2 dimension?</b></summary>
<code>LAG()</code> (detects tier changes), <code>SUM(isNewState) OVER ()</code> (groups states), and <code>LEAD()</code> (computes expiration dates).
</details>

<details>
<summary><b>Q55: What is the expiration date for the currently active SCD2 record?</b></summary>
<code>TIMESTAMP('9999-12-31 23:59:59')</code> with <code>isCurrent = TRUE</code>.
</details>

<details>
<summary><b>Q56: How do fact tables join against SCD2 dimensions?</b></summary>
<code>ON fact.userId = dim.userId AND fact.eventTimestamp BETWEEN dim.rowActivationDate AND dim.rowExpirationDate</code>.
</details>

<details>
<summary><b>Q57: What is the surrogate key grain of <code>dim_users</code>?</b></summary>
<code>MD5(userId || subscriptionTier || rowActivationDate)</code>.
</details>

<details>
<summary><b>Q58: What is a conformed dimension?</b></summary>
A shared dimension (e.g. <code>dim_movies</code>, <code>dim_location</code>) that connects consistently across multiple fact tables.
</details>

<details>
<summary><b>Q59: What is the grain of <code>fact_ad_events</code>?</b></summary>
One row per individual ad impression, click, or interaction.
</details>

<details>
<summary><b>Q60: What is the grain of <code>daily_ad_metrics</code>?</b></summary>
One row per date, campaign ID, and subscription tier.
</details>

---

## Category 7: Data Quality & Contracts (Q61–Q68)

<details>
<summary><b>Q61: What is a data contract?</b></summary>
A formal declarative agreement in YAML defining the expected schema, datatypes, and constraints between data producers and consumers.
</details>

<details>
<summary><b>Q62: Where are data contracts located in the repository?</b></summary>
In <code>contracts/watch_events.yml</code>, <code>contracts/ad_events.yml</code>, etc.
</details>

<details>
<summary><b>Q63: What tool validates data contracts automatically?</b></summary>
<code>scripts/validate_contracts.py</code>, executed in CI and local validation.
</details>

<details>
<summary><b>Q64: What happens if an upstream producer drops a required column?</b></summary>
Contract validation and CI halt the deployment with an explicit error before any ingestion runs.
</details>

<details>
<summary><b>Q65: What singular test validates <code>dim_users</code> date ranges?</b></summary>
<code>dbt/tests/assert_dim_users_valid_date_ranges.sql</code>.
</details>

<details>
<summary><b>Q66: How many Python unit tests are in <code>tests/unit/</code>?</b></summary>
19 unit tests executing in ~0.022 seconds.
</details>

<details>
<summary><b>Q67: What does <code>scripts/check_schema.py</code> do?</b></summary>
Detects schema drift between target database tables and expected definitions.
</details>

<details>
<summary><b>Q68: What is the 4-tier testing hierarchy?</b></summary>
Unit Tests (Tier 1) $\rightarrow$ dbt Schema Tests (Tier 2) $\rightarrow$ Singular SQL Tests (Tier 3) $\rightarrow$ Repository Validation Suite (Tier 4).
</details>

---

## Category 8: CI/CD, Governance & Reliability (Q69–Q78)

<details>
<summary><b>Q69: What are the 7 CI quality gates?</b></summary>
Ruff linting, SQLFluff, Python unit tests, DAG compile check, contract validator, secret scanner, and doc link validator.
</details>

<details>
<summary><b>Q70: How is PII isolated in the warehouse?</b></summary>
Demographic attributes are isolated exclusively in <code>dim_users</code>; downstream facts and marts use pseudonymous surrogate keys.
</details>

<details>
<summary><b>Q71: What is the RPO and RTO for the warehouse?</b></summary>
RPO $\le$ 1 hour; RTO $\le$ 30 minutes for single partitions.
</details>

<details>
<summary><b>Q72: How are accidentally dropped BigQuery tables restored?</b></summary>
Using BigQuery 7-day Time Travel snapshots via <code>FOR SYSTEM_TIME AS OF</code>.
</details>

<details>
<summary><b>Q73: What script runs all local validation checks?</b></summary>
<code>./scripts/validate.sh</code> (9 automated checks).
</details>

<details>
<summary><b>Q74: What is the recovery strategy for corrupted historical partitions?</b></summary>
Purge corrupted partition and replay raw GCS files via <code>scripts/backfill.py</code>.
</details>

<details>
<summary><b>Q75: What IAM role is assigned to Looker Studio?</b></summary>
<code>roles/bigquery.dataViewer</code> scoped strictly to the <code>marts</code> dataset.
</details>

<details>
<summary><b>Q76: How do we prevent Git from committing plaintext secrets?</b></summary>
Pre-commit secret scans, <code>.gitignore</code> rules, and automated CI secret scanning.
</details>

<details>
<summary><b>Q77: What happens when an Airflow worker node crashes mid-DAG?</b></summary>
Kubernetes/Composer restarts the pod; the scheduler recovers state from the metadata DB and resumes idempotent tasks safely.
</details>

<details>
<summary><b>Q78: How is pipeline technical debt tracked?</b></summary>
In <code>docs/TECHNICAL_DEBT.md</code>, currently tracking 0 Critical, 2 Medium, and 2 Low items.
</details>

