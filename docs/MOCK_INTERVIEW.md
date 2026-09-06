# Simulated 30-Minute Mock Interview Script & Rubric

Use this script to conduct a realistic mock technical interview with a peer or practice out loud.

---

## Round 1: Introduction & Elevator Pitch (00:00 – 05:00)

**Interviewer**: *"Tell me about yourself and walk me through a recent data engineering project you built."*

**Candidate Script**:
> *"I'm a Data Engineer specialized in cloud data warehousing, orchestration, and reliable ELT pipelines. Recently, I built the Adaptive Ads data platform—an automated, idempotent data pipeline on Google Cloud Platform that unifies video playback streams and ad interaction telemetry to power executive monetization and subscriber analytics.*
>
> *The system ingests four event streams landing as Snappy Parquet in GCS, orchestrates dynamic parallel ingestion via Apache Airflow, and transforms data in Google BigQuery using dbt Core. Key technical innovations include a deterministic SCD Type 2 dimension using pure SQL window functions to accurately track user subscription tier upgrades, incremental fact tables with partition predicates saving 90% in query costs, and a 4-tier data quality hierarchy including declarative YAML data contracts and 19 automated unit tests.*
>
> *The entire system is hardened with 7 CI quality gates in GitHub Actions and serves pre-aggregated Looker Studio dashboards."*

**Scoring Rubric (1–5)**:
- 5: Clear context, concise tech stack, highlighted 2+ hard engineering problems, finished within 90 seconds.
- 3: Good overview but slightly rambled or omitted business impact.
- 1: Read code line-by-line or failed to state why the platform was built.

---

## Round 2: Architecture & Idempotency Deep Dive (05:00 – 12:00)

**Interviewer**: *"How do you guarantee idempotency when Airflow tasks fail and retry? Won't an append-only load create duplicates?"*

**Candidate Script**:
> *"That's exactly the risk with default `WRITE_APPEND` dispositions. We solve this by enforcing a two-tier idempotency model:*
>
> *First, at the staging ingestion layer in Airflow, our parameterized TaskGroup executes an atomic partition-scoped delete before inserting: `DELETE FROM staging.table WHERE partition_hour = execution_date`. When an hourly task retries, it purges only that specific hour slice before reloading the Parquet files from GCS. Running the task 1 time or 10 times yields the exact same staging partition.*
>
> *Second, at the warehouse modeling layer, dbt fact models use the `incremental` materialization with a `merge` strategy keyed on cryptographic surrogate keys (`adEventKey`). The `MERGE` statement updates existing matching keys or ignores them rather than creating duplicate rows. Furthermore, we restrict the destination partition scan to the last 3 days using `incremental_predicates`, optimizing both idempotency and scan costs."*

**Scoring Rubric (1–5)**:
- 5: Clearly distinguished staging partition deletion from warehouse `MERGE`, explained surrogate keys, and referenced actual Airflow code structure.
- 3: Mentioned `MERGE` or `DELETE`, but was vague on partition isolation or task retries.
- 1: Suggested using `SELECT DISTINCT *` on raw tables or didn't understand the retry issue.

---

## Round 3: Data Modeling & SCD Type 2 (12:00 – 18:00)

**Interviewer**: *"Why did you implement SCD Type 2 for users, and how did you build it in pure SQL instead of dbt snapshots?"*

**Candidate Script**:
> *"In our streaming platform, users frequently upgrade from Free to Premium. If we used SCD Type 1 in-place overwrites, historical ad impressions would be retroactively joined to the user's new Premium state, corrupting ad-revenue attribution metrics for past campaigns.*
>
> *We implemented SCD Type 2 using pure SQL window functions over immutable user authentication logs in `dbt/models/core/dim_users.sql`:*
> 1. *`LAG(subscriptionTier) OVER (PARTITION BY userId ORDER BY eventTimestamp)` detects state transitions.*
> 2. *`SUM(isNewState) OVER (PARTITION BY userId ORDER BY eventTimestamp)` establishes contiguous state groups.*
> 3. *`GROUP BY` calculates `MIN(eventTimestamp)` as `rowActivationDate` and `LEAD(MIN(eventTimestamp))` as `rowExpirationDate`.*
>
> *We chose pure SQL window functions over dbt snapshots because dbt snapshots are stateful point-in-time captures that cannot reconstruct past history if the pipeline is backfilled or replayed from scratch. Pure SQL window functions allow deterministic full rebuilds from day zero at any time."*

**Scoring Rubric (1–5)**:
- 5: Explained the business justification (Free $\rightarrow$ Premium attribution), walked through `LAG`/`SUM`/`LEAD`, and articulated why pure SQL beats dbt snapshots for replayability.
- 3: Explained SCD2 concept well, but stumbled on the exact window function mechanics.
- 1: Confused SCD1 and SCD2 or couldn't explain how fact tables join to SCD2 dimensions.

---

## Round 4: BigQuery Optimization & Trade-Offs (18:00 – 25:00)

**Interviewer**: *"Why not use Apache Kafka or Spark? And how do you keep BigQuery query scan costs low?"*

**Candidate Script**:
> *"We evaluated Kafka and Spark against our business SLA, which is 1-hour analytical reporting and executive BI aggregation, not sub-second ad bidding. Batch loading Parquet from GCS directly into BigQuery via Airflow satisfies the 1-hour SLA with zero idle infrastructure cost and zero cluster management. Operating a 24/7 Kafka cluster and Spark infrastructure would add heavy operational complexity and continuous billing without delivering additional business value.*
>
> *To optimize BigQuery query costs, we implemented three techniques:*
> 1. *Partitioning & Clustering: Fact tables are partitioned daily on `eventDate` and clustered on high-cardinality keys like `campaignId` and `adPlacement`.*
> 2. *Incremental Predicates: We configured `incremental_predicates` on dbt `MERGE` models, restricting destination scans to the last 3 days, reducing scanned bytes by up to 90%.*
> 3. *Projection Pruning: We eliminated `SELECT *` from core models, projecting only required columns to exploit BigQuery's columnar Capacitor storage."*

**Scoring Rubric (1–5)**:
- 5: Grounded technology choices in SLAs and costs, clearly explained BigQuery columnar mechanics, partitioning, clustering, and incremental predicates.
- 3: Knew BigQuery features but struggled to justify why Kafka was omitted.
- 1: Claimed Kafka is always required or didn't understand how BigQuery charges for queries.

---

## Round 5: Failures, Testing & Candidate Questions (25:00 – 30:00)

**Interviewer**: *"How do you test this pipeline and recover if an operator drops a table?"*

**Candidate Script**:
> *"We maintain a 4-tier testing pyramid:*
> 1. *Tier 1: 19 fast Python unit tests in `tests/unit/` (0.02s) testing configs, backfill logic, and contracts.*
> 2. *Tier 2: dbt generic schema tests (`unique`, `not_null`, `relationships`).*
> 3. *Tier 3: Singular SQL tests (`dbt/tests/`) checking business invariants like `rowActivationDate <= rowExpirationDate` and non-negative stream durations.*
> 4. *Tier 4: An automated 9-step local validation suite (`validate.sh`) integrated into GitHub Actions CI.*
>
> *If an operator accidentally drops a BigQuery table, we leverage BigQuery's native 7-day **Time Travel** to restore the table instantaneously: `CREATE TABLE core.fact_ad_events AS SELECT * FROM core.fact_ad_events FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`. No raw file reprocessing required."*

**Candidate's Questions for Interviewer**:
> 1. *"How does your team currently manage schema evolution between upstream mobile/backend producers and your warehouse models?"*
> 2. *"What does your on-call escalation rotation look like when an upstream SLA is at risk?"*

**Scoring Rubric (1–5)**:
- 5: Accurately outlined the 4 testing tiers, gave the exact BigQuery Time Travel recovery syntax, and asked senior, thoughtful questions.
- 3: Mentioned unit tests and backups, but was unsure of exact recovery procedures.
- 1: Had no testing strategy and didn't know how to recover dropped cloud tables.

