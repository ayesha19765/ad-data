# Interview Trap Questions & How to Answer Them

Senior interviewers often ask subtle "trap" questions to test whether an engineer truly understands their architecture or merely memorized buzzwords. Here are the top interview traps for this project and how to answer them accurately.

---

## 1. The Real-Time / Streaming Trap

### Trap Question:
> *"Why didn't you use Apache Kafka, Apache Flink, or Spark Streaming? Isn't real-time always better for ad analytics?"*

### Why It's a Trap:
Interviewers want to see if you over-engineer architectures without evaluating business requirements, cost, and SLAs.

### Strong Answer:
> *"Real-time streaming is essential for real-time programmatic ad bidding (sub-second RTB) and real-time fraud mitigation. However, our platform's business requirement is hourly executive reporting, ad campaign yield analysis, and subscriber engagement aggregation. Batching Parquet files from GCS into BigQuery via Airflow fulfills the 1-hour SLA at near-zero idle compute cost, with zero cluster management overhead. Introducing Kafka and Flink for hourly reporting adds significant operational complexity, ZooKeeper/broker management, and continuous 24/7 compute billing without delivering additional business value. If the SLA shifts to sub-minute bidding feedback loops, we have a documented roadmap in `REVISION_SCALABILITY.md` to introduce Cloud Pub/Sub and Apache Beam."*

---

## 2. The Idempotency & Retry Trap

### Trap Question:
> *"If an Airflow task fails halfway through loading data into BigQuery and retries, won't you get duplicate rows? How do you guarantee idempotency?"*

### Why It's a Trap:
Many candidates say "we use `DISTINCT` in our SQL" or "Airflow handles retries automatically."

### Strong Answer:
> *"Airflow handles the retry trigger, but not the storage idempotency. If you use a simple append, retrying inserts duplicate rows. We guarantee end-to-end idempotency through a two-stage pattern:*
> 1. *At the staging ingestion layer, our `TaskGroup` executes a partition-scoped delete: `DELETE FROM stg_table WHERE partition_hour = execution_date` before inserting the new Parquet slice. Running the task 1 time or 10 times yields the exact same staging partition.*
> 2. *At the warehouse modeling layer, dbt incremental models use a `merge` strategy keyed on cryptographic surrogate keys (`adEventKey`), updating or ignoring existing records rather than appending duplicates."*

---

## 3. The SCD Type 2 Rebuild Trap

### Trap Question:
> *"Why did you write custom SQL window functions (`LAG`, `LEAD`, running sums) for SCD Type 2 instead of using dbt snapshots?"*

### Why It's a Trap:
Tests whether you understand the fundamental difference between stateful snapshotting and deterministic event sourcing.

### Strong Answer:
> *"dbt snapshots are stateful point-in-time snapshots created during scheduled batch runs. If a pipeline is paused, backfilled, or rebuilt from scratch, dbt snapshots cannot reconstruct past state history—they only capture the state when the snapshot was executed. By using pure SQL window functions over immutable user authentication and tier-change event logs, our SCD2 dimension is 100% deterministic and replayable. We can rebuild the entire historical subscriber timeline from day zero at any time without data loss."*

---

## 4. The BigQuery Cost Trap

### Trap Question:
> *"BigQuery is serverless, but query costs can explode on large tables. How did you optimize BigQuery costs in your dbt models?"*

### Why It's a Trap:
Tests whether you understand BigQuery's columnar storage, partitioning, clustering, and incremental merge mechanics.

### Strong Answer:
> *"We implemented three specific optimizations to eliminate runaway scan costs:*
> 1. *Partitioning & Clustering: Fact tables are partitioned daily on `eventDate` and clustered on high-cardinality query keys (`campaignId`, `adPlacement`).*
> 2. *Incremental Predicates: In standard dbt `MERGE` materializations, BigQuery scans the entire target table to match keys. We configured `incremental_predicates` restricting the destination scan to the last 3 days (`eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)`), saving up to 90% in scanned bytes.*
> 3. *Projection Pruning: We eliminated all `SELECT *` from core models, projecting only the exact columns required by downstream marts, reducing byte reads from Capacitor columnar storage."*

---

## 5. The Surrogate Key Generation Trap

### Trap Question:
> *"Why did you use MD5 hashes for surrogate keys instead of auto-incrementing integers (Identity columns)?"*

### Why It's a Trap:
Tests whether you understand distributed MPP systems vs. single-node relational databases.

### Strong Answer:
> *"In a distributed cloud warehouse like BigQuery, generating auto-incrementing sequential integers requires a centralized coordinator lock across all worker slots, serializing execution and severely degrading parallel throughput. Cryptographic hashes (MD5 / SHA256 via `dbt_utils.generate_surrogate_key`) are deterministic and calculated independently across distributed nodes without coordination. Furthermore, generating hashes is idempotent across `dev`, `test`, and `prod` environments."*

