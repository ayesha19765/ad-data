# The "Why Not" Reference: Architectural Alternatives & Trade-Offs

A key indicator of senior engineering maturity is understanding not just why a technology was chosen, but why alternative technologies were deliberately omitted or deferred.

---

## 1. Streaming & Ingestion Alternatives

### Q1: Why NOT Apache Kafka or Google Cloud Pub/Sub in the current phase?
**Answer**:
- **Volume & SLA alignment**: The current business requirement is hourly analytical reporting and executive BI aggregation, not sub-second ad bidding or fraud detection. Batch ingestion of partitioned Parquet files directly from GCS to BigQuery satisfies the 1-hour SLA at a fraction of the cost and operational overhead.
- **Operational complexity**: Operating and maintaining a production Kafka cluster requires ZooKeeper/KRaft cluster management, partition rebalancing, schema registries, dead-letter queues, and round-the-clock on-call support.
- **Cost**: A dedicated Kafka cluster running 24/7 incurs continuous compute and broker costs regardless of telemetry volume, whereas serverless BigQuery batch loading incurs zero idle infrastructure costs.
- *When we would adopt Kafka*: If business requirements introduce sub-minute real-time fraud alerts or dynamic ad-bidding feedback loops (Scale $\ge$ 1,000x / 10M events/sec).

### Q2: Why NOT Apache Spark (Dataproc / EMR)?
**Answer**:
- **ELT vs. ETL paradigm**: Spark is primarily designed for distributed processing where transformations happen outside the data warehouse (ETL). In modern cloud data architectures, BigQuery's distributed SQL engine can perform complex transformations (joins, window functions, incremental merges) natively inside the warehouse (ELT) without moving data across network boundaries.
- **Resource overhead & cluster spin-up time**: Spinning up Spark clusters on Dataproc introduces cluster provisioning delays (3–5 minutes per batch) and memory tuning overhead (JVM garbage collection, shuffle partitions).
- **dbt synergy**: dbt allows pure SQL transformations that compile directly into BigQuery SQL, lowering developer friction.
- *When we would adopt Spark*: If we need complex unstructured data processing, machine learning feature extraction, or custom graph algorithms not expressible in SQL.

---

## 3. Data Warehouse & Storage Alternatives

### Q3: Why NOT Snowflake or Databricks Lakehouse?
**Answer**:
- **GCP Native Ecosystem**: The target ecosystem leverages Google Cloud Storage, Cloud Composer (Airflow), and Looker Studio. BigQuery provides zero-egress, zero-configuration integration with GCS and Looker Studio BI Engine.
- **Zero-Cluster Serverless Model**: Snowflake and Databricks require configuring and managing virtual warehouses or compute clusters with auto-suspend/resume timeouts. BigQuery is 100% serverless, eliminating cluster sizing and idle compute billing.

### Q4: Why NOT Delta Lake or Apache Iceberg on object storage?
**Answer**:
- **BigQuery Native Storage**: BigQuery manages its own columnar storage (Capacitor format) with built-in metadata management, Time Travel (7 days), failover, and automatic partitioning/clustering. Introducing open table formats like Iceberg or Delta Lake adds an extra layer of catalog management (e.g., BigLake Metastore or AWS Glue) without immediate benefits for an all-GCP warehouse workload.

---

## 4. Modeling & Transformation Alternatives

### Q5: Why NOT dbt Snapshots for SCD Type 2?
**Answer**:
- **Replayability from Scratch**: dbt snapshots are stateful point-in-time snapshots created during scheduled batch runs. If historical staging data is replayed from scratch or a historical backfill is executed, dbt snapshots cannot reconstruct past state changes.
- **Deterministic SQL**: Pure SQL window functions (`LAG`, `LEAD`, and running sums) over immutable auth/user event logs allow the entire SCD2 historical dimension to be rebuilt deterministically from historical raw events at any time.

### Q6: Why NOT Truncate & Load for Fact Tables?
**Answer**:
- **Cost & Scaling**: Truncating and rebuilding multi-million or billion-row fact tables every hour results in exponential compute costs and expanding job runtimes. Incremental `MERGE` with partition predicates processes only the latest partition windows while preserving historical facts.

### Q7: Why NOT Append-Only Ingestion with Runtime Deduplication Views?
**Answer**:
- **Query Performance & Cost Penalty**: In an append-only architecture, every downstream analytics query must execute an expensive `ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC)` view to filter out duplicates. This inflates compute scan costs on every single dashboard load. Partition-scoped `DELETE + INSERT` cleans duplicates at ingestion time so downstream analytical queries run at maximum speed.

---

## 5. Operations & Infrastructure Alternatives

### Q8: Why NOT Great Expectations or SaaS Data Observability (Monte Carlo)?
**Answer**:
- **Lightweight & Self-Contained**: Multi-tier testing with dbt schema tests, custom SQL business assertions, and local Python contract validators achieves 100% test coverage without running additional Python daemon services or paying high third-party SaaS subscription fees.

### Q9: Why NOT Terraform / Kubernetes in the primary codebase?
**Answer**:
- **Separation of Concerns**: This repository focuses on the core Data Engineering transformation, modeling, orchestration, and validation pipeline. Infrastructure-as-Code (Terraform) is maintained in separate centralized platform repositories to prevent tight coupling between application code and cloud resource provisioning.

