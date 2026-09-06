# Architectural Decision Records (ADR) Cheat Sheet

## 1. Executive Decision Matrix

| Decision ID | Architectural Choice | Chosen Option | Rejected Alternatives | Primary Justification | Key Trade-off / Limitation |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **ADR-001** | Orchestration Engine | **Apache Airflow 2.8+** | Prefect, Dagster, Cron, Cloud Composer | Rich ecosystem, TaskGroup modularity, robust backfilling, native GCP operators. | Heavier infrastructure footprint compared to lightweight runners like Prefect. |
| **ADR-002** | Cloud Data Warehouse | **Google BigQuery** | Snowflake, Databricks, Redshift | Serverless scalability, zero storage/compute cluster maintenance, native nested JSON, cost-effective per-TB query billing. | Potential runaway query scan costs without strict partition filters and clustering. |
| **ADR-003** | Transformation Layer | **dbt Core** | Custom Python scripts, Spark SQL, BigQuery Stored Procs | Declarative SQL modeling, automatic DAG lineage, built-in schema testing pyramid, version-controlled git workflows. | Orchestration boundary separation (requires Airflow BashOperator / Cosmos / dbt Cloud). |
| **ADR-004** | Storage Format | **Columnar Snappy Parquet** | CSV, JSON Lines, Avro | 4–10x compression ratio over JSON, columnar projection pruning, typed schema preservation. | Binary format requires schema tools to inspect raw files locally. |
| **ADR-005** | Ingestion Idempotency | **Partition-Scoped Delete + Insert** | Append-only with deduplication views, Truncate & Load | Deterministic replayability, safe backfills, zero duplicate records on Airflow retries. | Requires strict date/hour partition boundaries in all staging queries. |
| **ADR-006** | Fact Materialization | **Incremental `merge` with Predicates** | Full Table Replacement (`table`), Append-only (`insert_overwrite`) | Dramatically reduces compute scan costs and runtimes while seamlessly handling late-arriving updates. | Slightly higher compute overhead per batch compared to pure appends. |
| **ADR-007** | Dimension Tracking | **SQL Window Function SCD Type 2** | dbt Snapshots, In-place Overwrite (SCD1) | Deterministic replay from raw event logs; tracks full state history without database-level state locks. | Window functions can become computationally heavy on massive datasets. |
| **ADR-008** | Surrogate Key Strategy | **Cryptographic MD5 / SHA256 Hashes** | Auto-incrementing Integers (Identity Columns) | Deterministic and distributed generation without centralized sequence coordinate locking. | Hash collision probability (negligible at $10^{-15}$) and 16-byte storage overhead. |
| **ADR-009** | Data Quality Framework | **Multi-Tier Pyramid (Contracts + Tests)** | Great Expectations, Soda Core, Monte Carlo | Zero third-party SaaS dependency, native dbt schema tests + singular SQL tests + contract validation scripts. | Lacks real-time streaming anomaly detection out-of-the-box. |
| **ADR-010** | CI/CD Pipeline | **GitHub Actions** | GitLab CI, Jenkins, CircleCI | Native GitHub integration, zero external CI runners required, fast feedback loops with pre-commit hooks. | 2,000 free runner minutes per month limit on private repositories. |

---

## 2. In-Depth Trade-Off Analysis

### Trade-off 1: Partition-Scoped Ingestion vs. Append-Only Deduplication
- **Chosen**: Partition-Scoped `DELETE + INSERT` in BigQuery Staging.
- **Why**: When an hourly Airflow task fails halfway and restarts, appending raw data causes staging pollution. Partition-scoped deletion ensures that running the task 1 time or 100 times produces identical staging state.
- **Trade-off**: Requires every ingestion job to know its precise execution partition interval (`ds` and `hour`).

### Trade-off 2: Incremental Merge with 3-Day Sliding Window vs. Full Scan
- **Chosen**: `incremental_predicates = ["DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)"]`.
- **Why**: Prevents BigQuery from scanning 100% of historical partitions during incremental `MERGE` operations, reducing scanned bytes by up to 90%.
- **Trade-off**: Events arriving later than 3 days require a manual backfill execution using [`scripts/backfill.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/backfill.py).

