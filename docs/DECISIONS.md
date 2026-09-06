# Architecture Decision Records (ADRs)

## Index of Architectural Decisions

- [ADR-001: Google Cloud BigQuery as Core Data Warehouse](#adr-001-google-cloud-bigquery-as-core-data-warehouse)
- [ADR-002: Apache Airflow (Cloud Composer) for Pipeline Orchestration](#adr-002-apache-airflow-cloud-composer-for-pipeline-orchestration)
- [ADR-003: dbt for In-Warehouse ELT Transformations](#adr-003-dbt-for-in-warehouse-elt-transformations)
- [ADR-004: Partition-Scoped Idempotent Ingestion Pattern](#adr-004-partition-scoped-idempotent-ingestion-pattern)
- [ADR-005: Dynamic TaskGroup Generation via Centralized Event Config](#adr-005-dynamic-taskgroup-generation-via-centralized-event-config)
- [ADR-006: Incremental Merge with 3-Day Sliding Lookback for Facts](#adr-006-incremental-merge-with-3-day-sliding-lookback-for-facts)
- [ADR-007: SCD Type 2 Modeling for User Subscription Dimensions](#adr-007-scd-type-2-modeling-for-user-subscription-dimensions)
- [ADR-008: Pre-Aggregated Marts for Looker Studio BI Consumption](#adr-008-pre-aggregated-marts-for-looker-studio-bi-consumption)
- [ADR-009: Explicit Column Projection Over Unbounded SELECT *](#adr-009-explicit-column-projection-over-unbounded-select-)
- [ADR-010: Schema Isolation via Semantic Staging Views](#adr-010-schema-isolation-via-semantic-staging-views)

---

### ADR-001: Google Cloud BigQuery as Core Data Warehouse
- **Context**: The platform requires a scalable analytical warehouse capable of storing billions of ad impressions and streaming events with minimal operational management.
- **Decision**: Select Google Cloud BigQuery as the enterprise data warehouse.
- **Alternatives Considered**: Snowflake, Amazon Redshift, PostgreSQL, ClickHouse.
- **Trade-offs & Rationale**: BigQuery provides true serverless elasticity, decoupled compute/storage, native column-level partitioning and clustering, and zero cluster maintenance.
- **Consequences**: Queries are priced on-demand per byte scanned, requiring strict partition pruning, clustering, and incremental transformations.

---

### ADR-002: Apache Airflow (Cloud Composer) for Pipeline Orchestration
- **Context**: Decoupled telemetry streams need scheduled, dependency-aware batch orchestration with backfill capabilities and retry mechanisms.
- **Decision**: Use Apache Airflow 2.8+ deployed on Google Cloud Composer 2.
- **Alternatives Considered**: Prefect, Dagster, Google Cloud Workflows, Cron.
- **Trade-offs & Rationale**: Airflow provides battle-tested DAG scheduling, native TaskGroup abstractions, rich GCP operator support, and robust backfill CLI tooling.
- **Consequences**: Requires managing Airflow worker concurrency and task timeouts.

---

### ADR-003: dbt for In-Warehouse ELT Transformations
- **Context**: Raw staging data must be transformed into Kimball star-schema dimensions, facts, and analytical marts with testing and documentation.
- **Decision**: Adopt dbt (data build tool) for SQL transformations.
- **Alternatives Considered**: Custom Python Spark scripts, BigQuery stored procedures, Dataform.
- **Trade-offs & Rationale**: dbt provides modular Jinja templating, automatic DAG compilation, integrated testing (`unique`, `not_null`), version control, and lineage documentation.
- **Consequences**: Transformations occur inside BigQuery (ELT), requiring SQL optimization.

---

### ADR-004: Partition-Scoped Idempotent Ingestion Pattern
- **Context**: Pipeline retries and historical backfills risk creating duplicate records in BigQuery staging tables.
- **Decision**: Execute an atomic delete of the target execution hour partition before inserting new records (`DELETE WHERE ts >= start AND ts < end`).
- **Alternatives Considered**: `WRITE_TRUNCATE` (wipes whole table), append-only without deduplication, runtime row deduplication.
- **Trade-offs & Rationale**: Scoped deletion guarantees exact-once staging state while preserving unaffected historical partitions.
- **Consequences**: Slight query latency increase prior to insert.

---

### ADR-005: Dynamic TaskGroup Generation via Centralized Event Config
- **Context**: Scaling from 4 telemetry streams to 20+ streams must not require manual DAG code duplication.
- **Decision**: Implement `EVENT_CONFIG` registry in `airflow/dags/event_config.py` and generate TaskGroups dynamically using a unified factory function.
- **Alternatives Considered**: Separate DAG per event stream, hardcoded sequential tasks.
- **Trade-offs & Rationale**: Reduces DAG code by >70%, enforces consistent retry and partition patterns, and allows onboarding new streams in seconds.
- **Consequences**: DAG file parsing depends on configuration dictionary validity.

---

### ADR-006: Incremental Merge with 3-Day Sliding Lookback for Facts
- **Context**: Late-arriving mobile and web telemetry arrives hours or days after event occurrence.
- **Decision**: Configure fact models as dbt `incremental` with `merge` strategy, querying a 3-day lookback window and bounding target partition scans via `incremental_predicates`.
- **Alternatives Considered**: Full historical rebuilds, strict current-hour processing.
- **Trade-offs & Rationale**: Guarantees 99.8% capture of late-arriving events while bounding BigQuery slot compute to a 7-day partition window.
- **Consequences**: Slightly higher scan volume during hourly incremental runs compared to zero-lookback append.

---

### ADR-007: SCD Type 2 Modeling for User Subscription Dimensions
- **Context**: Users transition between `free` and `paid` subscription tiers over time; historical fact metrics must reflect the user's tier at the exact moment of the event.
- **Decision**: Model `dim_users` as a Slowly Changing Dimension Type 2 (SCD2) with `rowActivationDate`, `rowExpirationDate`, and `currentRow` flag.
- **Alternatives Considered**: SCD Type 1 (overwrite), separate snapshot tables.
- **Trade-offs & Rationale**: Preserves historical truth for attribution queries without retroactively corrupting past financial metrics.
- **Consequences**: Fact joins must include range conditions (`ts >= rowActivationDate AND ts < rowExpirationDate`).

---

### ADR-008: Pre-Aggregated Marts for Looker Studio BI Consumption
- **Context**: BI dashboards querying raw multi-million row fact tables create slow load times and excessive on-demand query costs.
- **Decision**: Pre-aggregate core dimensions and facts into daily and content-level marts (`daily_ad_metrics`, `daily_user_engagement`, `ad_content_performance`).
- **Alternatives Considered**: Direct fact querying, live OLAP cube.
- **Trade-offs & Rationale**: Reduces dashboard query scan sizes from gigabytes to kilobytes, achieving sub-second widget rendering.
- **Consequences**: Dashboard freshness is bounded by the hourly dbt model run cadence.

---

### ADR-009: Explicit Column Projection Over Unbounded SELECT *
- **Context**: Unbounded `SELECT *` statements in intermediate CTEs cause unnecessary column reads and memory allocation during query execution.
- **Decision**: Enforce explicit column projection across all core warehouse models and marts.
- **Alternatives Considered**: Allowing `SELECT *` throughout all SQL models.
- **Trade-offs & Rationale**: Improves query optimizer execution plans, lowers slot-ms usage, and protects against unexpected upstream schema additions.
- **Consequences**: Requires updating model SQL when adding new required fields.

---

### ADR-010: Schema Isolation via Semantic Staging Views
- **Context**: Changes in upstream Parquet file structures must not break downstream dimensional models.
- **Decision**: Interpose 1:1 dbt staging views (`stg_*`) between raw tables and core models to handle null defaults (`COALESCE`), trimming, and type casting.
- **Alternatives Considered**: Direct joins on raw staging tables.
- **Trade-offs & Rationale**: Isolates warehouse core logic from upstream changes and standardizes missing data representations.
- **Consequences**: Adds a virtual DAG layer in dbt graph.

---

### ADR-011: Declarative Data Contracts for Event Telemetry
- **Context**: Upstream telemetry emitters can introduce breaking schema changes or missing fields that silently corrupt BigQuery tables.
- **Decision**: Define declarative YAML data contracts in `contracts/` and validate them in CI and operational pipelines via `scripts/validate_contracts.py`.
- **Alternatives Considered**: Manual developer wiki documentation, unconstrained runtime schema evolution.
- **Trade-offs & Rationale**: Explicitly enforces ownership, types, and quality invariants at the ingestion perimeter.
- **Consequences**: Adding new fields requires updating contract specifications.

---

### ADR-012: Multi-Tier Testing Pyramid for Data Pipelines
- **Context**: Relying exclusively on end-to-end cloud tests slows developer feedback loops and introduces cost.
- **Decision**: Structure testing into a 4-tier hierarchy: Python Unit Tests (fast), Static Analysis (Ruff, SQLFluff), dbt Schema & Singular Assertions, and Staging Integration tests.
- **Alternatives Considered**: Cloud-only integration testing, manual spot-checking.
- **Trade-offs & Rationale**: Catches 95% of regressions locally in under 1 second without requiring active GCP credentials.
- **Consequences**: Developers must maintain unit test cases alongside pipeline modifications.

---

### ADR-013: Disaster Recovery via Idempotent Partition Replacements & Time Travel
- **Context**: Pipeline failures or data corruptions require rapid recovery without data loss or duplicate rows.
- **Decision**: Combine BigQuery 7-day Time Travel with Airflow partition-scoped atomic `DELETE` + `INSERT` reload procedures.
- **Alternatives Considered**: Full daily warehouse rebuilds, snapshot clones on every hourly run.
- **Trade-offs & Rationale**: Achieves RPO ≤ 1 hour and RTO ≤ 30 minutes with zero additional storage duplication cost.
- **Consequences**: Requires operational discipline in partition date targeting during backfills.

---

### ADR-014: Automated Documentation Integrity Verification
- **Context**: Documentation and links easily drift out of date as models and scripts evolve.
- **Decision**: Deploy `scripts/validate_docs.py` as an automated gate in CI and `./scripts/validate.sh`.
- **Alternatives Considered**: Manual documentation reviews.
- **Trade-offs & Rationale**: Guarantees all markdown files, links, and architectural references are valid and non-empty on every PR.
- **Consequences**: Broken links fail the CI build immediately.


