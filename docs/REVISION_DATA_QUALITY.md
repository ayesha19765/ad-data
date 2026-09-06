# Subsystem Revision Guide: Data Quality & Testing Hierarchy

## 1. The 4-Tier Quality Pyramid

Data quality is enforced across four distinct evaluation boundaries, catching bugs from local development up to production warehouse execution:

```
                  ▲
                 / \
                /   \     Tier 4: Repository Validation (`validate.sh`)
               / Tier\    - 9 automated static analysis & smoke checks
              /   4   \
             /─────────\  Tier 3: Singular SQL Business Tests (`dbt/tests/`)
            /  Tier 3   \ - Complex domain assertions & ratio bounds
           /─────────────\ Tier 2: dbt Schema Tests (`schema.yml`)
          /    Tier 2     \- unique, not_null, accepted_values, relationships
         /─────────────────\Tier 1: Fast Python Unit Tests (`tests/unit/`)
        /      Tier 1       \- 19 tests in 0.02s: contracts, backfill, configs
       /─────────────────────\
```

---

## 2. Tier Breakdown & Implementation

### Tier 1: Local Python Unit Tests (19 Tests, 0.022s)
Located in [`tests/unit/`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/tests/unit/):
- **`test_event_config.py`**: Verifies schema integrity, required attributes, and physical SQL template existence for all 4 telemetry streams.
- **`test_backfill.py`**: Verifies ISO-8601 parsing, hour interval slicing, and date validation.
- **`test_schema_check.py`**: Tests schema drift detection for added/removed columns and type changes.
- **`test_contracts.py`**: Tests declarative YAML contract validation against schema registries.

### Tier 2: dbt Generic Schema Tests
Configured in `dbt/models/**/schema.yml`:
- **Uniqueness & Non-null**: Enforced on all primary keys (`adEventKey`, `streamKey`, `userKey`, `movieKey`).
- **Referential Integrity (`relationships`)**: Validates foreign key constraints between facts and dimensions.
- **Accepted Values**: Validates categorical enumerations (e.g. `subscriptionTier IN ('Free', 'Premium', 'Family_Premium')`).

### Tier 3: dbt Singular Business Logic Tests
Located in [`dbt/tests/`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/dbt/tests/):
- **`assert_dim_users_valid_date_ranges.sql`**: Asserts `rowActivationDate <= rowExpirationDate`.
- **`assert_fact_ad_events_valid_timestamps.sql`**: Asserts timestamps are non-null and not in the future.
- **`assert_fact_streams_valid_duration.sql`**: Asserts `watchDurationSeconds >= 0`.
- **`assert_daily_ad_metrics_rates_bounded.sql`**: Asserts CTR and conversion ratios remain strictly $\in [0.0, 1.0]$.

### Tier 4: Repository Validation Suite (`scripts/validate.sh`)
Executes 9 automated gates in sequence:
1. Python compilation (`py_compile`)
2. Python unit tests (`unittest discover`)
3. Airflow DAG and config mapping verification
4. SQL file integrity and syntax scanning
5. dbt schema YAML graph validation
6. Secret and credential hygiene scan
7. Partition pruning and performance safeguards
8. Operational tooling smoke test (`backfill.py`, `check_schema.py`, `validate_contracts.py`)
9. Markdown documentation link and integrity validation (`validate_docs.py`)

---

## 3. Declarative Data Contracts

Located in [`contracts/*.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/contracts/) and enforced by [`scripts/validate_contracts.py`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/scripts/validate_contracts.py):
- Establishes explicit interface agreements between telemetry emitter teams and the data platform.
- Defines expected datatypes, required vs. optional fields, and partition keys.
- Fails CI workflows if an unapproved schema breaking change is committed.

