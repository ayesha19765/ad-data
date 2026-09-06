# Environment Reproducibility & Deployment Separation

## 1. Technical Baseline & System Requirements

The **Adaptive Ads** data platform is engineered for 100% deterministic reproducibility across developer workstations, CI/CD runners, and Google Cloud production environments.

```
┌────────────────────────────────────────────────────────────────────────┐
│                     CANONICAL STACK SPECIFICATION                      │
├──────────────────────────┬─────────────────────────────────────────────┤
│ Python Runtime           │ Python 3.9 / 3.11                           │
│ Orchestrator             │ Apache Airflow 2.8.1                        │
│ Data Build Tool          │ dbt-core 1.6.0 / dbt-bigquery 1.6.0         │
│ Data Warehouse           │ Google BigQuery (Standard SQL 2026 dialect) │
│ Container Engine         │ Docker 24.0+ & Docker Compose v2            │
│ Linters & Testing        │ Ruff 0.3+, SQLFluff 3.0+, Pytest / Unittest │
└──────────────────────────┴─────────────────────────────────────────────┘
```

---

## 2. Environment Separation Architecture

The platform defines three isolated operational environments:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        ENVIRONMENT TOPOLOGY                            │
└────────────────────────────────────────────────────────────────────────┘
  [ 1. LOCAL / DEV ]     ──► Docker Compose + SQLite/DuckDB/Staging mock
  [ 2. CI / TEST ]       ──► GitHub Actions runners + ephemeral PR datasets
  [ 3. PRODUCTION ]      ──► Cloud Composer 2 + `adaptive_ads_prod`
```

| Dimension | Local / Dev | CI / Testing | Production (Cloud) |
| :--- | :--- | :--- | :--- |
| **Orchestration** | Local Docker Compose | GitHub Actions CI | Google Cloud Composer 2 |
| **GCP Project** | `test-gcp-project` (mock) | `ci-project-runner` | `adaptive-ads-enterprise` |
| **Staging Dataset** | `adaptive_ads_stg` | `pr_<pr_number>_stg` | `adaptive_ads_stg` |
| **Core & Marts** | `adaptive_ads_prod` | `pr_<pr_number>_prod`| `adaptive_ads_prod` |
| **GCS Bucket** | `test-telemetry-bucket` | `ci-telemetry-landing` | `gs://adaptive-ads-telemetry-prod` |
| **dbt Target** | `dev` | `ci` | `prod` |

---

## 3. Step-by-Step Local Environment Setup

```bash
# 1. Clone repository and navigate to root directory
git clone https://github.com/ayesha19765/ad-data.git
cd ad-data

# 2. Configure environment variables
cp airflow/.env.example airflow/.env

# 3. Start local containerized Airflow cluster
cd airflow && docker-compose up -d && cd ..

# 4. Run full developer validation suite (7/7 checks)
./scripts/validate.sh

# 5. Execute Python unit tests
python3 -m unittest discover tests

# 6. Execute data contract and schema compliance checks
python3 scripts/validate_contracts.py --strict
python3 scripts/check_schema.py --strict
```

