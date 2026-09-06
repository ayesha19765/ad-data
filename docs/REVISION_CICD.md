# Subsystem Revision Guide: CI/CD & Automated Quality Gates

## 1. CI/CD Architecture & Quality Gates

Continuous Integration and Continuous Deployment are automated via GitHub Actions in [`.github/workflows/ci.yml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/.github/workflows/ci.yml). Every pull request and push to `main` must pass 7 automated quality gates:

```
[Push / Pull Request]
         │
         ▼
[GitHub Actions CI Pipeline: `.github/workflows/ci.yml`]
  ├── Gate 1: Python Code Linting (`ruff check .`)
  ├── Gate 2: SQL Linting & Dialect Check (`sqlfluff lint`)
  ├── Gate 3: Python Unit Tests (19 unit tests in `tests/unit/`)
  ├── Gate 4: Airflow DAG Compilation & Integrity Scan
  ├── Gate 5: Data Contract Schema Validation (`validate_contracts.py`)
  ├── Gate 6: Secret & Credential Hygiene Scanner
  └── Gate 7: Documentation Integrity & Link Verification (`validate_docs.py`)
         │
         ▼
[Merge Approval & Deployment]
```

---

## 2. Gate Verification Details

| Gate # | Name | Tool / Script | Failure Condition | Remediation Action |
| :---: | :--- | :--- | :--- | :--- |
| **1** | Python Linting | `ruff check .` | Unused imports, PEP8 violations, syntax errors | Run `ruff check --fix .` locally |
| **2** | SQL Linting | `sqlfluff lint` | Non-standard SQL formatting, reserved keyword misuses | Run `sqlfluff fix` locally |
| **3** | Unit Tests | `python3 -m unittest` | Any assertion failure in `tests/unit/` | Fix logic in `event_config.py`, `backfill.py`, etc. |
| **4** | DAG Compilation | `python3 -m py_compile` | Python syntax error or invalid operator parameter | Fix DAG code in `airflow/dags/` |
| **5** | Contract Validation | `scripts/validate_contracts.py` | Schema field missing or type mismatch against contracts | Update YAML contract or dbt model schema |
| **6** | Secret Hygiene | Git scan / regex | Unencrypted GCP service account key or `.env` committed | Revoke secret, remove file, rewrite git history |
| **7** | Doc Integrity | `scripts/validate_docs.py` | Broken markdown link or missing referenced doc | Fix broken links in markdown files |

---

## 3. Local Pre-Commit & Validation

Before pushing code, engineers run the local validation suite:

```bash
# Run the 9-step automated local validation suite
./scripts/validate.sh
```

Pre-commit hooks configured in [`.pre-commit-config.yaml`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/.pre-commit-config.yaml) automatically format code and check YAML formatting upon every `git commit`.

