# Release Management & Rollback Strategy

## 1. Release Lifecycle & Git Workflow

The **Adaptive Ads** platform follows trunk-based development with short-lived feature branches and strict CI validation gates.

```
[ Feature Branch (`feature/*`) ]
               │
               ▼
[ Pull Request to `main` ] ──► [ Automated CI Validation (7 Gates) ]
                                             │
                                             ▼
                                [ Peer Code Review Approval ]
                                             │
                                             ▼
[ Merge to `main` ] ──► [ Semantic Version Tag (`v1.x.x`) ] ──► [ CD Deployment ]
```

---

## 2. Mandatory Pre-Merge Quality Gates

Every pull request must pass all automated verification checks before merge approval:
1. **Python Compilation & Syntax**: `py_compile` on all DAGs and scripts.
2. **Python Unit Tests**: All `tests/unit/` tests pass via `unittest` / `pytest`.
3. **Python Linting**: Clean Ruff inspection with zero warnings.
4. **SQLFluff Linting**: BigQuery SQL dialect syntax and convention verification.
5. **Airflow DAG Parsing**: Zero `DagBag` import errors.
6. **Data Contract Compliance**: `scripts/validate_contracts.py --strict` passes.
7. **Secret Leak Detection**: Zero uncommitted credentials or `.env` files.

---

## 3. Rollback Playbooks by Failure Domain

| Failure Domain | Trigger Condition | Rollback Procedure | Verification Step |
| :--- | :--- | :--- | :--- |
| **Faulty Airflow Python Code** | DAG import error or task syntax failure | `git revert HEAD` & push to `main` | Verify `DagBag` loads in Composer |
| **Faulty dbt SQL Model** | Query runtime error or downstream test failure | Revert SQL commit; run `dbt run --select <model>` | Run `dbt test --select <model>` |
| **Corrupted Partition Data** | Anomaly in staging or fact records | Re-run ingestion via `scripts/backfill.py` | Verify row counts and singular tests |
| **Accidental Schema Deletion** | Dropped table or altered partition | BigQuery Time Travel snapshot restore | Validate table schema and data |

