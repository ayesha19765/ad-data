#!/usr/bin/env bash
# ==============================================================================
# Adaptive Ads Data Engineering Platform - Local Validation Suite
# ==============================================================================
# Executes multi-tier static validation across Python, Unit Tests, Airflow,
# SQL, dbt, Security, Data Contracts, Performance Pruning, and Documentation.
#
# Usage: ./scripts/validate.sh
# ==============================================================================

set -eo pipefail

BOLD="\033[1m"
GREEN="\033[0;32m"
RED="\033[0;31m"
BLUE="\033[0;34m"
RESET="\033[0m"

echo -e "\n${BOLD}${BLUE}=====================================================${RESET}"
echo -e "${BOLD}${BLUE}  Adaptive Ads Data Engineering - Validation Suite   ${RESET}"
echo -e "${BOLD}${BLUE}=====================================================${RESET}\n"

FAILURES=0

# ------------------------------------------------------------------------------
# 1. Python Syntax & Compilation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[1/9] Checking Python Compilation...${RESET}"
if python3 -m py_compile airflow/dags/*.py scripts/*.py tests/unit/*.py; then
    echo -e "${GREEN}  ✓ Python compilation passed.${RESET}\n"
else
    echo -e "${RED}  ✗ Python compilation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 2. Python Unit Tests Suite
# ------------------------------------------------------------------------------
echo -e "${BOLD}[2/9] Executing Python Unit Tests Suite...${RESET}"
if python3 -m unittest discover tests; then
    echo -e "${GREEN}  ✓ All Python unit tests passed.${RESET}\n"
else
    echo -e "${RED}  ✗ Python unit tests failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 3. Airflow DAG & Configuration Structure
# ------------------------------------------------------------------------------
echo -e "${BOLD}[3/9] Validating Airflow DAG Definitions & References...${RESET}"
python3 -c "
import sys, os
sys.path.insert(0, 'airflow/dags')

from event_config import EVENT_CONFIG
from schema import schema

print(f'  ✓ Found {len(EVENT_CONFIG)} configured event streams in EVENT_CONFIG:')
for event, meta in EVENT_CONFIG.items():
    sql_path = os.path.join('airflow/dags', meta['sql_template'])
    if not os.path.isfile(sql_path):
        print(f'  ✗ Missing SQL template: {sql_path}', file=sys.stderr)
        sys.exit(1)
    if event not in schema:
        print(f'  ✗ Missing schema mapping for {event}', file=sys.stderr)
        sys.exit(1)
    print(f'    - {event}: {meta[\"staging_table\"]} ({meta[\"source_format\"]}) -> {sql_path}')
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ Airflow DAG structure and template mapping verified.${RESET}\n"
else
    echo -e "${RED}  ✗ Airflow DAG validation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 4. SQL & Jinja Template Validation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[4/9] Validating SQL File Integrity...${RESET}"
python3 -c "
import glob, sys

sql_files = glob.glob('dbt/models/**/*.sql', recursive=True) + glob.glob('airflow/dags/sql/*.sql') + glob.glob('dbt/tests/*.sql')
print(f'  ✓ Scanning {len(sql_files)} SQL files across dbt and Airflow...')
for f in sql_files:
    with open(f, 'r') as fp:
        content = fp.read()
        if len(content.strip()) == 0:
            print(f'  ✗ Empty SQL file: {f}', file=sys.stderr)
            sys.exit(1)
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ All SQL model, test, and template files are populated and accessible.${RESET}\n"
else
    echo -e "${RED}  ✗ SQL file validation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 5. dbt Model & Schema Graph Validation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[5/9] Checking dbt Models & Schema Definitions...${RESET}"
python3 -c "
import yaml, glob, sys

schema_files = glob.glob('dbt/models/**/schema.yml', recursive=True)
print(f'  ✓ Validating {len(schema_files)} dbt schema YAML files:')
for sf in schema_files:
    with open(sf, 'r') as fp:
        try:
            data = yaml.safe_load(fp)
            version = data.get('version')
            models = [m['name'] for m in data.get('models', [])]
            sources = [s['name'] for s in data.get('sources', [])]
            print(f'    - {sf}: version {version}, {len(models)} models, {len(sources)} sources')
        except Exception as e:
            print(f'  ✗ YAML syntax error in {sf}: {e}', file=sys.stderr)
            sys.exit(1)
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ dbt schema definitions parsed successfully.${RESET}\n"
else
    echo -e "${RED}  ✗ dbt schema validation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 6. Security & Credential Hygiene Check
# ------------------------------------------------------------------------------
echo -e "${BOLD}[6/9] Scanning for Credentials & Secret Leaks...${RESET}"
SECRET_COUNT=0
if grep -rn "private_key" airflow/creds/ 2>/dev/null; then
    echo -e "${RED}  ✗ Active private key found in airflow/creds/!${RESET}"
    SECRET_COUNT=$((SECRET_COUNT + 1))
fi
if [ -f "airflow/.env" ]; then
    echo -e "${RED}  ✗ Unignored airflow/.env detected in working tree!${RESET}"
    SECRET_COUNT=$((SECRET_COUNT + 1))
fi

if [ $SECRET_COUNT -eq 0 ]; then
    echo -e "${GREEN}  ✓ Secret hygiene verified. No exposed keys or environment files.${RESET}\n"
else
    echo -e "${RED}  ✗ Security check failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 7. Performance & Pruning Safeguards Check
# ------------------------------------------------------------------------------
echo -e "${BOLD}[7/9] Auditing Partition Pruning & Query Safeguards...${RESET}"
python3 -c "
import glob, sys

# Check that all incremental fact models declare incremental_predicates
fact_files = glob.glob('dbt/models/core/fact_*.sql')
for ff in fact_files:
    with open(ff, 'r') as fp:
        content = fp.read()
        if 'incremental_predicates' not in content:
            print(f'  ✗ Missing incremental_predicates in {ff}', file=sys.stderr)
            sys.exit(1)
print(f'  ✓ Verified incremental_predicates partition bounds across {len(fact_files)} fact models.')

# Check that core dimensions/facts avoid unprojected SELECT * in outer models
core_files = glob.glob('dbt/models/core/*.sql')
for cf in core_files:
    with open(cf, 'r') as fp:
        lines = fp.readlines()
        for i, line in enumerate(lines, 1):
            if 'SELECT *' in line and 'dim_datetime.sql' not in cf:
                print(f'  ✗ Warning: Unbounded SELECT * found in {cf}:{i}', file=sys.stderr)
                sys.exit(1)
print(f'  ✓ Explicit column projections verified across {len(core_files)} core warehouse models.')
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ Performance and partition pruning safeguards verified.${RESET}\n"
else
    echo -e "${RED}  ✗ Performance safeguards check failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 8. Operational Tooling Smoke Test (Contracts, Schema Drift & Backfill)
# ------------------------------------------------------------------------------
echo -e "${BOLD}[8/9] Validating Operational Scripts (Contracts, Schema Drift & Backfill)...${RESET}"
if python3 scripts/validate_contracts.py --strict > /dev/null && \
   python3 scripts/check_schema.py --strict > /dev/null && \
   python3 scripts/backfill.py --start "2026-09-01T00:00:00" --end "2026-09-01T01:00:00" --dry-run > /dev/null; then
    echo -e "${GREEN}  ✓ Operational utilities (validate_contracts, check_schema, backfill) executed successfully.${RESET}\n"
else
    echo -e "${RED}  ✗ Operational tooling check failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 9. Documentation Integrity & Link Validation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[9/9] Checking Documentation & Cross-Reference Integrity...${RESET}"
if python3 scripts/validate_docs.py > /dev/null; then
    echo -e "${GREEN}  ✓ Documentation integrity verified (All markdown documents and README links valid).${RESET}\n"
else
    echo -e "${RED}  ✗ Documentation validation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# Summary & Exit
# ------------------------------------------------------------------------------
echo -e "${BOLD}${BLUE}=====================================================${RESET}"
if [ $FAILURES -eq 0 ]; then
    echo -e "${BOLD}${GREEN}  ALL VALIDATION CHECKS PASSED (9/9)               ${RESET}"
    echo -e "${BOLD}${BLUE}=====================================================${RESET}\n"
    exit 0
else
    echo -e "${BOLD}${RED}  VALIDATION SUITE FAILED WITH $FAILURES FAILURE(S)     ${RESET}"
    echo -e "${BOLD}${BLUE}=====================================================${RESET}\n"
    exit 1
fi
