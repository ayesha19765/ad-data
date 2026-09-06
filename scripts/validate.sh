#!/usr/bin/env bash
# ==============================================================================
# Adaptive Ads Data Engineering Platform - Local Validation Script
# ==============================================================================
# Executes static validation across Python, Airflow DAGs, SQL, dbt, and security.
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
echo -e "${BOLD}[1/5] Checking Python Compilation...${RESET}"
if python3 -m py_compile airflow/dags/*.py; then
    echo -e "${GREEN}  ✓ Python compilation passed.${RESET}\n"
else
    echo -e "${RED}  ✗ Python compilation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 2. Airflow DAG & Configuration Structure
# ------------------------------------------------------------------------------
echo -e "${BOLD}[2/5] Validating Airflow DAG Definitions & References...${RESET}"
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
# 3. SQL & Jinja Template Validation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[3/5] Validating SQL File Integrity...${RESET}"
python3 -c "
import glob, sys

sql_files = glob.glob('dbt/models/**/*.sql', recursive=True) + glob.glob('airflow/dags/sql/*.sql')
print(f'  ✓ Scanning {len(sql_files)} SQL files across dbt and Airflow...')
for f in sql_files:
    with open(f, 'r') as fp:
        content = fp.read()
        if len(content.strip()) == 0:
            print(f'  ✗ Empty SQL file: {f}', file=sys.stderr)
            sys.exit(1)
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ All SQL model and template files are populated and accessible.${RESET}\n"
else
    echo -e "${RED}  ✗ SQL file validation failed!${RESET}\n"
    FAILURES=$((FAILURES + 1))
fi

# ------------------------------------------------------------------------------
# 4. dbt Model & Schema Graph Validation
# ------------------------------------------------------------------------------
echo -e "${BOLD}[4/5] Checking dbt Models & Schema Definitions...${RESET}"
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
# 5. Security & Credential Hygiene Check
# ------------------------------------------------------------------------------
echo -e "${BOLD}[5/5] Scanning for Credentials & Secret Leaks...${RESET}"
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
# Summary & Exit
# ------------------------------------------------------------------------------
echo -e "${BOLD}${BLUE}=====================================================${RESET}"
if [ $FAILURES -eq 0 ]; then
    echo -e "${BOLD}${GREEN}  ALL VALIDATION CHECKS PASSED (5/5)               ${RESET}"
    echo -e "${BOLD}${BLUE}=====================================================${RESET}\n"
    exit 0
else
    echo -e "${BOLD}${RED}  VALIDATION SUITE FAILED WITH $FAILURES FAILURE(S)     ${RESET}"
    echo -e "${BOLD}${BLUE}=====================================================${RESET}\n"
    exit 1
fi

