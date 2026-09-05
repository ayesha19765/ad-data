"""
Adaptive Ads Main Orchestration Pipeline.

Hourly ELT workflow that:
1. Ingests independent telemetry event streams (watch, ad, page_view, auth) in parallel TaskGroups.
2. Loads and deduplicates data into BigQuery staging tables using partition-scoped replacement.
3. Builds reference seeds (state_codes) in dbt.
4. Executes core dimension and fact transformations in BigQuery via dbt.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from event_config import EVENT_CONFIG
from task_templates import build_event_ingestion_task_group


# Environment configuration
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "adaptive_ads_stg")

# Jinja macro variables for execution window
EXECUTION_MONTH = '{{ logical_date.strftime("%-m") }}'
EXECUTION_DAY = '{{ logical_date.strftime("%-d") }}'
EXECUTION_HOUR = '{{ logical_date.strftime("%-H") }}'
EXECUTION_DATETIME_STR = '{{ logical_date.strftime("%m%d%H") }}'

# Dynamic table mapping from centralized event configuration
TABLE_MAP = {f"{meta['staging_table'].upper()}_TABLE": meta["staging_table"] for meta in EVENT_CONFIG.values()}

MACRO_VARS = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "BIGQUERY_DATASET": BIGQUERY_DATASET,
    "EXECUTION_DATETIME_STR": EXECUTION_DATETIME_STR,
}
MACRO_VARS.update(TABLE_MAP)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="adaptive_ads_dag",
    default_args=default_args,
    description="Modular hourly pipeline for decoupled ingestion and dbt transformation of Adaptive Ads data",
    schedule_interval="5 * * * *",  # 5th minute of every hour
    start_date=datetime(2024, 5, 21, 18),
    catchup=False,
    max_active_runs=1,
    user_defined_macros=MACRO_VARS,
    tags=["adaptive_ads", "ingestion", "dbt"],
) as dag:

    # 1. dbt Seed task: Load reference dimension seeds (e.g. US state codes)
    initiate_dbt_task = BashOperator(
        task_id="dbt_initiate",
        bash_command="cd /dbt && dbt deps && dbt seed --select state_codes --profiles-dir . --target prod",
    )

    # 2. dbt Run task: Execute core dimensions, facts, and analytical models
    execute_dbt_task = BashOperator(
        task_id="dbt_adaptive_ads_run",
        bash_command="cd /dbt && dbt deps && dbt run --profiles-dir . --target prod",
    )

    # 3. Dynamic Parallel Ingestion TaskGroups
    ingestion_groups = []
    for event_key, event_meta in EVENT_CONFIG.items():
        tg = build_event_ingestion_task_group(
            event_key=event_key,
            event_meta=event_meta,
            gcp_project_id=GCP_PROJECT_ID,
            bigquery_dataset=BIGQUERY_DATASET,
            gcs_bucket=GCP_GCS_BUCKET,
            execution_datetime_str=EXECUTION_DATETIME_STR,
            execution_month=EXECUTION_MONTH,
            execution_day=EXECUTION_DAY,
            execution_hour=EXECUTION_HOUR,
        )
        ingestion_groups.append(tg)

    # Fan-in: All parallel ingestion branches must finish before starting dbt transformations
    ingestion_groups >> initiate_dbt_task >> execute_dbt_task
