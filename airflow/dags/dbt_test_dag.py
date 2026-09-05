from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    dag_id="dbt_test_dag",
    default_args=default_args,
    description="Execute dbt data quality tests and validations for Adaptive Ads models",
    schedule_interval="@once",
    start_date=datetime(2024, 5, 20),
    catchup=False,
    tags=["adaptive_ads", "dbt", "testing"],
) as dag:

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="cd /dbt && dbt deps && dbt test --profiles-dir . --target prod",
    )

    dbt_test_task
