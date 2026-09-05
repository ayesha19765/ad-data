"""
Modular Task Templates and TaskGroup Factory for Airflow Ingestion.

Provides reusable BigQuery operator builders and a unified TaskGroup factory
to orchestrate isolated, idempotent event ingestion pipelines.
"""

import logging
from typing import Any, Dict, Optional

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)


def create_external_table(
    event: str,
    gcp_project_id: str,
    bigquery_dataset: str,
    external_table_name: str,
    gcp_gcs_bucket: str,
    events_path: str,
    source_format: str = "PARQUET",
) -> BigQueryCreateExternalTableOperator:
    """
    Create a transient external table in BigQuery pointing to GCS raw files.
    """
    logger.debug(
        "Creating external table task for event: %s, table: %s.%s.%s",
        event,
        gcp_project_id,
        bigquery_dataset,
        external_table_name,
    )
    return BigQueryCreateExternalTableOperator(
        task_id=f"{event}_create_external_table",
        table_resource={
            "tableReference": {
                "projectId": gcp_project_id,
                "datasetId": bigquery_dataset,
                "tableId": external_table_name,
            },
            "externalDataConfiguration": {
                "sourceFormat": source_format,
                "sourceUris": [f"gs://{gcp_gcs_bucket}/{events_path}/*"],
            },
        },
    )


def create_empty_table(
    event: str,
    gcp_project_id: str,
    bigquery_dataset: str,
    bigquery_table_name: str,
    events_schema: list,
    partition_field: str = "ts",
    partition_type: str = "HOUR",
) -> BigQueryCreateEmptyTableOperator:
    """
    Ensure the target partitioned staging table exists in BigQuery.
    """
    logger.debug(
        "Creating staging table task for event: %s, table: %s.%s.%s",
        event,
        gcp_project_id,
        bigquery_dataset,
        bigquery_table_name,
    )
    return BigQueryCreateEmptyTableOperator(
        task_id=f"{event}_create_empty_table",
        project_id=gcp_project_id,
        dataset_id=bigquery_dataset,
        table_id=bigquery_table_name,
        schema_fields=events_schema,
        time_partitioning={
            "type": partition_type,
            "field": partition_field,
        },
        exists_ok=True,
    )


def insert_job(
    event: str,
    insert_query: str,
    bigquery_dataset: str,
    gcp_project_id: str,
    timeout: int = 300000,
) -> BigQueryInsertJobOperator:
    """
    Execute SQL transformation/insert query from external staging table to final table.
    """
    logger.debug(
        "Creating insert job task for event: %s in dataset: %s",
        event,
        bigquery_dataset,
    )
    return BigQueryInsertJobOperator(
        task_id=f"{event}_execute_insert_query",
        configuration={
            "query": {
                "query": insert_query,
                "useLegacySql": False,
            },
            "timeoutMs": timeout,
            "defaultDataset": {
                "datasetId": bigquery_dataset,
                "projectId": gcp_project_id,
            },
        },
    )


def delete_external_table(
    event: str,
    gcp_project_id: str,
    bigquery_dataset: str,
    external_table_name: str,
) -> BigQueryDeleteTableOperator:
    """
    Clean up transient external table from BigQuery.
    """
    logger.debug(
        "Creating delete external table task for event: %s, table: %s",
        event,
        external_table_name,
    )
    return BigQueryDeleteTableOperator(
        task_id=f"{event}_delete_external_table",
        deletion_dataset_table=f"{gcp_project_id}.{bigquery_dataset}.{external_table_name}",
        ignore_if_missing=True,
    )


def build_event_ingestion_task_group(
    event_key: str,
    event_meta: Dict[str, Any],
    gcp_project_id: str,
    bigquery_dataset: str,
    gcs_bucket: str,
    execution_datetime_str: str,
    execution_month: str,
    execution_day: str,
    execution_hour: str,
    parent_group: Optional[TaskGroup] = None,
) -> TaskGroup:
    """
    Factory function to construct an isolated, modular TaskGroup for an event stream.

    Encapsulates:
    1. External table creation pointing to partition in GCS
    2. Idempotent target staging table creation
    3. Idempotent partition load (delete + insert)
    4. Transient external table cleanup
    """
    group_id = f"ingest_{event_key}"
    staging_table = event_meta.get("staging_table", event_key)
    external_table_name = f"{staging_table}_{execution_datetime_str}"
    gcs_path = event_meta.get("gcs_path_template", f"{event_key}/month={{month}}/day={{day}}/hour={{hour}}").format(
        month=execution_month,
        day=execution_day,
        hour=execution_hour,
    )
    schema_fields = event_meta["schema"]
    sql_file = event_meta.get("sql_template", f"sql/{event_key}.sql")
    insert_query = f"{{% include '{sql_file}' %}}"
    partition_field = event_meta.get("partition_field", "ts")
    partition_type = event_meta.get("partition_type", "HOUR")
    source_format = event_meta.get("source_format", "PARQUET")

    with TaskGroup(group_id=group_id, parent_group=parent_group) as tg:
        create_ext_task = create_external_table(
            event=event_key,
            gcp_project_id=gcp_project_id,
            bigquery_dataset=bigquery_dataset,
            external_table_name=external_table_name,
            gcp_gcs_bucket=gcs_bucket,
            events_path=gcs_path,
            source_format=source_format,
        )

        create_staging_task = create_empty_table(
            event=event_key,
            gcp_project_id=gcp_project_id,
            bigquery_dataset=bigquery_dataset,
            bigquery_table_name=staging_table,
            events_schema=schema_fields,
            partition_field=partition_field,
            partition_type=partition_type,
        )

        execute_load_task = insert_job(
            event=event_key,
            insert_query=insert_query,
            bigquery_dataset=bigquery_dataset,
            gcp_project_id=gcp_project_id,
        )

        delete_ext_task = delete_external_table(
            event=event_key,
            gcp_project_id=gcp_project_id,
            bigquery_dataset=bigquery_dataset,
            external_table_name=external_table_name,
        )

        create_ext_task >> create_staging_task >> execute_load_task >> delete_ext_task

    return tg
