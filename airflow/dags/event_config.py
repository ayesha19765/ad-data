"""
Centralized Event Ingestion Configuration for Adaptive Ads Data Engineering Platform.

This module provides a unified registry of ingestion metadata, schema definitions,
and partition strategies for all telemetry event streams.
"""

from schema import schema

EVENT_CONFIG = {
    "watch_events": {
        "event_name": "watch_events",
        "description": "User video and stream playback telemetry events",
        "source_format": "PARQUET",
        "gcs_path_template": "watch_events/month={month}/day={day}/hour={hour}",
        "staging_table": "watch_events",
        "schema": schema["watch_events"],
        "sql_template": "sql/watch_events.sql",
        "partition_field": "ts",
        "partition_type": "HOUR",
    },
    "ad_events": {
        "event_name": "ad_events",
        "description": "Ad impressions, interactions, and ad playback events",
        "source_format": "PARQUET",
        "gcs_path_template": "ad_events/month={month}/day={day}/hour={hour}",
        "staging_table": "ad_events",
        "schema": schema["ad_events"],
        "sql_template": "sql/ad_events.sql",
        "partition_field": "ts",
        "partition_type": "HOUR",
    },
    "page_view_events": {
        "event_name": "page_view_events",
        "description": "Web and mobile app page navigation telemetry",
        "source_format": "PARQUET",
        "gcs_path_template": "page_view_events/month={month}/day={day}/hour={hour}",
        "staging_table": "page_view_events",
        "schema": schema["page_view_events"],
        "sql_template": "sql/page_view_events.sql",
        "partition_field": "ts",
        "partition_type": "HOUR",
    },
    "auth_events": {
        "event_name": "auth_events",
        "description": "User authentication, login, and registration events",
        "source_format": "PARQUET",
        "gcs_path_template": "auth_events/month={month}/day={day}/hour={hour}",
        "staging_table": "auth_events",
        "schema": schema["auth_events"],
        "sql_template": "sql/auth_events.sql",
        "partition_field": "ts",
        "partition_type": "HOUR",
    },
}

