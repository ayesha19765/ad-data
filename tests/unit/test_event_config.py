"""
Unit tests for centralized Event Configuration (EVENT_CONFIG) and schema registry.
"""

import os
import sys
import unittest

DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags"))
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

from event_config import EVENT_CONFIG
from schema import schema as SCHEMA_REGISTRY


class TestEventConfig(unittest.TestCase):
    """Validates metadata completeness and structural integrity of EVENT_CONFIG."""

    EXPECTED_EVENTS = {"watch_events", "ad_events", "page_view_events", "auth_events"}
    REQUIRED_METADATA_KEYS = {
        "event_name",
        "description",
        "source_format",
        "gcs_path_template",
        "staging_table",
        "schema",
        "sql_template",
        "partition_field",
        "partition_type",
    }

    def test_all_expected_event_streams_present(self):
        """Verify that all canonical event streams exist in EVENT_CONFIG."""
        self.assertEqual(
            set(EVENT_CONFIG.keys()),
            self.EXPECTED_EVENTS,
            f"EVENT_CONFIG streams do not match expected set: {self.EXPECTED_EVENTS}",
        )

    def test_required_metadata_keys_exist_for_every_stream(self):
        """Verify that each stream defines all mandatory configuration attributes."""
        for event_name, config in EVENT_CONFIG.items():
            with self.subTest(event=event_name):
                missing_keys = self.REQUIRED_METADATA_KEYS - set(config.keys())
                self.assertFalse(
                    missing_keys,
                    f"Stream '{event_name}' is missing required metadata keys: {missing_keys}",
                )

    def test_sql_template_files_exist_on_disk(self):
        """Verify that SQL template files referenced in EVENT_CONFIG physically exist."""
        for event_name, config in EVENT_CONFIG.items():
            with self.subTest(event=event_name):
                sql_path = os.path.join(DAGS_DIR, config["sql_template"])
                self.assertTrue(
                    os.path.isfile(sql_path),
                    f"SQL template file for '{event_name}' not found at: {sql_path}",
                )

    def test_schema_field_definitions_are_valid(self):
        """Verify schema fields are non-empty lists with name, type, and mode."""
        for event_name, config in EVENT_CONFIG.items():
            with self.subTest(event=event_name):
                fields = config["schema"]
                self.assertIsInstance(fields, list)
                self.assertGreater(len(fields), 0, f"Schema for '{event_name}' is empty")
                for field in fields:
                    self.assertIn("name", field)
                    self.assertIn("type", field)
                    self.assertIn("mode", field)

    def test_partition_field_exists_in_schema(self):
        """Verify partition_field declared in config is an actual field in the schema."""
        for event_name, config in EVENT_CONFIG.items():
            with self.subTest(event=event_name):
                field_names = [f["name"] for f in config["schema"]]
                partition_field = config["partition_field"]
                self.assertIn(
                    partition_field,
                    field_names,
                    f"Partition field '{partition_field}' not found in schema for '{event_name}'",
                )

    def test_schema_registry_matches_event_config(self):
        """Verify that schema.py registry is in sync with EVENT_CONFIG."""
        for event_name in self.EXPECTED_EVENTS:
            with self.subTest(event=event_name):
                self.assertIn(event_name, SCHEMA_REGISTRY)
                self.assertEqual(EVENT_CONFIG[event_name]["schema"], SCHEMA_REGISTRY[event_name])


if __name__ == "__main__":
    unittest.main()

