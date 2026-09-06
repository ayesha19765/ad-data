"""
Unit tests for Schema Drift Checker (scripts/check_schema.py).
"""

import os
import sys
import unittest

SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from check_schema import compare_schemas


class TestSchemaCheck(unittest.TestCase):
    """Validates schema comparison algorithm across field additions, removals, and mutations."""

    def test_identical_schemas_report_zero_drift(self):
        """Verify identical expected and actual schemas produce no drift."""
        schema_a = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "userId", "type": "INT64", "mode": "NULLABLE"},
        ]
        diff = compare_schemas(schema_a, schema_a)
        self.assertEqual(diff["added"], [])
        self.assertEqual(diff["removed"], [])
        self.assertEqual(diff["type_changes"], [])
        self.assertEqual(diff["mode_changes"], [])

    def test_detects_added_column(self):
        """Verify new incoming columns are flagged as added."""
        expected = [{"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"}]
        actual = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "newField", "type": "STRING", "mode": "NULLABLE"},
        ]
        diff = compare_schemas(expected, actual)
        self.assertEqual(diff["added"], ["newField"])
        self.assertEqual(diff["removed"], [])

    def test_detects_removed_column(self):
        """Verify missing columns in incoming schema are flagged as removed."""
        expected = [
            {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deprecatedField", "type": "STRING", "mode": "NULLABLE"},
        ]
        actual = [{"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"}]
        diff = compare_schemas(expected, actual)
        self.assertEqual(diff["removed"], ["deprecatedField"])
        self.assertEqual(diff["added"], [])

    def test_detects_type_change(self):
        """Verify changes in data type are flagged."""
        expected = [{"name": "userId", "type": "INT64", "mode": "NULLABLE"}]
        actual = [{"name": "userId", "type": "STRING", "mode": "NULLABLE"}]
        diff = compare_schemas(expected, actual)
        self.assertEqual(diff["type_changes"], [("userId", "INT64", "STRING")])

    def test_detects_nullability_change(self):
        """Verify changes in mode (NULLABLE vs REQUIRED) are flagged."""
        expected = [{"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"}]
        actual = [{"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"}]
        diff = compare_schemas(expected, actual)
        self.assertEqual(diff["mode_changes"], [("ts", "REQUIRED", "NULLABLE")])


if __name__ == "__main__":
    unittest.main()

