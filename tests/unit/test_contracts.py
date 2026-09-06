"""
Unit tests for Data Contract validation (scripts/validate_contracts.py).
"""

import glob
import os
import sys
import unittest

SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from validate_contracts import compare_contract_to_schema, load_contract, validate_contract_structure

CONTRACTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "contracts"))


class TestContracts(unittest.TestCase):
    """Validates contract structure and schema synchronization."""

    def test_all_contract_files_exist(self):
        """Verify presence of all 4 expected data contract YAML files."""
        expected = {"watch_events.yml", "ad_events.yml", "page_view_events.yml", "auth_events.yml"}
        found = {os.path.basename(p) for p in glob.glob(os.path.join(CONTRACTS_DIR, "*.yml"))}
        self.assertEqual(expected, found)

    def test_contract_structures_valid(self):
        """Verify all contract files conform to mandatory structure requirements."""
        for c_path in glob.glob(os.path.join(CONTRACTS_DIR, "*.yml")):
            filename = os.path.basename(c_path)
            with self.subTest(contract=filename):
                contract = load_contract(c_path)
                errors = validate_contract_structure(contract, filename)
                self.assertEqual(errors, [], f"Contract {filename} has structural errors: {errors}")

    def test_compare_contract_detects_mismatch(self):
        """Verify contract comparator flags missing and type-mismatched fields."""
        contract_cols = [{"name": "colA", "type": "STRING", "mode": "NULLABLE"}]
        schema_cols = [{"name": "colB", "type": "INT64", "mode": "NULLABLE"}]
        errors = compare_contract_to_schema("test_ds", contract_cols, schema_cols)
        self.assertTrue(len(errors) > 0)


if __name__ == "__main__":
    unittest.main()

