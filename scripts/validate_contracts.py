#!/usr/bin/env python3
"""
Adaptive Ads Data Engineering Platform - Data Contract Validation Utility

Validates that declarative contract definitions in contracts/ align with
canonical schemas in airflow/dags/schema.py and Airflow EVENT_CONFIG.

Usage:
    python3 scripts/validate_contracts.py
    python3 scripts/validate_contracts.py --strict
"""

import argparse
import glob
import os
import sys
from typing import Any, Dict, List

import yaml

DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

try:
    from schema import schema as SCHEMA_REGISTRY
except ImportError:
    SCHEMA_REGISTRY = {}

CONTRACTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "contracts"))


def load_contract(file_path: str) -> Dict[str, Any]:
    """Load and parse YAML data contract file."""
    with open(file_path, "r") as fp:
        return yaml.safe_load(fp)


def validate_contract_structure(contract: Dict[str, Any], filename: str) -> List[str]:
    """Validate mandatory fields in a data contract."""
    errors = []
    required_keys = ["version", "dataset", "owner", "target_table", "partition", "columns"]
    for key in required_keys:
        if key not in contract:
            errors.append(f"Contract {filename} missing top-level key: '{key}'")

    if "columns" in contract:
        for idx, col in enumerate(contract["columns"]):
            for attr in ["name", "type", "mode"]:
                if attr not in col:
                    errors.append(f"Contract {filename} column #{idx} missing attribute: '{attr}'")

    return errors


def compare_contract_to_schema(
    dataset_name: str,
    contract_cols: List[Dict[str, Any]],
    schema_cols: List[Dict[str, Any]],
) -> List[str]:
    """Compare contract columns with canonical schema registry."""
    errors = []
    contract_map = {c["name"]: c for c in contract_cols}
    schema_map = {s["name"]: s for s in schema_cols}

    missing_in_schema = set(contract_map.keys()) - set(schema_map.keys())
    if missing_in_schema:
        errors.append(f"Dataset '{dataset_name}': Contract defines fields missing in schema: {missing_in_schema}")

    missing_in_contract = set(schema_map.keys()) - set(contract_map.keys())
    if missing_in_contract:
        errors.append(f"Dataset '{dataset_name}': Schema defines fields missing in contract: {missing_in_contract}")

    for name in set(contract_map.keys()).intersection(set(schema_map.keys())):
        c_type = contract_map[name]["type"].upper()
        s_type = schema_map[name]["type"].upper()
        if c_type != s_type:
            errors.append(
                f"Dataset '{dataset_name}' column '{name}' type mismatch: contract={c_type} vs schema={s_type}"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Contract validator for Adaptive Ads.")
    parser.add_argument("--strict", action="store_true", help="Exit with non-zero code on any contract discrepancy")
    args = parser.parse_args()

    contract_files = glob.glob(os.path.join(CONTRACTS_DIR, "*.yml"))
    if not contract_files:
        print(f"Error: No contract YAML files found in {CONTRACTS_DIR}", file=sys.stderr)
        return 1

    print("=" * 70)
    print("         ADAPTIVE ADS - DATA CONTRACT COMPLIANCE AUDIT")
    print("=" * 70)

    total_errors = 0
    print(f"Scanning {len(contract_files)} data contract(s)...")

    for c_file in contract_files:
        basename = os.path.basename(c_file)
        try:
            contract = load_contract(c_file)
            dataset = contract.get("dataset", "")
            print(f"\n[CONTRACT] Auditing {basename} (dataset: '{dataset}', owner: '{contract.get('owner')}')")

            struct_errors = validate_contract_structure(contract, basename)
            if struct_errors:
                for err in struct_errors:
                    print(f"  ✗ Structure Error: {err}")
                total_errors += len(struct_errors)
                continue

            if dataset not in SCHEMA_REGISTRY:
                print(f"  ✗ Dataset '{dataset}' not registered in schema.py")
                total_errors += 1
                continue

            schema_cols = SCHEMA_REGISTRY[dataset]
            comp_errors = compare_contract_to_schema(dataset, contract["columns"], schema_cols)
            if comp_errors:
                for err in comp_errors:
                    print(f"  ✗ Compliance Drift: {err}")
                total_errors += len(comp_errors)
            else:
                print(f"  ✓ Contract complies with schema ({len(contract['columns'])} fields verified).")

        except Exception as e:
            print(f"  ✗ Error reading contract {basename}: {e}")
            total_errors += 1

    print("\n" + "=" * 70)
    if total_errors == 0:
        print(f"  ALL {len(contract_files)} DATA CONTRACTS ARE COMPLIANT & ENFORCED")
        print("=" * 70 + "\n")
        return 0
    else:
        print(f"  CONTRACT AUDIT ENCOUNTERED {total_errors} VIOLATION(S)")
        print("=" * 70 + "\n")
        return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())

