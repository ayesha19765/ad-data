#!/usr/bin/env python3
"""
Adaptive Ads Data Engineering Platform - Documentation Integrity Validator

Verifies that all documentation markdown files indexed in README.md physically exist,
contain non-empty content, and have valid relative cross-references.

Usage:
    python3 scripts/validate_docs.py
"""

import glob
import os
import re
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")
README_PATH = os.path.join(PROJECT_ROOT, "README.md")


def validate_doc_files() -> int:
    """Audit all documentation markdown files."""
    print("=" * 70)
    print("      ADAPTIVE ADS - DOCUMENTATION INTEGRITY VALIDATOR")
    print("=" * 70)

    doc_files = glob.glob(os.path.join(DOCS_DIR, "*.md"))
    print(f"Discovered {len(doc_files)} documentation markdown files in docs/")

    errors = 0
    for doc in sorted(doc_files):
        basename = os.path.basename(doc)
        with open(doc, "r", encoding="utf-8") as fp:
            content = fp.read().strip()
            if len(content) == 0:
                print(f"  ✗ Empty document: docs/{basename}")
                errors += 1
            else:
                lines = content.split("\n")
                first_header = lines[0] if lines else "No Header"
                print(f"  ✓ docs/{basename:<28} ({len(lines):>4} lines) - {first_header[:35]}")

    # Check README links pointing to docs/
    if os.path.isfile(README_PATH):
        with open(README_PATH, "r", encoding="utf-8") as fp:
            readme_text = fp.read()
            doc_links = re.findall(r"\]\((docs/[a-zA-Z0-9_\.]+\.md)\)", readme_text)
            print(f"\nChecking {len(doc_links)} documentation links in README.md...")
            for link in sorted(set(doc_links)):
                target_path = os.path.join(PROJECT_ROOT, link)
                if not os.path.isfile(target_path):
                    print(f"  ✗ Broken link in README: {link}")
                    errors += 1
                else:
                    print(f"  ✓ Valid link: {link}")

    print("\n" + "=" * 70)
    if errors == 0:
        print(f"  ALL {len(doc_files)} DOCUMENTATION FILES & LINKS ARE VALID")
        print("=" * 70 + "\n")
        return 0
    else:
        print(f"  DOCUMENTATION AUDIT FAILED WITH {errors} ERROR(S)")
        print("=" * 70 + "\n")
        return 1


if __name__ == "__main__":
    sys.exit(validate_doc_files())

