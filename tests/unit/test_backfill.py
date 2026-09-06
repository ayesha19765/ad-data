"""
Unit tests for Backfill Utility (scripts/backfill.py).
"""

import argparse
import os
import sys
import unittest
from datetime import datetime

SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from backfill import generate_hourly_intervals, parse_iso_datetime


class TestBackfillUtility(unittest.TestCase):
    """Validates date parsing, interval generation, and boundary logic for backfill script."""

    def test_parse_iso_datetime_valid_formats(self):
        """Verify parsing of various standard ISO date string formats."""
        dt1 = parse_iso_datetime("2026-09-01T14:30:00")
        self.assertEqual(dt1, datetime(2026, 9, 1, 14, 30, 0))

        dt2 = parse_iso_datetime("2026-09-01 14:30:00")
        self.assertEqual(dt2, datetime(2026, 9, 1, 14, 30, 0))

        dt3 = parse_iso_datetime("2026-09-01")
        self.assertEqual(dt3, datetime(2026, 9, 1, 0, 0, 0))

    def test_parse_iso_datetime_invalid_raises(self):
        """Verify invalid datetime strings raise ArgumentTypeError."""
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_iso_datetime("invalid-date-string")

    def test_generate_hourly_intervals_single_hour(self):
        """Verify interval calculation for a 1-hour range."""
        start = datetime(2026, 9, 1, 10, 0, 0)
        end = datetime(2026, 9, 1, 11, 0, 0)
        intervals = generate_hourly_intervals(start, end)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0], (start, end))

    def test_generate_hourly_intervals_multi_hour(self):
        """Verify interval calculation across 24 hours (1 full day)."""
        start = datetime(2026, 9, 1, 0, 0, 0)
        end = datetime(2026, 9, 2, 0, 0, 0)
        intervals = generate_hourly_intervals(start, end)
        self.assertEqual(len(intervals), 24)
        self.assertEqual(intervals[0], (datetime(2026, 9, 1, 0, 0, 0), datetime(2026, 9, 1, 1, 0, 0)))
        self.assertEqual(intervals[-1], (datetime(2026, 9, 1, 23, 0, 0), datetime(2026, 9, 2, 0, 0, 0)))

    def test_generate_hourly_intervals_empty_on_equal_dates(self):
        """Verify zero intervals generated when start equals end."""
        start = datetime(2026, 9, 1, 12, 0, 0)
        intervals = generate_hourly_intervals(start, start)
        self.assertEqual(len(intervals), 0)


if __name__ == "__main__":
    unittest.main()

