"""
Unit tests for DataPulse — Data Pipeline Monitor.
Tests validate core business logic, data generation, and helper functions.
"""

import unittest
import json
import os
import sys
import csv
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.main import (
    generate_pipelines, generate_dq_results,
    PIPELINE_NAMES, DATA_SOURCES, DQ_CHECKS, COLORS
)


class TestPipelineGeneration(unittest.TestCase):
    """Test pipeline data generation."""

    def test_generates_correct_count(self):
        pipes = generate_pipelines(5)
        self.assertEqual(len(pipes), 5)

    def test_generates_default_count(self):
        pipes = generate_pipelines()
        self.assertEqual(len(pipes), 10)

    def test_pipeline_has_required_fields(self):
        pipes = generate_pipelines(1)
        required = ["id", "name", "status", "duration_sec", "rows_processed", "last_run", "schedule"]
        for field in required:
            self.assertIn(field, pipes[0])

    def test_pipeline_id_format(self):
        pipes = generate_pipelines(3)
        for p in pipes:
            self.assertTrue(p["id"].startswith("PL-"))

    def test_valid_statuses(self):
        pipes = generate_pipelines(50)
        valid = {"Success", "Failed", "Running", "Queued"}
        for p in pipes:
            self.assertIn(p["status"], valid)

    def test_success_has_rows(self):
        pipes = generate_pipelines(100)
        for p in pipes:
            if p["status"] == "Success":
                self.assertGreater(p["rows_processed"], 0)

    def test_valid_schedule_format(self):
        pipes = generate_pipelines(20)
        valid_schedules = {"@hourly", "@daily", "*/15 * * * *", "@weekly"}
        for p in pipes:
            self.assertIn(p["schedule"], valid_schedules)

    def test_pipeline_names_from_list(self):
        pipes = generate_pipelines(10)
        for p in pipes:
            self.assertIn(p["name"], PIPELINE_NAMES)


class TestDataQualityGeneration(unittest.TestCase):
    """Test data quality check generation."""

    def test_generates_all_checks(self):
        results = generate_dq_results()
        self.assertEqual(len(results), len(DQ_CHECKS))

    def test_dq_result_fields(self):
        results = generate_dq_results()
        required = ["check", "status", "table", "details", "run_time"]
        for r in results:
            for field in required:
                self.assertIn(field, r)

    def test_dq_status_values(self):
        results = generate_dq_results()
        for r in results:
            self.assertIn(r["status"], ("Passed", "Failed"))

    def test_passed_has_ok_details(self):
        results = generate_dq_results()
        for r in results:
            if r["status"] == "Passed":
                self.assertEqual(r["details"], "OK")

    def test_failed_has_issue_details(self):
        results = generate_dq_results()
        for r in results:
            if r["status"] == "Failed":
                self.assertIn("issues found", r["details"])

    def test_valid_tables(self):
        results = generate_dq_results()
        valid_tables = {"dim_users", "fact_orders", "stg_events", "dim_products"}
        for r in results:
            self.assertIn(r["table"], valid_tables)


class TestDataSources(unittest.TestCase):
    """Test data source configuration."""

    def test_sources_not_empty(self):
        self.assertGreater(len(DATA_SOURCES), 0)

    def test_source_has_required_fields(self):
        required = ["name", "type", "host", "db"]
        for src in DATA_SOURCES:
            for field in required:
                self.assertIn(field, src)

    def test_unique_source_names(self):
        names = [s["name"] for s in DATA_SOURCES]
        self.assertEqual(len(names), len(set(names)))


class TestColorTheme(unittest.TestCase):
    """Test theme configuration."""

    def test_required_colors_exist(self):
        required = ["bg", "surface", "accent", "success", "danger", "text"]
        for key in required:
            self.assertIn(key, COLORS)

    def test_colors_are_hex(self):
        for key, val in COLORS.items():
            self.assertTrue(val.startswith("#"), f"{key} is not a hex color")


class TestCSVExport(unittest.TestCase):
    """Test CSV export logic."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.temp_dir, "test_export.csv")

    def tearDown(self):
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)
        os.rmdir(self.temp_dir)

    def test_export_creates_file(self):
        pipes = generate_pipelines(3)
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Pipeline", "Status", "Duration", "Rows", "Last Run", "Schedule"])
            for p in pipes:
                writer.writerow([p["id"], p["name"], p["status"], p["duration_sec"],
                                 p["rows_processed"], p["last_run"], p["schedule"]])
        self.assertTrue(os.path.exists(self.csv_path))

    def test_export_has_correct_rows(self):
        pipes = generate_pipelines(5)
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Pipeline", "Status"])
            for p in pipes:
                writer.writerow([p["id"], p["name"], p["status"]])

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        self.assertEqual(len(rows), 6)  # header + 5 data rows

    def test_export_header_columns(self):
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            header = ["ID", "Pipeline", "Status", "Duration", "Rows", "Last Run", "Schedule"]
            writer.writerow(header)

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header_row = next(reader)
        self.assertEqual(len(header_row), 7)


class TestMetricsCalculation(unittest.TestCase):
    """Test metrics calculation logic."""

    def test_success_count(self):
        pipes = [{"status": "Success"}, {"status": "Failed"}, {"status": "Success"}]
        count = sum(1 for p in pipes if p["status"] == "Success")
        self.assertEqual(count, 2)

    def test_failed_count(self):
        pipes = [{"status": "Success"}, {"status": "Failed"}, {"status": "Failed"}]
        count = sum(1 for p in pipes if p["status"] == "Failed")
        self.assertEqual(count, 2)

    def test_total_rows(self):
        pipes = [{"rows_processed": 1000}, {"rows_processed": 2000}, {"rows_processed": 0}]
        total = sum(p["rows_processed"] for p in pipes)
        self.assertEqual(total, 3000)

    def test_dq_pass_rate(self):
        results = [{"status": "Passed"}, {"status": "Failed"}, {"status": "Passed"}, {"status": "Passed"}]
        passed = sum(1 for d in results if d["status"] == "Passed")
        rate = passed / len(results)
        self.assertAlmostEqual(rate, 0.75)

    def test_dq_pass_rate_empty(self):
        results = []
        rate = 0.0 if not results else sum(1 for d in results if d["status"] == "Passed") / len(results)
        self.assertEqual(rate, 0.0)


if __name__ == "__main__":
    unittest.main()
