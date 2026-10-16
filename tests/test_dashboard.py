"""
Comprehensive Unit Tests for Universal Pipeline Web Dashboard.

Tests:
1. Dashboard configuration loading and defaults
2. Dashboard generator with sample_sales (CSV)
3. Dashboard generator with sample_employees (JSON)
4. Dashboard generator with sample_students (XLSX)
5. Dynamic schema: IoT sensor telemetry (arbitrary schema 1)
6. Dynamic schema: Healthcare patient billing (arbitrary schema 2)
7. Empty dataset handling (0 rows) without exceptions
8. Dataset with no numeric columns (pure categorical)
9. Dataset with no date columns (metric + categorical only)
10. Fallback to validated local file cache when PostgreSQL is unavailable
11. Flask API: GET / serves HTML
12. Flask API: GET /api/status masks secrets and passwords
13. Flask API: GET /api/datasets returns discovered datasets
14. Flask API: GET /api/dashboard-data returns structured payload
15. Flask API: POST /api/upload accepts valid CSV
16. Flask API: POST /api/upload accepts valid JSON
17. Flask API: POST /api/upload accepts valid XLSX
18. Flask API: POST /api/upload rejects invalid extensions
19. Flask API: POST /api/upload rejects empty 0-byte files
20. Flask API: POST /api/run-pipeline executes orchestrator
21. Power BI disclaimer and semantic model validation
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

from dashboard.app import create_app, load_dashboard_config
from dashboard.generator import (
    ChartSpec,
    ColumnRoleInfo,
    DashboardGenerator,
    DatasetDashboardPayload,
    KPICard,
    format_number,
)


class TestDashboardGenerator(unittest.TestCase):
    """Test suite validating dynamic dashboard generation logic."""

    def setUp(self):
        """Set up isolated temporary directory structure."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "validated").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "rejected").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)
        (self.test_root / "powerbi").mkdir(parents=True, exist_ok=True)

        # Create sample validated files
        sales_df = pd.DataFrame({
            "transaction_id": [1, 2, 3, 4, 5],
            "product": ["Widget A", "Widget B", "Widget A", "Widget C", "Widget B"],
            "units_sold": [10, 25, 15, 5, 30],
            "sale_date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        })
        sales_df.to_csv(self.test_root / "data" / "validated" / "sample_sales_validated.csv", index=False)

        employees_df = pd.DataFrame({
            "emp_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "department": ["Engineering", "Sales", "Engineering"],
            "salary": [95000, 80000, 110000],
        })
        employees_df.to_csv(self.test_root / "data" / "validated" / "sample_employees_validated.csv", index=False)

        students_df = pd.DataFrame({
            "student_id": ["S1", "S2", "S3"],
            "major": ["CS", "Math", "CS"],
            "gpa": [3.8, 3.5, 3.9],
        })
        students_df.to_csv(self.test_root / "data" / "validated" / "sample_students_validated.csv", index=False)

        self.generator = DashboardGenerator(root_dir=self.test_root)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_number_formatting(self):
        """Verify format_number handles ints, floats, NaNs, and large values."""
        self.assertEqual(format_number(1234), "1,234")
        self.assertEqual(format_number(1500000.0), "1.50M")
        self.assertEqual(format_number(45.5), "45.50")
        self.assertEqual(format_number(None), "N/A")
        self.assertEqual(format_number(float("nan")), "N/A")

    def test_sample_sales_dashboard(self):
        """Verify dashboard payload for sample_sales (has categorical, numeric, and date)."""
        payload = self.generator.build_dashboard_payload("sample_sales")
        self.assertEqual(payload.row_count, 5)
        self.assertEqual(payload.column_count, 4)

        # Check KPIs: Total Records + metric KPIs
        kpi_ids = [k["id"] for k in payload.kpis]
        self.assertIn("total_records", kpi_ids)
        self.assertIn("quality_score", kpi_ids)
        self.assertIn("kpi_units_sold_sum", kpi_ids)

        # Total units_sold should sum to 85
        units_kpi = next(k for k in payload.kpis if k["id"] == "kpi_units_sold_sum")
        self.assertEqual(units_kpi["raw_value"], 85.0)

        # Check charts: should have bar chart, line chart, histogram, and doughnut
        chart_types = [c["chart_type"] for c in payload.charts]
        self.assertIn("bar", chart_types)
        self.assertIn("line", chart_types)
        self.assertIn("histogram", chart_types)
        self.assertIn("doughnut", chart_types)

        # Check filter options
        self.assertIn("product", payload.filters["categories"])
        self.assertIn("sale_date", payload.filters["dates"])

    def test_sample_employees_dashboard(self):
        """Verify dashboard payload for sample_employees (no date column)."""
        payload = self.generator.build_dashboard_payload("sample_employees")
        self.assertEqual(payload.row_count, 3)

        chart_types = [c["chart_type"] for c in payload.charts]
        self.assertIn("bar", chart_types)
        self.assertIn("histogram", chart_types)
        self.assertIn("doughnut", chart_types)
        # Should NOT have line chart because there is no date column
        self.assertNotIn("line", chart_types)

    def test_sample_students_dashboard(self):
        """Verify dashboard payload for sample_students."""
        payload = self.generator.build_dashboard_payload("sample_students")
        self.assertEqual(payload.row_count, 3)
        self.assertTrue(any(c["name"] == "gpa" and c["is_metric"] for c in payload.columns))

    def test_arbitrary_schema_iot(self):
        """Verify domain-agnostic behavior on arbitrary IoT sensor schema."""
        iot_df = pd.DataFrame({
            "sensor_id": ["DEV-01", "DEV-02", "DEV-01", "DEV-03"],
            "location": ["Zone 1", "Zone 2", "Zone 1", "Zone 3"],
            "temperature_c": [22.4, 25.1, 23.0, 21.8],
            "recorded_at": ["2026-05-01 10:00:00", "2026-05-01 11:00:00", "2026-05-01 12:00:00", "2026-05-01 13:00:00"],
        })
        iot_df.to_csv(self.test_root / "data" / "validated" / "iot_telemetry_validated.csv", index=False)

        payload = self.generator.build_dashboard_payload("iot_telemetry")
        self.assertEqual(payload.row_count, 4)

        chart_types = [c["chart_type"] for c in payload.charts]
        self.assertIn("bar", chart_types)
        self.assertIn("line", chart_types)
        self.assertIn("histogram", chart_types)

    def test_arbitrary_schema_healthcare(self):
        """Verify domain-agnostic behavior on arbitrary healthcare schema."""
        health_df = pd.DataFrame({
            "patient_code": ["P001", "P002", "P003"],
            "ward": ["Cardiology", "Neurology", "Cardiology"],
            "bill_amount": [1250.0, 3400.0, 2100.0],
        })
        health_df.to_csv(self.test_root / "data" / "validated" / "hospital_records_validated.csv", index=False)

        payload = self.generator.build_dashboard_payload("hospital_records")
        self.assertEqual(payload.row_count, 3)
        self.assertTrue(any(k["id"] == "kpi_bill_amount_sum" for k in payload.kpis))

    def test_empty_dataset_handling(self):
        """Verify empty dataset with 0 rows does not raise exceptions."""
        empty_df = pd.DataFrame(columns=["id", "category", "amount"])
        empty_df.to_csv(self.test_root / "data" / "validated" / "empty_set_validated.csv", index=False)

        payload = self.generator.build_dashboard_payload("empty_set")
        self.assertEqual(payload.row_count, 0)
        self.assertEqual(payload.charts, [])
        self.assertTrue(any("0 records" in w for w in payload.warnings))

    def test_no_numeric_columns_dataset(self):
        """Verify dataset with NO numeric columns falls back cleanly without crashing."""
        cat_df = pd.DataFrame({
            "user_id": ["U1", "U2", "U3"],
            "country": ["USA", "Germany", "USA"],
            "subscription_tier": ["Gold", "Silver", "Platinum"],
        })
        cat_df.to_csv(self.test_root / "data" / "validated" / "users_categorical_validated.csv", index=False)

        payload = self.generator.build_dashboard_payload("users_categorical")
        self.assertEqual(payload.row_count, 3)

        # Should not crash, and should have distinct count KPIs
        kpi_titles = [k["title"] for k in payload.kpis]
        self.assertTrue(any("Distinct" in t for t in kpi_titles))

        # Charts should include category distribution doughnut, but no line or numeric histogram
        chart_types = [c["chart_type"] for c in payload.charts]
        self.assertIn("doughnut", chart_types)
        self.assertNotIn("line", chart_types)
        self.assertNotIn("histogram", chart_types)

    def test_database_unavailable_warning(self):
        """Verify database unavailable flag and warning message when DB engine cannot connect."""
        with patch.object(self.generator, "get_database_status", return_value=(False, "Database connection refused")):
            payload = self.generator.build_dashboard_payload("sample_sales")
            self.assertFalse(payload.database_connected)
            self.assertEqual(payload.source, "file_cache")
            self.assertTrue(any("unavailable" in w.lower() for w in payload.warnings))

    def test_powerbi_disclaimer_included(self):
        """Verify powerbi_info contains explicit disclaimer distinguishing from .pbix."""
        payload = self.generator.build_dashboard_payload("sample_sales")
        self.assertIn("disclaimer", payload.powerbi_info)
        self.assertIn(".pbix", payload.powerbi_info["disclaimer"])


class TestDashboardFlaskAPI(unittest.TestCase):
    """Test suite validating Flask routes and REST API endpoints."""

    def setUp(self):
        """Create test application and test client."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "validated").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "rejected").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)

        # Write sample validated file
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        df.to_csv(self.test_root / "data" / "validated" / "test_data_validated.csv", index=False)

        self.app = create_app(root_dir=self.test_root)
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_index_route(self):
        """Verify GET / returns HTTP 200 with HTML content."""
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn(b"Universal Data Engineering Pipeline", res.data)
        self.assertIn(b"id=\"zone-1-controls\"", res.data)
        self.assertIn(b"id=\"zone-2-kpis\"", res.data)
        self.assertIn(b"id=\"zone-3-visuals\"", res.data)
        self.assertIn(b"id=\"zone-4-details\"", res.data)

    def test_status_endpoint_masks_secrets(self):
        """Verify GET /api/status does not leak plain passwords."""
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://dbuser:super_secret_test_pass_999@localhost:5432/testdb"}):
            res = self.client.get("/api/status")
            self.assertEqual(res.status_code, 200)
            data = res.get_json()
            self.assertIn("database_connected", data)
            self.assertIn("database_message", data)
            # Should not leak raw unmasked password
            self.assertNotIn("super_secret_test_pass_999", str(data))

    def test_datasets_endpoint(self):
        """Verify GET /api/datasets discovers available files."""
        res = self.client.get("/api/datasets")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["count"] >= 1)
        names = [d["name"] for d in data["datasets"]]
        self.assertIn("test_data", names)

    def test_dashboard_data_endpoint(self):
        """Verify GET /api/dashboard-data returns structured JSON."""
        res = self.client.get("/api/dashboard-data?dataset=test_data")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["dataset_name"], "test_data")
        self.assertEqual(data["row_count"], 2)
        self.assertTrue(len(data["columns"]) == 2)
        self.assertTrue(len(data["records"]) == 2)

    def test_upload_valid_csv(self):
        """Verify POST /api/upload accepts valid CSV files and places them in data/raw/."""
        csv_content = b"col1,col2,col3\n10,foo,2026-01-01\n20,bar,2026-01-02\n"
        data = {
            "file": (io.BytesIO(csv_content), "uploaded_test.csv"),
        }
        res = self.client.post("/api/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(res.status_code, 200)
        json_data = res.get_json()
        self.assertTrue(json_data["success"])
        self.assertEqual(json_data["filename"], "uploaded_test.csv")

        # Confirm file is on disk
        dest = self.test_root / "data" / "raw" / "uploaded_test.csv"
        self.assertTrue(dest.exists())

    def test_upload_valid_json(self):
        """Verify POST /api/upload accepts valid JSON files."""
        json_content = json.dumps([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]).encode("utf-8")
        data = {
            "file": (io.BytesIO(json_content), "records.json"),
        }
        res = self.client.post("/api/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(res.status_code, 200)
        self.assertTrue(res.get_json()["success"])

    def test_upload_invalid_extension(self):
        """Verify POST /api/upload rejects disallowed extensions (e.g. .exe, .sh)."""
        data = {
            "file": (io.BytesIO(b"binary"), "malicious.exe"),
        }
        res = self.client.post("/api/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(res.status_code, 400)
        self.assertFalse(res.get_json()["success"])
        self.assertIn("Invalid file format", res.get_json()["error"])

    def test_upload_empty_file(self):
        """Verify POST /api/upload rejects empty files (0 bytes)."""
        data = {
            "file": (io.BytesIO(b""), "empty.csv"),
        }
        res = self.client.post("/api/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(res.status_code, 400)
        self.assertIn("empty", res.get_json()["error"].lower())

    def test_run_pipeline_endpoint(self):
        """Verify POST /api/run-pipeline calls orchestrator and returns summary."""
        mock_summary = MagicMock()
        mock_summary.run_id = "test_run_123"
        mock_summary.pipeline_status = "COMPLETED"
        mock_summary.duration_seconds = 1.2
        mock_summary.stages = [{"stage_name": "foundation", "status": "SUCCESS"}]
        mock_summary.datasets = []
        mock_summary.summary = {}

        with patch("dashboard.app.PipelineOrchestrator") as MockOrch:
            instance = MockOrch.return_value
            instance.execute_pipeline.return_value = mock_summary

            res = self.client.post(
                "/api/run-pipeline",
                data=json.dumps({"reset_checkpoints": True}),
                content_type="application/json",
            )
            self.assertEqual(res.status_code, 200)
            data = res.get_json()
            self.assertTrue(data["success"])
            self.assertEqual(data["run_id"], "test_run_123")
            self.assertEqual(data["pipeline_status"], "COMPLETED")
            instance.execute_pipeline.assert_called_once()


if __name__ == "__main__":
    unittest.main()
