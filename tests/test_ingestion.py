"""
Unit tests for Universal Data Ingestion & File Detection (Stage 2).
"""

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from pipeline.ingestion import (
    detect_file_type,
    discover_input_files,
    format_file_size,
    generate_ingestion_report,
    load_csv,
    load_dataset,
    load_excel,
    load_json,
    log_ingestion_event,
)


class TestIngestionModule(unittest.TestCase):
    """Test suite covering format detection, loading, error resilience, and reporting."""

    def setUp(self):
        """Create a temporary directory for isolated test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary test fixtures."""
        self.temp_dir.cleanup()

    # 1. Format Detection Tests
    def test_01_detect_csv(self):
        """Test CSV file format detection."""
        self.assertEqual(detect_file_type(self.test_path / "data.csv"), "CSV")
        self.assertEqual(detect_file_type("UPPERCASE.CSV"), "CSV")

    def test_02_detect_excel(self):
        """Test Excel file format detection."""
        self.assertEqual(detect_file_type(self.test_path / "sheet.xlsx"), "EXCEL")
        self.assertEqual(detect_file_type("REPORT.XLSX"), "EXCEL")

    def test_03_detect_json(self):
        """Test JSON file format detection."""
        self.assertEqual(detect_file_type(self.test_path / "records.json"), "JSON")
        self.assertEqual(detect_file_type("payload.JSON"), "JSON")

    def test_04_detect_unsupported(self):
        """Test detection of unsupported file formats."""
        self.assertEqual(detect_file_type(self.test_path / "document.pdf"), "UNSUPPORTED")
        self.assertEqual(detect_file_type(self.test_path / "notes.txt"), "UNSUPPORTED")
        self.assertEqual(detect_file_type(self.test_path / "archive.zip"), "UNSUPPORTED")

    # 2. File Loading Tests
    def test_05_load_csv_valid(self):
        """Test loading a valid structured CSV file."""
        csv_file = self.test_path / "valid.csv"
        csv_file.write_text("id,name,value\n1,Alpha,10.5\n2,Beta,20.0\n", encoding="utf-8")

        df, err = load_csv(csv_file)
        self.assertIsNone(err)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ["id", "name", "value"])

    def test_06_load_json_valid_list_of_dicts(self):
        """Test loading a valid JSON list of record objects."""
        json_file = self.test_path / "records.json"
        data = [
            {"emp_id": 1, "dept": "IT"},
            {"emp_id": 2, "dept": "HR"},
        ]
        json_file.write_text(json.dumps(data), encoding="utf-8")

        df, err = load_json(json_file)
        self.assertIsNone(err)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ["emp_id", "dept"])

    def test_07_load_excel_valid(self):
        """Test loading a valid Excel worksheet."""
        excel_file = self.test_path / "test.xlsx"
        original_df = pd.DataFrame({
            "code": ["A1", "B2"],
            "score": [95, 88],
        })
        original_df.to_excel(excel_file, index=False, engine="openpyxl")

        df, err = load_excel(excel_file)
        self.assertIsNone(err)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ["code", "score"])

    # 3. Edge Case & Error Handling Tests
    def test_08_empty_files_handling(self):
        """Test empty file detection across formats."""
        empty_csv = self.test_path / "empty.csv"
        empty_csv.write_text("", encoding="utf-8")
        df_csv, err_csv = load_csv(empty_csv)
        self.assertIsNone(df_csv)
        self.assertIn("empty", err_csv.lower())

        empty_json = self.test_path / "empty.json"
        empty_json.write_text("", encoding="utf-8")
        df_json, err_json = load_json(empty_json)
        self.assertIsNone(df_json)
        self.assertIn("empty", err_json.lower())

    def test_09_invalid_json_handling(self):
        """Test invalid JSON syntax and non-tabular JSON handling."""
        malformed_json = self.test_path / "malformed.json"
        malformed_json.write_text("{'broken_json': 123,", encoding="utf-8")
        df_bad, err_bad = load_json(malformed_json)
        self.assertIsNone(df_bad)
        self.assertIn("syntax", err_bad.lower())

        scalar_list_json = self.test_path / "scalars.json"
        scalar_list_json.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
        df_scalar, err_scalar = load_json(scalar_list_json)
        self.assertIsNone(df_scalar)
        self.assertIn("unsupported", err_scalar.lower())

    # 4. File Discovery & Orchestration Tests
    def test_10_automatic_file_discovery(self):
        """Test automatic directory discovery excluding hidden and .gitkeep files."""
        (self.test_path / "file1.csv").write_text("a,b\n1,2", encoding="utf-8")
        (self.test_path / "file2.json").write_text('[{"a": 1}]', encoding="utf-8")
        (self.test_path / ".gitkeep").write_text("", encoding="utf-8")
        (self.test_path / ".hidden.csv").write_text("a\n1", encoding="utf-8")

        discovered = discover_input_files(self.test_path)
        discovered_names = [f.name for f in discovered]

        self.assertIn("file1.csv", discovered_names)
        self.assertIn("file2.json", discovered_names)
        self.assertNotIn(".gitkeep", discovered_names)
        self.assertNotIn(".hidden.csv", discovered_names)

    def test_11_dataset_orchestration_unsupported_file(self):
        """Test that unsupported files are marked SKIPPED without failing execution."""
        pdf_file = self.test_path / "manual.pdf"
        pdf_file.write_text("%PDF-1.4 dummy binary content", encoding="utf-8")

        result = load_dataset(pdf_file)
        self.assertEqual(result.status, "SKIPPED")
        self.assertEqual(result.file_type, "UNSUPPORTED")
        self.assertIn("Unsupported", result.error_message)

    def test_12_dataset_orchestration_resilience(self):
        """Test that bad files do not stop or corrupt valid files."""
        valid_csv = self.test_path / "good.csv"
        valid_csv.write_text("x,y\n10,20\n", encoding="utf-8")

        bad_json = self.test_path / "corrupt.json"
        bad_json.write_text("{not a json", encoding="utf-8")

        res_good = load_dataset(valid_csv)
        res_bad = load_dataset(bad_json)

        self.assertEqual(res_good.status, "SUCCESS")
        self.assertEqual(res_bad.status, "FAILED")

        # Test report generation includes both
        report = generate_ingestion_report([res_good, res_bad])
        self.assertIn("good.csv", report)
        self.assertIn("corrupt.json", report)
        self.assertIn("Successfully loaded: 1", report)
        self.assertIn("Failed: 1", report)

    def test_13_logging_generation(self):
        """Test that ingestion outcomes are cleanly logged to disk."""
        log_dir = self.test_path / "logs"
        csv_file = self.test_path / "logged.csv"
        csv_file.write_text("col1\nval1\n", encoding="utf-8")

        res = load_dataset(csv_file)
        log_ingestion_event(res, log_dir=log_dir)

        log_file = log_dir / "ingestion.log"
        self.assertTrue(log_file.exists())
        log_content = log_file.read_text(encoding="utf-8")
        self.assertIn("logged.csv", log_content)
        self.assertIn("SUCCESS", log_content)


if __name__ == "__main__":
    unittest.main()
