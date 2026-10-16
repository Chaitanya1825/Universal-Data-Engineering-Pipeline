"""
Unit tests for Universal Data Quality & Validation Engine (Stage 5).
"""

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from pipeline.validator import (
    ValidationConfig,
    load_validation_config,
    save_validation_outputs,
    save_validation_reports,
    validate_dataset,
    validate_logical_types,
    validate_nullability,
    validate_numeric_ranges,
    validate_regex_formats,
    validate_schema,
    validate_uniqueness,
)


class TestValidator(unittest.TestCase):
    """Test suite covering data quality rules, partitioning, metrics, and reports."""

    def setUp(self):
        """Create an isolated temporary directory for test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # 1. Required Column Validation
    def test_01_required_column_validation(self):
        """Test detection of missing required columns at schema level."""
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        cfg = ValidationConfig(required_columns=["col_a", "col_missing"])

        _, errors = validate_schema(df, cfg)
        self.assertEqual(len(errors), 1)
        self.assertIn("col_missing", errors[0])

    # 2. Missing/Null Validation
    def test_02_missing_null_validation(self):
        """Test record-level detection of null values in required columns."""
        df = pd.DataFrame({"id": [1, 2, 3], "status": ["Active", None, "Pending"]})
        cfg = ValidationConfig(required_columns=["status"])

        failures, viol_count = validate_nullability(df, cfg)
        self.assertEqual(viol_count, 1)
        self.assertIn(1, failures)
        self.assertEqual(failures[1][0].rule, "nullability")
        self.assertEqual(failures[1][0].column, "status")

    # 3. Uniqueness Validation
    def test_03_uniqueness_validation(self):
        """Test detection of duplicate records in unique columns."""
        df = pd.DataFrame({"code": ["A1", "B2", "A1", "C3"]})
        cfg = ValidationConfig(unique_columns=["code"])

        failures, viol_count = validate_uniqueness(df, cfg)
        self.assertEqual(viol_count, 1)
        self.assertIn(2, failures)  # Second occurrence of A1 flagged
        self.assertEqual(failures[2][0].rule, "uniqueness")

    # 4. Numeric Minimum Validation
    def test_04_numeric_minimum_validation(self):
        """Test detection of numeric values below configured minimum."""
        df = pd.DataFrame({"amount": [10.5, -3.0, 50.0]})
        cfg = ValidationConfig(range_rules={"amount": {"min": 0.0}})

        failures, viol_count = validate_numeric_ranges(df, cfg)
        self.assertEqual(viol_count, 1)
        self.assertIn(1, failures)
        self.assertEqual(failures[1][0].actual_value, -3.0)

    # 5. Numeric Maximum Validation
    def test_05_numeric_maximum_validation(self):
        """Test detection of numeric values exceeding configured maximum."""
        df = pd.DataFrame({"score": [85, 92, 105, 78]})
        cfg = ValidationConfig(range_rules={"score": {"max": 100}})

        failures, viol_count = validate_numeric_ranges(df, cfg)
        self.assertEqual(viol_count, 1)
        self.assertIn(2, failures)
        self.assertEqual(failures[2][0].actual_value, 105.0)

    # 6. Regex Validation
    def test_06_regex_validation(self):
        """Test format validation using regex pattern matching."""
        df = pd.DataFrame({"code": ["TX1001", "INVALID_CODE", "TX1002"]})
        cfg = ValidationConfig(regex_rules={"code": r"^TX\d+$"})

        failures, viol_count = validate_regex_formats(df, cfg)
        self.assertEqual(viol_count, 1)
        self.assertIn(1, failures)
        self.assertEqual(failures[1][0].column, "code")

    # 7. Logical Type Validation
    def test_07_logical_type_validation(self):
        """Test validation of values against expected logical types."""
        df = pd.DataFrame({
            "num": [10, "not_a_number", 30],
            "flag": [True, "invalid_bool", False],
        })
        cfg = ValidationConfig(type_rules={"num": "Integer", "flag": "Boolean"})

        failures, viol_count = validate_logical_types(df, cfg)
        self.assertEqual(viol_count, 2)
        self.assertIn(1, failures)

    # 8. Valid Record Detection
    def test_08_valid_record_detection(self):
        """Test that conforming rows are preserved in the valid partition."""
        df = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
        cfg = ValidationConfig(range_rules={"val": {"min": 0}})

        res, valid_df, rejected_df = validate_dataset(df, "data.csv", config=cfg)
        self.assertEqual(len(valid_df), 3)
        self.assertEqual(len(rejected_df), 0)
        self.assertEqual(res.status, "PASSED")

    # 9. Invalid Record Detection
    def test_09_invalid_record_detection(self):
        """Test that non-conforming rows are correctly routed to rejected partition."""
        df = pd.DataFrame({"id": [1, 2, 3], "val": [10, -5, 30]})
        cfg = ValidationConfig(range_rules={"val": {"min": 0}})

        res, valid_df, rejected_df = validate_dataset(df, "data.csv", config=cfg)
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(res.metrics.invalid_rows, 1)
        self.assertEqual(res.status, "FAILED")

    # 10. Multiple Rule Failures on One Record
    def test_10_multiple_rule_failures_on_one_record(self):
        """Test that multiple rule violations on a single row are all recorded."""
        df = pd.DataFrame({"id": ["BAD1", "OK2"], "val": [-10, 50]})
        cfg = ValidationConfig(
            regex_rules={"id": r"^OK\d+$"},
            range_rules={"val": {"min": 0}},
        )

        res, _, rejected_df = validate_dataset(df, "data.csv", config=cfg)
        self.assertEqual(len(rejected_df), 1)
        # Verify both reasons are attached
        reasons = rejected_df["_rejection_reasons"].iloc[0]
        self.assertIn("regex", reasons)
        self.assertIn("range", reasons)

    # 11. Rejected File Generation
    def test_11_rejected_file_generation(self):
        """Test writing quarantined records with audit metadata to data/rejected/."""
        df = pd.DataFrame({"code": ["A", "B"], "qty": [10, -2]})
        cfg = ValidationConfig(range_rules={"qty": {"min": 0}})

        _, valid_df, rejected_df = validate_dataset(df, "sample.csv", config=cfg)
        val_dir = self.test_path / "validated"
        rej_dir = self.test_path / "rejected"

        _, rej_file = save_validation_outputs(
            "sample.csv", valid_df, rejected_df, validated_dir=val_dir, rejected_dir=rej_dir
        )
        self.assertIsNotNone(rej_file)
        self.assertTrue(rej_file.exists())
        self.assertEqual(rej_file.name, "sample_rejected.csv")

        loaded_rej = pd.read_csv(rej_file)
        self.assertIn("_rejection_reasons", loaded_rej.columns)
        self.assertIn("_row_index", loaded_rej.columns)

    # 12. Validated File Generation
    def test_12_validated_file_generation(self):
        """Test writing validated records to data/validated/."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        cfg = ValidationConfig()

        _, valid_df, rejected_df = validate_dataset(df, "test.csv", config=cfg)
        val_dir = self.test_path / "validated"
        rej_dir = self.test_path / "rejected"

        val_file, rej_file = save_validation_outputs(
            "test.csv", valid_df, rejected_df, validated_dir=val_dir, rejected_dir=rej_dir
        )
        self.assertTrue(val_file.exists())
        self.assertIsNone(rej_file)  # No rejected rows
        self.assertEqual(val_file.name, "test_validated.csv")

    # 13. Quality Score Calculation
    def test_13_quality_score_calculation(self):
        """Test exact calculation of percentage-based quality score."""
        # 4 valid rows out of 5 total -> 80.0%
        df = pd.DataFrame({"val": [10, 20, 30, -5, 50]})
        cfg = ValidationConfig(range_rules={"val": {"min": 0}})

        res, _, _ = validate_dataset(df, "score.csv", config=cfg)
        self.assertEqual(res.metrics.total_rows, 5)
        self.assertEqual(res.metrics.valid_rows, 4)
        self.assertEqual(res.metrics.invalid_rows, 1)
        self.assertEqual(res.metrics.quality_score, 80.0)

    # 14. Validation JSON Report
    def test_14_validation_json_report(self):
        """Test serializing validation outcome to JSON report."""
        df = pd.DataFrame({"v": [1, 2]})
        res, _, _ = validate_dataset(df, "batch.csv")
        reports_dir = self.test_path / "reports"

        json_path, _ = save_validation_reports([res], reports_dir=reports_dir)
        self.assertTrue(json_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data[0]["source_filename"], "batch.csv")
        self.assertEqual(data[0]["status"], "PASSED")

    # 15. Validation CSV Report
    def test_15_validation_csv_report(self):
        """Test serializing validation summary to tabular CSV report."""
        df = pd.DataFrame({"v": [1, 2]})
        res, _, _ = validate_dataset(df, "batch.csv")
        reports_dir = self.test_path / "reports"

        _, csv_path = save_validation_reports([res], reports_dir=reports_dir)
        self.assertTrue(csv_path.exists())

        csv_df = pd.read_csv(csv_path)
        self.assertEqual(len(csv_df), 1)
        self.assertIn("quality_score_pct", csv_df.columns)

    # 16. Empty Dataset Handling
    def test_16_empty_dataset_handling(self):
        """Test that empty DataFrames pass gracefully without raising exceptions."""
        df_empty = pd.DataFrame()
        res, valid_df, rejected_df = validate_dataset(df_empty, "empty.csv")
        self.assertEqual(res.metrics.total_rows, 0)
        self.assertEqual(res.metrics.quality_score, 100.0)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 0)

    # 17. No-Rule Permissive Behavior
    def test_17_no_rule_permissive_behavior(self):
        """Test that datasets without configured rules are accepted with 100% score."""
        df = pd.DataFrame({"any_col": [1, "mixed", 3.14, None]})
        cfg = ValidationConfig()

        res, valid_df, rejected_df = validate_dataset(df, "arbitrary.csv", config=cfg)
        self.assertEqual(res.status, "PASSED")
        self.assertEqual(res.metrics.quality_score, 100.0)
        self.assertEqual(len(valid_df), 4)

    # 18. Multiple Dataset Isolation
    def test_18_multiple_dataset_isolation(self):
        """Test validating multiple datasets with different schemas and rules independently."""
        df1 = pd.DataFrame({"emp_id": [1, 2], "age": [25, 150]})
        df2 = pd.DataFrame({"order_id": ["O1", "O2"], "amt": [50.0, 100.0]})

        cfg1 = ValidationConfig(range_rules={"age": {"max": 100}})
        cfg2 = ValidationConfig(range_rules={"amt": {"min": 0}})

        res1, _, rej1 = validate_dataset(df1, "employees.csv", config=cfg1)
        res2, _, rej2 = validate_dataset(df2, "orders.csv", config=cfg2)

        self.assertEqual(res1.metrics.invalid_rows, 1)
        self.assertEqual(res2.metrics.invalid_rows, 0)

    # 19. Raw Data Immutability
    def test_19_raw_data_immutability(self):
        """Verify raw datasets on disk are not altered during validation."""
        raw_file = self.test_path / "raw.csv"
        raw_file.write_text("a,b\n1,2\n3,-4\n", encoding="utf-8")
        original_size = raw_file.stat().st_size

        df = pd.read_csv(raw_file)
        cfg = ValidationConfig(range_rules={"b": {"min": 0}})
        validate_dataset(df, str(raw_file), config=cfg)

        self.assertEqual(raw_file.stat().st_size, original_size)

    # 20. Cleaned Data Immutability
    def test_20_cleaned_data_immutability(self):
        """Verify cleaned DataFrame passed into validate_dataset is not mutated in place."""
        df_cleaned = pd.DataFrame({"col_x": [10, -20, 30], "col_y": ["A", "B", "C"]})
        df_snapshot = df_cleaned.copy(deep=True)

        cfg = ValidationConfig(range_rules={"col_x": {"min": 0}})
        validate_dataset(df_cleaned, "cleaned.csv", config=cfg)

        pd.testing.assert_frame_equal(df_cleaned, df_snapshot)


if __name__ == "__main__":
    unittest.main()
