"""
Unit tests for Universal Schema Detection & Data Profiling (Stage 3).
"""

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from pipeline.schema_detector import (
    calculate_categorical_stats,
    calculate_numeric_stats,
    calculate_text_stats,
    detect_boolean_type,
    detect_datetime_type,
    detect_identifier,
    infer_logical_type,
    profile_column,
    profile_dataset,
    save_profiling_reports,
)


class TestSchemaDetector(unittest.TestCase):
    """Test suite covering type inference, statistics, heuristics, and profiling reports."""

    def setUp(self):
        """Create an isolated temporary directory for test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # 1. Integer Detection
    def test_01_integer_detection(self):
        """Verify integer detection for native ints, string ints, and float-masked ints with nulls."""
        s1 = pd.Series([10, 20, 30, 40])
        self.assertEqual(infer_logical_type(s1, len(s1)), "Integer")

        s2 = pd.Series([100.0, 200.0, np.nan, 300.0])  # Float due to NaN but integer values
        self.assertEqual(infer_logical_type(s2, len(s2)), "Integer")

        s3 = pd.Series(["10", "20", "30", "40", "50"])  # String integers
        self.assertEqual(infer_logical_type(s3, len(s3)), "Integer")

    # 2. Float Detection
    def test_02_float_detection(self):
        """Verify float detection for fractional numbers."""
        s1 = pd.Series([10.5, 20.75, 30.12])
        self.assertEqual(infer_logical_type(s1, len(s1)), "Float")

        s2 = pd.Series(["10.50", "20.75", "30.12"])
        self.assertEqual(infer_logical_type(s2, len(s2)), "Float")

    # 3. Boolean Detection
    def test_03_boolean_detection(self):
        """Verify boolean detection for native bools, Yes/No, True/False, and 1/0."""
        s_native = pd.Series([True, False, True])
        is_bool, conf, _ = detect_boolean_type(s_native)
        self.assertTrue(is_bool)
        self.assertEqual(conf, 1.0)
        self.assertEqual(infer_logical_type(s_native, len(s_native)), "Boolean")

        s_yes_no = pd.Series(["Yes", "No", "Yes", "Yes", "No"])
        is_bool, conf, _ = detect_boolean_type(s_yes_no)
        self.assertTrue(is_bool)
        self.assertEqual(infer_logical_type(s_yes_no, len(s_yes_no)), "Boolean")

        s_tf = pd.Series(["true", "false", "true"])
        is_bool, _, _ = detect_boolean_type(s_tf)
        self.assertTrue(is_bool)

    # 4. Date/Time Detection
    def test_04_date_detection(self):
        """Verify ISO and common date string detection without falsely converting pure numbers."""
        s_date = pd.Series(["2026-01-15", "2026-02-20", "2026-03-25", "2026-04-10"])
        is_dt, conf, _ = detect_datetime_type(s_date)
        self.assertTrue(is_dt)
        self.assertEqual(infer_logical_type(s_date, len(s_date)), "DateTime")

        # Pure numbers should NOT be detected as dates
        s_num = pd.Series([101, 102, 103, 104])
        is_dt_num, _, _ = detect_datetime_type(s_num)
        self.assertFalse(is_dt_num)

    # 5. String/Text Detection
    def test_05_string_text_detection(self):
        """Verify high-cardinality free-text columns are detected as String/Text."""
        s_text = pd.Series([
            "Quarterly performance exceeds annual target",
            "Urgent customer ticket regarding payment latency",
            "Routine system maintenance scheduled for weekend",
            "Revised terms of service accepted by vendor",
        ])
        self.assertEqual(infer_logical_type(s_text, len(s_text)), "String/Text")

    # 6. Categorical Detection
    def test_06_categorical_detection(self):
        """Verify low-cardinality repeated string columns are detected as Categorical."""
        s_cat = pd.Series(["North", "South", "East", "West", "North", "South", "North", "East"])
        self.assertEqual(infer_logical_type(s_cat, len(s_cat)), "Categorical")

    # 7. Identifier Heuristic
    def test_07_identifier_heuristic(self):
        """Verify multiple-signal identifier heuristics."""
        s_id = pd.Series(["ID_101", "ID_102", "ID_103", "ID_104", "ID_105"])
        id_info = detect_identifier("account_id", s_id, "String/Text", len(s_id))
        self.assertTrue(id_info.likely_identifier)
        self.assertGreaterEqual(id_info.confidence, 0.90)

        # High uniqueness without identifier name in small dataset should NOT be classified as identifier
        s_name = pd.Series(["Alice", "Bob", "Charlie", "David", "Eve"])
        name_info = detect_identifier("first_name", s_name, "String/Text", len(s_name))
        self.assertFalse(name_info.likely_identifier)

    # 8. Missing Value Calculation
    def test_08_missing_value_calculation(self):
        """Verify accurate null counts and percentages."""
        s = pd.Series([1.0, 2.0, np.nan, 4.0, np.nan])
        col_prof = profile_column("metric", s, total_rows=len(s))
        self.assertEqual(col_prof.total_values, 5)
        self.assertEqual(col_prof.missing_count, 2)
        self.assertEqual(col_prof.missing_percentage, 40.0)
        self.assertEqual(col_prof.non_null_count, 3)
        self.assertTrue(col_prof.is_nullable)

    # 9. Unique Value Calculation
    def test_09_unique_value_calculation(self):
        """Verify distinct counts and duplicate counts."""
        s = pd.Series(["A", "B", "A", "C", "B"])
        col_prof = profile_column("code", s, total_rows=len(s))
        self.assertEqual(col_prof.unique_count, 3)
        self.assertEqual(col_prof.distinct_percentage, 60.0)
        self.assertEqual(col_prof.duplicate_count, 2)

    # 10. Duplicate Row Detection
    def test_10_duplicate_row_detection(self):
        """Verify dataset-level duplicate row calculations."""
        df = pd.DataFrame({
            "a": [1, 2, 1, 4],
            "b": ["x", "y", "x", "z"],
        })
        prof = profile_dataset(df, filename="test_duplicates.csv")
        self.assertEqual(prof.row_count, 4)
        self.assertEqual(prof.duplicate_rows, 1)
        self.assertEqual(prof.duplicate_row_percentage, 25.0)

    # 11. Numeric Statistics
    def test_11_numeric_statistics(self):
        """Verify accurate calculation of mean, median, min, max, std, and quartiles."""
        s = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        stats = calculate_numeric_stats(s)
        self.assertIsNotNone(stats)
        self.assertEqual(stats.count, 5)
        self.assertEqual(stats.mean, 30.0)
        self.assertEqual(stats.median, 30.0)
        self.assertEqual(stats.min, 10.0)
        self.assertEqual(stats.max, 50.0)
        self.assertEqual(stats.q25, 20.0)
        self.assertEqual(stats.q75, 40.0)

    # 12. Text Length Statistics
    def test_12_text_length_statistics(self):
        """Verify min, max, and avg character length metrics."""
        s = pd.Series(["cat", "elephant", "dog"])
        stats = calculate_text_stats(s)
        self.assertIsNotNone(stats)
        self.assertEqual(stats.min_length, 3)
        self.assertEqual(stats.max_length, 8)
        self.assertAlmostEqual(stats.avg_length, 4.67, places=2)

    # 13. Mixed and Invalid Values
    def test_13_mixed_invalid_values(self):
        """Verify resilient handling of mixed corrupted inputs and all-null series."""
        s_mixed = pd.Series([10, "corrupt", 30, np.nan, 50])
        col_prof = profile_column("mixed_data", s_mixed, total_rows=len(s_mixed))
        self.assertIsNotNone(col_prof)

        s_all_null = pd.Series([np.nan, np.nan, None])
        null_prof = profile_column("empty_col", s_all_null, total_rows=len(s_all_null))
        self.assertEqual(null_prof.missing_count, 3)
        self.assertEqual(null_prof.missing_percentage, 100.0)

    # 14. Empty DataFrame Handling
    def test_14_empty_dataframe_handling(self):
        """Verify that empty dataframes do not raise exceptions."""
        df_empty = pd.DataFrame()
        prof = profile_dataset(df_empty, filename="empty.csv")
        self.assertEqual(prof.row_count, 0)
        self.assertEqual(prof.column_count, 0)
        self.assertEqual(len(prof.columns), 0)

    # 15. Multiple Dataset Profiling
    def test_15_multiple_dataset_profiling(self):
        """Verify isolated profiling across multiple distinct domain dataframes."""
        df1 = pd.DataFrame({"sales_id": [1, 2], "amount": [100.5, 200.0]})
        df2 = pd.DataFrame({"student_code": ["S1", "S2"], "grade": ["A", "B"]})

        prof1 = profile_dataset(df1, "sales.csv")
        prof2 = profile_dataset(df2, "students.csv")

        self.assertEqual(prof1.filename, "sales.csv")
        self.assertEqual(prof1.column_count, 2)
        self.assertEqual(prof2.filename, "students.csv")
        self.assertEqual(prof2.column_count, 2)
        self.assertIn("sales_id", prof1.columns)
        self.assertIn("student_code", prof2.columns)

    # 16. Profile JSON and CSV Generation
    def test_16_profile_json_generation(self):
        """Verify serialization of profiling results to JSON and CSV files."""
        df = pd.DataFrame({
            "record_id": [101, 102],
            "category": ["A", "B"],
            "val": [1.5, 2.5],
        })
        prof = profile_dataset(df, "records.csv")
        reports_dir = self.test_path / "reports"

        json_path, csv_path = save_profiling_reports([prof], reports_dir=reports_dir)

        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["filename"], "records.csv")

        csv_df = pd.read_csv(csv_path)
        self.assertEqual(len(csv_df), 3)  # 3 columns profiled
        self.assertIn("column_name", csv_df.columns)
        self.assertIn("inferred_type", csv_df.columns)


if __name__ == "__main__":
    unittest.main()
