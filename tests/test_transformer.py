"""
Unit tests for Universal Data Cleaning & Transformation Engine (Stage 4).
"""

from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from pipeline.transformer import (
    TransformationConfig,
    clean_whitespace,
    convert_types_and_dates,
    handle_missing_values,
    normalize_column_name,
    normalize_columns,
    normalize_string_case,
    remove_duplicates,
    save_processed_dataset,
    save_transformation_report,
    transform_dataset,
)


class TestTransformer(unittest.TestCase):
    """Test suite covering column normalization, cleaning, imputation, typing, and reports."""

    def setUp(self):
        """Create an isolated temporary directory for test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # 1. Column-name Normalization
    def test_01_column_name_normalization(self):
        """Test conversion of messy headers to clean snake_case."""
        self.assertEqual(normalize_column_name(" First Name "), "first_name")
        self.assertEqual(normalize_column_name("Customer ID"), "customer_id")
        self.assertEqual(normalize_column_name("Sale-Date"), "sale_date")
        self.assertEqual(normalize_column_name("Product   Name"), "product_name")
        self.assertEqual(normalize_column_name("Total$$Amount"), "total_amount")

        df = pd.DataFrame(columns=[" First Name ", "Customer ID", "Sale-Date"])
        df_norm, rename_map = normalize_columns(df)
        self.assertEqual(list(df_norm.columns), ["first_name", "customer_id", "sale_date"])
        self.assertEqual(rename_map[" First Name "], "first_name")

    # 2. Whitespace Stripping
    def test_02_whitespace_stripping(self):
        """Test outer trimming and collapsing of internal duplicate spaces."""
        df = pd.DataFrame({
            "name": ["  John   Smith  ", "   Finance   ", "Normal"],
            "val": [10, 20, 30],
        })
        df_clean = clean_whitespace(df)
        self.assertEqual(df_clean["name"].tolist(), ["John Smith", "Finance", "Normal"])
        self.assertEqual(df_clean["val"].tolist(), [10, 20, 30])  # Numeric untouched

    # 3. Configurable String Case
    def test_03_configurable_string_case(self):
        """Test configurable string casing (lower, upper, title, none)."""
        df = pd.DataFrame({"city": ["new york", "LONDON", "paris"]})

        df_lower = normalize_string_case(df, case="lower")
        self.assertEqual(df_lower["city"].tolist(), ["new york", "london", "paris"])

        df_upper = normalize_string_case(df, case="upper")
        self.assertEqual(df_upper["city"].tolist(), ["NEW YORK", "LONDON", "PARIS"])

        df_title = normalize_string_case(df, case="title")
        self.assertEqual(df_title["city"].tolist(), ["New York", "London", "Paris"])

    # 4. Numeric Type Conversion
    def test_04_numeric_type_conversion(self):
        """Test conversion of string numbers into native integer and float types."""
        df = pd.DataFrame({
            "int_col": ["10", "20", "30"],
            "float_col": ["10.5", "20.25", "30.75"],
        })
        logical_types = {"int_col": "Integer", "float_col": "Float"}
        df_cast, convs, warns = convert_types_and_dates(df, logical_types)

        self.assertTrue(pd.api.types.is_integer_dtype(df_cast["int_col"]))
        self.assertTrue(pd.api.types.is_float_dtype(df_cast["float_col"]))
        self.assertEqual(len(warns), 0)

    # 5. DateTime Conversion
    def test_05_datetime_conversion(self):
        """Test conversion and standardization of date strings to datetime64."""
        df = pd.DataFrame({"event_date": ["2026-01-15", "2026-02-20", "2026-03-25"]})
        logical_types = {"event_date": "DateTime"}
        df_cast, _, _ = convert_types_and_dates(df, logical_types, standardize_dates=True)

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_cast["event_date"]))
        self.assertEqual(df_cast["event_date"].iloc[0].year, 2026)

    # 6. Boolean Conversion
    def test_06_boolean_conversion(self):
        """Test conversion of various boolean string representations."""
        df = pd.DataFrame({
            "flag1": ["Yes", "No", "Yes"],
            "flag2": ["1", "0", "1"],
            "flag3": ["true", "false", "true"],
        })
        logical_types = {"flag1": "Boolean", "flag2": "Boolean", "flag3": "Boolean"}
        df_cast, _, _ = convert_types_and_dates(df, logical_types)

        self.assertEqual(df_cast["flag1"].tolist(), [True, False, True])
        self.assertEqual(df_cast["flag2"].tolist(), [True, False, True])
        self.assertEqual(df_cast["flag3"].tolist(), [True, False, True])

    # 7. Missing-Value Mean Strategy
    def test_07_missing_value_mean_strategy(self):
        """Test missing numeric value imputation using mean."""
        df = pd.DataFrame({"val": [10.0, 20.0, np.nan, 30.0]})
        logical_types = {"val": "Float"}
        strategies = {"numeric": "mean"}

        df_imp, ops = handle_missing_values(df, logical_types, strategies)
        self.assertAlmostEqual(df_imp["val"].iloc[2], 20.0)
        self.assertEqual(len(ops), 1)

    # 8. Missing-Value Median Strategy
    def test_08_missing_value_median_strategy(self):
        """Test missing numeric value imputation using median."""
        df = pd.DataFrame({"val": [10.0, 20.0, np.nan, 100.0]})
        logical_types = {"val": "Float"}
        strategies = {"numeric": "median"}

        df_imp, _ = handle_missing_values(df, logical_types, strategies)
        self.assertEqual(df_imp["val"].iloc[2], 20.0)

    # 9. Missing-Value Mode Strategy
    def test_09_missing_value_mode_strategy(self):
        """Test missing categorical value imputation using mode."""
        df = pd.DataFrame({"cat": ["A", "B", "A", None]})
        logical_types = {"cat": "Categorical"}
        strategies = {"categorical": "mode"}

        df_imp, _ = handle_missing_values(df, logical_types, strategies)
        self.assertEqual(df_imp["cat"].iloc[3], "A")

    # 10. Constant-Value Strategy
    def test_10_constant_value_strategy(self):
        """Test missing value replacement with a defined constant."""
        df = pd.DataFrame({"notes": ["Alpha", None, "Beta"]})
        logical_types = {"notes": "String/Text"}
        strategies = {"text": "constant", "text_constant": "N/A"}

        df_imp, _ = handle_missing_values(df, logical_types, strategies)
        self.assertEqual(df_imp["notes"].iloc[1], "N/A")

    # 11. Forward Fill
    def test_11_forward_fill(self):
        """Test sequence imputation using forward-fill."""
        df = pd.DataFrame({"ts": [pd.Timestamp("2026-01-01"), None, pd.Timestamp("2026-01-03")]})
        logical_types = {"ts": "DateTime"}
        strategies = {"datetime": "ffill"}

        df_imp, _ = handle_missing_values(df, logical_types, strategies)
        self.assertEqual(df_imp["ts"].iloc[1], pd.Timestamp("2026-01-01"))

    # 12. Backward Fill
    def test_12_backward_fill(self):
        """Test sequence imputation using backward-fill."""
        df = pd.DataFrame({"ts": [pd.Timestamp("2026-01-01"), None, pd.Timestamp("2026-01-03")]})
        logical_types = {"ts": "DateTime"}
        strategies = {"datetime": "bfill"}

        df_imp, _ = handle_missing_values(df, logical_types, strategies)
        self.assertEqual(df_imp["ts"].iloc[1], pd.Timestamp("2026-01-03"))

    # 13. Duplicate Detection
    def test_13_duplicate_detection(self):
        """Test counting of exact duplicate rows."""
        df = pd.DataFrame({
            "a": [1, 2, 1, 3],
            "b": ["x", "y", "x", "z"],
        })
        _, dups_removed = remove_duplicates(df)
        self.assertEqual(dups_removed, 1)

    # 14. Duplicate Removal
    def test_14_duplicate_removal(self):
        """Test removal of full row duplicates and subset-based duplicates."""
        df = pd.DataFrame({
            "id": [1, 2, 1],
            "data": ["A", "B", "C"],
        })
        # Subset deduplication on ID
        df_dedup, dups = remove_duplicates(df, subset=["id"])
        self.assertEqual(dups, 1)
        self.assertEqual(len(df_dedup), 2)
        self.assertEqual(df_dedup["data"].tolist(), ["A", "B"])

    # 15. Invalid Conversion Handling
    def test_15_invalid_conversion_handling(self):
        """Test that unconvertible values emit warnings and coerce safely without crashing."""
        df = pd.DataFrame({"amount": ["100.5", "corrupt_data", "200.0"]})
        logical_types = {"amount": "Float"}

        df_cast, _, warns = convert_types_and_dates(df, logical_types)
        self.assertEqual(len(warns), 1)
        self.assertTrue(pd.isna(df_cast["amount"].iloc[1]))
        self.assertEqual(df_cast["amount"].iloc[0], 100.5)

    # 16. Raw Data Immutability
    def test_16_raw_data_immutability(self):
        """Verify that transform_dataset does not alter the input DataFrame in place."""
        original_df = pd.DataFrame({
            " First Name ": ["  Alice  ", "Bob"],
            "Score": ["90", "85"],
        })
        df_copy = original_df.copy(deep=True)

        res = transform_dataset(original_df, "test.csv")
        self.assertEqual(res.status, "SUCCESS")
        # Verify original_df has unchanged columns and data
        pd.testing.assert_frame_equal(original_df, df_copy)

    # 17. Processed Output Generation
    def test_17_processed_output_generation(self):
        """Test saving cleaned data to data/processed with dynamic filename."""
        df_clean = pd.DataFrame({"id": [1, 2], "val": [10.5, 20.0]})
        proc_dir = self.test_path / "processed"

        out_path = save_processed_dataset(df_clean, "inventory_batch1.xlsx", processed_dir=proc_dir)
        self.assertTrue(out_path.exists())
        self.assertEqual(out_path.name, "inventory_batch1_cleaned.csv")

        loaded = pd.read_csv(out_path)
        self.assertEqual(len(loaded), 2)

    # 18. Transformation Report Generation
    def test_18_transformation_report_generation(self):
        """Test serializing transformation outcomes to JSON report."""
        df = pd.DataFrame({"  Col A ": [" x ", " y "]})
        res = transform_dataset(df, "data.csv")
        reports_dir = self.test_path / "reports"

        rep_path = save_transformation_report([res], reports_dir=reports_dir)
        self.assertTrue(rep_path.exists())

        with open(rep_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["source_filename"], "data.csv")
        self.assertIn("columns_renamed", data[0])

    # 19. Multiple Dataset Isolation
    def test_19_multiple_dataset_isolation(self):
        """Verify processing multiple distinct datasets in sequence maintains full isolation."""
        df1 = pd.DataFrame({"emp_code": [1, 2], "dept": ["HR", "IT"]})
        df2 = pd.DataFrame({"sensor_id": ["S1", "S2"], "temp_c": [22.4, 23.1]})

        res1 = transform_dataset(df1, "employees.csv")
        res2 = transform_dataset(df2, "sensors.csv")

        self.assertEqual(res1.source_filename, "employees.csv")
        self.assertEqual(res2.source_filename, "sensors.csv")
        self.assertIn("emp_code", res1.cleaned_df.columns)
        self.assertIn("sensor_id", res2.cleaned_df.columns)

    # 20. Empty DataFrame Handling
    def test_20_empty_dataframe_handling(self):
        """Verify graceful handling of 0-row DataFrames."""
        df_empty = pd.DataFrame()
        res = transform_dataset(df_empty, "empty.csv")
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.rows_before, 0)
        self.assertEqual(res.rows_after, 0)


if __name__ == "__main__":
    unittest.main()
