"""
Comprehensive Unit Tests for Stage 9 SQL Analytics Layer.

Covers:
1. Safe table-name validation
2. Safe column-name validation
3. Invalid identifier rejection
4. Unknown table rejection
5. Unknown column rejection
6. Aggregation allowlist
7. COUNT
8. SUM
9. AVG
10. MIN
11. MAX
12. GROUP BY
13. Parameterized filtering
14. Date trend
15. Day grain
16. Month grain
17. Year grain
18. Ranking (RANK)
19. Dense ranking (DENSE_RANK)
20. Row numbering (ROW_NUMBER)
21. Running total
22. Top-N
23. Null analysis
24. Distinct count
25. Result serialization (to_dataframe)
26. JSON export
27. CSV export
28. Analytics report generation
29. Database unavailable handling
30. Configuration validation
31. Multiple tables isolation
32. Arbitrary dataset schema (IoT device telemetry)
33. Arbitrary dataset schema (Genomics specimen analysis)
34. SQL injection-like identifier rejection
35. SQL injection-like filter value parameter handling
36. Pipeline orchestrator integration
"""

from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import create_engine

from pipeline.analytics import (
    AnalyticsConfig,
    AnalyticsExecutionRecord,
    QueryResult,
    SQLAnalyticsEngine,
    is_safe_identifier,
    load_analytics_config,
    log_analytics_event,
    save_analytics_reports,
    validate_identifier,
)
from pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineStatus,
    StageExecutionRecord,
    StageStatus,
)


class TestSQLAnalyticsLayer(unittest.TestCase):
    """Test suite covering SQL Analytics Layer functionality."""

    def setUp(self):
        """Create isolated temporary environment and in-memory SQLite database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "logs").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports" / "analytics").mkdir(parents=True, exist_ok=True)
        (self.test_root / "sql" / "analytics").mkdir(parents=True, exist_ok=True)

        # In-memory SQLite engine for analytical verification
        self.engine = create_engine("sqlite:///:memory:")

        # Populate sample generic tabular dataset
        df = pd.DataFrame({
            "record_id": [1, 2, 3, 4, 5, 6],
            "category": ["A", "B", "A", "C", "B", "A"],
            "amount": [10.5, 20.0, 15.25, 5.0, 30.0, 25.5],
            "score": [100, 200, 150, 50, 300, 250],
            "entry_date": [
                "2026-01-15",
                "2026-01-20",
                "2026-02-10",
                "2026-02-15",
                "2026-03-01",
                "2026-03-05",
            ],
            "optional_note": ["alpha", None, "beta", None, "gamma", "delta"],
        })
        df.to_sql("financial_ledger", con=self.engine, if_exists="replace", index=False)

        self.analytics_engine = SQLAnalyticsEngine(
            engine=self.engine,
            queries_dir=self.test_root / "sql" / "analytics",
        )

    def tearDown(self):
        """Clean up database connection and isolated filesystem environment."""
        self.engine.dispose()
        self.temp_dir.cleanup()

    # 1. Safe Table-Name Validation
    def test_01_safe_table_name_validation(self):
        """Verify is_safe_identifier accepts alphanumeric and underscore table names."""
        self.assertTrue(is_safe_identifier("sales_data"))
        self.assertTrue(is_safe_identifier("Table_123"))
        self.assertTrue(is_safe_identifier("_hidden_table"))
        self.assertEqual(validate_identifier("valid_table"), "valid_table")

    # 2. Safe Column-Name Validation
    def test_02_safe_column_name_validation(self):
        """Verify is_safe_identifier accepts valid column names."""
        self.assertTrue(is_safe_identifier("transaction_amount"))
        self.assertTrue(is_safe_identifier("col_1"))
        self.assertEqual(validate_identifier("user_score", "column"), "user_score")

    # 3. Invalid Identifier Rejection
    def test_03_invalid_identifier_rejection(self):
        """Verify that identifiers with spaces, punctuation, or special chars raise ValueError."""
        invalid_names = [
            "table name with spaces",
            "table-with-hyphens",
            "table;DROP TABLE users;--",
            "col'name",
            "col\"name",
            "123_starts_with_digit",
            "",
            None,
        ]
        for bad_id in invalid_names:
            self.assertFalse(is_safe_identifier(bad_id))
            with self.assertRaises(ValueError):
                validate_identifier(bad_id)

    # 4. Unknown Table Rejection
    def test_04_unknown_table_rejection(self):
        """Verify querying a nonexistent table returns clean FAILED status."""
        res = self.analytics_engine.get_table_schema("nonexistent_table")
        self.assertFalse(res["exists"])
        self.assertEqual(res["columns"], [])

        query_res = self.analytics_engine.aggregate("nonexistent_table", "amount")
        self.assertEqual(query_res.status, "FAILED")
        self.assertIn("no such table", query_res.error_message.lower())

    # 5. Unknown Column Rejection
    def test_05_unknown_column_rejection(self):
        """Verify aggregating a nonexistent column raises error or returns FAILED."""
        res = self.analytics_engine.aggregate("financial_ledger", "ghost_column")
        self.assertEqual(res.status, "FAILED")
        self.assertIn("no such column", res.error_message.lower())

    # 6. Aggregation Allowlist
    def test_06_aggregation_allowlist(self):
        """Verify unsupported aggregation functions are rejected with ValueError."""
        with self.assertRaises(ValueError):
            self.analytics_engine.aggregate("financial_ledger", "amount", functions=["MEDIAN"])

        with self.assertRaises(ValueError):
            self.analytics_engine.group_by("financial_ledger", "category", aggregation="CUSTOM_AGG")

    # 7. COUNT Aggregation
    def test_07_aggregate_count(self):
        """Verify COUNT aggregation calculation."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount", functions=["COUNT"])
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.rows[0]["count_amount"], 6)

    # 8. SUM Aggregation
    def test_08_aggregate_sum(self):
        """Verify SUM aggregation calculation."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount", functions=["SUM"])
        self.assertEqual(res.status, "SUCCESS")
        expected_sum = 10.5 + 20.0 + 15.25 + 5.0 + 30.0 + 25.5
        self.assertAlmostEqual(float(res.rows[0]["sum_amount"]), expected_sum, places=2)

    # 9. AVG Aggregation
    def test_09_aggregate_avg(self):
        """Verify AVG aggregation calculation."""
        res = self.analytics_engine.aggregate("financial_ledger", "score", functions=["AVG"])
        self.assertEqual(res.status, "SUCCESS")
        expected_avg = (100 + 200 + 150 + 50 + 300 + 250) / 6.0
        self.assertAlmostEqual(float(res.rows[0]["avg_score"]), expected_avg, places=2)

    # 10. MIN Aggregation
    def test_10_aggregate_min(self):
        """Verify MIN aggregation calculation."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount", functions=["MIN"])
        self.assertEqual(res.status, "SUCCESS")
        self.assertAlmostEqual(float(res.rows[0]["min_amount"]), 5.0, places=2)

    # 11. MAX Aggregation
    def test_11_aggregate_max(self):
        """Verify MAX aggregation calculation."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount", functions=["MAX"])
        self.assertEqual(res.status, "SUCCESS")
        self.assertAlmostEqual(float(res.rows[0]["max_amount"]), 30.0, places=2)

    # 12. GROUP BY
    def test_12_group_by(self):
        """Verify GROUP BY aggregation with metric calculation and ordering."""
        res = self.analytics_engine.group_by(
            table_name="financial_ledger",
            group_column="category",
            metric_column="amount",
            aggregation="SUM",
            order_desc=True,
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 3)  # Categories: A, B, C
        # Category A sum: 10.5 + 15.25 + 25.5 = 51.25 (highest, top position)
        self.assertEqual(res.rows[0]["category"], "A")
        self.assertAlmostEqual(float(res.rows[0]["sum_amount"]), 51.25, places=2)

    # 13. Parameterized Filtering
    def test_13_parameterized_filtering(self):
        """Verify parameterized WHERE filtering with bound parameters."""
        res = self.analytics_engine.group_by(
            table_name="financial_ledger",
            group_column="category",
            metric_column="amount",
            aggregation="COUNT",
            filter_condition="amount > :min_val",
            filter_params={"min_val": 15.0},
        )
        self.assertEqual(res.status, "SUCCESS")
        # Amounts > 15.0: 20.0 (B), 15.25 (A), 30.0 (B), 25.5 (A) -> Categories A and B only
        categories = [r["category"] for r in res.rows]
        self.assertNotIn("C", categories)

    # 14. Date Trend
    def test_14_date_trend(self):
        """Verify date trend aggregation bucketed by month."""
        res = self.analytics_engine.date_trend(
            table_name="financial_ledger",
            date_column="entry_date",
            metric_column="amount",
            aggregation="SUM",
            grain="month",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 3)  # 2026-01, 2026-02, 2026-03

    # 15. Day Grain
    def test_15_date_trend_day_grain(self):
        """Verify date trend with grain='day'."""
        res = self.analytics_engine.date_trend(
            table_name="financial_ledger",
            date_column="entry_date",
            grain="day",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertGreaterEqual(res.row_count, 5)

    # 16. Month Grain
    def test_16_date_trend_month_grain(self):
        """Verify date trend with grain='month'."""
        res = self.analytics_engine.date_trend(
            table_name="financial_ledger",
            date_column="entry_date",
            grain="month",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 3)

    # 17. Year Grain
    def test_17_date_trend_year_grain(self):
        """Verify date trend with grain='year'."""
        res = self.analytics_engine.date_trend(
            table_name="financial_ledger",
            date_column="entry_date",
            grain="year",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 1)  # All 2026

    # 18. Ranking (RANK)
    def test_18_ranking_rank(self):
        """Verify window function RANK()."""
        res = self.analytics_engine.rank_rows(
            table_name="financial_ledger",
            order_by_column="amount",
            method="RANK",
            direction="DESC",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 6)
        # Highest amount (30.0) should have rank 1
        self.assertEqual(res.rows[0]["calculated_rank"], 1)
        self.assertEqual(res.rows[0]["amount"], 30.0)

    # 19. Dense Ranking (DENSE_RANK)
    def test_19_ranking_dense_rank(self):
        """Verify window function DENSE_RANK()."""
        res = self.analytics_engine.rank_rows(
            table_name="financial_ledger",
            order_by_column="amount",
            method="DENSE_RANK",
            direction="ASC",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.rows[0]["calculated_rank"], 1)
        self.assertEqual(res.rows[0]["amount"], 5.0)

    # 20. Row Numbering (ROW_NUMBER)
    def test_20_ranking_row_number(self):
        """Verify window function ROW_NUMBER()."""
        res = self.analytics_engine.rank_rows(
            table_name="financial_ledger",
            order_by_column="record_id",
            method="ROW_NUMBER",
            direction="ASC",
        )
        self.assertEqual(res.status, "SUCCESS")
        ranks = [r["calculated_rank"] for r in res.rows]
        self.assertEqual(ranks, [1, 2, 3, 4, 5, 6])

    # 21. Running Total
    def test_21_running_total(self):
        """Verify cumulative running total over sequential entries."""
        res = self.analytics_engine.running_total(
            table_name="financial_ledger",
            order_by_column="record_id",
            metric_column="amount",
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 6)
        # First row running total is 10.5
        self.assertAlmostEqual(float(res.rows[0]["running_total"]), 10.5, places=2)
        # Second row running total is 10.5 + 20.0 = 30.5
        self.assertAlmostEqual(float(res.rows[1]["running_total"]), 30.5, places=2)

    # 22. Top-N Analysis
    def test_22_top_n(self):
        """Verify top_n limits output rows correctly."""
        res = self.analytics_engine.top_n(
            table_name="financial_ledger",
            group_column="category",
            metric_column="amount",
            aggregation="SUM",
            n=2,
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.row_count, 2)

    # 23. Null Analysis
    def test_23_null_analysis(self):
        """Verify data-quality null summary computes accurate counts and percentages."""
        res = self.analytics_engine.null_summary("financial_ledger")
        self.assertEqual(res.status, "SUCCESS")
        # Check optional_note column has 2 nulls out of 6 rows (33.33%)
        note_row = next(r for r in res.rows if r["column_name"] == "optional_note")
        self.assertEqual(note_row["total_rows"], 6)
        self.assertEqual(note_row["null_count"], 2)
        self.assertAlmostEqual(float(note_row["null_percentage"]), 33.33, places=1)

    # 24. Distinct Count
    def test_24_distinct_count(self):
        """Verify data-quality distinct count computes unique values accurately."""
        res = self.analytics_engine.distinct_summary("financial_ledger")
        self.assertEqual(res.status, "SUCCESS")
        # Category column has 3 distinct values (A, B, C)
        cat_row = next(r for r in res.rows if r["column_name"] == "category")
        self.assertEqual(cat_row["distinct_count"], 3)

    # 25. Result Serialization (to_dataframe)
    def test_25_result_serialization_to_dataframe(self):
        """Verify QueryResult converts to a valid Pandas DataFrame."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount")
        df = res.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    # 26. JSON Export
    def test_26_json_export(self):
        """Verify export_result writes well-formed JSON."""
        res = self.analytics_engine.aggregate("financial_ledger", "amount")
        out_files = self.analytics_engine.export_result(res, reports_dir=self.test_root / "reports" / "analytics")

        self.assertIn("json", out_files)
        json_path = Path(out_files["json"])
        self.assertTrue(json_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["status"], "SUCCESS")
        self.assertEqual(data["table_name"], "financial_ledger")

    # 27. CSV Export
    def test_27_csv_export(self):
        """Verify export_result writes valid CSV file."""
        res = self.analytics_engine.group_by("financial_ledger", "category")
        out_files = self.analytics_engine.export_result(res, reports_dir=self.test_root / "reports" / "analytics")

        self.assertIn("csv", out_files)
        csv_path = Path(out_files["csv"])
        self.assertTrue(csv_path.exists())

        df_csv = pd.read_csv(csv_path)
        self.assertEqual(len(df_csv), 3)

    # 28. Analytics Report Generation
    def test_28_analytics_report_generation(self):
        """Verify save_analytics_reports writes unified JSON and CSV report files."""
        records = [
            AnalyticsExecutionRecord(
                run_id="run_test",
                query_name="basic_summary",
                table="financial_ledger",
                status="SUCCESS",
                execution_time=0.012,
                result_rows=1,
            )
        ]
        json_p, csv_p = save_analytics_reports(records, reports_dir=self.test_root / "reports", run_id="run_test")
        self.assertTrue(json_p.exists())
        self.assertTrue(csv_p.exists())

    # 29. Database Unavailable Handling
    def test_29_database_unavailable_handling(self):
        """Verify engine gracefully returns DATABASE_UNAVAILABLE when engine is None."""
        offline_engine = SQLAnalyticsEngine(engine=None)
        with patch.object(offline_engine, "connect", return_value=(False, "Connection refused")):
            res = offline_engine.aggregate("some_table", "amount")
            self.assertEqual(res.status, "DATABASE_UNAVAILABLE")
            self.assertIn("Connection refused", res.error_message)

    # 30. Configuration Validation
    def test_30_configuration_validation(self):
        """Verify config parsing and validation."""
        cfg_file = self.test_root / "config" / "test_analytics.json"
        cfg_file.write_text('{"default_limit": 500, "allowed_aggregations": ["COUNT", "SUM"]}', encoding="utf-8")

        cfg, err = load_analytics_config(cfg_file)
        self.assertIsNone(err)
        self.assertEqual(cfg.default_limit, 500)
        self.assertEqual(cfg.allowed_aggregations, ["COUNT", "SUM"])

    # 31. Multiple Tables
    def test_31_multiple_tables(self):
        """Verify engine queries and manages multiple distinct tables."""
        df2 = pd.DataFrame({"product_id": [101, 102], "product_name": ["Widget", "Gadget"]})
        df2.to_sql("inventory_items", con=self.engine, if_exists="replace", index=False)

        tables = self.analytics_engine.list_tables()
        self.assertIn("financial_ledger", tables)
        self.assertIn("inventory_items", tables)

        res = self.analytics_engine.aggregate("inventory_items", "product_id", functions=["COUNT"])
        self.assertEqual(res.rows[0]["count_product_id"], 2)

    # 32. Arbitrary Dataset Schema: IoT Device Telemetry
    def test_32_arbitrary_dataset_schema_iot(self):
        """Verify domain-agnostic automated suite handles IoT sensor schema."""
        iot_df = pd.DataFrame({
            "device_name": ["sensor_north", "sensor_south", "sensor_north", "sensor_east"],
            "reading": [23.5, 24.1, 22.8, 26.0],
            "recorded_at": ["2026-03-01 10:00:00", "2026-03-01 10:05:00", "2026-03-01 10:10:00", "2026-03-01 10:15:00"],
            "active": [1, 1, 0, 1],
        })
        iot_df.to_sql("device_telemetry", con=self.engine, if_exists="replace", index=False)

        results = self.analytics_engine.run_automated_analytics_suite(
            table_name="device_telemetry",
            reports_dir=self.test_root / "reports" / "analytics",
        )
        self.assertGreaterEqual(len(results), 4)
        statuses = [r.status for r in results]
        self.assertTrue(all(s == "SUCCESS" for s in statuses))

    # 33. Arbitrary Dataset Schema: Genomics Specimen Analysis
    def test_33_arbitrary_dataset_schema_genomics(self):
        """Verify domain-agnostic automated suite handles genomics dataset."""
        genomics_df = pd.DataFrame({
            "specimen_id": ["SPEC_001", "SPEC_002", "SPEC_003"],
            "coverage_depth": [45.2, 52.8, 48.0],
            "sampled_date": ["2026-02-01", "2026-02-02", "2026-02-03"],
            "is_mutated": [0, 1, 0],
        })
        genomics_df.to_sql("genomics_specimens", con=self.engine, if_exists="replace", index=False)

        results = self.analytics_engine.run_automated_analytics_suite(
            table_name="genomics_specimens",
            reports_dir=self.test_root / "reports" / "analytics",
        )
        self.assertGreaterEqual(len(results), 4)

    # 34. SQL Injection-Like Identifier Rejection
    def test_34_sql_injection_identifier_rejection(self):
        """Verify malicious identifier inputs are rejected before SQL generation."""
        malicious_identifiers = [
            "table_name; DROP TABLE financial_ledger; --",
            "category' OR '1'='1",
            "admin\"; SELECT * FROM secrets; --",
        ]
        for injection_str in malicious_identifiers:
            with self.assertRaises(ValueError):
                self.analytics_engine.aggregate(injection_str, "amount")

            with self.assertRaises(ValueError):
                self.analytics_engine.group_by("financial_ledger", injection_str)

    # 35. SQL Injection-Like Filter Value Handling
    def test_35_sql_injection_filter_value_handling(self):
        """Verify parameterized filter values safely escape malicious strings."""
        malicious_val = "' OR '1'='1' --"
        res = self.analytics_engine.group_by(
            table_name="financial_ledger",
            group_column="category",
            filter_condition="category = :val",
            filter_params={"val": malicious_val},
        )
        self.assertEqual(res.status, "SUCCESS")
        # No category equals the literal injection string, so 0 rows returned
        self.assertEqual(res.row_count, 0)

    # 36. Orchestrator Integration
    def test_36_orchestrator_integration(self):
        """Verify orchestrator executes Stage 9 cleanly when database is offline or unconfigured."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        # Create minimal raw file
        raw_csv = self.test_root / "data" / "raw" / "records.csv"
        raw_csv.write_text("id,val\n1,one\n", encoding="utf-8")

        summary = orch.execute_pipeline(display_summary=False)
        stage_names = [st["stage_name"] for st in summary.stages]
        self.assertIn("analytics", stage_names)

        an_st = next(s for s in summary.stages if s["stage_name"] == "analytics")
        # When PostgreSQL is offline, analytics reports DATABASE_UNAVAILABLE honestly
        self.assertEqual(an_st["status"], StageStatus.DATABASE_UNAVAILABLE.value)


if __name__ == "__main__":
    unittest.main()
