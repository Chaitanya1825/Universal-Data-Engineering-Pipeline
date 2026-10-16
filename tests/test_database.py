"""
Unit and Integration Tests for PostgreSQL Database & Dynamic Data Modeling (Stage 6).

Covers:
- Dynamic table name generation & sanitization
- SQL type mapping across logical data types
- Configuration loading & defaults
- Missing and masked DATABASE_URL handling
- In-memory SQLite table creation and data loading
- Existing table modes (append, replace, fail)
- Schema inspection & row counting
- JSON and CSV load report generation
- Audit logging without credentials
- Empty DataFrame handling
- Configurable primary key strategy
- Dynamic dataset schema handling (arbitrary domains like IoT telemetry)
- Multiple dataset isolation
- Conditional PostgreSQL integration testing
"""

from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
import unittest

import pandas as pd
from sqlalchemy import create_engine, inspect

from pipeline.database import (
    DatabaseConfig,
    DatabaseLoadResult,
    format_database_summary,
    generate_table_name,
    get_database_engine,
    get_database_url,
    inspect_table_schema,
    load_database_config,
    load_project_env,
    load_validated_dataset,
    log_database_event,
    map_logical_type_to_sql,
    mask_database_url,
    normalize_database_url,
    save_database_load_reports,
    test_database_connection,
)
from pipeline.schema_detector import (
    ColumnProfile,
    DatasetProfile,
    IdentifierInfo,
    profile_dataset,
)


class TestDatabaseStage6(unittest.TestCase):
    """Test suite for Stage 6 dynamic database operations without requiring PostgreSQL."""

    def setUp(self):
        """Create isolated test directory and test in-memory engine."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.temp_dir.name)
        self.sqlite_engine = create_engine("sqlite:///:memory:")

    def tearDown(self):
        """Clean up resources."""
        self.sqlite_engine.dispose()
        self.temp_dir.cleanup()

    # 1. Dynamic Table Name Generation
    def test_01_table_name_generation(self):
        """Test safe, standardized table name generation from filenames."""
        # Pipeline stage suffixes removed
        self.assertEqual(generate_table_name("sample_sales_validated.csv"), "sample_sales")
        self.assertEqual(generate_table_name("sample_employees_validated.csv"), "sample_employees")
        self.assertEqual(generate_table_name("customer_transactions_validated.csv"), "customer_transactions")
        self.assertEqual(generate_table_name("inventory_cleaned.xlsx"), "inventory")
        self.assertEqual(generate_table_name("records_raw.json"), "records")

        # Spaces and uppercase
        self.assertEqual(generate_table_name("Annual Revenue 2024.csv"), "annual_revenue_2024")

        # Special unsafe characters
        self.assertEqual(generate_table_name("sensor-reading#v2.1!@.json"), "sensor_reading_v2_1")

        # Collapsing multiple underscores and stripping edges
        self.assertEqual(generate_table_name("___test___table___validated.csv"), "test_table")
        self.assertEqual(generate_table_name("data_file_raw.csv"), "data_file")

        # Fallback for empty or purely punctuation names
        self.assertEqual(generate_table_name("....csv"), "universal_table")

        # PostgreSQL 63-character truncation
        long_name = "a" * 80 + ".csv"
        generated = generate_table_name(long_name)
        self.assertLessEqual(len(generated), 63)
        self.assertEqual(generated, "a" * 63)

    # 2. Dynamic SQL Type Mapping
    def test_02_sql_type_mapping(self):
        """Test mapping from logical data types to SQLAlchemy SQL types."""
        int_t = map_logical_type_to_sql("Integer")
        self.assertEqual(int_t.__class__.__name__, "BIGINT")

        float_t = map_logical_type_to_sql("Float")
        self.assertEqual(float_t.__class__.__name__, "FLOAT")

        bool_t = map_logical_type_to_sql("Boolean")
        self.assertEqual(bool_t.__class__.__name__, "BOOLEAN")

        date_t = map_logical_type_to_sql("DateTime")
        self.assertEqual(date_t.__class__.__name__, "TIMESTAMP")

        cat_t = map_logical_type_to_sql("Categorical")
        self.assertEqual(cat_t.__class__.__name__, "TEXT")

        str_t = map_logical_type_to_sql("String/Text")
        self.assertEqual(str_t.__class__.__name__, "TEXT")

        # Unknown / arbitrary fallback
        unknown_t = map_logical_type_to_sql("ComplexObject")
        self.assertEqual(unknown_t.__class__.__name__, "TEXT")

    # 3. Configuration Loading
    def test_03_load_database_config(self):
        """Test database configuration loading with safe defaults."""
        # Non-existent config
        cfg_default = load_database_config(self.test_path / "nonexistent.json")
        self.assertEqual(cfg_default.if_exists, "append")
        self.assertEqual(cfg_default.schema, "public")
        self.assertTrue(cfg_default.create_tables)
        self.assertFalse(cfg_default.auto_primary_key)
        self.assertEqual(cfg_default.batch_size, 1000)

        # Custom configuration
        custom_file = self.test_path / "custom_db.json"
        with open(custom_file, "w", encoding="utf-8") as f:
            json.dump({
                "if_exists": "replace",
                "schema": "analytics",
                "create_tables": True,
                "auto_primary_key": True,
                "batch_size": 250,
                "primary_keys": {"orders": "order_id"},
            }, f)

        cfg_custom = load_database_config(custom_file)
        self.assertEqual(cfg_custom.if_exists, "replace")
        self.assertEqual(cfg_custom.schema, "analytics")
        self.assertEqual(cfg_custom.batch_size, 250)
        self.assertEqual(cfg_custom.primary_keys.get("orders"), "order_id")

    # 4. Database URL Masking & Security
    def test_04_mask_database_url(self):
        """Test that database URLs with credentials have passwords sanitized."""
        url_with_pass = "postgresql+psycopg2://dbuser:super_secret_password@db.server.com:5432/warehouse"
        masked = mask_database_url(url_with_pass)
        self.assertNotIn("super_secret_password", masked)
        self.assertIn("dbuser:***@db.server.com:5432/warehouse", masked)

        # URL without password
        url_no_pass = "sqlite:///:memory:"
        self.assertEqual(mask_database_url(url_no_pass), "sqlite:///:memory:")

        # None / empty
        self.assertEqual(mask_database_url(None), "[UNCONFIGURED]")
        self.assertEqual(mask_database_url(""), "[UNCONFIGURED]")

    # 5. Missing Database URL Handling
    def test_05_missing_database_url_handling(self):
        """Test graceful handling when DATABASE_URL is not set."""
        orig_env = os.environ.get("DATABASE_URL")
        try:
            engine, err = get_database_engine(database_url=None)
            # Note: If no .env is providing DATABASE_URL, this will return None, err
            # Test directly passing empty string
            engine_empty, err_empty = get_database_engine(database_url="")
            self.assertIsNone(engine_empty)
            self.assertIn("DATABASE_URL is not set", err_empty)

            # Test loading with None engine
            df = pd.DataFrame({"col_a": [1, 2, 3]})
            result = load_validated_dataset(df, "sales_validated.csv", engine=None)
            self.assertEqual(result.database_status, "CONFIG_MISSING")
            self.assertEqual(result.rows_attempted, 3)
            self.assertEqual(result.rows_inserted, 0)
            self.assertEqual(result.rows_failed, 3)
            self.assertIn("DATABASE_URL missing", result.error_message)
        finally:
            if orig_env is not None:
                os.environ["DATABASE_URL"] = orig_env
            else:
                os.environ.pop("DATABASE_URL", None)

    # 6. Database Connection Failure Handling
    def test_06_connection_failure_handling(self):
        """Test graceful failure when database server is unreachable."""
        # Unreachable host with 1s timeout
        engine = create_engine(
            "postgresql+psycopg2://user:dummy_pass@127.0.0.1:59999/dummy_db",
            connect_args={"connect_timeout": 1},
        )
        connected, err = test_database_connection(engine)
        self.assertFalse(connected)
        self.assertIsNotNone(err)
        # Verify credentials are not exposed in connection error
        self.assertNotIn("dummy_pass", str(err))

        # Dataset load returns DATABASE_UNAVAILABLE without crashing
        df = pd.DataFrame({"a": [1, 2]})
        res = load_validated_dataset(df, "test_file.csv", engine=engine)
        self.assertEqual(res.database_status, "DATABASE_UNAVAILABLE")
        self.assertEqual(res.rows_inserted, 0)
        self.assertEqual(res.rows_failed, 2)
        self.assertIn("Database connection failed", res.error_message)

    # 7. In-Memory SQLite Table Creation and Loading
    def test_07_in_memory_table_creation_and_loading(self):
        """Test dynamic table creation and data loading using in-memory database."""
        df = pd.DataFrame({
            "id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 88.0, 92.3],
            "is_active": [True, False, True],
        })

        res = load_validated_dataset(
            df=df,
            source_filename="student_evaluations_validated.csv",
            engine=self.sqlite_engine,
        )

        self.assertEqual(res.database_status, "SUCCESS")
        self.assertEqual(res.connection_status, "CONNECTED")
        self.assertEqual(res.target_table, "student_evaluations")
        self.assertEqual(res.rows_attempted, 3)
        self.assertEqual(res.rows_inserted, 3)
        self.assertEqual(res.rows_failed, 0)

        # Verify through schema inspection
        schema_info = inspect_table_schema(self.sqlite_engine, "student_evaluations")
        self.assertTrue(schema_info["exists"])
        self.assertEqual(schema_info["row_count"], 3)
        col_names = [c["name"] for c in schema_info["columns"]]
        self.assertEqual(col_names, ["id", "name", "score", "is_active"])

    # 8. Existing Table Behavior - Append Mode
    def test_08_existing_table_behavior_append(self):
        """Test append mode when table already exists."""
        df1 = pd.DataFrame({"item_id": [1, 2], "qty": [10, 20]})
        df2 = pd.DataFrame({"item_id": [3, 4], "qty": [30, 40]})

        cfg = DatabaseConfig(if_exists="append")
        res1 = load_validated_dataset(df1, "items_validated.csv", self.sqlite_engine, config=cfg)
        res2 = load_validated_dataset(df2, "items_validated.csv", self.sqlite_engine, config=cfg)

        self.assertEqual(res1.rows_inserted, 2)
        self.assertEqual(res2.rows_inserted, 2)

        schema_info = inspect_table_schema(self.sqlite_engine, "items")
        self.assertEqual(schema_info["row_count"], 4)

    # 9. Existing Table Behavior - Replace Mode
    def test_09_existing_table_behavior_replace(self):
        """Test replace mode when table already exists."""
        df1 = pd.DataFrame({"val": [1, 2, 3, 4]})
        df2 = pd.DataFrame({"val": [10, 20]})

        cfg_append = DatabaseConfig(if_exists="append")
        cfg_replace = DatabaseConfig(if_exists="replace")

        load_validated_dataset(df1, "replace_test_validated.csv", self.sqlite_engine, config=cfg_append)
        schema_info_before = inspect_table_schema(self.sqlite_engine, "replace_test")
        self.assertEqual(schema_info_before["row_count"], 4)

        load_validated_dataset(df2, "replace_test_validated.csv", self.sqlite_engine, config=cfg_replace)
        schema_info_after = inspect_table_schema(self.sqlite_engine, "replace_test")
        self.assertEqual(schema_info_after["row_count"], 2)

    # 10. Existing Table Behavior - Fail Mode
    def test_10_existing_table_behavior_fail(self):
        """Test fail mode when table already exists."""
        df1 = pd.DataFrame({"val": [1, 2]})
        df2 = pd.DataFrame({"val": [3, 4]})

        cfg_fail = DatabaseConfig(if_exists="fail")
        load_validated_dataset(df1, "fail_test_validated.csv", self.sqlite_engine, config=cfg_fail)

        # Second load should cleanly report LOAD_FAILED
        res = load_validated_dataset(df2, "fail_test_validated.csv", self.sqlite_engine, config=cfg_fail)
        self.assertEqual(res.database_status, "LOAD_FAILED")
        self.assertEqual(res.rows_inserted, 0)
        self.assertEqual(res.rows_failed, 2)
        self.assertIsNotNone(res.error_message)

    # 11. Schema Inspection for Non-Existent and Existent Tables
    def test_11_schema_inspection(self):
        """Test schema inspection on existing and non-existing tables."""
        not_found = inspect_table_schema(self.sqlite_engine, "non_existent_table")
        self.assertFalse(not_found["exists"])
        self.assertEqual(not_found["columns"], [])
        self.assertEqual(not_found["row_count"], 0)

        df = pd.DataFrame({"col1": ["val1"], "col2": [123]})
        load_validated_dataset(df, "inspection_table_validated.csv", self.sqlite_engine)

        found = inspect_table_schema(self.sqlite_engine, "inspection_table")
        self.assertTrue(found["exists"])
        self.assertEqual(len(found["columns"]), 2)
        self.assertEqual(found["row_count"], 1)

    # 12. Database Load Reports Generation (JSON & CSV)
    def test_12_load_report_generation(self):
        """Test generating JSON and CSV database load reports."""
        results = [
            DatabaseLoadResult(
                source_dataset="sales_validated.csv",
                target_table="sales",
                database_status="SUCCESS",
                connection_status="CONNECTED",
                rows_attempted=100,
                rows_inserted=100,
                rows_failed=0,
                columns_created=["id", "amount"],
                sql_types={"id": "BIGINT", "amount": "FLOAT"},
                load_mode="append",
                duration_seconds=0.045,
            ),
            DatabaseLoadResult(
                source_dataset="employees_validated.csv",
                target_table="employees",
                database_status="DATABASE_UNAVAILABLE",
                connection_status="FAILED",
                rows_attempted=50,
                rows_inserted=0,
                rows_failed=50,
                error_message="Host unreachable",
            ),
        ]

        reports_dir = self.test_path / "reports"
        json_path, csv_path = save_database_load_reports(results, reports_dir=reports_dir)

        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())

        # Validate JSON content
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["database_status"], "SUCCESS")
        self.assertEqual(data[1]["database_status"], "DATABASE_UNAVAILABLE")

        # Validate CSV content
        df_csv = pd.read_csv(csv_path)
        self.assertEqual(len(df_csv), 2)
        self.assertEqual(df_csv.iloc[0]["rows_inserted"], 100)
        self.assertEqual(df_csv.iloc[1]["rows_failed"], 50)

    # 13. Audit Logging
    def test_13_database_audit_logging(self):
        """Test appending database outcomes to logs/database.log."""
        log_dir = self.test_path / "logs"
        res = DatabaseLoadResult(
            source_dataset="test_log.csv",
            target_table="test_log",
            database_status="SUCCESS",
            connection_status="CONNECTED",
            rows_attempted=10,
            rows_inserted=10,
            rows_failed=0,
            load_mode="append",
            duration_seconds=0.012,
        )

        log_database_event(res, log_dir=log_dir)
        log_file = log_dir / "database.log"
        self.assertTrue(log_file.exists())

        with open(log_file, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("[SUCCESS]", content)
        self.assertIn("Dataset: test_log.csv -> Table: test_log", content)
        self.assertIn("Rows: 10/10 inserted", content)

    # 14. Empty DataFrame Handling
    def test_14_empty_dataframe_handling(self):
        """Test that empty DataFrames are handled cleanly without errors."""
        empty_df = pd.DataFrame(columns=["empty_id", "empty_label"])
        res = load_validated_dataset(
            df=empty_df,
            source_filename="empty_dataset_validated.csv",
            engine=self.sqlite_engine,
        )

        self.assertEqual(res.database_status, "SUCCESS")
        self.assertEqual(res.rows_attempted, 0)
        self.assertEqual(res.rows_inserted, 0)
        self.assertEqual(res.rows_failed, 0)

        schema_info = inspect_table_schema(self.sqlite_engine, "empty_dataset")
        self.assertTrue(schema_info["exists"])
        self.assertEqual(schema_info["row_count"], 0)

    # 15. Primary Key Strategy
    def test_15_primary_key_strategy(self):
        """Test configurable primary key assignment."""
        df = pd.DataFrame({"pk_col": [1, 2, 3], "data_col": ["a", "b", "c"]})

        # Explicit configuration
        cfg = DatabaseConfig(primary_keys={"pk_test": "pk_col"})
        res = load_validated_dataset(df, "pk_test_validated.csv", self.sqlite_engine, config=cfg)
        self.assertEqual(res.database_status, "SUCCESS")

        schema_info = inspect_table_schema(self.sqlite_engine, "pk_test")
        self.assertIn("pk_col", schema_info["primary_keys"])

    # 16. Dynamic Dataset Test (Requirement 19)
    def test_16_dynamic_dataset_arbitrary_schema(self):
        """
        Prove system handles a completely new dataset schema with arbitrary columns.

        Dataset fields: device_name, reading, recorded_at, active
        Verifies:
        - Table name derived dynamically
        - Columns mapped dynamically to SQL types
        - Table created and data loaded without hardcoded domain knowledge
        """
        arbitrary_data = pd.DataFrame({
            "device_name": ["Sensor-A101", "Sensor-B202", "Sensor-C303"],
            "reading": [23.45, 24.12, 22.89],
            "recorded_at": ["2026-10-01 10:00:00", "2026-10-01 10:01:00", "2026-10-01 10:02:00"],
            "active": [True, True, False],
        })

        # Profile dataset using Stage 3 schema profiling engine
        dataset_profile = profile_dataset(
            df=arbitrary_data,
            filename="iot_telemetry_validated.csv",
        )

        res = load_validated_dataset(
            df=arbitrary_data,
            source_filename="iot_telemetry_validated.csv",
            engine=self.sqlite_engine,
            schema_profile=dataset_profile,
        )

        self.assertEqual(res.database_status, "SUCCESS")
        self.assertEqual(res.target_table, "iot_telemetry")
        self.assertEqual(res.rows_inserted, 3)

        # Inspect resulting table in SQLite
        inspection = inspect_table_schema(self.sqlite_engine, "iot_telemetry")
        self.assertTrue(inspection["exists"])
        self.assertEqual(inspection["row_count"], 3)
        col_names = [col["name"] for col in inspection["columns"]]
        self.assertEqual(col_names, ["device_name", "reading", "recorded_at", "active"])

    # 17. Terminal Summary Formatting
    def test_17_summary_formatting(self):
        """Test formatting of database load outcomes for display."""
        success_res = DatabaseLoadResult(
            source_dataset="sales_validated.csv",
            target_table="sales",
            database_status="SUCCESS",
            connection_status="CONNECTED",
            rows_attempted=50,
            rows_inserted=50,
            rows_failed=0,
            columns_created=["id", "amt"],
            load_mode="append",
            duration_seconds=0.02,
        )
        summary = format_database_summary(success_res)
        self.assertIn("Target Table: 'sales'", summary)
        self.assertIn("Rows Inserted: 50 / 50", summary)

        unavail_res = DatabaseLoadResult(
            source_dataset="employees_validated.csv",
            target_table="employees",
            database_status="DATABASE_UNAVAILABLE",
            connection_status="FAILED",
            rows_attempted=20,
            rows_inserted=0,
            rows_failed=20,
            error_message="Host unreachable",
        )
        summary_unavail = format_database_summary(unavail_res)
        self.assertIn("DATABASE_UNAVAILABLE", summary_unavail)
        self.assertIn("Host unreachable", summary_unavail)

    # 18. Multiple Dataset Isolation
    def test_18_multiple_dataset_isolation(self):
        """Test that multiple different datasets load into distinct isolated tables."""
        df_alpha = pd.DataFrame({"alpha_key": [1, 2], "metric": [10.5, 20.5]})
        df_beta = pd.DataFrame({"beta_code": ["B1", "B2"], "status": ["OK", "WARN"]})

        res_alpha = load_validated_dataset(df_alpha, "dataset_alpha_validated.csv", self.sqlite_engine)
        res_beta = load_validated_dataset(df_beta, "dataset_beta_validated.csv", self.sqlite_engine)

        self.assertEqual(res_alpha.target_table, "dataset_alpha")
        self.assertEqual(res_beta.target_table, "dataset_beta")

        alpha_info = inspect_table_schema(self.sqlite_engine, "dataset_alpha")
        beta_info = inspect_table_schema(self.sqlite_engine, "dataset_beta")

        self.assertTrue(alpha_info["exists"])
        self.assertTrue(beta_info["exists"])
        self.assertEqual(alpha_info["row_count"], 2)
        self.assertEqual(beta_info["row_count"], 2)

    # 19. Real PostgreSQL Integration Test (Conditional)
    def test_19_conditional_real_postgres(self):
        """
        Integration test against a live PostgreSQL server if configured.

        Skipped automatically if no reachable PostgreSQL instance is present.
        """
        test_url = os.getenv("TEST_DATABASE_URL")
        if not test_url:
            self.skipTest("TEST_DATABASE_URL environment variable not configured. Skipping live PostgreSQL test.")

        engine, err = get_database_engine(test_url)
        if engine is None:
            self.skipTest(f"Failed to create test PostgreSQL engine: {err}")

        connected, conn_err = test_database_connection(engine)
        if not connected:
            self.skipTest(f"PostgreSQL test instance unreachable: {conn_err}")

        df_pg = pd.DataFrame({"test_id": [1, 2], "val": ["x", "y"]})
        res = load_validated_dataset(df_pg, "pg_integration_test_validated.csv", engine)
        self.assertEqual(res.database_status, "SUCCESS")

    # 20. Dotenv Configuration Loading & DATABASE_URL Recognition Test
    def test_20_project_dotenv_loading_and_url_recognition(self):
        """Test that database configuration correctly recognizes DATABASE_URL from .env."""
        # Verify URL normalization helper
        self.assertEqual(
            normalize_database_url("postgresql://user:pass@localhost:5432/db"),
            "postgresql+psycopg2://user:pass@localhost:5432/db",
        )
        self.assertEqual(
            normalize_database_url("postgres://user:pass@localhost:5432/db"),
            "postgresql+psycopg2://user:pass@localhost:5432/db",
        )
        self.assertEqual(
            normalize_database_url("sqlite:///:memory:"),
            "sqlite:///:memory:",
        )

        # Isolated loading from an explicit .env file
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_env = Path(tmp_dir) / ".env"
            temp_env.write_text(
                "DATABASE_URL=postgresql://custom_user:custom_secret@127.0.0.1:5432/custom_db\n",
                encoding="utf-8",
            )
            # Temporarily clear env to verify dotenv file parsing
            orig_env = os.environ.pop("DATABASE_URL", None)
            try:
                loaded_url = get_database_url(dotenv_path=temp_env)
                self.assertIsNotNone(loaded_url)
                self.assertTrue(loaded_url.startswith("postgresql+psycopg2://"))
                self.assertIn("custom_user", loaded_url)
                self.assertIn("127.0.0.1:5432/custom_db", loaded_url)

                # Verify masking hides password
                masked = mask_database_url(loaded_url)
                self.assertNotIn("custom_secret", masked)
                self.assertIn("custom_user:***@127.0.0.1:5432/custom_db", masked)
            finally:
                if orig_env is not None:
                    os.environ["DATABASE_URL"] = orig_env
                else:
                    os.environ.pop("DATABASE_URL", None)

        # Recognition of project root .env when present
        project_root = Path(__file__).resolve().parent.parent
        root_env = project_root / ".env"
        if root_env.exists():
            orig_root_env = os.environ.get("DATABASE_URL")
            try:
                root_url = get_database_url(dotenv_path=root_env)
                self.assertIsNotNone(root_url, "Project root .env is present but get_database_url returned None")
                self.assertTrue(
                    root_url.startswith("postgresql+psycopg2://") or root_url.startswith("sqlite://"),
                    f"Unexpected database URL scheme: {root_url.split('://')[0]}",
                )
                # Verify SQLAlchemy engine can be initialized cleanly
                engine, err = get_database_engine(dotenv_path=root_env)
                self.assertIsNotNone(engine, f"Engine should initialize from project .env, got error: {err}")
                self.assertIsNone(err)
                if engine:
                    engine.dispose()
            finally:
                if orig_root_env is not None:
                    os.environ["DATABASE_URL"] = orig_root_env
                else:
                    os.environ.pop("DATABASE_URL", None)


if __name__ == "__main__":
    unittest.main()
