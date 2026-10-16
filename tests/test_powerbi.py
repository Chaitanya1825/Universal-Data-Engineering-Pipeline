"""
Comprehensive Unit Tests for Stage 10 Power BI Integration & BI Layer.

Tests:
1. Configuration loading
2. Invalid configuration rejection
3. PostgreSQL unavailable handling
4. Dynamic table discovery
5. Dynamic column discovery
6. Numeric role detection
7. Categorical role detection
8. Datetime role detection
9. Boolean role detection
10. Identifier role detection
11. Foreign-key discovery
12. No fabricated relationships
13. Semantic model generation
14. Empty schema handling
15. Generic DAX generation
16. Visualization recommendation for numeric data
17. Visualization recommendation for categorical + numeric data
18. Visualization recommendation for datetime + numeric data
19. Missing required visualization fields validation
20. JSON report generation
21. CSV report generation
22. Arbitrary schema 1 (IoT/Sensors: device_name, reading, recorded_at, active)
23. Arbitrary schema 2 (Healthcare: patient_id, blood_pressure, admission_date, department)
24. Secret masking
25. Orchestrator integration
26. Database unavailable propagation
27. No hardcoded sample datasets
28. Invalid table/identifier handling
29. Configured table limit
30. Documentation & template generation
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, create_engine

from pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineStatus,
    StageExecutionRecord,
    StageStatus,
)
from pipeline.powerbi import (
    AnalyticalRole,
    ColumnMetadata,
    PowerBIConfig,
    PowerBIExecutionRecord,
    PowerBIIntegrationEngine,
    RelationshipMetadata,
    TableMetadata,
    VisualizationRecommendation,
    load_powerbi_config,
    log_powerbi_event,
    save_powerbi_reports,
)


class TestPowerBIIntegrationLayer(unittest.TestCase):
    """Test suite validating Power BI Integration Layer functionality."""

    def setUp(self):
        """Create isolated temporary directory and in-memory test database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "logs").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports" / "powerbi").mkdir(parents=True, exist_ok=True)
        (self.test_root / "powerbi").mkdir(parents=True, exist_ok=True)
        (self.test_root / "powerbi" / "dax").mkdir(parents=True, exist_ok=True)

        # In-memory SQLite engine for tests
        self.engine = create_engine("sqlite:///:memory:")

        # Create multi-table generic dataset
        metadata = MetaData()
        self.categories_table = Table(
            "dim_categories",
            metadata,
            Column("category_id", Integer, primary_key=True),
            Column("category_name", String(50), nullable=False),
            Column("is_active", Boolean, default=True),
        )
        self.transactions_table = Table(
            "fact_transactions",
            metadata,
            Column("transaction_id", Integer, primary_key=True),
            Column("category_id", Integer, ForeignKey("dim_categories.category_id"), nullable=False),
            Column("amount", Float, nullable=False),
            Column("units", Integer, nullable=False),
            Column("occurred_at", DateTime, nullable=False),
            Column("status", String(20), nullable=False),
        )
        metadata.create_all(self.engine)

        # Insert sample rows
        with self.engine.begin() as conn:
            conn.execute(
                self.categories_table.insert(),
                [
                    {"category_id": 1, "category_name": "Alpha", "is_active": True},
                    {"category_id": 2, "category_name": "Beta", "is_active": True},
                ],
            )
            conn.execute(
                self.transactions_table.insert(),
                [
                    {
                        "transaction_id": 101,
                        "category_id": 1,
                        "amount": 99.5,
                        "units": 2,
                        "occurred_at": datetime(2026, 3, 1, 10, 0, 0),
                        "status": "APPROVED",
                    },
                    {
                        "transaction_id": 102,
                        "category_id": 2,
                        "amount": 149.0,
                        "units": 1,
                        "occurred_at": datetime(2026, 3, 2, 12, 30, 0),
                        "status": "PENDING",
                    },
                ],
            )

        self.pbi_engine = PowerBIIntegrationEngine(
            engine=self.engine,
            powerbi_dir=self.test_root / "powerbi",
        )

    def tearDown(self):
        """Clean up temporary test directory."""
        self.temp_dir.cleanup()

    # 1. Configuration Loading
    def test_01_configuration_loading(self):
        """Verify load_powerbi_config parses valid settings."""
        cfg_file = self.test_root / "config" / "powerbi.json"
        cfg_file.write_text(
            json.dumps({
                "enabled": True,
                "connection_mode": "directquery",
                "max_tables": 25,
            }),
            encoding="utf-8",
        )
        cfg, err = load_powerbi_config(cfg_file)
        self.assertIsNone(err)
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.connection_mode, "directquery")
        self.assertEqual(cfg.max_tables, 25)

    # 2. Invalid Configuration Rejection
    def test_02_invalid_configuration(self):
        """Verify corrupted or invalid configuration returns clean error."""
        cfg_file = self.test_root / "config" / "bad_powerbi.json"

        # Invalid connection mode
        cfg_file.write_text('{"connection_mode": "invalid_mode"}', encoding="utf-8")
        cfg, err = load_powerbi_config(cfg_file)
        self.assertIsNotNone(err)
        self.assertIn("Unsupported connection_mode", err)

        # Negative max_tables
        cfg_file.write_text('{"max_tables": -5}', encoding="utf-8")
        cfg, err = load_powerbi_config(cfg_file)
        self.assertIsNotNone(err)
        self.assertIn("max_tables", err)

    # 3. PostgreSQL Unavailable Handling
    def test_03_database_unavailable_handling(self):
        """Verify engine gracefully yields DATABASE_UNAVAILABLE when engine is None."""
        offline_engine = PowerBIIntegrationEngine(engine=None, powerbi_dir=self.test_root / "powerbi")
        with patch.object(offline_engine, "connect", return_value=(False, "Connection refused")):
            rec, semantic, dash = offline_engine.run_integration_suite(run_id="run_offline")
            self.assertEqual(rec.status, "DATABASE_UNAVAILABLE")
            self.assertEqual(semantic["status"], "DATABASE_UNAVAILABLE")
            self.assertEqual(dash["status"], "DATABASE_UNAVAILABLE")
            self.assertEqual(rec.tables_discovered, 0)

    # 4. Dynamic Table Discovery
    def test_04_dynamic_table_discovery(self):
        """Verify tables are discovered dynamically from database catalog."""
        tables = self.pbi_engine.discover_tables()
        self.assertIn("dim_categories", tables)
        self.assertIn("fact_transactions", tables)

    # 5. Dynamic Column Discovery
    def test_05_dynamic_column_discovery(self):
        """Verify column names, types, nullability, and primary keys are retrieved."""
        tbl_meta = self.pbi_engine.inspect_table("fact_transactions")
        self.assertEqual(tbl_meta.table_name, "fact_transactions")
        self.assertEqual(tbl_meta.row_count, 2)
        col_names = [c.name for c in tbl_meta.columns]
        self.assertIn("transaction_id", col_names)
        self.assertIn("amount", col_names)
        self.assertIn("occurred_at", col_names)

        tx_col = next(c for c in tbl_meta.columns if c.name == "transaction_id")
        self.assertTrue(tx_col.primary_key)

    # 6. Numeric Role Detection
    def test_06_numeric_role_detection(self):
        """Verify numeric metric columns receive NUMERIC_METRIC analytical role."""
        role = self.pbi_engine.classify_column_role("reading_value", "FLOAT", is_pk=False)
        self.assertEqual(role, AnalyticalRole.NUMERIC_METRIC.value)

        role2 = self.pbi_engine.classify_column_role("amount", "DOUBLE PRECISION", is_pk=False)
        self.assertEqual(role2, AnalyticalRole.NUMERIC_METRIC.value)

    # 7. Categorical Role Detection
    def test_07_categorical_role_detection(self):
        """Verify text/varchar category fields receive CATEGORICAL_DIMENSION."""
        role = self.pbi_engine.classify_column_role("department", "VARCHAR(50)", is_pk=False)
        self.assertEqual(role, AnalyticalRole.CATEGORICAL_DIMENSION.value)

        role2 = self.pbi_engine.classify_column_role("status", "TEXT", is_pk=False)
        self.assertEqual(role2, AnalyticalRole.CATEGORICAL_DIMENSION.value)

    # 8. Datetime Role Detection
    def test_08_datetime_role_detection(self):
        """Verify dates and timestamps receive DATETIME_DIMENSION."""
        role = self.pbi_engine.classify_column_role("created_at", "TIMESTAMP", is_pk=False)
        self.assertEqual(role, AnalyticalRole.DATETIME_DIMENSION.value)

        role2 = self.pbi_engine.classify_column_role("event_date", "DATE", is_pk=False)
        self.assertEqual(role2, AnalyticalRole.DATETIME_DIMENSION.value)

    # 9. Boolean Role Detection
    def test_09_boolean_role_detection(self):
        """Verify boolean columns and binary flags receive BOOLEAN_DIMENSION."""
        role = self.pbi_engine.classify_column_role("is_active", "BOOLEAN", is_pk=False)
        self.assertEqual(role, AnalyticalRole.BOOLEAN_DIMENSION.value)

        role2 = self.pbi_engine.classify_column_role("active", "INTEGER", is_pk=False)
        self.assertEqual(role2, AnalyticalRole.BOOLEAN_DIMENSION.value)

    # 10. Identifier Role Detection
    def test_10_identifier_role_detection(self):
        """Verify primary keys and key suffixes receive IDENTIFIER."""
        role_pk = self.pbi_engine.classify_column_role("any_col", "INTEGER", is_pk=True)
        self.assertEqual(role_pk, AnalyticalRole.IDENTIFIER.value)

        role_suffix = self.pbi_engine.classify_column_role("device_id", "INTEGER", is_pk=False)
        self.assertEqual(role_suffix, AnalyticalRole.IDENTIFIER.value)

    # 11. Foreign-Key Discovery
    def test_11_foreign_key_discovery(self):
        """Verify foreign-key constraints are detected between related tables."""
        tables_meta = [
            self.pbi_engine.inspect_table("dim_categories"),
            self.pbi_engine.inspect_table("fact_transactions"),
        ]
        rels = self.pbi_engine.discover_relationships(tables_meta)
        self.assertTrue(len(rels) >= 1)
        rel = rels[0]
        self.assertEqual(rel.parent_table, "dim_categories")
        self.assertEqual(rel.child_table, "fact_transactions")
        self.assertEqual(rel.status, "CONFIRMED")

    # 12. No Fabricated Relationships
    def test_12_no_fabricated_relationships(self):
        """Verify relationships are not fabricated when no explicit foreign keys exist."""
        # Standalone table with no FK
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        df.to_sql("isolated_table", con=self.engine, if_exists="replace", index=False)
        t_meta = self.pbi_engine.inspect_table("isolated_table")

        rels = self.pbi_engine.discover_relationships([t_meta])
        self.assertEqual(rels, [])

        model = self.pbi_engine.build_semantic_model([t_meta], rels)
        self.assertEqual(model["relationships"], [])
        self.assertIn("No explicit PostgreSQL foreign-key relationships discovered", model["relationship_note"])

    # 13. Semantic Model Generation
    def test_13_semantic_model_generation(self):
        """Verify semantic model dictionary contains tables, measures, and dimensions."""
        tables_meta = [
            self.pbi_engine.inspect_table("dim_categories"),
            self.pbi_engine.inspect_table("fact_transactions"),
        ]
        rels = self.pbi_engine.discover_relationships(tables_meta)
        model = self.pbi_engine.build_semantic_model(tables_meta, rels)

        self.assertEqual(model["model_type"], "dynamic")
        self.assertEqual(model["status"], "SUCCESS")
        self.assertEqual(len(model["tables"]), 2)
        self.assertTrue(len(model["measures"]) >= 1)
        self.assertTrue(len(model["dimensions"]) >= 1)

    # 14. Empty Schema Handling
    def test_14_empty_schema_handling(self):
        """Verify empty database returns clean empty semantic model without errors."""
        empty_eng = create_engine("sqlite:///:memory:")
        empty_pbi = PowerBIIntegrationEngine(engine=empty_eng)
        rec, model, dash = empty_pbi.run_integration_suite(
            output_dir=self.test_root / "powerbi",
            run_id="run_empty",
        )
        self.assertEqual(rec.status, "SUCCESS")
        self.assertEqual(rec.tables_discovered, 0)
        self.assertEqual(model["tables"], [])
        self.assertEqual(dash["recommendations"], [])

    # 15. Generic DAX Generation
    def test_15_generic_dax_generation(self):
        """Verify generated DAX measures match table columns with valid syntax."""
        t_meta = self.pbi_engine.inspect_table("fact_transactions")
        measures = self.pbi_engine.generate_dax_templates_for_table(t_meta)

        dax_text = "\n".join(m["dax"] for m in measures)
        self.assertIn("Total fact_transactions Rows = COUNTROWS('fact_transactions')", dax_text)
        self.assertIn("Total Amount = SUM('fact_transactions'[amount])", dax_text)
        self.assertIn("Avg Amount = AVERAGE('fact_transactions'[amount])", dax_text)
        self.assertIn("Distinct Transaction Id Count = DISTINCTCOUNT('fact_transactions'[transaction_id])", dax_text)

    # 16. Visualization Recommendation for Numeric Data (KPI Card)
    def test_16_vis_rec_numeric_kpi(self):
        """Verify numeric metrics generate KPI card recommendations."""
        t_meta = self.pbi_engine.inspect_table("fact_transactions")
        recs = self.pbi_engine.generate_visualization_recommendations([t_meta])

        kpis = [r for r in recs if r.visualization_type == "kpi_card"]
        self.assertTrue(len(kpis) >= 1)
        kpi = kpis[0]
        self.assertEqual(kpi.target_table, "fact_transactions")
        self.assertIn("amount", kpi.required_columns)
        self.assertIn("measure", kpi.fields)

    # 17. Visualization Recommendation for Categorical + Numeric
    def test_17_vis_rec_categorical_numeric(self):
        """Verify combination of category and metric generates column chart."""
        t_meta = self.pbi_engine.inspect_table("fact_transactions")
        recs = self.pbi_engine.generate_visualization_recommendations([t_meta])

        col_charts = [r for r in recs if r.visualization_type == "column_chart"]
        self.assertTrue(len(col_charts) >= 1)
        chart = col_charts[0]
        self.assertIn("category", chart.fields)
        self.assertIn("values", chart.fields)

    # 18. Visualization Recommendation for Datetime + Numeric
    def test_18_vis_rec_datetime_numeric(self):
        """Verify combination of datetime and metric generates line trend chart."""
        t_meta = self.pbi_engine.inspect_table("fact_transactions")
        recs = self.pbi_engine.generate_visualization_recommendations([t_meta])

        line_charts = [r for r in recs if r.visualization_type == "line_chart"]
        self.assertTrue(len(line_charts) >= 1)
        trend = line_charts[0]
        self.assertIn("axis", trend.fields)
        self.assertIn("occurred_at", trend.required_columns)

    # 19. Missing Required Visualization Fields Validation
    def test_19_missing_required_vis_fields(self):
        """Verify recommendations are not produced if required columns are absent."""
        t_meta = TableMetadata(
            table_name="empty_table",
            schema="public",
            row_count=0,
            table_type="TABULAR",
            columns=[],  # No columns
        )
        recs = self.pbi_engine.generate_visualization_recommendations([t_meta])
        self.assertEqual(recs, [])

    # 20. JSON Report Generation
    def test_20_json_report_generation(self):
        """Verify save_powerbi_reports outputs valid JSON audit file."""
        rec = PowerBIExecutionRecord(
            run_id="run_101",
            status="SUCCESS",
            database_status="CONNECTED",
            tables_discovered=2,
            columns_discovered=9,
            relationships_discovered=1,
            measures_generated=5,
            visualizations_recommended=4,
            semantic_model_generated=True,
            documentation_generated=True,
            execution_time=0.05,
        )
        json_path, csv_path = save_powerbi_reports(
            rec, reports_dir=self.test_root / "reports" / "powerbi", run_id="run_101"
        )
        self.assertTrue(json_path.exists())
        data = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(data[0]["run_id"], "run_101")
        self.assertEqual(data[0]["status"], "SUCCESS")

    # 21. CSV Report Generation
    def test_21_csv_report_generation(self):
        """Verify save_powerbi_reports outputs valid CSV file with expected columns."""
        rec = PowerBIExecutionRecord(
            run_id="run_102",
            status="SUCCESS",
            database_status="CONNECTED",
            tables_discovered=1,
            columns_discovered=4,
            relationships_discovered=0,
            measures_generated=3,
            visualizations_recommended=2,
            semantic_model_generated=True,
            documentation_generated=True,
            execution_time=0.03,
        )
        json_path, csv_path = save_powerbi_reports(
            rec, reports_dir=self.test_root / "reports" / "powerbi", run_id="run_102"
        )
        self.assertTrue(csv_path.exists())
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run_102")
            self.assertEqual(rows[0]["status"], "SUCCESS")

    # 22. Arbitrary Schema 1 (IoT/Sensors)
    def test_22_arbitrary_schema_1(self):
        """Verify Power BI engine dynamically handles an arbitrary IoT schema."""
        iot_df = pd.DataFrame({
            "device_name": ["Sensor_A", "Sensor_B", "Sensor_A"],
            "reading": [24.5, 31.0, 25.2],
            "recorded_at": pd.to_datetime(["2026-03-01 00:00:00", "2026-03-01 01:00:00", "2026-03-01 02:00:00"]),
            "active": [True, True, False],
        })
        iot_df.to_sql("sensor_telemetry", con=self.engine, if_exists="replace", index=False)

        t_meta = self.pbi_engine.inspect_table("sensor_telemetry")
        self.assertEqual(t_meta.table_name, "sensor_telemetry")
        roles = {c.name: c.analytical_role for c in t_meta.columns}
        self.assertEqual(roles["device_name"], AnalyticalRole.CATEGORICAL_DIMENSION.value)
        self.assertEqual(roles["reading"], AnalyticalRole.NUMERIC_METRIC.value)
        self.assertEqual(roles["recorded_at"], AnalyticalRole.DATETIME_DIMENSION.value)
        self.assertEqual(roles["active"], AnalyticalRole.BOOLEAN_DIMENSION.value)

        recs = self.pbi_engine.generate_visualization_recommendations([t_meta])
        rec_types = {r.visualization_type for r in recs}
        self.assertIn("kpi_card", rec_types)
        self.assertIn("line_chart", rec_types)
        self.assertIn("column_chart", rec_types)

    # 23. Arbitrary Schema 2 (Healthcare)
    def test_23_arbitrary_schema_2(self):
        """Verify Power BI engine dynamically handles an arbitrary Healthcare schema."""
        med_df = pd.DataFrame({
            "patient_id": [1001, 1002, 1003],
            "blood_pressure": [120, 135, 118],
            "admission_date": pd.to_datetime(["2026-02-10", "2026-02-11", "2026-02-12"]),
            "department": ["Cardiology", "Neurology", "Cardiology"],
        })
        med_df.to_sql("patient_admissions", con=self.engine, if_exists="replace", index=False)

        t_meta = self.pbi_engine.inspect_table("patient_admissions")
        roles = {c.name: c.analytical_role for c in t_meta.columns}
        self.assertEqual(roles["patient_id"], AnalyticalRole.IDENTIFIER.value)
        self.assertEqual(roles["blood_pressure"], AnalyticalRole.NUMERIC_METRIC.value)
        self.assertEqual(roles["admission_date"], AnalyticalRole.DATETIME_DIMENSION.value)
        self.assertEqual(roles["department"], AnalyticalRole.CATEGORICAL_DIMENSION.value)

    # 24. Secret Masking
    def test_24_secret_masking(self):
        """Verify logging masks database URLs and passwords."""
        rec = PowerBIExecutionRecord(
            run_id="run_leak_test",
            status="FAILED",
            database_status="ERROR",
            errors="Connection error to postgresql://user:super_secret_pw@localhost:5432/db",
        )
        log_powerbi_event(rec, log_dir=self.test_root / "logs")
        log_content = (self.test_root / "logs" / "powerbi.log").read_text(encoding="utf-8")
        self.assertNotIn("super_secret_pw", log_content)
        self.assertIn("***@", log_content)

    # 25. Orchestrator Integration
    def test_25_orchestrator_integration(self):
        """Verify orchestrator runs Stage 10 and reports DATABASE_UNAVAILABLE honestly when offline."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        raw_csv = self.test_root / "data" / "raw" / "items.csv"
        raw_csv.write_text("item_id,val\n1,10\n", encoding="utf-8")

        summary = orch.execute_pipeline(display_summary=False)
        stage_names = [s["stage_name"] for s in summary.stages]
        self.assertIn("powerbi", stage_names)

        pbi_st = next(s for s in summary.stages if s["stage_name"] == "powerbi")
        self.assertEqual(pbi_st["status"], StageStatus.DATABASE_UNAVAILABLE.value)

    # 26. Database Unavailable Propagation
    def test_26_database_unavailable_propagation(self):
        """Verify that when database dependency is DATABASE_UNAVAILABLE, powerbi cascades to DATABASE_UNAVAILABLE."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        # Simulate database stage ending with DATABASE_UNAVAILABLE
        orch.stage_results["database"] = StageExecutionRecord(
            run_id=orch.run_id,
            stage_name="database",
            status=StageStatus.DATABASE_UNAVAILABLE.value,
            start_time="",
            end_time="",
            duration_seconds=0.0,
        )
        can_run, pre_status, reason = orch.can_stage_run("powerbi")
        self.assertFalse(can_run)
        self.assertEqual(pre_status, StageStatus.DATABASE_UNAVAILABLE.value)

    # 27. No Hardcoded Sample Datasets
    def test_27_no_hardcoded_sample_datasets(self):
        """Verify engine contains no hardcoded references to sample sales, employees, or students."""
        import inspect as py_inspect
        from pipeline import powerbi as pbi_module

        src = py_inspect.getsource(pbi_module)
        self.assertNotIn("sample_sales", src)
        self.assertNotIn("sample_employees", src)
        self.assertNotIn("sample_students", src)

    # 28. Invalid Table / Identifier Handling
    def test_28_invalid_table_identifier_handling(self):
        """Verify engine rejects malicious or unsafe SQL table names."""
        with self.assertRaises(ValueError):
            self.pbi_engine.inspect_table("table; DROP TABLE users; --")

    # 29. Configured Table Limit
    def test_29_configured_table_limit(self):
        """Verify discover_tables respects max_tables limit."""
        cfg = PowerBIConfig(max_tables=1)
        limited_engine = PowerBIIntegrationEngine(engine=self.engine, config=cfg)
        tables = limited_engine.discover_tables()
        self.assertEqual(len(tables), 1)

    # 30. Documentation & Template Generation
    def test_30_documentation_generation(self):
        """Verify end-to-end execution generates semantic_model.json and dashboard_spec.json."""
        rec, model, dash = self.pbi_engine.run_integration_suite(
            output_dir=self.test_root / "powerbi",
            run_id="run_full_suite",
        )
        self.assertEqual(rec.status, "SUCCESS")
        self.assertTrue((self.test_root / "powerbi" / "semantic_model.json").exists())
        self.assertTrue((self.test_root / "powerbi" / "dashboard_spec.json").exists())
        self.assertTrue((self.test_root / "powerbi" / "dax" / "concrete_measures.dax").exists())


if __name__ == "__main__":
    unittest.main()
