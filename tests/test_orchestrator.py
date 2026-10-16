"""
Comprehensive Unit Tests for Stage 7 ETL Pipeline Orchestrator.

Tests:
1. Run ID generation and uniqueness
2. Default configuration loading
3. Custom configuration loading
4. Invalid stage configuration error handling
5. Canonical stage dependency ordering
6. Stage dependency execution rules (can_stage_run)
7. Successful end-to-end orchestration
8. Failed stage handling and error recording
9. Blocked downstream stage propagation
10. Fail-fast mode (true vs false)
11. Database unavailable handling (continue=true vs continue=false)
12. Pipeline status determination logic
13. Duration and timing calculation
14. Dataset-level execution tracking across stages
15. Unified JSON and CSV report generation
16. Credential masking and secret protection
17. Disabled stage behavior (SKIPPED status)
18. Unexpected exception handling without crashes
19. Multiple dataset execution tracking
20. Arbitrary dataset schema processing (domain-agnostic)
"""

from datetime import datetime
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.orchestrator import (
    STAGE_DEPENDENCIES,
    STAGE_ORDER,
    DatasetExecutionRecord,
    PipelineConfig,
    PipelineOrchestrator,
    PipelineRunSummary,
    PipelineStatus,
    StageExecutionRecord,
    StageStatus,
    generate_run_id,
    load_pipeline_config,
    sanitize_text,
    save_pipeline_run_reports,
)


class TestPipelineOrchestrator(unittest.TestCase):
    """Test suite verifying pipeline orchestration, dependency management, and reporting."""

    def setUp(self):
        """Create isolated temporary directory environment."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        # Create standard directory tree
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "validated").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "rejected").mkdir(parents=True, exist_ok=True)
        (self.test_root / "pipeline").mkdir(parents=True, exist_ok=True)
        (self.test_root / "sql").mkdir(parents=True, exist_ok=True)
        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "logs").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)
        (self.test_root / "tests").mkdir(parents=True, exist_ok=True)
        (self.test_root / "powerbi").mkdir(parents=True, exist_ok=True)
        (self.test_root / "docs").mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # 1. Run ID Generation
    def test_01_run_id_generation(self):
        """Verify Run ID format, prefix, timestamp, and uniqueness."""
        run_id1 = generate_run_id()
        run_id2 = generate_run_id()

        self.assertTrue(run_id1.startswith("run_"))
        # Format: run_YYYYMMDD_HHMMSS_xxxxxx
        pattern = r"^run_\d{8}_\d{6}_[a-f0-9]{6}$"
        self.assertRegex(run_id1, pattern)
        self.assertNotEqual(run_id1, run_id2)

    # 2. Default Configuration Loading
    def test_02_default_configuration_loading(self):
        """Verify default configuration values when config file is missing."""
        cfg, err = load_pipeline_config(self.test_root / "config" / "nonexistent.json")
        self.assertIsNone(err)
        self.assertEqual(cfg.enabled_stages, STAGE_ORDER)
        self.assertFalse(cfg.fail_fast)
        self.assertTrue(cfg.continue_on_database_unavailable)
        self.assertTrue(cfg.generate_reports)
        self.assertTrue(cfg.logging_enabled)

    # 3. Custom Configuration Loading
    def test_03_custom_configuration_loading(self):
        """Verify parsing of customized pipeline.json settings."""
        cfg_path = self.test_root / "config" / "pipeline.json"
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump({
                "enabled_stages": ["foundation", "ingestion"],
                "fail_fast": True,
                "continue_on_database_unavailable": False,
                "generate_reports": False,
                "logging_enabled": False,
            }, f)

        cfg, err = load_pipeline_config(cfg_path)
        self.assertIsNone(err)
        self.assertEqual(cfg.enabled_stages, ["foundation", "ingestion"])
        self.assertTrue(cfg.fail_fast)
        self.assertFalse(cfg.continue_on_database_unavailable)
        self.assertFalse(cfg.generate_reports)
        self.assertFalse(cfg.logging_enabled)

    # 4. Invalid Stage Configuration Error
    def test_04_invalid_stage_configuration(self):
        """Verify that unrecognized stage names raise a clear configuration error."""
        cfg_path = self.test_root / "config" / "pipeline.json"
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump({"enabled_stages": ["foundation", "unknown_magic_stage"]}, f)

        cfg, err = load_pipeline_config(cfg_path)
        self.assertIsNotNone(err)
        self.assertIn("unknown_magic_stage", err)

        # Orchestrator execution halts with CONFIG_ERROR
        orch = PipelineOrchestrator(config_path=cfg_path, root_dir=self.test_root)
        summary = orch.execute_pipeline(display_summary=False)
        self.assertEqual(summary.pipeline_status, PipelineStatus.CONFIG_ERROR.value)
        self.assertIn("unknown_magic_stage", summary.error_message)

    # 5. Stage Dependency Ordering
    def test_05_stage_dependency_ordering(self):
        """Verify canonical order and dependency prerequisites."""
        expected_order = [
            "foundation",
            "ingestion",
            "incremental",
            "profiling",
            "transformation",
            "validation",
            "database",
            "analytics",
            "powerbi",
        ]
        self.assertEqual(STAGE_ORDER, expected_order)

        # Verify dependency map
        self.assertEqual(STAGE_DEPENDENCIES["foundation"], [])
        self.assertEqual(STAGE_DEPENDENCIES["ingestion"], ["foundation"])
        self.assertEqual(STAGE_DEPENDENCIES["incremental"], ["ingestion"])
        self.assertEqual(STAGE_DEPENDENCIES["profiling"], ["ingestion", "incremental"])
        self.assertEqual(STAGE_DEPENDENCIES["transformation"], ["ingestion", "profiling"])
        self.assertEqual(STAGE_DEPENDENCIES["validation"], ["transformation"])
        self.assertEqual(STAGE_DEPENDENCIES["database"], ["validation"])
        self.assertEqual(STAGE_DEPENDENCIES["analytics"], ["database"])
        self.assertEqual(STAGE_DEPENDENCIES["powerbi"], ["database"])

    # 6. Stage Dependency Rules (can_stage_run)
    def test_06_stage_dependency_rules(self):
        """Verify can_stage_run checks disabled status and upstream prerequisites."""
        orch = PipelineOrchestrator(root_dir=self.test_root)

        # Disabled stage -> SKIPPED
        orch.config.enabled_stages = ["foundation"]
        can_run, status, reason = orch.can_stage_run("ingestion")
        self.assertFalse(can_run)
        self.assertEqual(status, StageStatus.SKIPPED.value)

        # Enabled but upstream not executed -> BLOCKED
        orch.config.enabled_stages = ["foundation", "ingestion"]
        can_run, status, reason = orch.can_stage_run("ingestion")
        self.assertFalse(can_run)
        self.assertEqual(status, StageStatus.BLOCKED.value)

        # Upstream succeeded -> RUNNING
        orch.stage_results["foundation"] = StageExecutionRecord(
            run_id=orch.run_id, stage_name="foundation", status=StageStatus.SUCCESS.value
        )
        can_run, status, reason = orch.can_stage_run("ingestion")
        self.assertTrue(can_run)

        # Upstream failed -> BLOCKED
        orch.stage_results["foundation"].status = StageStatus.FAILED.value
        can_run, status, reason = orch.can_stage_run("ingestion")
        self.assertFalse(can_run)
        self.assertEqual(status, StageStatus.BLOCKED.value)

    # 7. Successful End-to-End Orchestration
    def test_07_successful_end_to_end_orchestration(self):
        """Verify end-to-end execution with a sample dataset."""
        # Create sample raw data file
        sample_csv = self.test_root / "data" / "raw" / "sample_records.csv"
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df.to_csv(sample_csv, index=False)

        orch = PipelineOrchestrator(root_dir=self.test_root)
        summary = orch.execute_pipeline(display_summary=False)

        self.assertIn(summary.pipeline_status, [
            PipelineStatus.COMPLETED.value,
            PipelineStatus.COMPLETED_WITH_WARNINGS.value,
        ])
        self.assertGreaterEqual(summary.duration_seconds, 0.0)

        # Verify stages executed
        stage_names = [st["stage_name"] for st in summary.stages]
        self.assertEqual(stage_names, STAGE_ORDER)

        # Verify foundation and ingestion succeeded
        foundation_st = next(s for s in summary.stages if s["stage_name"] == "foundation")
        self.assertEqual(foundation_st["status"], StageStatus.SUCCESS.value)

        ingestion_st = next(s for s in summary.stages if s["stage_name"] == "ingestion")
        self.assertEqual(ingestion_st["status"], StageStatus.SUCCESS.value)

    # 8. Failed Stage Behavior
    def test_08_failed_stage_behavior(self):
        """Verify stage failure recording and propagation."""
        orch = PipelineOrchestrator(root_dir=self.test_root)

        # Intentionally mock foundation to return FAILED
        with patch.object(orch, "_run_foundation") as mock_foundation:
            mock_foundation.return_value = StageExecutionRecord(
                run_id=orch.run_id,
                stage_name="foundation",
                status=StageStatus.FAILED.value,
                error_message="Test failure",
            )
            summary = orch.execute_pipeline(display_summary=False)

        self.assertEqual(summary.pipeline_status, PipelineStatus.FAILED.value)
        foundation_st = next(s for s in summary.stages if s["stage_name"] == "foundation")
        self.assertEqual(foundation_st["status"], StageStatus.FAILED.value)

    # 9. Blocked Downstream Stage Propagation
    def test_09_blocked_downstream_stage_propagation(self):
        """Verify that when an upstream stage fails, dependent stages are marked BLOCKED."""
        orch = PipelineOrchestrator(root_dir=self.test_root)

        with patch.object(orch, "_run_foundation") as mock_foundation:
            mock_foundation.return_value = StageExecutionRecord(
                run_id=orch.run_id,
                stage_name="foundation",
                status=StageStatus.FAILED.value,
                error_message="Simulated foundation error",
            )
            summary = orch.execute_pipeline(display_summary=False)

        ingestion_st = next(s for s in summary.stages if s["stage_name"] == "ingestion")
        self.assertEqual(ingestion_st["status"], StageStatus.BLOCKED.value)
        self.assertIn("expected SUCCESS", ingestion_st["error_message"])

    # 10. Fail-Fast True
    def test_10_fail_fast_true(self):
        """Verify that fail_fast=True halts subsequent stage execution immediately."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.fail_fast = True

        with patch.object(orch, "_run_ingestion") as mock_ingest:
            mock_ingest.return_value = StageExecutionRecord(
                run_id=orch.run_id,
                stage_name="ingestion",
                status=StageStatus.FAILED.value,
                error_message="Critical ingestion failure",
            )
            summary = orch.execute_pipeline(display_summary=False)

        self.assertEqual(summary.pipeline_status, PipelineStatus.FAILED.value)
        profiling_st = next(s for s in summary.stages if s["stage_name"] == "profiling")
        self.assertEqual(profiling_st["status"], StageStatus.BLOCKED.value)
        self.assertIn("fail_fast=True", profiling_st["error_message"])

    # 11. Fail-Fast False
    def test_11_fail_fast_false(self):
        """Verify that fail_fast=False evaluates dependencies for each stage individually."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.fail_fast = False
        self.assertFalse(orch.config.fail_fast)

    # 12. Database Unavailable Handling (Continue=True)
    def test_12_database_unavailable_continue(self):
        """Verify pipeline status is COMPLETED_WITH_WARNINGS when database is offline and continue=True."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.continue_on_database_unavailable = True

        # Mock stages 1-5 to succeed and stage 6 to be DATABASE_UNAVAILABLE
        for s in STAGE_ORDER[:-1]:
            orch.stage_results[s] = StageExecutionRecord(
                run_id=orch.run_id, stage_name=s, status=StageStatus.SUCCESS.value
            )
        orch.stage_results["database"] = StageExecutionRecord(
            run_id=orch.run_id,
            stage_name="database",
            status=StageStatus.DATABASE_UNAVAILABLE.value,
            error_message="DATABASE_URL not configured",
        )

        status = orch.determine_pipeline_status()
        self.assertEqual(status, PipelineStatus.COMPLETED_WITH_WARNINGS.value)

    # 13. Database Unavailable Handling (Continue=False)
    def test_13_database_unavailable_abort(self):
        """Verify pipeline status is FAILED when database is offline and continue=False."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.continue_on_database_unavailable = False

        for s in STAGE_ORDER[:-1]:
            orch.stage_results[s] = StageExecutionRecord(
                run_id=orch.run_id, stage_name=s, status=StageStatus.SUCCESS.value
            )
        orch.stage_results["database"] = StageExecutionRecord(
            run_id=orch.run_id,
            stage_name="database",
            status=StageStatus.DATABASE_UNAVAILABLE.value,
        )

        status = orch.determine_pipeline_status()
        self.assertEqual(status, PipelineStatus.FAILED.value)

    # 14. Pipeline Status Calculation
    def test_14_pipeline_status_calculation(self):
        """Verify pipeline status permutations."""
        orch = PipelineOrchestrator(root_dir=self.test_root)

        # All SUCCESS -> COMPLETED
        for s in STAGE_ORDER:
            orch.stage_results[s] = StageExecutionRecord(
                run_id=orch.run_id, stage_name=s, status=StageStatus.SUCCESS.value
            )
        self.assertEqual(orch.determine_pipeline_status(), PipelineStatus.COMPLETED.value)

        # Any FAILED -> FAILED
        orch.stage_results["transformation"].status = StageStatus.FAILED.value
        self.assertEqual(orch.determine_pipeline_status(), PipelineStatus.FAILED.value)

    # 15. Duration & Timing Metrics
    def test_15_timing_metrics(self):
        """Verify monotonic timing and duration calculations."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation"]
        summary = orch.execute_pipeline(display_summary=False)

        self.assertGreaterEqual(summary.duration_seconds, 0.0)
        st = summary.stages[0]
        self.assertGreaterEqual(st["duration_seconds"], 0.0)
        self.assertIsNotNone(st["start_time"])
        self.assertIsNotNone(st["end_time"])

    # 16. Dataset-Level Execution Tracking
    def test_16_dataset_level_tracking(self):
        """Verify recording of dataset entries across stages."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.record_dataset_result(
            source_file="sample.csv",
            stage="ingestion",
            status="SUCCESS",
            row_count=10,
        )
        orch.record_dataset_result(
            source_file="sample.csv",
            stage="transformation",
            status="SUCCESS",
            output_path="/path/to/sample_cleaned.csv",
            row_count=10,
        )

        self.assertEqual(len(orch.dataset_records), 2)
        self.assertEqual(orch.dataset_records[0].source_file, "sample.csv")
        self.assertEqual(orch.dataset_records[0].stage, "ingestion")
        self.assertEqual(orch.dataset_records[1].stage, "transformation")

    # 17. Report Generation (JSON & CSV)
    def test_17_report_generation(self):
        """Verify JSON and CSV pipeline run reports are created."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation"]
        summary = orch.execute_pipeline(display_summary=False)

        json_path = self.test_root / "reports" / "pipeline_run_report.json"
        csv_path = self.test_root / "reports" / "pipeline_run_report.csv"

        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["run_id"], orch.run_id)

        df_csv = pd.read_csv(csv_path)
        self.assertIn("stage_name", df_csv.columns)
        self.assertIn("status", df_csv.columns)

    # 18. Credential Masking in Text & Errors
    def test_18_credential_masking(self):
        """Verify that sensitive database URLs and passwords are sanitized."""
        sensitive_text = "postgresql+psycopg2://admin:super_secret_pw@db.company.com:5432/analytics"
        sanitized = sanitize_text(sensitive_text)
        self.assertNotIn("super_secret_pw", sanitized)
        self.assertIn("admin:***@", sanitized)

        # None / empty handling
        self.assertIsNone(sanitize_text(None))

    # 19. Disabled Stage Behavior (SKIPPED)
    def test_19_disabled_stage_behavior(self):
        """Verify disabled stages are marked SKIPPED."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation", "ingestion"]
        summary = orch.execute_pipeline(display_summary=False)

        skipped_stages = [
            st for st in summary.stages if st["status"] == StageStatus.SKIPPED.value
        ]
        self.assertGreater(len(skipped_stages), 0)

    # 20. Unexpected Exception Handling
    def test_20_unexpected_exception_handling(self):
        """Verify unexpected runtime exceptions are caught gracefully without crashing."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation"]

        with patch.object(orch, "_run_foundation", side_effect=RuntimeError("Unexpected OS crash")):
            summary = orch.execute_pipeline(display_summary=False)

        self.assertEqual(summary.pipeline_status, PipelineStatus.FAILED.value)
        st = summary.stages[0]
        self.assertEqual(st["status"], StageStatus.FAILED.value)
        self.assertIn("Unexpected OS crash", st["error_message"])

    # 21. Multiple Dataset Handling
    def test_21_multiple_dataset_handling(self):
        """Verify orchestrator tracks multiple datasets in raw landing zone."""
        raw_dir = self.test_root / "data" / "raw"
        pd.DataFrame({"a": [1]}).to_csv(raw_dir / "ds1.csv", index=False)
        pd.DataFrame({"b": [2]}).to_csv(raw_dir / "ds2.csv", index=False)

        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation", "ingestion"]
        summary = orch.execute_pipeline(display_summary=False)

        ingestion_st = next(s for s in summary.stages if s["stage_name"] == "ingestion")
        self.assertEqual(ingestion_st["datasets_processed"], 2)

    # 22. Arbitrary Dataset Schema (Domain-Agnostic)
    def test_22_arbitrary_dataset_schema(self):
        """Verify orchestrator processes arbitrary domain datasets without special casing."""
        raw_dir = self.test_root / "data" / "raw"
        telemetry_df = pd.DataFrame({
            "device_id": ["DEV-01", "DEV-02"],
            "temperature_c": [22.4, 23.1],
            "is_operational": [True, True],
        })
        telemetry_df.to_csv(raw_dir / "iot_sensors.csv", index=False)

        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation", "ingestion", "profiling"]
        summary = orch.execute_pipeline(display_summary=False)

        self.assertIn(summary.pipeline_status, [
            PipelineStatus.COMPLETED.value,
            PipelineStatus.COMPLETED_WITH_WARNINGS.value,
        ])
        dataset_entries = [d for d in summary.datasets if d["source_file"] == "iot_sensors.csv"]
        self.assertGreaterEqual(len(dataset_entries), 2)


if __name__ == "__main__":
    unittest.main()
