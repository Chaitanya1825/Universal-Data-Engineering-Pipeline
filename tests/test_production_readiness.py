"""
Stage 11 Production Readiness, Monitoring & Health Verification Tests.

Validates:
1. Centralized monitoring and performance tracking (PipelineMonitor)
2. Comprehensive pre-flight health diagnostics (PipelineHealthChecker)
3. Run summary report generation (reports/run_summary.json and .csv)
4. Data lineage traceability (reports/lineage.json and .csv)
5. Configuration validation across all config files (ConfigurationValidator)
6. Error classification and secret sanitization
7. Idempotency (UNCHANGED -> SKIP, CHANGED -> REPROCESS)
8. Deterministic reproducibility across pipeline stages
9. Checkpoint store corruption recovery
10. Production readiness reports (reports/production_readiness_report.json and .csv)
"""

from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

import pandas as pd
from sqlalchemy import create_engine

from pipeline.config_validator import (
    ConfigValidationIssue,
    ConfigValidationResult,
    ConfigurationValidator,
)
from pipeline.health import (
    ComponentHealth,
    HealthCheckResult,
    HealthStatus,
    PipelineHealthChecker,
    format_health_summary,
)
from pipeline.incremental import (
    CheckpointStore,
    DatasetCheckpoint,
    IncrementalConfig,
    evaluate_dataset_incremental,
)
from pipeline.lineage import (
    DatasetLineageRecord,
    PipelineLineageTracker,
)
from pipeline.monitoring import (
    PipelineErrorCategory,
    PipelineErrorRecord,
    PipelineMonitor,
    RunMetrics,
    sanitize_sensitive_text,
)
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.schema_detector import profile_dataset
from pipeline.transformer import TransformationConfig, transform_dataset
from pipeline.validator import ValidationConfig, validate_dataset


class TestProductionReadiness(unittest.TestCase):
    """Test suite covering Stage 11 production readiness and observability."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="stage11_test_")
        self.temp_path = Path(self.temp_dir)
        self.logs_dir = self.temp_path / "logs"
        self.reports_dir = self.temp_path / "reports"
        self.config_dir = self.temp_path / "config"
        self.data_dir = self.temp_path / "data"

        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_monitor_initialization(self):
        """PipelineMonitor initializes with clean metrics and creates log directory."""
        monitor = PipelineMonitor(run_id="run_test_01", log_dir=self.logs_dir)
        self.assertEqual(monitor.run_id, "run_test_01")
        self.assertEqual(monitor.metrics.overall_status, "PENDING")
        self.assertTrue(self.logs_dir.exists())

    def test_02_monitor_stage_metrics(self):
        """PipelineMonitor tracks stage timing, throughput, and completion metrics."""
        monitor = PipelineMonitor(run_id="run_test_02", log_dir=self.logs_dir)
        monitor.start_stage("ingestion")
        metric = monitor.end_stage(
            stage_name="ingestion",
            status="SUCCESS",
            records_in=100,
            records_out=100,
            records_rejected=0,
        )
        self.assertEqual(metric.stage_name, "ingestion")
        self.assertEqual(metric.status, "SUCCESS")
        self.assertEqual(metric.records_out, 100)
        self.assertIn("ingestion", monitor.metrics.stage_durations)

    def test_03_monitor_error_classification(self):
        """Errors are categorized with recoverable flags and classified types."""
        monitor = PipelineMonitor(run_id="run_test_03", log_dir=self.logs_dir)
        rec = monitor.record_error(
            stage="database",
            error_type=PipelineErrorCategory.DATABASE_UNAVAILABLE,
            message="PostgreSQL connection timed out on port 5432",
            recoverable=True,
        )
        self.assertEqual(rec.error_type, "DATABASE_UNAVAILABLE")
        self.assertTrue(rec.recoverable)
        self.assertEqual(len(monitor.metrics.errors), 1)

    def test_04_secret_sanitization(self):
        """Sensitive passwords and credentials in URLs and query strings are masked."""
        raw_conn = "postgresql://analytics_user:SecretPass123@db.prod.internal:5432/warehouse"
        masked = sanitize_sensitive_text(raw_conn)
        self.assertNotIn("SecretPass123", masked)
        self.assertIn(":***", masked)

        raw_param = "connecting with token=abc123secret and api_key=xyz987"
        masked_param = sanitize_sensitive_text(raw_param)
        self.assertNotIn("abc123secret", masked_param)
        self.assertNotIn("xyz987", masked_param)

    def test_05_health_checker_runtime(self):
        """Health checker validates Python version."""
        checker = PipelineHealthChecker(root_dir=self.temp_path)
        runtime_health = checker.check_python_environment()
        self.assertEqual(runtime_health.status, HealthStatus.HEALTHY.value)

    def test_06_health_checker_directories(self):
        """Health checker confirms foundational directories exist and are writable."""
        checker = PipelineHealthChecker(root_dir=self.temp_path)
        dir_health = checker.check_directories()
        self.assertEqual(dir_health.status, HealthStatus.HEALTHY.value)

    def test_07_health_checker_missing_directory(self):
        """Health checker reports error if directory is missing and unwritable."""
        fake_root = Path("Z:\\non_existent_drive_12345")
        checker = PipelineHealthChecker(root_dir=fake_root)
        dir_health = checker.check_directories()
        self.assertEqual(dir_health.status, HealthStatus.ERROR.value)

    def test_08_health_checker_configs(self):
        """Health checker checks configuration files in real project repository."""
        repo_root = Path(__file__).resolve().parent.parent
        checker = PipelineHealthChecker(root_dir=repo_root)
        cfg_health = checker.check_configurations()
        self.assertEqual(cfg_health.status, HealthStatus.HEALTHY.value)

    def test_09_health_checker_missing_config(self):
        """Health checker detects missing configuration files."""
        checker = PipelineHealthChecker(root_dir=self.temp_path)
        cfg_health = checker.check_configurations()
        self.assertEqual(cfg_health.status, HealthStatus.CONFIGURATION_ERROR.value)

    def test_10_health_checker_corrupted_config(self):
        """Health checker detects malformed JSON in configuration."""
        bad_json_file = self.config_dir / "pipeline.json"
        bad_json_file.write_text("{ unquoted_invalid_json }", encoding="utf-8")
        checker = PipelineHealthChecker(root_dir=self.temp_path)
        cfg_health = checker.check_configurations()
        self.assertEqual(cfg_health.status, HealthStatus.CONFIGURATION_ERROR.value)

    def test_11_health_checker_database_unconfigured(self):
        """Database connectivity probe reports DATABASE_UNAVAILABLE when DATABASE_URL is unset."""
        orig_url = os.environ.pop("DATABASE_URL", None)
        try:
            checker = PipelineHealthChecker(root_dir=self.temp_path)
            db_health, connected = checker.check_database_connectivity()
            self.assertEqual(db_health.status, HealthStatus.DATABASE_UNAVAILABLE.value)
            self.assertFalse(connected)
        finally:
            if orig_url:
                os.environ["DATABASE_URL"] = orig_url

    def test_12_health_checker_database_connected(self):
        """Database connectivity probe returns healthy when SQLite database is provided."""
        test_db = self.temp_path / "test.db"
        sqlite_url = f"sqlite:///{test_db}"
        orig_url = os.environ.get("DATABASE_URL")
        try:
            os.environ["DATABASE_URL"] = sqlite_url
            checker = PipelineHealthChecker(root_dir=self.temp_path)
            db_health, connected = checker.check_database_connectivity()
            self.assertEqual(db_health.status, HealthStatus.HEALTHY.value)
            self.assertTrue(connected)
        finally:
            if orig_url:
                os.environ["DATABASE_URL"] = orig_url
            else:
                os.environ.pop("DATABASE_URL", None)

    def test_13_health_checker_overall_status(self):
        """Health checker executes consolidate diagnostic suite and formats summary."""
        repo_root = Path(__file__).resolve().parent.parent
        checker = PipelineHealthChecker(root_dir=repo_root)
        result = checker.run_health_check()
        self.assertIn(result.overall_status, [
            HealthStatus.HEALTHY.value,
            HealthStatus.WARNING.value,
            HealthStatus.DATABASE_UNAVAILABLE.value,
        ])
        formatted = format_health_summary(result)
        self.assertIn("PIPELINE HEALTH", formatted)

    def test_14_run_summary_reports(self):
        """save_run_summary_reports outputs valid JSON and CSV run summaries."""
        monitor = PipelineMonitor(run_id="run_test_summary", log_dir=self.logs_dir)
        monitor.update_aggregate_metrics(
            dataset_count=2,
            successful_dataset_count=2,
            failed_dataset_count=0,
            records_read=50,
            records_processed=50,
            records_rejected=0,
            records_loaded=50,
            records_skipped=0,
            records_reprocessed=0,
            validation_quality_score=98.5,
            database_status="SUCCESS",
        )
        monitor.finalize_run("COMPLETED")
        j_path, c_path = monitor.save_run_summary_reports(reports_dir=self.reports_dir)

        self.assertTrue(j_path.exists())
        self.assertTrue(c_path.exists())

        with open(j_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["run_id"], "run_test_summary")
        self.assertEqual(data["overall_status"], "COMPLETED")
        self.assertEqual(data["records_read"], 50)

    def test_15_data_lineage_tracking(self):
        """PipelineLineageTracker records end-to-end stage attributes per dataset."""
        tracker = PipelineLineageTracker(run_id="run_test_lineage")
        tracker.record_ingestion("telemetry.csv", "CSV", 100)
        tracker.record_transformation("telemetry.csv", 100)
        tracker.record_validation("telemetry.csv", valid_rows=95, rejected_rows=5, quarantine_path="data/rejected/telemetry_quarantine.csv")
        tracker.record_incremental("telemetry.csv", "NEW", "PROCESS", "hash_abc123")
        tracker.record_database_load("telemetry.csv", "telemetry", 95, "SUCCESS")
        tracker.record_downstream_status("telemetry.csv", "SUCCESS", "SUCCESS")

        rec = tracker.get_or_create_record("telemetry.csv")
        self.assertEqual(rec.input_row_count, 100)
        self.assertEqual(rec.valid_row_count, 95)
        self.assertEqual(rec.rejected_row_count, 5)
        self.assertEqual(rec.loaded_row_count, 95)
        self.assertEqual(rec.target_table_name, "telemetry")

    def test_16_data_lineage_export(self):
        """Lineage records serialize to valid lineage.json and lineage.csv."""
        tracker = PipelineLineageTracker(run_id="run_test_lineage_export")
        tracker.record_ingestion("sensors.json", "JSON", 25)
        j_path, c_path = tracker.save_lineage_reports(reports_dir=self.reports_dir)

        self.assertTrue(j_path.exists())
        self.assertTrue(c_path.exists())

        with open(j_path, "r", encoding="utf-8") as f:
            lineage_list = json.load(f)
        self.assertEqual(len(lineage_list), 1)
        self.assertEqual(lineage_list[0]["source_filename"], "sensors.json")

    def test_17_config_validator_pipeline(self):
        """ConfigurationValidator validates pipeline.json settings and stage list."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        valid_data = {
            "enabled_stages": ["foundation", "ingestion", "database"],
            "fail_fast": False,
        }
        validator.validate_pipeline_config(valid_data, issues)
        self.assertEqual(len(issues), 0)

        invalid_data = {"enabled_stages": ["foundation", "invalid_magic_stage"]}
        validator.validate_pipeline_config(invalid_data, issues)
        self.assertTrue(any("invalid_magic_stage" in i.message for i in issues))

    def test_18_config_validator_transformations(self):
        """ConfigurationValidator checks string_normalization options."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        validator.validate_transformations_config({"string_normalization": "invalid_case"}, issues)
        self.assertTrue(any("invalid_case" in i.message for i in issues))

    def test_19_config_validator_database(self):
        """ConfigurationValidator enforces positive batch_size in database.json."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        validator.validate_database_config({"batch_size": -50}, issues)
        self.assertTrue(any("batch_size" in i.field_name for i in issues))

    def test_20_config_validator_incremental(self):
        """ConfigurationValidator rejects unsupported hashing algorithms."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        validator.validate_incremental_config({"hash_algorithm": "sha1_insecure"}, issues)
        self.assertTrue(any("Unsupported hash algorithm" in i.message for i in issues))

    def test_21_config_validator_analytics(self):
        """ConfigurationValidator detects when default_limit exceeds max_limit."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        validator.validate_analytics_config({"default_limit": 50000, "max_limit": 10000}, issues)
        self.assertTrue(any("cannot exceed 'max_limit'" in i.message for i in issues))

    def test_22_config_validator_powerbi(self):
        """ConfigurationValidator detects invalid Power BI connection mode."""
        validator = ConfigurationValidator()
        issues: list[ConfigValidationIssue] = []
        validator.validate_powerbi_config({
            "connection_mode": "hybrid_mesh",
            "supported_connection_modes": ["import", "directquery"],
        }, issues)
        self.assertTrue(any("hybrid_mesh" in i.message for i in issues))

    def test_23_config_validator_all_valid(self):
        """Repository configuration directory validates with zero errors."""
        repo_root = Path(__file__).resolve().parent.parent
        validator = ConfigurationValidator(config_dir=repo_root / "config")
        res = validator.validate_all()
        self.assertTrue(res.is_valid)
        self.assertEqual(len(res.errors), 0)

    def test_24_production_readiness_report(self):
        """PipelineMonitor generates production_readiness_report.json and .csv."""
        monitor = PipelineMonitor(run_id="run_test_readiness", log_dir=self.logs_dir)
        monitor.finalize_run("COMPLETED_WITH_WARNINGS")
        j_path, c_path = monitor.save_production_readiness_reports(
            reports_dir=self.reports_dir,
            health_status="DATABASE_UNAVAILABLE",
            database_connected=False,
        )
        self.assertTrue(j_path.exists())
        self.assertTrue(c_path.exists())

        with open(j_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertTrue(data["production_ready"])
        self.assertEqual(data["health_status"], "DATABASE_UNAVAILABLE")
        self.assertIn("architecture", data)
        self.assertIn("reliability", data)
        self.assertIn("observability", data)

    def test_25_idempotency_unchanged_dataset_skip(self):
        """Idempotency test: unchanged dataset on subsequent evaluation is marked UNCHANGED -> SKIP."""
        data_file = self.data_dir / "devices.csv"
        data_file.write_text("device_id,reading,status\nD1,12.5,active\nD2,18.0,idle\n", encoding="utf-8")

        ckpt_file = self.temp_path / "checkpoints.json"
        store = CheckpointStore(ckpt_file)
        cfg = IncrementalConfig(enabled=True, checkpoint_file=str(ckpt_file))

        df = pd.read_csv(data_file)
        # First execution -> NEW
        dec1 = evaluate_dataset_incremental(data_file, df, cfg, store)
        self.assertEqual(dec1.decision, "NEW")
        self.assertEqual(dec1.action, "PROCESS")
        cp1 = DatasetCheckpoint(
            source_path=str(data_file),
            file_hash=dec1.current_hash,
            file_size=data_file.stat().st_size,
            modified_time=datetime.fromtimestamp(data_file.stat().st_mtime).isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=2,
        )
        store.update("devices.csv", cp1)
        store.save()

        # Second execution with identical file -> UNCHANGED -> SKIP
        dec2 = evaluate_dataset_incremental(data_file, df, cfg, store)
        self.assertEqual(dec2.decision, "UNCHANGED")
        self.assertEqual(dec2.action, "SKIP")

    def test_26_idempotency_modified_dataset_reprocess(self):
        """Idempotency test: modifying dataset contents triggers CHANGED -> REPROCESS."""
        data_file = self.data_dir / "readings.csv"
        data_file.write_text("id,val\n1,100\n", encoding="utf-8")

        ckpt_file = self.temp_path / "checkpoints.json"
        store = CheckpointStore(ckpt_file)
        cfg = IncrementalConfig(enabled=True, checkpoint_file=str(ckpt_file))

        df1 = pd.read_csv(data_file)
        dec1 = evaluate_dataset_incremental(data_file, df1, cfg, store)
        cp1 = DatasetCheckpoint(
            source_path=str(data_file),
            file_hash=dec1.current_hash,
            file_size=data_file.stat().st_size,
            modified_time=datetime.fromtimestamp(data_file.stat().st_mtime).isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
        )
        store.update("readings.csv", cp1)
        store.save()

        # Modify content
        data_file.write_text("id,val\n1,100\n2,200\n", encoding="utf-8")
        df2 = pd.read_csv(data_file)
        dec2 = evaluate_dataset_incremental(data_file, df2, cfg, store)
        self.assertEqual(dec2.decision, "CHANGED")
        self.assertEqual(dec2.action, "REPROCESS")

    def test_27_reproducibility_deterministic_profile(self):
        """Deterministic reproducibility: identical input produces identical schema profiling."""
        data = {
            "sensor_id": ["S101", "S102", "S103"],
            "temperature": [22.4, 25.1, 23.8],
            "calibrated": [True, True, False],
        }
        df1 = pd.DataFrame(data)
        df2 = pd.DataFrame(data)

        prof1 = profile_dataset(df1, "sensors.csv")
        prof2 = profile_dataset(df2, "sensors.csv")

        self.assertEqual(prof1.row_count, prof2.row_count)
        self.assertEqual(prof1.numeric_columns, prof2.numeric_columns)
        self.assertEqual(prof1.columns["temperature"].inferred_type, prof2.columns["temperature"].inferred_type)

    def test_28_checkpoint_corruption_recovery(self):
        """Corrupted checkpoints file is safely recovered without pipeline crash."""
        ckpt_file = self.temp_path / "corrupted_checkpoints.json"
        ckpt_file.write_text("INVALID_JSON_CORRUPTED_CONTENT", encoding="utf-8")

        # Loading corrupted file should catch JSONDecodeError and initialize clean state
        store = CheckpointStore(ckpt_file)
        self.assertEqual(len(store.checkpoints), 0)

    def test_29_error_recovery_graceful_dataset_isolation(self):
        """Valid datasets succeed and transform cleanly even when isolated invalid records occur."""
        data = {
            "patient_code": ["P001", "P002", "INVALID"],
            "systolic": [120, 130, -999],
        }
        df = pd.DataFrame(data)
        val_cfg = ValidationConfig(
            range_rules={"systolic": {"min": 50, "max": 250}},
        )
        val_res, valid_df, rejected_df = validate_dataset(df, "patients.csv", config=val_cfg)
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(rejected_df), 1)

    def test_30_orchestrator_stage11_integration(self):
        """PipelineOrchestrator generates all Stage 11 reports during execute_pipeline."""
        repo_root = Path(__file__).resolve().parent.parent
        orchestrator = PipelineOrchestrator(root_dir=repo_root)
        summary = orchestrator.execute_pipeline(display_summary=False)

        # Check reports exist
        rep_dir = repo_root / "reports"
        self.assertTrue((rep_dir / "run_summary.json").exists())
        self.assertTrue((rep_dir / "run_summary.csv").exists())
        self.assertTrue((rep_dir / "lineage.json").exists())
        self.assertTrue((rep_dir / "lineage.csv").exists())
        self.assertTrue((rep_dir / "production_readiness_report.json").exists())
        self.assertTrue((rep_dir / "production_readiness_report.csv").exists())
        self.assertIn(summary.pipeline_status, ["COMPLETED", "COMPLETED_WITH_WARNINGS"])


if __name__ == "__main__":
    unittest.main()
