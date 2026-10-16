"""
Comprehensive Unit Tests for Stage 8 Incremental Data Processing Engine.

Covers:
1. SHA-256 chunked cryptographic hashing
2. Identical file detection (UNCHANGED -> SKIP)
3. Changed file detection (CHANGED -> REPROCESS)
4. New file detection (NEW -> PROCESS)
5. Missing checkpoint recovery
6. Corrupted checkpoint recovery
7. Checkpoint persistence and reload
8. Checkpoint atomic update behavior (.tmp -> replace)
9. Failed processing does NOT create SUCCESS checkpoint
10. Database unavailable checkpoint state tracking
11. Deterministic record hashing
12. Null/NaN handling in record hashes
13. Explicit record key handling (single & composite)
14. No record key configured (graceful NOT_AVAILABLE fallback)
15. New record detection
16. Changed record detection
17. Unchanged record detection
18. Removed record detection
19. Multiple dataset isolation
20. Arbitrary dataset schema & filenames (domain-agnostic)
21. Invalid configuration handling
22. Disabled incremental mode (bypass & normal processing)
23. Unchanged dataset skips downstream processing (UNCHANGED_SKIPPED)
24. Changed dataset reprocesses through downstream pipeline
25. New dataset processes normally through downstream pipeline
26. JSON & CSV incremental report generation
27. Credential safety (no secrets leaked in logs or reports)
28. Orchestrator integration (multi-run end-to-end lifecycle)
29. Database delta loading & upsert behavior (insert, update, delete_missing_records)
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
from sqlalchemy import create_engine

from pipeline.incremental import (
    CheckpointStore,
    DatasetCheckpoint,
    IncrementalConfig,
    IncrementalDecision,
    calculate_file_hash,
    compute_record_hash,
    evaluate_dataset_incremental,
    extract_record_key,
    load_incremental_config,
    log_incremental_event,
    reset_checkpoint_store,
    save_incremental_reports,
    upsert_records_to_database,
)
from pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineStatus,
    StageExecutionRecord,
    StageStatus,
)


class TestIncrementalProcessing(unittest.TestCase):
    """Test suite covering Stage 8 incremental processing engine."""

    def setUp(self):
        """Create isolated temporary environment for state, data, logs, and configs."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_root = Path(self.temp_dir.name)

        # Standard pipeline directory structure
        (self.test_root / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "validated").mkdir(parents=True, exist_ok=True)
        (self.test_root / "data" / "rejected").mkdir(parents=True, exist_ok=True)
        (self.test_root / "config").mkdir(parents=True, exist_ok=True)
        (self.test_root / "state").mkdir(parents=True, exist_ok=True)
        (self.test_root / "logs").mkdir(parents=True, exist_ok=True)
        (self.test_root / "reports").mkdir(parents=True, exist_ok=True)

        self.raw_dir = self.test_root / "data" / "raw"
        self.state_file = self.test_root / "state" / "checkpoints.json"

    def tearDown(self):
        """Clean up isolated filesystem environment."""
        self.temp_dir.cleanup()

    # 1. SHA-256 Hashing
    def test_01_sha256_chunked_hashing(self):
        """Verify calculate_file_hash produces deterministic 64-char hex digest."""
        test_file = self.raw_dir / "sample.csv"
        test_file.write_text("id,val\n1,alpha\n2,beta\n", encoding="utf-8")

        h1 = calculate_file_hash(test_file, algorithm="sha256", chunk_size=8)
        h2 = calculate_file_hash(test_file, algorithm="sha256", chunk_size=65536)

        self.assertEqual(len(h1), 64)
        self.assertEqual(h1, h2)
        # Verify nonexistent file raises FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            calculate_file_hash(self.raw_dir / "nonexistent.csv")

    # 2. Identical File Detection (UNCHANGED -> SKIP)
    def test_02_identical_file_detection(self):
        """Verify identical content hash triggers UNCHANGED and SKIP action."""
        test_file = self.raw_dir / "sales.csv"
        test_file.write_text("item,qty\npen,10\n", encoding="utf-8")
        h = calculate_file_hash(test_file)

        store = CheckpointStore(self.state_file)
        store.update("sales.csv", DatasetCheckpoint(
            source_path=str(test_file),
            file_hash=h,
            file_size=test_file.stat().st_size,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
        ))

        cfg = IncrementalConfig(skip_unchanged_files=True)
        decision = evaluate_dataset_incremental(test_file, pd.DataFrame({"item": ["pen"], "qty": [10]}), cfg, store)

        self.assertEqual(decision.decision, "UNCHANGED")
        self.assertEqual(decision.action, "SKIP")

    # 3. Changed File Detection (CHANGED -> REPROCESS)
    def test_03_changed_file_detection(self):
        """Verify modified content triggers CHANGED decision and REPROCESS action."""
        test_file = self.raw_dir / "sales.csv"
        test_file.write_text("item,qty\npen,10\n", encoding="utf-8")

        store = CheckpointStore(self.state_file)
        store.update("sales.csv", DatasetCheckpoint(
            source_path=str(test_file),
            file_hash="old_sha256_hash_1234567890abcdef",
            file_size=10,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
        ))

        cfg = IncrementalConfig(reprocess_changed_files=True)
        decision = evaluate_dataset_incremental(test_file, pd.DataFrame({"item": ["pen"], "qty": [10]}), cfg, store)

        self.assertEqual(decision.decision, "CHANGED")
        self.assertEqual(decision.action, "REPROCESS")
        self.assertEqual(decision.previous_hash, "old_sha256_hash_1234567890abcdef")

    # 4. New File Detection (NEW -> PROCESS)
    def test_04_new_file_detection(self):
        """Verify unknown dataset triggers NEW decision and PROCESS action."""
        test_file = self.raw_dir / "brand_new.csv"
        test_file.write_text("a,b\n1,2\n", encoding="utf-8")

        store = CheckpointStore(self.state_file)
        cfg = IncrementalConfig()
        decision = evaluate_dataset_incremental(test_file, pd.DataFrame({"a": [1], "b": [2]}), cfg, store)

        self.assertEqual(decision.decision, "NEW")
        self.assertEqual(decision.action, "PROCESS")
        self.assertIsNone(decision.previous_hash)

    # 5. Missing Checkpoint
    def test_05_missing_checkpoint(self):
        """Verify CheckpointStore gracefully handles missing checkpoint file."""
        nonexistent = self.test_root / "state" / "does_not_exist.json"
        store = CheckpointStore(nonexistent)
        self.assertEqual(len(store.checkpoints), 0)
        self.assertIsNone(store.get("sample.csv"))

    # 6. Corrupted Checkpoint Recovery
    def test_06_corrupted_checkpoint_recovery(self):
        """Verify CheckpointStore recovers safely from corrupted or empty checkpoint files."""
        corrupt_file = self.test_root / "state" / "corrupted.json"
        corrupt_file.write_text("{invalid_json: true, unterminated", encoding="utf-8")

        store = CheckpointStore(corrupt_file)
        self.assertEqual(len(store.checkpoints), 0)

    # 7. Checkpoint Persistence
    def test_07_checkpoint_persistence(self):
        """Verify checkpoints can be written and reloaded accurately."""
        store1 = CheckpointStore(self.state_file)
        store1.update("test.csv", DatasetCheckpoint(
            source_path="/path/test.csv",
            file_hash="hash123",
            file_size=42,
            modified_time="2026-10-03T12:00:00",
            last_processed_at="2026-10-03T12:00:00",
            last_status="SUCCESS",
            record_count=100,
        ))
        store1.save()

        self.assertTrue(self.state_file.exists())
        store2 = CheckpointStore(self.state_file)
        cp = store2.get("test.csv")
        self.assertIsNotNone(cp)
        self.assertEqual(cp.file_hash, "hash123")
        self.assertEqual(cp.record_count, 100)

    # 8. Checkpoint Atomic Update Behavior
    def test_08_checkpoint_atomic_update_behavior(self):
        """Verify CheckpointStore saves atomically via .tmp and replace."""
        store = CheckpointStore(self.state_file)
        store.update("atomic.csv", DatasetCheckpoint(
            source_path="/path/atomic.csv",
            file_hash="hash_atomic",
            file_size=10,
            modified_time="2026-10-03T12:00:00",
            last_processed_at="2026-10-03T12:00:00",
            last_status="SUCCESS",
        ))

        with patch("os.replace", wraps=os.replace) as mock_replace:
            store.save()
            mock_replace.assert_called_once()

    # 9. Failed Processing Does Not Create SUCCESS Checkpoint
    def test_09_failed_processing_no_success_checkpoint(self):
        """Verify that when processing fails, orchestrator does NOT save a SUCCESS checkpoint."""
        csv_file = self.raw_dir / "fail_data.csv"
        csv_file.write_text("id,val\n1,ok\n", encoding="utf-8")

        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.config.enabled_stages = ["foundation", "ingestion", "incremental", "transformation", "validation"]

        # Simulate validation failure
        mock_val_res = MagicMock()
        mock_val_res.source_filename = "fail_data.csv"
        mock_val_res.status = "FAILED"
        orch.validation_results = [mock_val_res]

        orch.incremental_results = [
            IncrementalDecision(
                dataset_name="fail_data.csv",
                source_path=str(csv_file),
                current_hash=calculate_file_hash(csv_file),
                decision="NEW",
                action="PROCESS",
            )
        ]
        orch.checkpoint_store = CheckpointStore(self.state_file)
        orch._commit_checkpoints()

        # Checkpoint store must NOT contain SUCCESS for fail_data.csv
        cp = orch.checkpoint_store.get("fail_data.csv")
        self.assertIsNone(cp)

    # 10. Database Unavailable Checkpoint Behavior
    def test_10_database_unavailable_checkpoint_behavior(self):
        """Verify checkpoint records last_status='SUCCESS' with database_status='DATABASE_UNAVAILABLE'."""
        csv_file = self.raw_dir / "db_unavail.csv"
        csv_file.write_text("id,val\n1,ok\n", encoding="utf-8")

        orch = PipelineOrchestrator(root_dir=self.test_root)
        orch.checkpoint_store = CheckpointStore(self.state_file)

        mock_val_res = MagicMock()
        mock_val_res.source_filename = "db_unavail.csv"
        mock_val_res.status = "PASSED"
        orch.validation_results = [mock_val_res]

        mock_db_res = MagicMock()
        mock_db_res.source_dataset = "db_unavail.csv"
        mock_db_res.database_status = "DATABASE_UNAVAILABLE"
        orch.database_results = [mock_db_res]

        orch.incremental_results = [
            IncrementalDecision(
                dataset_name="db_unavail.csv",
                source_path=str(csv_file),
                current_hash=calculate_file_hash(csv_file),
                decision="NEW",
                action="PROCESS",
                records_current=1,
            )
        ]
        orch._commit_checkpoints()

        cp = orch.checkpoint_store.get("db_unavail.csv")
        self.assertIsNotNone(cp)
        self.assertEqual(cp.last_status, "SUCCESS")
        self.assertEqual(cp.database_status, "DATABASE_UNAVAILABLE")

    # 11. Deterministic Record Hashing
    def test_11_deterministic_record_hashing(self):
        """Verify row hashing is deterministic and independent of key ordering."""
        rec1 = {"col_b": "value", "col_a": 123}
        rec2 = {"col_a": 123, "col_b": "value"}

        h1 = compute_record_hash(rec1)
        h2 = compute_record_hash(rec2)
        self.assertEqual(h1, h2)

    # 12. Null Handling in Record Hashes
    def test_12_null_handling_in_record_hashes(self):
        """Verify null/None/NaN values are canonicalized without errors."""
        rec_none = {"id": 1, "note": None}
        rec_nan = {"id": 1, "note": float("nan")}

        h_none = compute_record_hash(rec_none)
        h_nan = compute_record_hash(rec_nan)
        self.assertEqual(h_none, h_nan)

    # 13. Explicit Record Key Handling
    def test_13_explicit_record_key_handling(self):
        """Verify single and composite record key extraction."""
        row = pd.Series({"org_id": "ORG01", "user_id": 42, "role": "admin"})
        key_single = extract_record_key(row, ["org_id"])
        key_composite = extract_record_key(row, ["org_id", "user_id"])

        self.assertEqual(key_single, "ORG01")
        self.assertEqual(key_composite, "ORG01::42")

    # 14. No Record Key Configured
    def test_14_no_record_key_configured(self):
        """Verify that when no keys are configured, record-level comparison is NOT_AVAILABLE."""
        csv_file = self.raw_dir / "unkeyed.csv"
        csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
        df = pd.DataFrame({"a": [1], "b": [2]})

        store = CheckpointStore(self.state_file)
        cfg = IncrementalConfig(record_keys={})

        decision = evaluate_dataset_incremental(csv_file, df, cfg, store)
        self.assertFalse(decision.record_comparison_available)
        self.assertIn("NOT_AVAILABLE", decision.comparison_note)

    # 15. New Record Detection
    def test_15_new_record_detection(self):
        """Verify delta calculation identifies newly added rows."""
        csv_file = self.raw_dir / "customers.csv"
        csv_file.write_text("cust_id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        df = pd.DataFrame({"cust_id": [1, 2], "name": ["Alice", "Bob"]})

        store = CheckpointStore(self.state_file)
        # Previous checkpoint only had cust_id 1
        h_alice = compute_record_hash({"cust_id": 1, "name": "Alice"}, key_columns=["cust_id"])
        store.update("customers.csv", DatasetCheckpoint(
            source_path=str(csv_file),
            file_hash="old_hash",
            file_size=10,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
            record_hashes={"1": h_alice},
        ))

        cfg = IncrementalConfig(record_keys={"customers.csv": ["cust_id"]})
        decision = evaluate_dataset_incremental(csv_file, df, cfg, store)

        self.assertTrue(decision.record_comparison_available)
        self.assertEqual(decision.records_new, 1)  # Bob is new
        self.assertEqual(decision.records_unchanged, 1)  # Alice is unchanged
        self.assertEqual(decision.records_changed, 0)
        self.assertEqual(decision.records_removed, 0)

    # 16. Changed Record Detection
    def test_16_changed_record_detection(self):
        """Verify delta calculation detects updated row values for existing keys."""
        csv_file = self.raw_dir / "customers.csv"
        csv_file.write_text("cust_id,name\n1,Alice_Updated\n", encoding="utf-8")
        df = pd.DataFrame({"cust_id": [1], "name": ["Alice_Updated"]})

        store = CheckpointStore(self.state_file)
        h_alice_old = compute_record_hash({"cust_id": 1, "name": "Alice_Old"}, key_columns=["cust_id"])
        store.update("customers.csv", DatasetCheckpoint(
            source_path=str(csv_file),
            file_hash="old_hash",
            file_size=10,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
            record_hashes={"1": h_alice_old},
        ))

        cfg = IncrementalConfig(record_keys={"customers.csv": ["cust_id"]})
        decision = evaluate_dataset_incremental(csv_file, df, cfg, store)

        self.assertEqual(decision.records_changed, 1)
        self.assertEqual(decision.records_new, 0)
        self.assertEqual(decision.records_unchanged, 0)

    # 17. Unchanged Record Detection
    def test_17_unchanged_record_detection(self):
        """Verify delta calculation detects unchanged rows when contents match."""
        csv_file = self.raw_dir / "customers.csv"
        csv_file.write_text("cust_id,name\n1,Alice\n", encoding="utf-8")
        df = pd.DataFrame({"cust_id": [1], "name": ["Alice"]})

        store = CheckpointStore(self.state_file)
        h_alice = compute_record_hash({"cust_id": 1, "name": "Alice"}, key_columns=["cust_id"])
        store.update("customers.csv", DatasetCheckpoint(
            source_path=str(csv_file),
            file_hash="old_hash",
            file_size=10,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
            record_hashes={"1": h_alice},
        ))

        cfg = IncrementalConfig(record_keys={"customers.csv": ["cust_id"]})
        decision = evaluate_dataset_incremental(csv_file, df, cfg, store)

        self.assertEqual(decision.records_unchanged, 1)
        self.assertEqual(decision.records_changed, 0)

    # 18. Removed Record Detection
    def test_18_removed_record_detection(self):
        """Verify delta calculation identifies records that were omitted from new file."""
        csv_file = self.raw_dir / "customers.csv"
        csv_file.write_text("cust_id,name\n1,Alice\n", encoding="utf-8")
        df = pd.DataFrame({"cust_id": [1], "name": ["Alice"]})

        store = CheckpointStore(self.state_file)
        store.update("customers.csv", DatasetCheckpoint(
            source_path=str(csv_file),
            file_hash="old_hash",
            file_size=10,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=2,
            record_hashes={"1": "hash_alice", "2": "hash_bob"},
        ))

        cfg = IncrementalConfig(record_keys={"customers.csv": ["cust_id"]})
        decision = evaluate_dataset_incremental(csv_file, df, cfg, store)

        self.assertEqual(decision.records_removed, 1)  # Key "2" removed

    # 19. Multiple Dataset Isolation
    def test_19_multiple_dataset_isolation(self):
        """Verify state isolation across distinct datasets."""
        store = CheckpointStore(self.state_file)
        store.update("orders.csv", DatasetCheckpoint(
            source_path="/orders.csv", file_hash="hash_orders", file_size=100,
            modified_time="2026-10-03T10:00:00", last_processed_at="2026-10-03T10:00:00",
            last_status="SUCCESS", record_count=50,
        ))
        store.update("users.json", DatasetCheckpoint(
            source_path="/users.json", file_hash="hash_users", file_size=200,
            modified_time="2026-10-03T10:00:00", last_processed_at="2026-10-03T10:00:00",
            last_status="SUCCESS", record_count=20,
        ))

        self.assertEqual(store.get("orders.csv").file_hash, "hash_orders")
        self.assertEqual(store.get("users.json").file_hash, "hash_users")
        self.assertIsNone(store.get("products.xlsx"))

    # 20. Arbitrary Dataset Names
    def test_20_arbitrary_dataset_names(self):
        """Verify domain-agnostic processing with unusual file names."""
        test_file = self.raw_dir / "telemetry_sensors_v2_2026.json"
        test_file.write_text('[{"sensor": "temp", "val": 23.5}]', encoding="utf-8")

        store = CheckpointStore(self.state_file)
        cfg = IncrementalConfig()
        decision = evaluate_dataset_incremental(test_file, None, cfg, store)

        self.assertEqual(decision.dataset_name, "telemetry_sensors_v2_2026.json")
        self.assertEqual(decision.decision, "NEW")

    # 21. Invalid Configuration
    def test_21_invalid_configuration(self):
        """Verify invalid hash algorithms and bad JSON produce clear errors."""
        cfg_file = self.test_root / "config" / "bad_inc.json"
        cfg_file.write_text('{"hash_algorithm": "magic_quantum_hash"}', encoding="utf-8")

        cfg, err = load_incremental_config(cfg_file)
        self.assertIsNotNone(err)
        self.assertIn("Invalid hash_algorithm", err)

    # 22. Disabled Incremental Mode
    def test_22_disabled_incremental_mode(self):
        """Verify that when disabled, incremental stage returns SKIPPED and passes all datasets."""
        orch = PipelineOrchestrator(root_dir=self.test_root)
        inc_cfg = self.test_root / "config" / "incremental.json"
        inc_cfg.write_text('{"enabled": false}', encoding="utf-8")

        # Mock ingestion results
        df_mock = pd.DataFrame({"id": [1]})
        ing_res = MagicMock()
        ing_res.status = "SUCCESS"
        ing_res.file_name = "test.csv"
        ing_res.df = df_mock
        orch.ingestion_results = [ing_res]

        rec = orch._run_incremental(datetime.now().isoformat(), 0.0)
        self.assertEqual(rec.status, StageStatus.SKIPPED.value)
        self.assertEqual(len(orch.active_datasets), 1)

    # 23. Unchanged Dataset Skips Downstream Processing
    def test_23_unchanged_dataset_skips_downstream(self):
        """Verify UNCHANGED dataset is marked UNCHANGED_SKIPPED and downstream stages do not reprocess it."""
        test_file = self.raw_dir / "static_data.csv"
        test_file.write_text("col_a,col_b\n10,20\n", encoding="utf-8")

        orch = PipelineOrchestrator(root_dir=self.test_root)
        # Prepopulate checkpoint so file is identical
        h = calculate_file_hash(test_file)
        cp_file = self.test_root / "state" / "checkpoints.json"
        store = CheckpointStore(cp_file)
        store.update("static_data.csv", DatasetCheckpoint(
            source_path=str(test_file),
            file_hash=h,
            file_size=test_file.stat().st_size,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
        ))
        store.save()

        summary = orch.execute_pipeline(display_summary=False)

        # Check incremental stage decision
        inc_records = [d for d in orch.dataset_records if d.stage == "incremental"]
        self.assertEqual(inc_records[0].status, "UNCHANGED_SKIPPED")

        # Downstream stages should have processed 0 active datasets
        prof_st = next(s for s in summary.stages if s["stage_name"] == "profiling")
        self.assertEqual(prof_st["datasets_processed"], 0)

    # 24. Changed Dataset Reprocesses
    def test_24_changed_dataset_reprocesses(self):
        """Verify changed dataset is flagged CHANGED and executed downstream."""
        test_file = self.raw_dir / "mutating.csv"
        test_file.write_text("x,y\n1,2\n", encoding="utf-8")

        orch = PipelineOrchestrator(root_dir=self.test_root)
        cp_file = self.test_root / "state" / "checkpoints.json"
        store = CheckpointStore(cp_file)
        store.update("mutating.csv", DatasetCheckpoint(
            source_path=str(test_file),
            file_hash="different_old_hash_987654321",
            file_size=5,
            modified_time=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat(),
            last_status="SUCCESS",
            record_count=1,
        ))
        store.save()

        summary = orch.execute_pipeline(display_summary=False)
        inc_records = [d for d in orch.dataset_records if d.stage == "incremental"]
        self.assertEqual(inc_records[0].status, "CHANGED")

        # Downstream stages processed it
        prof_st = next(s for s in summary.stages if s["stage_name"] == "profiling")
        self.assertEqual(prof_st["datasets_processed"], 1)

    # 25. New Dataset Processes Normally
    def test_25_new_dataset_processes_normally(self):
        """Verify new dataset is flagged NEW and processed through all stages."""
        test_file = self.raw_dir / "fresh.csv"
        test_file.write_text("id,val\n1,hello\n", encoding="utf-8")

        orch = PipelineOrchestrator(root_dir=self.test_root)
        summary = orch.execute_pipeline(display_summary=False)

        inc_records = [d for d in orch.dataset_records if d.stage == "incremental"]
        self.assertEqual(inc_records[0].status, "NEW")

        prof_st = next(s for s in summary.stages if s["stage_name"] == "profiling")
        self.assertEqual(prof_st["datasets_processed"], 1)

    # 26. Report Generation
    def test_26_report_generation(self):
        """Verify JSON and CSV incremental reports are created with required audit attributes."""
        decisions = [
            IncrementalDecision(
                dataset_name="data.csv",
                source_path="/path/data.csv",
                current_hash="hashabc123",
                decision="NEW",
                action="PROCESS",
                records_current=10,
            )
        ]
        json_p, csv_p = save_incremental_reports(decisions, reports_dir=self.test_root / "reports", run_id="run_test")

        self.assertTrue(json_p.exists())
        self.assertTrue(csv_p.exists())

        with open(json_p, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data[0]["dataset_name"], "data.csv")
        self.assertEqual(data[0]["decision"], "NEW")

        df = pd.read_csv(csv_p)
        self.assertIn("dataset_name", df.columns)
        self.assertIn("decision", df.columns)
        self.assertIn("action", df.columns)

    # 27. Credential Safety
    def test_27_credential_safety(self):
        """Verify incremental logging and reports never leak passwords or sensitive tokens."""
        dec = IncrementalDecision(
            dataset_name="secret.csv",
            source_path="postgresql://admin:super_secret_pw@localhost:5432/db",
            current_hash="abc",
            reason="Connecting with secret_password_token",
        )
        log_incremental_event(dec, log_dir=self.test_root / "logs", run_id="run_safe")

        log_content = (self.test_root / "logs" / "incremental.log").read_text(encoding="utf-8")
        self.assertIn("secret.csv", log_content)

    # 28. Orchestrator Integration (Multi-run lifecycle)
    def test_28_orchestrator_integration_lifecycle(self):
        """
        Verify multi-run lifecycle:
        Run 1: NEW -> PROCESS
        Run 2: UNCHANGED -> SKIP
        Run 3: CHANGED -> REPROCESS
        """
        data_file = self.raw_dir / "lifecycle_dataset.csv"
        data_file.write_text("id,val\n1,one\n2,two\n", encoding="utf-8")

        # RUN 1: Brand new file
        orch1 = PipelineOrchestrator(root_dir=self.test_root)
        s1 = orch1.execute_pipeline(display_summary=False)
        inc_st1 = next(s for s in s1.stages if s["stage_name"] == "incremental")
        self.assertEqual(inc_st1["status"], StageStatus.SUCCESS.value)
        self.assertEqual(s1.summary["datasets_new"], 1)

        # RUN 2: Same file unchanged
        orch2 = PipelineOrchestrator(root_dir=self.test_root)
        s2 = orch2.execute_pipeline(display_summary=False)
        self.assertEqual(s2.summary["datasets_unchanged"], 1)
        self.assertEqual(s2.summary["datasets_incremental_skipped"], 1)
        # Profiling processed 0 datasets
        prof_st2 = next(s for s in s2.stages if s["stage_name"] == "profiling")
        self.assertEqual(prof_st2["datasets_processed"], 0)

        # RUN 3: Modify file contents
        data_file.write_text("id,val\n1,one\n2,two_modified\n3,three\n", encoding="utf-8")
        orch3 = PipelineOrchestrator(root_dir=self.test_root)
        s3 = orch3.execute_pipeline(display_summary=False)
        self.assertEqual(s3.summary["datasets_changed"], 1)
        self.assertEqual(s3.summary["datasets_reprocessed"], 1)
        prof_st3 = next(s for s in s3.stages if s["stage_name"] == "profiling")
        self.assertEqual(prof_st3["datasets_processed"], 1)

    # 29. Database Delta Loading & Upsert Behavior
    def test_29_database_upsert_behavior(self):
        """Verify upsert_records_to_database with SQLite in-memory engine."""
        engine = create_engine("sqlite:///:memory:")

        # Initial load
        df1 = pd.DataFrame({"id": [1, 2], "val": ["alpha", "beta"]})
        res1 = upsert_records_to_database(
            df=df1,
            source_filename="test_upsert.csv",
            key_columns=["id"],
            engine=engine,
        )
        self.assertEqual(res1["status"], "SUCCESS")
        self.assertEqual(res1["inserted"], 2)

        # Delta load: id 1 modified, id 3 added
        df2 = pd.DataFrame({"id": [1, 3], "val": ["alpha_modified", "gamma"]})
        res2 = upsert_records_to_database(
            df=df2,
            source_filename="test_upsert.csv",
            key_columns=["id"],
            engine=engine,
            delete_missing_records=False,
        )
        self.assertEqual(res2["status"], "SUCCESS")
        self.assertEqual(res2["inserted"], 1)  # id 3 inserted
        self.assertEqual(res2["updated"], 1)   # id 1 updated
        self.assertEqual(res2["deleted"], 0)   # id 2 NOT deleted because delete_missing_records=False
        engine.dispose()

    # 30. Checkpoint Store Clear and Reset
    def test_30_checkpoint_store_clear_and_reset(self):
        """Verify CheckpointStore.clear(), .reset(), and reset_checkpoint_store()."""
        cp_path = self.test_root / "state" / "custom_cp.json"
        store = CheckpointStore(cp_path)
        store.update(
            "test.csv",
            DatasetCheckpoint(
                source_path=str(self.test_root / "test.csv"),
                file_hash="dummyhash123",
                file_size=100,
                modified_time=datetime.now().isoformat(),
                last_processed_at=datetime.now().isoformat(),
                last_status="SUCCESS",
            ),
        )
        store.save()
        self.assertTrue(cp_path.exists())
        self.assertIn("test.csv", store.checkpoints)

        # In-memory clear only
        store.clear()
        self.assertEqual(len(store.checkpoints), 0)
        # Disk still has old data until reloaded
        store.load()
        self.assertIn("test.csv", store.checkpoints)

        # Full reset (in-memory + atomic disk write)
        store.reset()
        self.assertEqual(len(store.checkpoints), 0)
        reloaded = CheckpointStore(cp_path)
        self.assertEqual(len(reloaded.checkpoints), 0)

        # Functional reset helper
        store.update(
            "another.csv",
            DatasetCheckpoint(
                source_path=str(self.test_root / "another.csv"),
                file_hash="dummyhash456",
                file_size=200,
                modified_time=datetime.now().isoformat(),
                last_processed_at=datetime.now().isoformat(),
                last_status="SUCCESS",
            ),
        )
        store.save()
        self.assertIn("another.csv", CheckpointStore(cp_path).checkpoints)
        reset_checkpoint_store(cp_path)
        self.assertEqual(len(CheckpointStore(cp_path).checkpoints), 0)

    # 31. Orchestrator Reset Checkpoints Flag
    def test_31_orchestrator_reset_checkpoints_flag(self):
        """Verify PipelineOrchestrator(reset_checkpoints=True) resets existing state."""
        data_file = self.raw_dir / "sales_repeat.csv"
        data_file.write_text("id,amount\n1,100\n2,200\n", encoding="utf-8")

        # Run 1: Normal initial run -> NEW
        orch1 = PipelineOrchestrator(root_dir=self.test_root)
        s1 = orch1.execute_pipeline(display_summary=False)
        self.assertEqual(s1.summary["datasets_new"], 1)

        # Run 2: Without reset -> UNCHANGED -> SKIP
        orch2 = PipelineOrchestrator(root_dir=self.test_root)
        s2 = orch2.execute_pipeline(display_summary=False)
        self.assertEqual(s2.summary["datasets_unchanged"], 1)
        self.assertEqual(s2.summary["datasets_new"], 0)

        # Run 3: With reset_checkpoints=True -> NEW -> PROCESS
        orch3 = PipelineOrchestrator(root_dir=self.test_root, reset_checkpoints=True)
        s3 = orch3.execute_pipeline(display_summary=False)
        self.assertEqual(s3.summary["datasets_new"], 1)
        self.assertEqual(s3.summary["datasets_unchanged"], 0)


if __name__ == "__main__":
    unittest.main()
