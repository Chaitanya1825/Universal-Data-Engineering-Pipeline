"""
Universal ETL Pipeline Orchestrator Module (Stage 7).

Coordinates end-to-end execution across:
- Stage 1: Foundation Verification
- Stage 2: Universal Data Ingestion & File Detection
- Stage 3: Schema Detection & Profiling
- Stage 4: Cleaning & Transformation
- Stage 5: Data Quality & Validation
- Stage 6: PostgreSQL Database & Dynamic Data Modeling

Provides:
- Unique Run IDs
- Dependency validation & stage blocking
- Configurable execution & fail-fast behavior
- Stage and dataset-level tracking & timing
- Graceful database unavailability handling
- Unified JSON and CSV execution reports
- Structured pipeline audit logging
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import json
import os
from pathlib import Path
import re
import secrets
import time
from typing import Any, Optional

import pandas as pd

from pipeline.analytics import (
    AnalyticsConfig,
    AnalyticsExecutionRecord,
    QueryResult,
    SQLAnalyticsEngine,
    load_analytics_config,
    log_analytics_event,
    save_analytics_reports,
)
from pipeline.database import (
    DatabaseConfig,
    DatabaseLoadResult,
    generate_table_name,
    get_database_engine,
    get_database_url,
    load_database_config,
    load_project_env,
    load_validated_dataset,
    log_database_event,
    mask_database_url,
    save_database_load_reports,
    test_database_connection,
)
from pipeline.incremental import (
    CheckpointStore,
    DatasetCheckpoint,
    IncrementalConfig,
    IncrementalDecision,
    calculate_file_hash,
    evaluate_dataset_incremental,
    load_incremental_config,
    log_incremental_event,
    reset_checkpoint_store,
    save_incremental_reports,
)
from pipeline.ingestion import IngestionResult, run_ingestion_pipeline
from pipeline.main import check_project_structure
from pipeline.schema_detector import (
    DatasetProfile,
    log_profiling_event,
    profile_dataset,
    save_profiling_reports,
)
from pipeline.transformer import (
    TransformationResult,
    load_transformation_config,
    log_transformation_event,
    save_processed_dataset,
    save_transformation_report,
    transform_dataset,
)
from pipeline.validator import (
    ValidationResult,
    load_validation_config,
    log_validation_event,
    save_validation_outputs,
    save_validation_reports,
    validate_dataset,
)

from pipeline.powerbi import (
    PowerBIConfig,
    PowerBIExecutionRecord,
    PowerBIIntegrationEngine,
    load_powerbi_config,
    log_powerbi_event,
    save_powerbi_reports,
)

from pipeline.monitoring import (
    PipelineErrorCategory,
    PipelineErrorRecord,
    PipelineMonitor,
    sanitize_sensitive_text,
)
from pipeline.health import (
    ComponentHealth,
    HealthCheckResult,
    PipelineHealthChecker,
    format_health_summary,
)
from pipeline.lineage import (
    DatasetLineageRecord,
    PipelineLineageTracker,
)
from pipeline.config_validator import (
    ConfigValidationResult,
    ConfigurationValidator,
)

# Canonical ordering of pipeline stages
STAGE_ORDER = [
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

# Explicit dependency map (a stage requires all listed stages to have succeeded)
STAGE_DEPENDENCIES: dict[str, list[str]] = {
    "foundation": [],
    "ingestion": ["foundation"],
    "incremental": ["ingestion"],
    "profiling": ["ingestion", "incremental"],
    "transformation": ["ingestion", "profiling"],
    "validation": ["transformation"],
    "database": ["validation"],
    "analytics": ["database"],
    "powerbi": ["database"],
}


class StageStatus(str, Enum):
    """Execution status for an individual stage."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    BLOCKED = "BLOCKED"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    CONFIG_ERROR = "CONFIG_ERROR"


class PipelineStatus(str, Enum):
    """Aggregate execution status for the entire pipeline run."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
    FAILED = "FAILED"
    CONFIG_ERROR = "CONFIG_ERROR"


def generate_run_id() -> str:
    """Generate a unique, human-readable run identifier with timestamp and hex salt."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rand_hex = secrets.token_hex(3)
    return f"run_{timestamp}_{rand_hex}"


def sanitize_text(text: Optional[str]) -> Optional[str]:
    """Sanitize text to strip passwords, secret credentials, and messy newlines."""
    if not text:
        return text
    # Mask URL passwords: scheme://user:pass@host -> scheme://user:***@host
    clean = re.sub(r"://([^:@\s]+):([^@\s]+)@", r"://\1:***@", str(text))
    # Replace newlines with single space
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


@dataclass
class PipelineConfig:
    """Orchestration settings for pipeline execution."""
    enabled_stages: list[str] = field(default_factory=lambda: list(STAGE_ORDER))
    fail_fast: bool = False
    continue_on_database_unavailable: bool = True
    generate_reports: bool = True
    logging_enabled: bool = True


@dataclass
class StageExecutionRecord:
    """Execution metrics and audit trail for a single stage."""
    run_id: str
    stage_name: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    datasets_processed: int = 0
    datasets_succeeded: int = 0
    datasets_failed: int = 0
    error_message: Optional[str] = None


@dataclass
class DatasetExecutionRecord:
    """Execution audit trail for a specific dataset within a stage."""
    run_id: str
    source_file: str
    stage: str
    status: str
    output_path: Optional[str] = None
    row_count: Optional[int] = None
    error_message: Optional[str] = None


@dataclass
class PipelineRunSummary:
    """Comprehensive summary of an orchestrated pipeline execution."""
    run_id: str
    pipeline_status: str
    start_time: str
    end_time: str
    duration_seconds: float
    configuration: dict[str, Any]
    stages: list[dict[str, Any]]
    datasets: list[dict[str, Any]]
    summary: dict[str, Any]
    error_message: Optional[str] = None


def load_pipeline_config(
    config_path: Path | str = "config/pipeline.json",
) -> tuple[PipelineConfig, Optional[str]]:
    """
    Load pipeline configuration from JSON.
    
    Returns (PipelineConfig, None) on success or (default_config, error_reason) if invalid.
    """
    path = Path(config_path)
    if not path.exists():
        return PipelineConfig(), None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_stages = data.get("enabled_stages", STAGE_ORDER)
        if not isinstance(raw_stages, list):
            return PipelineConfig(), "Configuration error: 'enabled_stages' must be a list of stage names."

        # Validate stage names
        invalid_stages = [s for s in raw_stages if s not in STAGE_ORDER]
        if invalid_stages:
            return (
                PipelineConfig(),
                f"Configuration error: Invalid stage name(s) {invalid_stages}. Allowed stages: {STAGE_ORDER}.",
            )

        cfg = PipelineConfig(
            enabled_stages=raw_stages,
            fail_fast=bool(data.get("fail_fast", False)),
            continue_on_database_unavailable=bool(data.get("continue_on_database_unavailable", True)),
            generate_reports=bool(data.get("generate_reports", True)),
            logging_enabled=bool(data.get("logging_enabled", True)),
        )
        return cfg, None

    except json.JSONDecodeError as e:
        return PipelineConfig(), f"Configuration error: Corrupted JSON in {path}: {e}"
    except Exception as e:
        return PipelineConfig(), f"Configuration error while loading {path}: {e}"


def save_pipeline_run_reports(
    summary: PipelineRunSummary, reports_dir: Path | str = "reports"
) -> tuple[Path, Path]:
    """
    Save unified execution summary to JSON and CSV reports without sensitive data.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "pipeline_run_report.json"
    csv_path = out_dir / "pipeline_run_report.csv"

    # Serialize JSON Report
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(asdict(summary), f, indent=2, default=str)

    # Serialize Stage-level CSV Summary
    stage_rows = []
    for st in summary.stages:
        stage_rows.append({
            "run_id": summary.run_id,
            "stage_name": st.get("stage_name"),
            "status": st.get("status"),
            "start_time": st.get("start_time"),
            "end_time": st.get("end_time"),
            "duration_seconds": st.get("duration_seconds"),
            "datasets_processed": st.get("datasets_processed"),
            "datasets_succeeded": st.get("datasets_succeeded"),
            "datasets_failed": st.get("datasets_failed"),
            "error_message": st.get("error_message") or "",
        })

    df_csv = pd.DataFrame(stage_rows)
    df_csv.to_csv(csv_path, index=False)

    return json_path, csv_path


class PipelineOrchestrator:
    """
    Central ETL Orchestrator coordinating all pipeline stages with dependency
    enforcement, observability, failure propagation, and reporting.
    """

    def __init__(
        self,
        config_path: Optional[Path | str] = None,
        root_dir: Optional[Path | str] = None,
        run_id: Optional[str] = None,
        reset_checkpoints: bool = False,
    ):
        self.root_dir = Path(root_dir).resolve() if root_dir else Path(__file__).resolve().parent.parent
        load_project_env(self.root_dir / ".env")
        self.reset_checkpoints = reset_checkpoints
        self.raw_dir = self.root_dir / "data" / "raw"
        self.processed_dir = self.root_dir / "data" / "processed"
        self.validated_dir = self.root_dir / "data" / "validated"
        self.rejected_dir = self.root_dir / "data" / "rejected"
        self.logs_dir = self.root_dir / "logs"
        self.reports_dir = self.root_dir / "reports"
        self.config_dir = self.root_dir / "config"

        self.run_id = run_id or generate_run_id()
        self.config_path = Path(config_path) if config_path else self.config_dir / "pipeline.json"

        # Load and validate configuration
        self.config, self.config_error = load_pipeline_config(self.config_path)

        # Execution tracking structures
        self.stage_results: dict[str, StageExecutionRecord] = {}
        self.dataset_records: list[DatasetExecutionRecord] = []

        # Inter-stage state caches
        self.ingestion_results: list[IngestionResult] = []
        self.incremental_results: list[IncrementalDecision] = []
        self.active_datasets: list[IngestionResult] = []
        self.checkpoint_store: Optional[CheckpointStore] = None
        self.incremental_config: Optional[IncrementalConfig] = None
        self.profiles: list[DatasetProfile] = []
        self.transformation_results: list[TransformationResult] = []
        self.validation_results: list[ValidationResult] = []
        self.validated_dfs: dict[str, pd.DataFrame] = {}
        self.database_results: list[DatabaseLoadResult] = []
        self.analytics_results: list[QueryResult] = []
        self.analytics_records: list[AnalyticsExecutionRecord] = []
        self.powerbi_record: Optional[PowerBIExecutionRecord] = None
        self.powerbi_semantic_model: Optional[dict[str, Any]] = None
        self.powerbi_dashboard_spec: Optional[dict[str, Any]] = None

        # Stage 11: Production Readiness & Observability
        self.monitor = PipelineMonitor(run_id=self.run_id, log_dir=self.logs_dir)
        self.lineage = PipelineLineageTracker(run_id=self.run_id)
        self.health_checker = PipelineHealthChecker(root_dir=self.root_dir)
        self.config_validator = ConfigurationValidator(config_dir=self.config_dir)
        self.health_result: Optional[HealthCheckResult] = None
        self.config_validation_result: Optional[ConfigValidationResult] = None

    def log(self, level: str, message: str) -> None:
        """Structured logging to logs/pipeline.log with automatic secret masking."""
        if not self.config.logging_enabled:
            return

        self.logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = self.logs_dir / "pipeline.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_msg = sanitize_text(message)

        log_entry = f"[{timestamp}] [{level.upper()}] [{self.run_id}] {clean_msg}\n"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)

    def record_stage_result(self, record: StageExecutionRecord) -> None:
        """Register stage execution metrics and log the event."""
        self.stage_results[record.stage_name] = record
        status_level = (
            "INFO"
            if record.status in ["SUCCESS", "SKIPPED"]
            else "WARNING"
            if record.status in ["DATABASE_UNAVAILABLE", "BLOCKED"]
            else "ERROR"
        )
        msg = f"Stage '{record.stage_name}' status: {record.status} ({record.duration_seconds}s)"
        if record.error_message:
            msg += f" - {record.error_message}"
        self.log(status_level, msg)

    def record_dataset_result(
        self,
        source_file: str,
        stage: str,
        status: str,
        output_path: Optional[str] = None,
        row_count: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Register dataset-level operation audit entry."""
        entry = DatasetExecutionRecord(
            run_id=self.run_id,
            source_file=source_file,
            stage=stage,
            status=status,
            output_path=output_path,
            row_count=row_count,
            error_message=sanitize_text(error_message),
        )
        self.dataset_records.append(entry)

    def can_stage_run(self, stage_name: str) -> tuple[bool, str, Optional[str]]:
        """
        Verify whether a stage is enabled and all required upstream stages succeeded.
        
        Returns: (can_run, status_if_blocked, reason)
        """
        if stage_name not in self.config.enabled_stages:
            return False, StageStatus.SKIPPED.value, "Stage disabled in configuration."

        dependencies = STAGE_DEPENDENCIES.get(stage_name, [])
        for dep in dependencies:
            if dep not in self.config.enabled_stages:
                # Upstream dependency was explicitly disabled by user config; bypass it
                continue
            dep_res = self.stage_results.get(dep)
            if not dep_res:
                return False, StageStatus.BLOCKED.value, f"Upstream dependency '{dep}' has not executed."
            if dep_res.status == StageStatus.DATABASE_UNAVAILABLE.value:
                return (
                    False,
                    StageStatus.DATABASE_UNAVAILABLE.value,
                    f"Upstream dependency '{dep}' is DATABASE_UNAVAILABLE.",
                )
            if dep_res.status != StageStatus.SUCCESS.value:
                return (
                    False,
                    StageStatus.BLOCKED.value,
                    f"Upstream dependency '{dep}' ended with status '{dep_res.status}' (expected SUCCESS).",
                )

        return True, StageStatus.RUNNING.value, None

    def execute_stage(self, stage_name: str) -> StageExecutionRecord:
        """Execute an individual pipeline stage with dependency checking and timing."""
        self.monitor.start_stage(stage_name)
        can_run, pre_status, reason = self.can_stage_run(stage_name)
        now_iso = datetime.now().isoformat()

        if not can_run:
            rec = StageExecutionRecord(
                run_id=self.run_id,
                stage_name=stage_name,
                status=pre_status,
                start_time=now_iso,
                end_time=now_iso,
                duration_seconds=0.0,
                error_message=reason,
            )
            if stage_name == "powerbi" and pre_status in [
                StageStatus.DATABASE_UNAVAILABLE.value,
                StageStatus.BLOCKED.value,
            ]:
                pbi_rec = PowerBIExecutionRecord(
                    run_id=self.run_id,
                    status=pre_status,
                    database_status="DATABASE_UNAVAILABLE",
                    execution_time=0.0,
                    errors=reason,
                )
                self.powerbi_record = pbi_rec
                if self.config.generate_reports:
                    save_powerbi_reports(pbi_rec, reports_dir=self.reports_dir / "powerbi", run_id=self.run_id)
                log_powerbi_event(pbi_rec, log_dir=self.logs_dir)
            self.record_stage_result(rec)
            self.monitor.end_stage(
                stage_name=stage_name,
                status=rec.status,
                records_in=0,
                records_out=0,
                records_rejected=0,
                error_message=rec.error_message,
            )
            return rec

        self.log("INFO", f"Stage '{stage_name}' started.")
        start_time_iso = datetime.now().isoformat()
        t0 = time.monotonic()

        try:
            if stage_name == "foundation":
                rec = self._run_foundation(start_time_iso, t0)
            elif stage_name == "ingestion":
                rec = self._run_ingestion(start_time_iso, t0)
            elif stage_name == "incremental":
                rec = self._run_incremental(start_time_iso, t0)
            elif stage_name == "profiling":
                rec = self._run_profiling(start_time_iso, t0)
            elif stage_name == "transformation":
                rec = self._run_transformation(start_time_iso, t0)
            elif stage_name == "validation":
                rec = self._run_validation(start_time_iso, t0)
            elif stage_name == "database":
                rec = self._run_database(start_time_iso, t0)
            elif stage_name == "analytics":
                rec = self._run_analytics(start_time_iso, t0)
            elif stage_name == "powerbi":
                rec = self._run_powerbi(start_time_iso, t0)
            else:
                duration = round(time.monotonic() - t0, 3)
                rec = StageExecutionRecord(
                    run_id=self.run_id,
                    stage_name=stage_name,
                    status=StageStatus.FAILED.value,
                    start_time=start_time_iso,
                    end_time=datetime.now().isoformat(),
                    duration_seconds=duration,
                    error_message=f"Unknown stage: {stage_name}",
                )

        except Exception as e:
            duration = round(time.monotonic() - t0, 3)
            rec = StageExecutionRecord(
                run_id=self.run_id,
                stage_name=stage_name,
                status=StageStatus.FAILED.value,
                start_time=start_time_iso,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message=f"Unhandled exception during stage execution: {sanitize_text(str(e))}",
            )

        self.record_stage_result(rec)

        # Stage 11: Monitoring & error classification
        if rec.status == StageStatus.FAILED.value:
            self.monitor.record_error(
                stage=stage_name,
                error_type=PipelineErrorCategory.UNEXPECTED_ERROR,
                message=rec.error_message or f"Stage {stage_name} failed.",
                recoverable=False,
            )
        elif rec.status == StageStatus.DATABASE_UNAVAILABLE.value:
            self.monitor.record_error(
                stage=stage_name,
                error_type=PipelineErrorCategory.DATABASE_UNAVAILABLE,
                message=rec.error_message or "PostgreSQL is offline or unconfigured.",
                recoverable=True,
            )

        self.monitor.end_stage(
            stage_name=stage_name,
            status=rec.status,
            records_in=rec.datasets_processed,
            records_out=rec.datasets_succeeded,
            records_rejected=rec.datasets_failed,
            error_message=rec.error_message,
        )
        return rec

    def _run_foundation(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 1: Foundation Directory Structure Verification."""
        is_valid = check_project_structure()
        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        if is_valid:
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="foundation",
                status=StageStatus.SUCCESS.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
            )
        else:
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="foundation",
                status=StageStatus.FAILED.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message="Missing required project foundation directories.",
            )

    def _run_ingestion(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 2: Universal Data Ingestion & File Detection."""
        results, _ = run_ingestion_pipeline(raw_dir=self.raw_dir, log_dir=self.logs_dir)
        self.ingestion_results = results

        total = len(results)
        succeeded = sum(1 for r in results if r.status == "SUCCESS")
        failed = sum(1 for r in results if r.status == "FAILED")

        for r in results:
            rows = len(r.df) if r.df is not None else 0
            fmt = getattr(r, "file_type", None) or "UNKNOWN"
            self.lineage.record_ingestion(
                dataset_name=r.file_name,
                source_format=fmt,
                row_count=rows,
            )
            self.record_dataset_result(
                source_file=r.file_name,
                stage="ingestion",
                status=r.status,
                row_count=rows,
                error_message=r.error_message,
            )

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        # If files were discovered and all failed, stage failed; if no files or some succeeded, stage succeeded
        status = StageStatus.SUCCESS.value if (total == 0 or succeeded > 0) else StageStatus.FAILED.value
        err = f"All {total} discovered datasets failed during ingestion." if (total > 0 and succeeded == 0) else None

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="ingestion",
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=total,
            datasets_succeeded=succeeded,
            datasets_failed=failed,
            error_message=err,
        )

    def _run_incremental(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 8: Incremental Data Processing Engine."""
        inc_cfg_path = self.config_dir / "incremental.json"
        cfg, cfg_err = load_incremental_config(inc_cfg_path)
        if cfg_err:
            duration = round(time.monotonic() - t0, 3)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="incremental",
                status=StageStatus.CONFIG_ERROR.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message=cfg_err,
            )

        self.incremental_config = cfg
        if not cfg.enabled:
            # Incremental processing disabled by config
            self.active_datasets = [
                r for r in self.ingestion_results if r.status == "SUCCESS" and r.df is not None
            ]
            duration = round(time.monotonic() - t0, 3)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="incremental",
                status=StageStatus.SKIPPED.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                datasets_processed=0,
                datasets_succeeded=0,
                datasets_failed=0,
                error_message="Incremental processing is disabled in configuration.",
            )

        cp_path = Path(cfg.checkpoint_file)
        if not cp_path.is_absolute():
            cp_path = self.root_dir / cp_path
        self.checkpoint_store = CheckpointStore(cp_path)
        if self.reset_checkpoints:
            self.checkpoint_store.reset()
            self.log("INFO", f"Incremental checkpoint state safely reset at {cp_path}")

        successful_ingestions = [
            r for r in self.ingestion_results if r.status == "SUCCESS" and r.df is not None
        ]

        decisions: list[IncrementalDecision] = []
        active_ingestions: list[IngestionResult] = []

        for ing_res in successful_ingestions:
            src_file = self.raw_dir / ing_res.file_name
            if not src_file.exists():
                continue

            decision = evaluate_dataset_incremental(
                file_path=src_file,
                df=ing_res.df,
                config=cfg,
                checkpoint_store=self.checkpoint_store,
            )
            decisions.append(decision)
            log_incremental_event(decision, log_dir=self.logs_dir, run_id=self.run_id)

            self.lineage.record_incremental(
                dataset_name=ing_res.file_name,
                decision=decision.decision,
                action=decision.action,
                file_hash=getattr(decision, "file_hash", None),
            )

            if decision.action in ["PROCESS", "REPROCESS"]:
                active_ingestions.append(ing_res)
                self.record_dataset_result(
                    source_file=ing_res.file_name,
                    stage="incremental",
                    status=decision.decision,
                    row_count=len(ing_res.df) if ing_res.df is not None else 0,
                    error_message=decision.comparison_note,
                )
            else:
                self.record_dataset_result(
                    source_file=ing_res.file_name,
                    stage="incremental",
                    status="UNCHANGED_SKIPPED",
                    row_count=len(ing_res.df) if ing_res.df is not None else 0,
                    error_message=decision.reason,
                )

        self.incremental_results = decisions
        self.active_datasets = active_ingestions

        if decisions and self.config.generate_reports:
            save_incremental_reports(decisions, reports_dir=self.reports_dir, run_id=self.run_id)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="incremental",
            status=StageStatus.SUCCESS.value,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(successful_ingestions),
            datasets_succeeded=len(decisions),
            datasets_failed=0,
        )

    def _get_active_ingestions(self) -> list[IngestionResult]:
        """Return active datasets for processing based on incremental decisions."""
        if (
            "incremental" in self.config.enabled_stages
            and self.incremental_config is not None
            and self.incremental_config.enabled
        ):
            return self.active_datasets
        return [r for r in self.ingestion_results if r.status == "SUCCESS" and r.df is not None]

    def _run_profiling(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 3: Schema Detection & Profiling."""
        active_ingestions = self._get_active_ingestions()
        skipped_names = [
            r.file_name
            for r in self.ingestion_results
            if r.status == "SUCCESS" and r.file_name not in {a.file_name for a in active_ingestions}
        ]
        for sk_name in skipped_names:
            self.record_dataset_result(
                source_file=sk_name,
                stage="profiling",
                status="UNCHANGED_SKIPPED",
                error_message="Skipped downstream profiling due to unchanged file content.",
            )

        profiles: list[DatasetProfile] = []
        for res in active_ingestions:
            try:
                prof = profile_dataset(res.df.copy(), filename=res.file_name)
                log_profiling_event(prof, status="SUCCESS", log_dir=self.logs_dir)
                profiles.append(prof)
                self.record_dataset_result(
                    source_file=res.file_name,
                    stage="profiling",
                    status="SUCCESS",
                    row_count=prof.row_count,
                )
            except Exception as e:
                self.record_dataset_result(
                    source_file=res.file_name,
                    stage="profiling",
                    status="FAILED",
                    error_message=str(e),
                )

        self.profiles = profiles
        if profiles and self.config.generate_reports:
            save_profiling_reports(profiles, reports_dir=self.reports_dir)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="profiling",
            status=StageStatus.SUCCESS.value,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(active_ingestions),
            datasets_succeeded=len(profiles),
            datasets_failed=len(active_ingestions) - len(profiles),
        )

    def _run_transformation(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 4: Data Cleaning & Transformation."""
        active_ingestions = self._get_active_ingestions()
        skipped_names = [
            r.file_name
            for r in self.ingestion_results
            if r.status == "SUCCESS" and r.file_name not in {a.file_name for a in active_ingestions}
        ]
        for sk_name in skipped_names:
            self.record_dataset_result(
                source_file=sk_name,
                stage="transformation",
                status="UNCHANGED_SKIPPED",
                error_message="Skipped downstream transformation due to unchanged file content.",
            )

        cfg = load_transformation_config(self.config_dir / "transformations.json")
        profile_map = {p.filename: p for p in self.profiles}
        transformation_results: list[TransformationResult] = []

        for res in active_ingestions:
            schema_prof = profile_map.get(res.file_name)
            trans_res = transform_dataset(
                df=res.df.copy(),
                source_filename=res.file_name,
                schema_profile=schema_prof,
                config=cfg,
            )

            out_path = None
            if trans_res.status == "SUCCESS" and trans_res.cleaned_df is not None:
                out_file = save_processed_dataset(
                    cleaned_df=trans_res.cleaned_df,
                    source_filename=res.file_name,
                    processed_dir=self.processed_dir,
                )
                trans_res.output_filename = out_file.name
                out_path = str(out_file)

            log_transformation_event(trans_res, log_dir=self.logs_dir)
            transformation_results.append(trans_res)

            rows = len(trans_res.cleaned_df) if trans_res.cleaned_df is not None else 0
            self.lineage.record_transformation(
                dataset_name=res.file_name,
                transformed_rows=rows,
            )
            err_msg = "; ".join(trans_res.errors) if getattr(trans_res, "errors", None) else getattr(trans_res, "error_message", None)
            self.record_dataset_result(
                source_file=res.file_name,
                stage="transformation",
                status=trans_res.status,
                output_path=out_path,
                row_count=rows,
                error_message=err_msg,
            )

        self.transformation_results = transformation_results
        if transformation_results and self.config.generate_reports:
            save_transformation_report(transformation_results, reports_dir=self.reports_dir)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()
        succeeded = sum(1 for t in transformation_results if t.status == "SUCCESS")

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="transformation",
            status=StageStatus.SUCCESS.value,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(active_ingestions),
            datasets_succeeded=succeeded,
            datasets_failed=len(active_ingestions) - succeeded,
        )

    def _run_validation(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 5: Data Quality & Validation Engine."""
        active_ingestions = self._get_active_ingestions()
        skipped_names = [
            r.file_name
            for r in self.ingestion_results
            if r.status == "SUCCESS" and r.file_name not in {a.file_name for a in active_ingestions}
        ]
        for sk_name in skipped_names:
            self.record_dataset_result(
                source_file=sk_name,
                stage="validation",
                status="UNCHANGED_SKIPPED",
                error_message="Skipped downstream validation due to unchanged file content.",
            )

        successful_transformations = [
            t for t in self.transformation_results if t.status == "SUCCESS" and t.cleaned_df is not None
        ]

        profile_map = {p.filename: p for p in self.profiles}
        validation_results: list[ValidationResult] = []
        validated_dfs: dict[str, pd.DataFrame] = {}

        for trans in successful_transformations:
            schema_prof = profile_map.get(trans.source_filename)
            val_cfg = load_validation_config(
                self.config_dir / "validation.json", dataset_name=trans.source_filename
            )

            val_res, valid_df, rejected_df = validate_dataset(
                df=trans.cleaned_df,
                source_filename=trans.source_filename,
                config=val_cfg,
                schema_profile=schema_prof,
            )

            val_file, rej_file = save_validation_outputs(
                source_filename=trans.source_filename,
                valid_df=valid_df,
                rejected_df=rejected_df,
                validated_dir=self.validated_dir,
                rejected_dir=self.rejected_dir,
            )

            val_res.validated_output_path = str(val_file)
            if rej_file:
                val_res.rejected_output_path = str(rej_file)

            log_validation_event(val_res, log_dir=self.logs_dir)
            validation_results.append(val_res)
            validated_dfs[trans.source_filename] = valid_df

            inv_count = val_res.metrics.invalid_rows if hasattr(val_res, "metrics") and val_res.metrics else 0
            err_note = f"{inv_count} quarantined" if inv_count > 0 else None
            self.lineage.record_validation(
                dataset_name=trans.source_filename,
                valid_rows=len(valid_df),
                rejected_rows=inv_count,
                quarantine_path=val_res.rejected_output_path,
            )
            self.record_dataset_result(
                source_file=trans.source_filename,
                stage="validation",
                status=val_res.status,
                output_path=str(val_file),
                row_count=len(valid_df),
                error_message=err_note,
            )

        self.validation_results = validation_results
        self.validated_dfs = validated_dfs

        if validation_results and self.config.generate_reports:
            save_validation_reports(validation_results, reports_dir=self.reports_dir)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()
        succeeded = sum(1 for v in validation_results if v.status == "PASSED")

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="validation",
            status=StageStatus.SUCCESS.value,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(successful_transformations),
            datasets_succeeded=succeeded,
            datasets_failed=len(successful_transformations) - succeeded,
        )

    def _run_database(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 6: PostgreSQL Database & Dynamic Data Modeling."""
        active_ingestions = self._get_active_ingestions()
        skipped_names = [
            r.file_name
            for r in self.ingestion_results
            if r.status == "SUCCESS" and r.file_name not in {a.file_name for a in active_ingestions}
        ]
        for sk_name in skipped_names:
            self.record_dataset_result(
                source_file=sk_name,
                stage="database",
                status="UNCHANGED_SKIPPED",
                error_message="Skipped database load due to unchanged file content.",
            )

        cfg = load_database_config(self.config_dir / "database.json")
        profile_map = {p.filename: p for p in self.profiles}

        # If no datasets need database loading (e.g. all were unchanged and skipped)
        if len(self.validated_dfs) == 0:
            duration = round(time.monotonic() - t0, 3)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="database",
                status=StageStatus.SUCCESS.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                datasets_processed=0,
                datasets_succeeded=0,
                datasets_failed=0,
            )

        dotenv_path = self.root_dir / ".env"
        engine, engine_err = get_database_engine(dotenv_path=dotenv_path)
        connected = False
        conn_err = None

        if engine is not None:
            connected, conn_err = test_database_connection(engine)

        db_results: list[DatabaseLoadResult] = []

        for source_name, valid_df in self.validated_dfs.items():
            schema_prof = profile_map.get(source_name)

            if not connected:
                configured = get_database_url(dotenv_path=dotenv_path) is not None
                status_label = "DATABASE_UNAVAILABLE" if engine is not None else "CONFIG_MISSING"
                if engine is not None:
                    reason_text = f"PostgreSQL server unreachable: {conn_err}"
                elif configured:
                    reason_text = f"PostgreSQL engine initialization failed: {engine_err}"
                else:
                    reason_text = "DATABASE_URL not configured in environment"
                res = DatabaseLoadResult(
                    source_dataset=source_name,
                    target_table=generate_table_name(source_name),
                    database_status=status_label,
                    connection_status="FAILED" if engine is not None else "UNCONFIGURED",
                    rows_attempted=len(valid_df),
                    rows_inserted=0,
                    rows_failed=len(valid_df),
                    error_message=sanitize_text(reason_text),
                )
            else:
                res = load_validated_dataset(
                    df=valid_df,
                    source_filename=source_name,
                    engine=engine,
                    config=cfg,
                    schema_profile=schema_prof,
                )

            log_database_event(res, log_dir=self.logs_dir)
            db_results.append(res)

            self.lineage.record_database_load(
                dataset_name=source_name,
                target_table=res.target_table,
                loaded_rows=res.rows_inserted,
                status=res.database_status,
            )

            self.record_dataset_result(
                source_file=source_name,
                stage="database",
                status=res.database_status,
                output_path=res.target_table,
                row_count=res.rows_inserted,
                error_message=res.error_message,
            )

        self.database_results = db_results
        if db_results and self.config.generate_reports:
            save_database_load_reports(db_results, reports_dir=self.reports_dir)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        if not connected:
            stage_status = StageStatus.DATABASE_UNAVAILABLE.value
            configured = get_database_url(dotenv_path=dotenv_path) is not None
            if engine is not None:
                err_msg = sanitize_text(conn_err)
            elif configured:
                err_msg = sanitize_text(f"Engine initialization failed: {engine_err}")
            else:
                err_msg = "DATABASE_URL is not set."
            succeeded_count = 0
            failed_count = len(self.validated_dfs)
        else:
            succeeded_count = sum(1 for r in db_results if r.database_status == "SUCCESS")
            failed_count = len(db_results) - succeeded_count
            stage_status = StageStatus.SUCCESS.value if failed_count == 0 else StageStatus.FAILED.value
            err_msg = "One or more datasets failed to load into database." if failed_count > 0 else None

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="database",
            status=stage_status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(self.validated_dfs),
            datasets_succeeded=succeeded_count,
            datasets_failed=failed_count,
            error_message=err_msg,
        )

    def _run_analytics(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 9: SQL Analytics Layer."""
        an_cfg_path = self.config_dir / "analytics.json"
        cfg, cfg_err = load_analytics_config(an_cfg_path)
        if cfg_err:
            duration = round(time.monotonic() - t0, 3)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="analytics",
                status=StageStatus.CONFIG_ERROR.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message=cfg_err,
            )

        if not cfg.enabled:
            duration = round(time.monotonic() - t0, 3)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="analytics",
                status=StageStatus.SKIPPED.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message="Analytics is disabled in configuration.",
            )

        # Check database connectivity
        dotenv_path = self.root_dir / ".env"
        engine, engine_err = get_database_engine(dotenv_path=dotenv_path)
        connected = False
        conn_err = None
        if engine is not None:
            connected, conn_err = test_database_connection(engine)

        if not connected:
            duration = round(time.monotonic() - t0, 3)
            configured = get_database_url(dotenv_path=dotenv_path) is not None
            if engine is not None:
                err_msg = f"PostgreSQL server unreachable: {conn_err}"
            elif configured:
                err_msg = f"PostgreSQL engine initialization failed: {engine_err}"
            else:
                err_msg = "DATABASE_URL not configured in environment"
            unavailable_rec = AnalyticsExecutionRecord(
                run_id=self.run_id,
                query_name="database_connectivity",
                table="N/A",
                status="DATABASE_UNAVAILABLE",
                execution_time=duration,
                result_rows=0,
                output_files=[],
                error_message=sanitize_text(err_msg),
            )
            if self.config.generate_reports:
                save_analytics_reports([unavailable_rec], reports_dir=self.reports_dir, run_id=self.run_id)
            log_analytics_event(unavailable_rec, log_dir=self.logs_dir)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="analytics",
                status=StageStatus.DATABASE_UNAVAILABLE.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                datasets_processed=0,
                datasets_succeeded=0,
                datasets_failed=0,
                error_message=sanitize_text(err_msg),
            )

        analytics_engine = SQLAnalyticsEngine(
            engine=engine,
            config=cfg,
            queries_dir=self.root_dir / "sql" / "analytics",
            root_dir=self.root_dir,
        )

        target_tables = [
            r.target_table for r in self.database_results if r.database_status == "SUCCESS"
        ]
        if not target_tables:
            target_tables = analytics_engine.list_tables()

        all_query_results: list[QueryResult] = []
        all_exec_records: list[AnalyticsExecutionRecord] = []
        reports_analytics_dir = self.reports_dir / "analytics"

        for tbl in target_tables:
            query_results = analytics_engine.run_automated_analytics_suite(
                table_name=tbl,
                reports_dir=reports_analytics_dir,
            )
            all_query_results.extend(query_results)

            for qr in query_results:
                exec_rec = AnalyticsExecutionRecord(
                    run_id=self.run_id,
                    query_name=qr.query_name,
                    table=qr.table_name,
                    status=qr.status,
                    execution_time=qr.execution_time_seconds,
                    result_rows=qr.row_count,
                    output_files=list(qr.output_files.values()),
                    error_message=qr.error_message,
                )
                all_exec_records.append(exec_rec)
                log_analytics_event(exec_rec, log_dir=self.logs_dir)

                self.record_dataset_result(
                    source_file=tbl,
                    stage="analytics",
                    status=qr.status,
                    output_path="; ".join(qr.output_files.values()) if qr.output_files else None,
                    row_count=qr.row_count,
                    error_message=qr.error_message,
                )

        self.analytics_results = all_query_results
        self.analytics_records = all_exec_records

        if all_exec_records and self.config.generate_reports:
            save_analytics_reports(all_exec_records, reports_dir=self.reports_dir, run_id=self.run_id)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()
        total_queries = len(all_exec_records)
        succeeded_queries = sum(1 for r in all_exec_records if r.status == "SUCCESS")
        failed_queries = total_queries - succeeded_queries

        stage_status = StageStatus.SUCCESS.value if (total_queries == 0 or failed_queries == 0) else StageStatus.FAILED.value
        err_note = f"{failed_queries} analytics queries failed." if failed_queries > 0 else None

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="analytics",
            status=stage_status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=len(target_tables),
            datasets_succeeded=succeeded_queries,
            datasets_failed=failed_queries,
            error_message=err_note,
        )

    def _run_powerbi(self, start_time: str, t0: float) -> StageExecutionRecord:
        """Stage 10: Power BI Integration & BI Readiness."""
        pbi_cfg_path = self.config_dir / "powerbi.json"
        cfg, cfg_err = load_powerbi_config(pbi_cfg_path)
        if cfg_err:
            duration = round(time.monotonic() - t0, 3)
            rec = PowerBIExecutionRecord(
                run_id=self.run_id,
                status=StageStatus.CONFIG_ERROR.value,
                database_status="CONFIG_ERROR",
                execution_time=duration,
                errors=cfg_err,
            )
            self.powerbi_record = rec
            if self.config.generate_reports:
                save_powerbi_reports(rec, reports_dir=self.reports_dir / "powerbi", run_id=self.run_id)
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="powerbi",
                status=StageStatus.CONFIG_ERROR.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message=cfg_err,
            )

        if not cfg.enabled:
            duration = round(time.monotonic() - t0, 3)
            rec = PowerBIExecutionRecord(
                run_id=self.run_id,
                status=StageStatus.SKIPPED.value,
                database_status="SKIPPED",
                execution_time=duration,
                errors="Power BI integration is disabled in configuration.",
            )
            self.powerbi_record = rec
            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="powerbi",
                status=StageStatus.SKIPPED.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                error_message="Power BI integration is disabled in configuration.",
            )

        # Check database connectivity
        dotenv_path = self.root_dir / ".env"
        engine, engine_err = get_database_engine(dotenv_path=dotenv_path)
        connected = False
        conn_err = None
        if engine is not None:
            connected, conn_err = test_database_connection(engine)

        powerbi_dir = self.root_dir / "powerbi"
        pbi_engine = PowerBIIntegrationEngine(
            engine=engine if connected else None,
            config=cfg,
            schema=cfg.schema,
            powerbi_dir=powerbi_dir,
            root_dir=self.root_dir,
        )

        if not connected:
            duration = round(time.monotonic() - t0, 3)
            configured = get_database_url(dotenv_path=dotenv_path) is not None
            if engine is not None:
                err_msg = f"PostgreSQL server unreachable: {conn_err}"
            elif configured:
                err_msg = f"PostgreSQL engine initialization failed: {engine_err}"
            else:
                err_msg = "DATABASE_URL not configured in environment"
            exec_record, semantic_model, dash_spec = pbi_engine.run_integration_suite(
                output_dir=powerbi_dir,
                run_id=self.run_id,
            )
            exec_record.errors = sanitize_text(err_msg)
            self.powerbi_record = exec_record
            self.powerbi_semantic_model = semantic_model
            self.powerbi_dashboard_spec = dash_spec

            if self.config.generate_reports:
                save_powerbi_reports(exec_record, reports_dir=self.reports_dir / "powerbi", run_id=self.run_id)
            log_powerbi_event(exec_record, log_dir=self.logs_dir)

            return StageExecutionRecord(
                run_id=self.run_id,
                stage_name="powerbi",
                status=StageStatus.DATABASE_UNAVAILABLE.value,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                duration_seconds=duration,
                datasets_processed=0,
                datasets_succeeded=0,
                datasets_failed=0,
                error_message=sanitize_text(err_msg),
            )

        # Full execution when database connected
        exec_record, semantic_model, dash_spec = pbi_engine.run_integration_suite(
            output_dir=powerbi_dir,
            run_id=self.run_id,
        )

        self.powerbi_record = exec_record
        self.powerbi_semantic_model = semantic_model
        self.powerbi_dashboard_spec = dash_spec

        if self.config.generate_reports:
            save_powerbi_reports(exec_record, reports_dir=self.reports_dir / "powerbi", run_id=self.run_id)
        log_powerbi_event(exec_record, log_dir=self.logs_dir)

        duration = round(time.monotonic() - t0, 3)
        end_time = datetime.now().isoformat()

        return StageExecutionRecord(
            run_id=self.run_id,
            stage_name="powerbi",
            status=exec_record.status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            datasets_processed=exec_record.tables_discovered,
            datasets_succeeded=exec_record.tables_discovered,
            datasets_failed=0 if exec_record.status == "SUCCESS" else 1,
            error_message=exec_record.errors,
        )

    def _commit_checkpoints(self) -> None:
        """
        Update checkpoint store with successfully processed datasets.

        Requirements:
        - Only update for datasets where pipeline processing succeeded.
        - If validation failed or transformation failed, DO NOT mark SUCCESS.
        - Clearly distinguish pipeline status from database persistence status.
        - Atomic save to disk.
        """
        if not self.checkpoint_store or not self.incremental_results:
            return

        val_status_map = {v.source_filename: v.status for v in self.validation_results}
        db_status_map = {r.source_dataset: r.database_status for r in self.database_results}

        updated = False
        for dec in self.incremental_results:
            # If dataset was skipped because it's UNCHANGED, its existing checkpoint is already valid
            if dec.action == "SKIP" and dec.decision == "UNCHANGED":
                continue

            filename = dec.dataset_name
            val_status = val_status_map.get(filename)
            db_status = db_status_map.get(filename)

            # Check if pipeline processing succeeded
            # A dataset succeeds if validation PASSED (or if validation was skipped/disabled, transformation SUCCESS)
            pipeline_success = (val_status == "PASSED") or (
                "validation" not in self.config.enabled_stages and any(
                    t.source_filename == filename and t.status == "SUCCESS" for t in self.transformation_results
                )
            )

            if pipeline_success:
                effective_db_status = db_status if db_status else (
                    "NOT_RUN" if "database" not in self.config.enabled_stages else "DATABASE_UNAVAILABLE"
                )

                src_path = Path(dec.source_path)
                file_size = src_path.stat().st_size if src_path.exists() else 0
                mod_time = (
                    datetime.fromtimestamp(src_path.stat().st_mtime).isoformat()
                    if src_path.exists()
                    else datetime.now().isoformat()
                )

                cp = DatasetCheckpoint(
                    source_path=dec.source_path,
                    file_hash=dec.current_hash,
                    file_size=file_size,
                    modified_time=mod_time,
                    last_processed_at=datetime.now().isoformat(),
                    last_status="SUCCESS",
                    database_status=effective_db_status,
                    record_count=dec.records_current,
                    record_hashes=dec.current_record_hashes,
                )
                self.checkpoint_store.update(filename, cp)
                updated = True
                self.log("INFO", f"Checkpoint updated for '{filename}': SUCCESS (DB: {effective_db_status})")
            else:
                self.log("WARNING", f"Checkpoint NOT updated to SUCCESS for '{filename}' due to pipeline processing failure.")

        if updated:
            self.checkpoint_store.save()

    def determine_pipeline_status(self) -> str:
        """Calculate overall pipeline run status based on stage outcomes and configuration."""
        if self.config_error:
            return PipelineStatus.CONFIG_ERROR.value

        enabled_statuses = [
            rec.status
            for stage, rec in self.stage_results.items()
            if stage in self.config.enabled_stages
        ]

        if any(s in [StageStatus.FAILED.value, StageStatus.CONFIG_ERROR.value] for s in enabled_statuses):
            return PipelineStatus.FAILED.value

        if any(s == StageStatus.BLOCKED.value for s in enabled_statuses):
            return PipelineStatus.FAILED.value

        # PostgreSQL unavailable check
        if any(s == StageStatus.DATABASE_UNAVAILABLE.value for s in enabled_statuses):
            if self.config.continue_on_database_unavailable:
                return PipelineStatus.COMPLETED_WITH_WARNINGS.value
            else:
                return PipelineStatus.FAILED.value

        # Check for quarantined records or skipped datasets
        has_quarantine_warnings = any(
            (getattr(v.metrics, "invalid_rows", 0) > 0)
            if hasattr(v, "metrics") and v.metrics
            else False
            for v in self.validation_results
        )
        has_skipped_datasets = any(
            r.status == "SKIPPED" for r in self.ingestion_results
        )

        if has_quarantine_warnings or has_skipped_datasets:
            return PipelineStatus.COMPLETED_WITH_WARNINGS.value

        if all(s == StageStatus.SUCCESS.value for s in enabled_statuses):
            return PipelineStatus.COMPLETED.value

        return PipelineStatus.COMPLETED.value

    def execute_pipeline(self, display_summary: bool = True) -> PipelineRunSummary:
        """
        Orchestrate complete pipeline execution across enabled stages in dependency order.
        """
        p_start_iso = datetime.now().isoformat()
        t_start = time.monotonic()

        self.log("INFO", f"Pipeline execution initiated. Run ID: {self.run_id}")

        if self.config_error:
            self.log("ERROR", f"Aborting pipeline execution due to configuration error: {self.config_error}")
            duration = round(time.monotonic() - t_start, 3)
            p_end_iso = datetime.now().isoformat()
            summary = PipelineRunSummary(
                run_id=self.run_id,
                pipeline_status=PipelineStatus.CONFIG_ERROR.value,
                start_time=p_start_iso,
                end_time=p_end_iso,
                duration_seconds=duration,
                configuration=asdict(self.config),
                stages=[],
                datasets=[],
                summary={
                    "total_stages": len(STAGE_ORDER),
                    "enabled_stages": len(self.config.enabled_stages),
                    "stages_succeeded": 0,
                    "stages_failed": 0,
                    "stages_skipped": 0,
                    "stages_blocked": 0,
                    "stages_database_unavailable": 0,
                    "total_datasets_discovered": 0,
                    "successful_datasets": 0,
                    "failed_datasets": 0,
                    "skipped_datasets": 0,
                    "database_unavailable_datasets": 0,
                },
                error_message=self.config_error,
            )
            if display_summary:
                self._display_summary(summary)
            if self.config.generate_reports:
                save_pipeline_run_reports(summary, reports_dir=self.reports_dir)
            return summary

        # Execute stages in canonical order
        for stage_name in STAGE_ORDER:
            # If fail_fast is enabled and an earlier stage failed, block remaining stages
            if self.config.fail_fast:
                prior_failed = any(
                    r.status == StageStatus.FAILED.value
                    for r in self.stage_results.values()
                )
                if prior_failed and stage_name in self.config.enabled_stages:
                    rec = StageExecutionRecord(
                        run_id=self.run_id,
                        stage_name=stage_name,
                        status=StageStatus.BLOCKED.value,
                        start_time=datetime.now().isoformat(),
                        end_time=datetime.now().isoformat(),
                        duration_seconds=0.0,
                        error_message="Execution blocked due to fail_fast=True after prior stage failure.",
                    )
                    self.record_stage_result(rec)
                    continue

            self.execute_stage(stage_name)

        # Commit updated checkpoints for successfully processed datasets
        self._commit_checkpoints()

        p_duration = round(time.monotonic() - t_start, 3)
        p_end_iso = datetime.now().isoformat()
        final_status = self.determine_pipeline_status()

        # Compute dataset aggregate counts
        total_discovered = len(self.ingestion_results)
        succeeded_datasets = sum(
            1 for v in self.validation_results if v.status == "PASSED"
        )
        failed_datasets = sum(
            1 for r in self.ingestion_results if r.status == "FAILED"
        ) + sum(
            1 for t in self.transformation_results if t.status == "FAILED"
        )
        skipped_datasets = sum(
            1 for r in self.ingestion_results if r.status == "SKIPPED"
        )
        db_unavail_datasets = len(self.validated_dfs) if any(
            s.status == StageStatus.DATABASE_UNAVAILABLE.value for s in self.stage_results.values()
        ) else 0

        summary = PipelineRunSummary(
            run_id=self.run_id,
            pipeline_status=final_status,
            start_time=p_start_iso,
            end_time=p_end_iso,
            duration_seconds=p_duration,
            configuration=asdict(self.config),
            stages=[asdict(r) for r in self.stage_results.values()],
            datasets=[asdict(d) for d in self.dataset_records],
            summary={
                "total_stages": len(STAGE_ORDER),
                "enabled_stages": len(self.config.enabled_stages),
                "stages_succeeded": sum(1 for r in self.stage_results.values() if r.status == StageStatus.SUCCESS.value),
                "stages_failed": sum(1 for r in self.stage_results.values() if r.status == StageStatus.FAILED.value),
                "stages_skipped": sum(1 for r in self.stage_results.values() if r.status == StageStatus.SKIPPED.value),
                "stages_blocked": sum(1 for r in self.stage_results.values() if r.status == StageStatus.BLOCKED.value),
                "stages_database_unavailable": sum(1 for r in self.stage_results.values() if r.status == StageStatus.DATABASE_UNAVAILABLE.value),
                "total_datasets_discovered": total_discovered,
                "successful_datasets": succeeded_datasets,
                "failed_datasets": failed_datasets,
                "skipped_datasets": skipped_datasets,
                "database_unavailable_datasets": db_unavail_datasets,
                "incremental_stage_status": self.stage_results.get("incremental", StageExecutionRecord(run_id=self.run_id, stage_name="incremental", status="NOT_RUN")).status,
                "datasets_new": sum(1 for d in self.incremental_results if d.decision == "NEW"),
                "datasets_unchanged": sum(1 for d in self.incremental_results if d.decision == "UNCHANGED"),
                "datasets_changed": sum(1 for d in self.incremental_results if d.decision == "CHANGED"),
                "datasets_reprocessed": sum(1 for d in self.incremental_results if d.action == "REPROCESS"),
                "datasets_incremental_skipped": sum(1 for d in self.incremental_results if d.action == "SKIP"),
                "record_delta_metrics": {
                    d.dataset_name: {
                        "records_previous": d.records_previous,
                        "records_current": d.records_current,
                        "records_new": d.records_new,
                        "records_changed": d.records_changed,
                        "records_unchanged": d.records_unchanged,
                        "records_removed": d.records_removed,
                        "record_comparison_available": d.record_comparison_available,
                        "comparison_note": d.comparison_note,
                    }
                    for d in self.incremental_results
                },
                "analytics_stage_status": self.stage_results.get("analytics", StageExecutionRecord(run_id=self.run_id, stage_name="analytics", status="NOT_RUN")).status,
                "analytics_queries_executed": len(self.analytics_records),
                "analytics_queries_succeeded": sum(1 for r in self.analytics_records if r.status == "SUCCESS"),
                "analytics_queries_failed": sum(1 for r in self.analytics_records if r.status != "SUCCESS"),
            },
        )

        self.log(
            "INFO" if final_status == "COMPLETED" else "WARNING",
            f"Pipeline finished with status '{final_status}' in {p_duration}s.",
        )

        # Stage 11: Production Readiness Metrics & Health Diagnostic
        health_res = self.health_checker.run_health_check()
        self.health_result = health_res

        records_read = sum(len(r.df) for r in self.ingestion_results if r.df is not None)
        records_processed = sum(len(t.cleaned_df) for t in self.transformation_results if t.cleaned_df is not None)
        records_rejected = sum((getattr(v.metrics, "invalid_rows", 0)) for v in self.validation_results if hasattr(v, "metrics") and v.metrics)
        records_loaded = sum(r.rows_inserted for r in self.database_results)
        records_skipped = sum(d.records_current for d in self.incremental_results if d.action == "SKIP")
        records_reprocessed = sum(d.records_current for d in self.incremental_results if d.action == "REPROCESS")
        avg_quality_score = (
            (sum(v.metrics.quality_score for v in self.validation_results if hasattr(v, "metrics") and v.metrics) / len(self.validation_results))
            if self.validation_results
            else 100.0
        )
        db_status = self.stage_results.get("database", StageExecutionRecord(run_id=self.run_id, stage_name="database", status="NOT_RUN")).status
        an_status = self.stage_results.get("analytics", StageExecutionRecord(run_id=self.run_id, stage_name="analytics", status="NOT_RUN")).status
        pbi_status = self.stage_results.get("powerbi", StageExecutionRecord(run_id=self.run_id, stage_name="powerbi", status="NOT_RUN")).status

        self.monitor.update_aggregate_metrics(
            dataset_count=total_discovered,
            successful_dataset_count=succeeded_datasets,
            failed_dataset_count=failed_datasets,
            records_read=records_read,
            records_processed=records_processed,
            records_rejected=records_rejected,
            records_loaded=records_loaded,
            records_skipped=records_skipped,
            records_reprocessed=records_reprocessed,
            validation_quality_score=avg_quality_score,
            database_status=db_status,
            analytics_status=an_status,
            powerbi_status=pbi_status,
        )
        self.monitor.finalize_run(final_status)

        if self.config.generate_reports:
            save_pipeline_run_reports(summary, reports_dir=self.reports_dir)
            self.monitor.save_run_summary_reports(reports_dir=self.reports_dir)
            self.lineage.save_lineage_reports(reports_dir=self.reports_dir)
            self.monitor.save_production_readiness_reports(
                reports_dir=self.reports_dir,
                health_status=health_res.overall_status,
                database_connected=health_res.database_connected,
            )

        if display_summary:
            self._display_summary(summary)
        return summary

    def _display_summary(self, summary: PipelineRunSummary) -> None:
        """Render concise, aesthetic terminal summary for user observability."""
        print("\n" + "=" * 52)
        print("UNIVERSAL DATA ENGINEERING PIPELINE")
        print("=" * 52)
        print(f"\nRun ID: {summary.run_id}\n")

        print(f"{'Stage':<18} {'Status':<22} {'Duration':<10}")
        print("-" * 52)

        for st in summary.stages:
            name = st.get("stage_name", "").capitalize()
            stat = st.get("status", "")
            dur = f"{st.get('duration_seconds', 0.0):.2f}s"
            print(f"{name:<18} {stat:<22} {dur:<10}")

        print("-" * 52)
        s = summary.summary
        print(f"Datasets discovered:   {s.get('total_datasets_discovered', 0)}")
        print(f"Datasets successful:   {s.get('successful_datasets', 0)}")
        print(f"Database unavailable:  {s.get('database_unavailable_datasets', 0)}")

        if self.incremental_results:
            print("-" * 52)
            print("Incremental Processing:")
            for d in self.incremental_results:
                print(f"  {d.dataset_name:<24} {d.decision:<12} -> {d.action}")
                if d.record_comparison_available:
                    print(f"    Deltas: +{d.records_new} new, ~{d.records_changed} changed, ={d.records_unchanged} unchanged, -{d.records_removed} removed")

        an_st = next((st for st in summary.stages if st.get("stage_name") == "analytics"), None)
        if an_st:
            print("-" * 52)
            print("SQL ANALYTICS")
            print("-" * 52)
            if self.analytics_records:
                print(f"{'Query':<30} {'Status':<14} {'Rows':<6}")
                print("-" * 52)
                for r in self.analytics_records:
                    print(f"{r.query_name:<30} {r.status:<14} {r.result_rows:<6}")
                print(f"\nAnalytics Status:\n{an_st.get('status')}")
            else:
                print(f"Analytics Status:\n{an_st.get('status')}")
                if an_st.get("error_message"):
                    print(f"Reason: {an_st.get('error_message')}")

        pbi_st = next((st for st in summary.stages if st.get("stage_name") == "powerbi"), None)
        if pbi_st:
            print("-" * 52)
            print("POWER BI INTEGRATION")
            print("-" * 52)
            print(f"Power BI Status:\n{pbi_st.get('status')}")
            if self.powerbi_record and self.powerbi_record.status == "SUCCESS":
                print(f"Tables Discovered:       {self.powerbi_record.tables_discovered}")
                print(f"Measures Generated:      {self.powerbi_record.measures_generated}")
                print(f"Visuals Recommended:     {self.powerbi_record.visualizations_recommended}")
                print(f"Relationships:           {self.powerbi_record.relationships_discovered}")
                print("\nArtifacts:\npowerbi/semantic_model.json\npowerbi/dashboard_spec.json")
            elif pbi_st.get("error_message"):
                print(f"Reason: {pbi_st.get('error_message')}")

        print("-" * 52)
        print("MONITORING & PRODUCTION READINESS")
        print("-" * 52)
        health_label = self.health_result.overall_status if self.health_result else "NOT_CHECKED"
        print(f"Health Status:         {health_label}")
        print(f"Records Read:          {self.monitor.metrics.records_read}")
        print(f"Records Processed:     {self.monitor.metrics.records_processed}")
        print(f"Records Quarantined:   {self.monitor.metrics.records_rejected}")
        print(f"Records Loaded:        {self.monitor.metrics.records_loaded}")
        print(f"Records Skipped:       {self.monitor.metrics.records_skipped}")
        print(f"Quality Score:         {self.monitor.metrics.validation_quality_score:.1f}%")

        print(f"\nPipeline Status:\n{summary.pipeline_status}")

        if summary.error_message:
            print(f"Reason: {summary.error_message}")

        if self.config.generate_reports:
            print(f"\nReport:\nreports/pipeline_run_report.json")
        print("=" * 52 + "\n")
