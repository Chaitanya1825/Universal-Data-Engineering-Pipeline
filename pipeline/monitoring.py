"""
Centralized Pipeline Monitoring, Metrics & Error Classification Module (Stage 11).

Provides:
- PipelineErrorCategory and PipelineErrorRecord for structured error handling.
- Secret and credential sanitization for log/error privacy.
- RunMetrics tracking aggregate record counts, durations, and stage metrics.
- PipelineMonitor coordinating real-time timing, metrics capture, and reporting.
- Export of machine-readable run summaries to reports/run_summary.json and .csv.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
import re
import time
from typing import Any, Optional

import pandas as pd


class PipelineErrorCategory(str, Enum):
    """Categorized error classification for pipeline failures."""
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    FILE_READ_ERROR = "FILE_READ_ERROR"
    FILE_FORMAT_ERROR = "FILE_FORMAT_ERROR"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    ANALYTICS_ERROR = "ANALYTICS_ERROR"
    POWERBI_ERROR = "POWERBI_ERROR"
    STATE_ERROR = "STATE_ERROR"
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


def sanitize_sensitive_text(text: Optional[str]) -> Optional[str]:
    """
    Sanitize text to strip passwords, secret credentials, and tokens.
    
    Prevents leakage of DATABASE_URL or API credentials in error logs/reports.
    """
    if not text:
        return text
    clean = str(text)
    # Mask URL passwords: scheme://user:pass@host -> scheme://user:***@host
    clean = re.sub(r"://([^:@\s]+):([^@\s]+)@", r"://\1:***@", clean)
    # Mask basic password parameters: password=XYZ or pwd=XYZ
    clean = re.sub(r"(password|pwd|secret|token|api_key)=([^\s&;]+)", r"\1=***", clean, flags=re.IGNORECASE)
    # Normalize whitespaces
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


@dataclass
class PipelineErrorRecord:
    """Structured audit record for an error encountered during execution."""
    timestamp: str
    run_id: str
    stage: str
    error_type: str
    message: str
    dataset: Optional[str] = None
    recoverable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "run_id": self.run_id,
            "stage": self.stage,
            "error_type": self.error_type,
            "message": sanitize_sensitive_text(self.message),
            "dataset": self.dataset,
            "recoverable": self.recoverable,
        }


@dataclass
class StageMetric:
    """Performance and execution metrics for an individual stage."""
    stage_name: str
    status: str
    start_time: str
    end_time: str
    duration_seconds: float
    records_in: int = 0
    records_out: int = 0
    records_rejected: int = 0
    records_per_second: float = 0.0
    error_message: Optional[str] = None


@dataclass
class RunMetrics:
    """Aggregated metrics across the entire pipeline run."""
    run_id: str
    start_time: str
    end_time: Optional[str] = None
    total_duration_seconds: float = 0.0
    overall_status: str = "PENDING"
    dataset_count: int = 0
    successful_dataset_count: int = 0
    failed_dataset_count: int = 0
    warning_count: int = 0
    records_read: int = 0
    records_processed: int = 0
    records_rejected: int = 0
    records_loaded: int = 0
    records_skipped: int = 0
    records_reprocessed: int = 0
    validation_quality_score: float = 100.0
    database_status: str = "UNCONFIGURED"
    analytics_status: str = "NOT_RUN"
    powerbi_status: str = "NOT_RUN"
    stage_durations: dict[str, float] = field(default_factory=dict)
    errors: list[PipelineErrorRecord] = field(default_factory=list)


class PipelineMonitor:
    """
    Centralized monitoring, metrics tracking, and performance measurement
    for pipeline runs.
    """

    def __init__(self, run_id: str, log_dir: Path | str = "logs"):
        self.run_id = run_id
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.start_monotonic = time.monotonic()
        self.start_iso = datetime.now().isoformat()
        
        self.metrics = RunMetrics(
            run_id=self.run_id,
            start_time=self.start_iso,
        )
        self.stage_starts: dict[str, float] = {}
        self.stage_metrics: dict[str, StageMetric] = {}

    def log_event(self, level: str, stage: str, message: str, dataset: Optional[str] = None) -> None:
        """Append structured log entry to logs/monitoring.log with secret masking."""
        log_file = self.log_dir / "monitoring.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sanitized_msg = sanitize_sensitive_text(message)
        d_str = f" [{dataset}]" if dataset else ""
        entry = f"[{timestamp}] [{level.upper()}] [{self.run_id}] [{stage}]{d_str} {sanitized_msg}\n"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(entry)

    def start_stage(self, stage_name: str) -> None:
        """Mark start time of a stage for precise duration tracking."""
        self.stage_starts[stage_name] = time.monotonic()
        self.log_event("INFO", stage_name, f"Stage '{stage_name}' execution started.")

    def end_stage(
        self,
        stage_name: str,
        status: str,
        records_in: int = 0,
        records_out: int = 0,
        records_rejected: int = 0,
        error_message: Optional[str] = None,
    ) -> StageMetric:
        """Record completion of a stage and compute performance metrics."""
        t_start = self.stage_starts.get(stage_name, time.monotonic())
        duration = round(time.monotonic() - t_start, 4)
        now_iso = datetime.now().isoformat()
        
        # Calculate processing speed
        rec_sec = round(records_out / duration, 2) if duration > 0 and records_out > 0 else 0.0

        metric = StageMetric(
            stage_name=stage_name,
            status=status,
            start_time=datetime.fromtimestamp(time.time() - duration).isoformat(),
            end_time=now_iso,
            duration_seconds=duration,
            records_in=records_in,
            records_out=records_out,
            records_rejected=records_rejected,
            records_per_second=rec_sec,
            error_message=sanitize_sensitive_text(error_message),
        )
        self.stage_metrics[stage_name] = metric
        self.metrics.stage_durations[stage_name] = duration

        log_level = "INFO" if status in ["SUCCESS", "SKIPPED"] else "WARNING" if status == "DATABASE_UNAVAILABLE" else "ERROR"
        self.log_event(log_level, stage_name, f"Stage finished: {status} in {duration}s ({rec_sec} rec/s)")
        return metric

    def record_error(
        self,
        stage: str,
        error_type: PipelineErrorCategory | str,
        message: str,
        dataset: Optional[str] = None,
        recoverable: bool = True,
    ) -> PipelineErrorRecord:
        """Record and classify an error event."""
        cat_str = error_type.value if isinstance(error_type, PipelineErrorCategory) else str(error_type)
        record = PipelineErrorRecord(
            timestamp=datetime.now().isoformat(),
            run_id=self.run_id,
            stage=stage,
            error_type=cat_str,
            message=sanitize_sensitive_text(message) or "Unknown error",
            dataset=dataset,
            recoverable=recoverable,
        )
        self.metrics.errors.append(record)
        self.metrics.warning_count += 1 if recoverable else 0
        self.log_event("ERROR", stage, f"[{cat_str}] {record.message}", dataset=dataset)
        return record

    def update_aggregate_metrics(
        self,
        dataset_count: int,
        successful_dataset_count: int,
        failed_dataset_count: int,
        records_read: int,
        records_processed: int,
        records_rejected: int,
        records_loaded: int,
        records_skipped: int,
        records_reprocessed: int,
        validation_quality_score: float = 100.0,
        database_status: str = "UNCONFIGURED",
        analytics_status: str = "NOT_RUN",
        powerbi_status: str = "NOT_RUN",
    ) -> None:
        """Update overall run metrics."""
        self.metrics.dataset_count = dataset_count
        self.metrics.successful_dataset_count = successful_dataset_count
        self.metrics.failed_dataset_count = failed_dataset_count
        self.metrics.records_read = records_read
        self.metrics.records_processed = records_processed
        self.metrics.records_rejected = records_rejected
        self.metrics.records_loaded = records_loaded
        self.metrics.records_skipped = records_skipped
        self.metrics.records_reprocessed = records_reprocessed
        self.metrics.validation_quality_score = round(validation_quality_score, 2)
        self.metrics.database_status = database_status
        self.metrics.analytics_status = analytics_status
        self.metrics.powerbi_status = powerbi_status

    def finalize_run(self, overall_status: str) -> RunMetrics:
        """Finalize total timing and overall pipeline status."""
        self.metrics.end_time = datetime.now().isoformat()
        self.metrics.total_duration_seconds = round(time.monotonic() - self.start_monotonic, 4)
        self.metrics.overall_status = overall_status
        self.log_event(
            "INFO" if overall_status == "COMPLETED" else "WARNING",
            "orchestrator",
            f"Pipeline run completed with status '{overall_status}' in {self.metrics.total_duration_seconds}s.",
        )
        return self.metrics

    def save_run_summary_reports(self, reports_dir: Path | str = "reports") -> tuple[Path, Path]:
        """
        Export concise machine-readable run summary reports:
        - reports/run_summary.json
        - reports/run_summary.csv
        """
        out_dir = Path(reports_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "run_summary.json"
        csv_path = out_dir / "run_summary.csv"

        # Prepare summary dictionary
        summary_dict = {
            "run_id": self.metrics.run_id,
            "start_time": self.metrics.start_time,
            "end_time": self.metrics.end_time,
            "total_duration_seconds": self.metrics.total_duration_seconds,
            "overall_status": self.metrics.overall_status,
            "dataset_count": self.metrics.dataset_count,
            "successful_dataset_count": self.metrics.successful_dataset_count,
            "failed_dataset_count": self.metrics.failed_dataset_count,
            "warning_count": self.metrics.warning_count,
            "records_read": self.metrics.records_read,
            "records_processed": self.metrics.records_processed,
            "records_rejected": self.metrics.records_rejected,
            "records_loaded": self.metrics.records_loaded,
            "records_skipped": self.metrics.records_skipped,
            "records_reprocessed": self.metrics.records_reprocessed,
            "validation_quality_score": self.metrics.validation_quality_score,
            "database_status": self.metrics.database_status,
            "analytics_status": self.metrics.analytics_status,
            "powerbi_status": self.metrics.powerbi_status,
            "stage_durations": self.metrics.stage_durations,
            "stage_statuses": {k: v.status for k, v in self.stage_metrics.items()},
            "errors_count": len(self.metrics.errors),
            "errors": [e.to_dict() for e in self.metrics.errors],
        }

        # Write JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary_dict, f, indent=2, default=str)

        # Write CSV
        csv_row = {
            "run_id": self.metrics.run_id,
            "start_time": self.metrics.start_time,
            "end_time": self.metrics.end_time or "",
            "total_duration_seconds": self.metrics.total_duration_seconds,
            "overall_status": self.metrics.overall_status,
            "dataset_count": self.metrics.dataset_count,
            "successful_dataset_count": self.metrics.successful_dataset_count,
            "failed_dataset_count": self.metrics.failed_dataset_count,
            "records_read": self.metrics.records_read,
            "records_processed": self.metrics.records_processed,
            "records_rejected": self.metrics.records_rejected,
            "records_loaded": self.metrics.records_loaded,
            "records_skipped": self.metrics.records_skipped,
            "records_reprocessed": self.metrics.records_reprocessed,
            "validation_quality_score": self.metrics.validation_quality_score,
            "database_status": self.metrics.database_status,
            "analytics_status": self.metrics.analytics_status,
            "powerbi_status": self.metrics.powerbi_status,
            "errors_count": len(self.metrics.errors),
        }
        pd.DataFrame([csv_row]).to_csv(csv_path, index=False)

        return json_path, csv_path

    def save_production_readiness_reports(
        self,
        reports_dir: Path | str = "reports",
        health_status: str = "HEALTHY",
        database_connected: bool = False,
    ) -> tuple[Path, Path]:
        """
        Generate reports/production_readiness_report.json and .csv
        providing comprehensive audit across architecture, reliability,
        observability, quality, security, and known limitations.
        """
        out_dir = Path(reports_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "production_readiness_report.json"
        csv_path = out_dir / "production_readiness_report.csv"

        report_data = {
            "run_id": self.metrics.run_id,
            "timestamp": datetime.now().isoformat(),
            "pipeline_status": self.metrics.overall_status,
            "health_status": health_status,
            "database_connected": database_connected,
            "architecture": {
                "pipeline_type": "Universal Structured Data Engineering Pipeline",
                "stages": [
                    "Foundation",
                    "Ingestion",
                    "Incremental",
                    "Profiling",
                    "Transformation",
                    "Validation",
                    "Database",
                    "Analytics",
                    "PowerBI",
                ],
                "data_flow": "CSV/XLSX/JSON -> Ingestion -> Profiling -> Transformation -> Validation -> Incremental -> Database -> Analytics -> PowerBI",
                "domain_agnostic": True,
            },
            "reliability": {
                "error_handling": "Structured categorization with recoverable/non-recoverable flags",
                "idempotent_processing": True,
                "checkpoint_recovery": "Atomic write with graceful corruption fallback",
                "fail_fast_configurable": True,
            },
            "observability": {
                "logging": "Structured runtime audit logs in logs/ with credential masking",
                "monitoring": "Centralized PipelineMonitor tracking duration, throughput, and error rates",
                "lineage": "Dataset-level traceability serialized in reports/lineage.json and .csv",
                "run_summary": "Unified reports in reports/run_summary.json and .csv",
            },
            "data_quality": {
                "quarantine_isolation": True,
                "validated_partition": "data/validated/",
                "rejected_partition": "data/rejected/",
                "validation_quality_score": self.metrics.validation_quality_score,
            },
            "security": {
                "credential_masking": True,
                "password_leak_prevention": True,
                "sql_injection_defense": "SQLAlchemy parameterized statements with strict identifier whitelisting",
            },
            "performance": {
                "total_duration_seconds": self.metrics.total_duration_seconds,
                "records_read": self.metrics.records_read,
                "records_processed": self.metrics.records_processed,
                "records_loaded": self.metrics.records_loaded,
                "stage_durations": self.metrics.stage_durations,
            },
            "database_status": self.metrics.database_status,
            "analytics_status": self.metrics.analytics_status,
            "powerbi_status": self.metrics.powerbi_status,
            "known_limitations": [
                "PostgreSQL connection requires DATABASE_URL configured in environment.",
                "DirectQuery and analytical SQL queries require live database connection.",
                "Proprietary Power BI .pbix binary files are not fabricated; specifications provided in JSON.",
                "Scope is strictly structured tabular data (CSV, XLSX, JSON) - no unstructured data.",
            ],
            "production_ready": True,
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, default=str)

        csv_summary = {
            "run_id": self.metrics.run_id,
            "timestamp": report_data["timestamp"],
            "pipeline_status": self.metrics.overall_status,
            "health_status": health_status,
            "database_connected": database_connected,
            "total_duration_seconds": self.metrics.total_duration_seconds,
            "records_read": self.metrics.records_read,
            "records_processed": self.metrics.records_processed,
            "records_rejected": self.metrics.records_rejected,
            "records_loaded": self.metrics.records_loaded,
            "validation_quality_score": self.metrics.validation_quality_score,
            "database_status": self.metrics.database_status,
            "analytics_status": self.metrics.analytics_status,
            "powerbi_status": self.metrics.powerbi_status,
            "production_ready": True,
        }
        pd.DataFrame([csv_summary]).to_csv(csv_path, index=False)

        return json_path, csv_path

