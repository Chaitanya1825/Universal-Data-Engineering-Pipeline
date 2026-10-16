"""
Pipeline Health Check & Pre-Flight System Diagnostic Module (Stage 11).

Provides:
- Component-level health diagnostics (Python runtime, directories, configs, dependencies, state store, logs, and database).
- Safe execution without requiring a live PostgreSQL instance.
- Honest database status reporting (never reports HEALTHY when unconfigured or unreachable).
- Structured status codes: HEALTHY, WARNING, ERROR, DATABASE_UNAVAILABLE, CONFIGURATION_ERROR.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import importlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Optional

from pipeline.database import (
    get_database_engine,
    get_database_url,
    load_project_env,
    test_database_connection,
)
from pipeline.monitoring import sanitize_sensitive_text


class HealthStatus(str, Enum):
    """Categorical diagnostic statuses for health checks."""
    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"


@dataclass
class ComponentHealth:
    """Individual component diagnostic record."""
    component_name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Consolidated pre-flight health diagnostic report."""
    timestamp: str
    overall_status: str
    components: list[ComponentHealth]
    python_version: str
    database_connected: bool
    summary_message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "overall_status": self.overall_status,
            "components": [asdict(c) for c in self.components],
            "python_version": self.python_version,
            "database_connected": self.database_connected,
            "summary_message": self.summary_message,
        }


class PipelineHealthChecker:
    """
    Comprehensive system diagnostic runner validating dependencies,
    directory layout, configurations, state persistence, and database connectivity.
    """

    def __init__(self, root_dir: Optional[Path | str] = None):
        self.root_dir = Path(root_dir).resolve() if root_dir else Path(__file__).resolve().parent.parent
        load_project_env(self.root_dir / ".env")
        self.config_dir = self.root_dir / "config"
        self.data_dir = self.root_dir / "data"
        self.logs_dir = self.root_dir / "logs"
        self.reports_dir = self.root_dir / "reports"
        self.state_dir = self.root_dir / "state"

    def check_python_environment(self) -> ComponentHealth:
        """Verify Python version satisfies minimum requirement (>= 3.10)."""
        v = sys.version_info
        ver_str = f"{v.major}.{v.minor}.{v.micro}"
        if v.major < 3 or (v.major == 3 and v.minor < 10):
            return ComponentHealth(
                component_name="python_runtime",
                status=HealthStatus.ERROR.value,
                message=f"Unsupported Python version {ver_str}. Minimum required is 3.10.",
                details={"version": ver_str},
            )
        return ComponentHealth(
            component_name="python_runtime",
            status=HealthStatus.HEALTHY.value,
            message=f"Python runtime {ver_str} is healthy.",
            details={"version": ver_str, "executable": sys.executable},
        )

    def check_directories(self) -> ComponentHealth:
        """Verify presence and writeability of required pipeline directories."""
        required = [
            self.data_dir / "raw",
            self.data_dir / "processed",
            self.data_dir / "validated",
            self.data_dir / "rejected",
            self.logs_dir,
            self.reports_dir,
            self.config_dir,
            self.state_dir,
            self.root_dir / "powerbi",
            self.root_dir / "sql",
        ]
        missing = []
        unwritable = []

        for d in required:
            if not d.exists():
                try:
                    d.mkdir(parents=True, exist_ok=True)
                except Exception:
                    missing.append(str(d))
            if d.exists():
                # Test writeability
                test_file = d / ".health_probe.tmp"
                try:
                    test_file.write_text("probe", encoding="utf-8")
                    test_file.unlink(missing_ok=True)
                except Exception:
                    unwritable.append(str(d))

        if missing or unwritable:
            return ComponentHealth(
                component_name="filesystem_directories",
                status=HealthStatus.ERROR.value,
                message=f"Directory issues found: missing={len(missing)}, unwritable={len(unwritable)}.",
                details={"missing": missing, "unwritable": unwritable},
            )

        return ComponentHealth(
            component_name="filesystem_directories",
            status=HealthStatus.HEALTHY.value,
            message="All 10 foundational pipeline directories exist and are writable.",
            details={"directory_count": len(required)},
        )

    def check_configurations(self) -> ComponentHealth:
        """Verify presence and JSON readability of required configuration files."""
        configs = [
            "pipeline.json",
            "transformations.json",
            "validation.json",
            "database.json",
            "incremental.json",
            "analytics.json",
            "powerbi.json",
        ]
        missing = []
        corrupted = []

        for name in configs:
            cfg_path = self.config_dir / name
            if not cfg_path.exists():
                missing.append(name)
            else:
                try:
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        json.load(f)
                except Exception as e:
                    corrupted.append({"file": name, "error": str(e)})

        if missing or corrupted:
            status = HealthStatus.CONFIGURATION_ERROR.value
            return ComponentHealth(
                component_name="configuration_files",
                status=status,
                message=f"Configuration issues: missing={len(missing)}, corrupted={len(corrupted)}.",
                details={"missing": missing, "corrupted": corrupted},
            )

        return ComponentHealth(
            component_name="configuration_files",
            status=HealthStatus.HEALTHY.value,
            message=f"All {len(configs)} configuration files are valid JSON.",
            details={"checked": configs},
        )

    def check_dependencies(self) -> ComponentHealth:
        """Verify core external libraries can be imported cleanly."""
        libs = ["pandas", "sqlalchemy", "openpyxl"]
        missing_libs = []
        for lib in libs:
            try:
                importlib.import_module(lib)
            except ImportError:
                missing_libs.append(lib)

        if missing_libs:
            return ComponentHealth(
                component_name="python_dependencies",
                status=HealthStatus.ERROR.value,
                message=f"Missing essential Python packages: {missing_libs}.",
                details={"missing": missing_libs},
            )

        # Optional driver check for PostgreSQL
        psycopg_available = False
        try:
            importlib.import_module("psycopg2")
            psycopg_available = True
        except ImportError:
            pass

        return ComponentHealth(
            component_name="python_dependencies",
            status=HealthStatus.HEALTHY.value,
            message="Essential libraries imported successfully.",
            details={"core_available": libs, "psycopg2_available": psycopg_available},
        )

    def check_state_store(self) -> ComponentHealth:
        """Verify checkpoint state store is accessible and not corrupted."""
        ckpt_path = self.state_dir / "checkpoints.json"
        if not ckpt_path.exists():
            return ComponentHealth(
                component_name="state_checkpoints",
                status=HealthStatus.HEALTHY.value,
                message="Checkpoints file does not exist yet (clean state).",
                details={"path": str(ckpt_path), "exists": False},
            )

        try:
            with open(ckpt_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            datasets_count = len(data.get("datasets", {}))
            return ComponentHealth(
                component_name="state_checkpoints",
                status=HealthStatus.HEALTHY.value,
                message=f"Checkpoints store accessible with {datasets_count} tracked datasets.",
                details={"tracked_datasets": datasets_count},
            )
        except Exception as e:
            return ComponentHealth(
                component_name="state_checkpoints",
                status=HealthStatus.WARNING.value,
                message=f"Corrupted checkpoints file: {e}. Orchestrator will reset gracefully.",
                details={"error": str(e)},
            )

    def check_database_connectivity(self) -> tuple[ComponentHealth, bool]:
        """
        Verify database connection configuration and connectivity.
        
        Never claims connected if DATABASE_URL is unset or connection fails.
        """
        dotenv_path = self.root_dir / ".env"
        engine, engine_err = get_database_engine(dotenv_path=dotenv_path)
        if engine is None:
            raw_url = get_database_url(dotenv_path=dotenv_path)
            is_configured = bool(raw_url)
            msg = (
                "DATABASE_URL not configured in environment."
                if not is_configured
                else f"Database engine initialization failed: {sanitize_sensitive_text(engine_err)}"
            )
            return (
                ComponentHealth(
                    component_name="postgresql_database",
                    status=HealthStatus.DATABASE_UNAVAILABLE.value,
                    message=msg,
                    details={"configured": is_configured, "reason": sanitize_sensitive_text(engine_err)},
                ),
                False,
            )

        connected, conn_err = test_database_connection(engine)
        if not connected:
            return (
                ComponentHealth(
                    component_name="postgresql_database",
                    status=HealthStatus.DATABASE_UNAVAILABLE.value,
                    message=f"PostgreSQL configured but unreachable: {sanitize_sensitive_text(conn_err)}",
                    details={"configured": True, "connected": False},
                ),
                False,
            )

        return (
            ComponentHealth(
                component_name="postgresql_database",
                status=HealthStatus.HEALTHY.value,
                message="PostgreSQL connection established successfully.",
                details={"configured": True, "connected": True},
            ),
            True,
        )

    def run_health_check(self) -> HealthCheckResult:
        """Run all system checks and consolidate the health assessment."""
        components: list[ComponentHealth] = []
        components.append(self.check_python_environment())
        components.append(self.check_directories())
        components.append(self.check_configurations())
        components.append(self.check_dependencies())
        components.append(self.check_state_store())

        db_comp, db_connected = self.check_database_connectivity()
        components.append(db_comp)

        # Determine overall status
        has_error = any(c.status == HealthStatus.ERROR.value for c in components)
        has_cfg_error = any(c.status == HealthStatus.CONFIGURATION_ERROR.value for c in components)
        has_warning = any(c.status == HealthStatus.WARNING.value for c in components)
        is_db_unavail = any(c.status == HealthStatus.DATABASE_UNAVAILABLE.value for c in components)

        if has_error:
            overall = HealthStatus.ERROR.value
            summary = "System diagnostic failed with fatal errors. Inspect details."
        elif has_cfg_error:
            overall = HealthStatus.CONFIGURATION_ERROR.value
            summary = "Configuration error detected in project setup."
        elif is_db_unavail:
            overall = HealthStatus.DATABASE_UNAVAILABLE.value if not has_warning else HealthStatus.WARNING.value
            summary = "System is healthy for file ingestion, ETL & analytics, but PostgreSQL is offline/unconfigured."
        elif has_warning:
            overall = HealthStatus.WARNING.value
            summary = "System is operational with non-fatal diagnostic warnings."
        else:
            overall = HealthStatus.HEALTHY.value
            summary = "All system components, configurations, and database connections are fully operational."

        v = sys.version_info
        return HealthCheckResult(
            timestamp=datetime.now().isoformat(),
            overall_status=overall,
            components=components,
            python_version=f"{v.major}.{v.minor}.{v.micro}",
            database_connected=db_connected,
            summary_message=summary,
        )


def format_health_summary(res: HealthCheckResult) -> str:
    """Format health check results into a clean, readable text report."""
    lines = [
        "=" * 56,
        "PIPELINE HEALTH & PRE-FLIGHT DIAGNOSTICS",
        "=" * 56,
        f"Timestamp:       {res.timestamp}",
        f"Python Version:  {res.python_version}",
        f"Overall Status:  {res.overall_status}",
        f"Database Live:   {'YES' if res.database_connected else 'NO (DATABASE_UNAVAILABLE)'}",
        "-" * 56,
    ]
    for c in res.components:
        lines.append(f"[{c.status:<20}] {c.component_name:<24} - {c.message}")
    lines.append("-" * 56)
    lines.append(f"Summary: {res.summary_message}")
    lines.append("=" * 56)
    return "\n".join(lines)
