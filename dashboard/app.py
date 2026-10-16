"""
Universal Data Engineering Pipeline - Interactive Web Dashboard Application.

Provides a lightweight Flask web interface and REST API for:
- Uploading CSV, XLSX, and JSON datasets to the raw landing zone
- Triggering orchestrated pipeline execution with live stage tracking
- Dynamically serving interactive dashboards for arbitrary structured datasets
- Providing real-time audit reports, data quality metrics, and Power BI semantic specifications
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
import re
from typing import Any, Optional

from flask import Flask, jsonify, render_template, request, send_from_directory
from werkzeug.utils import secure_filename

from dashboard.generator import DashboardGenerator
from pipeline.database import (
    load_project_env,
    mask_database_url,
    test_database_connection,
)
from pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineRunSummary,
    sanitize_text,
)


def load_dashboard_config(config_path: Path | str = "config/dashboard.json") -> dict[str, Any]:
    """Load dashboard configuration from JSON file or return defaults."""
    path = Path(config_path)
    default_cfg = {
        "host": "127.0.0.1",
        "port": 5000,
        "debug": False,
        "upload_folder": "data/raw",
        "allowed_extensions": ["csv", "xlsx", "json"],
        "max_content_length_mb": 50,
        "theme": "dark",
    }
    if not path.exists():
        return default_cfg

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            default_cfg.update(data)
    except Exception:
        pass
    return default_cfg


def create_app(root_dir: Optional[Path | str] = None) -> Flask:
    """Create and configure the Flask dashboard application."""
    project_root = Path(root_dir).resolve() if root_dir else Path(__file__).resolve().parent.parent
    load_project_env(project_root / ".env")

    cfg = load_dashboard_config(project_root / "config" / "dashboard.json")
    pkg_dir = Path(__file__).resolve().parent

    app = Flask(
        __name__,
        template_folder=str(pkg_dir / "templates"),
        static_folder=str(pkg_dir / "static"),
    )

    app.config["PROJECT_ROOT"] = project_root
    app.config["MAX_CONTENT_LENGTH"] = cfg.get("max_content_length_mb", 50) * 1024 * 1024
    app.config["ALLOWED_EXTENSIONS"] = set(cfg.get("allowed_extensions", ["csv", "xlsx", "json"]))
    app.config["UPLOAD_FOLDER"] = project_root / cfg.get("upload_folder", "data/raw")

    generator = DashboardGenerator(root_dir=project_root)

    def is_allowed_file(filename: str) -> bool:
        return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]

    @app.route("/")
    def index():
        """Serve main interactive dashboard web page."""
        return render_template("index.html")

    @app.route("/api/status", methods=["GET"])
    def get_system_status():
        """Return pipeline health, PostgreSQL connectivity, and latest run audit status."""
        db_ok, db_msg = generator.get_database_status()
        
        # Read latest run report if present
        latest_run = {}
        report_path = project_root / "reports" / "pipeline_run_report.json"
        if report_path.exists():
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    latest_run = json.load(f)
            except Exception:
                pass

        # Read production readiness if present
        health_status = "UNKNOWN"
        prod_report_path = project_root / "reports" / "production_readiness_report.json"
        if prod_report_path.exists():
            try:
                with open(prod_report_path, "r", encoding="utf-8") as f:
                    pdata = json.load(f)
                    health_status = pdata.get("health_status", "UNKNOWN")
            except Exception:
                pass

        return jsonify({
            "status": "healthy",
            "database_connected": db_ok,
            "database_message": db_msg,
            "pipeline_health": health_status,
            "latest_run": {
                "run_id": latest_run.get("run_id"),
                "status": latest_run.get("pipeline_status"),
                "start_time": latest_run.get("start_time"),
                "duration_seconds": latest_run.get("duration_seconds"),
                "stages": latest_run.get("stages", []),
            },
        })

    @app.route("/api/datasets", methods=["GET"])
    def list_datasets():
        """Return list of all discoverable datasets across DB, validated storage, and raw files."""
        datasets = generator.list_available_datasets()
        return jsonify({
            "datasets": datasets,
            "count": len(datasets),
        })

    @app.route("/api/dashboard-data", methods=["GET"])
    def get_dashboard_data():
        """Generate and return interactive dashboard specification and data for a given dataset."""
        dataset_name = request.args.get("dataset", "").strip()
        if not dataset_name:
            # Fallback to first available dataset
            available = generator.list_available_datasets()
            if available:
                dataset_name = available[0]["name"]
            else:
                dataset_name = "sample_sales"

        payload = generator.build_dashboard_payload(dataset_name)
        return jsonify(asdict(payload))

    @app.route("/api/upload", methods=["POST"])
    def upload_file():
        """
        Handle dataset file uploads (CSV, XLSX, JSON).
        Validates format, checks structure, and lands safely in data/raw/.
        """
        if "file" not in request.files:
            return jsonify({"success": False, "error": "No file part in request"}), 400

        file = request.files["file"]
        if not file or not file.filename or file.filename.strip() == "":
            return jsonify({"success": False, "error": "No file selected for upload"}), 400

        raw_filename = file.filename
        if not is_allowed_file(raw_filename):
            allowed = ", ".join(f".{ext}" for ext in app.config["ALLOWED_EXTENSIONS"])
            return jsonify({
                "success": False,
                "error": f"Invalid file format '{Path(raw_filename).suffix}'. Supported formats: {allowed}",
            }), 400

        clean_name = secure_filename(raw_filename)
        if not clean_name:
            clean_name = f"dataset_{int(datetime.now().timestamp())}{Path(raw_filename).suffix.lower()}"

        upload_dir = Path(app.config["UPLOAD_FOLDER"])
        upload_dir.mkdir(parents=True, exist_ok=True)
        dest_path = upload_dir / clean_name

        try:
            file.save(dest_path)
            size_bytes = dest_path.stat().st_size
            if size_bytes == 0:
                dest_path.unlink(missing_ok=True)
                return jsonify({"success": False, "error": "Uploaded file is empty (0 bytes)."}), 400

            # Quick structural sanity check
            suffix = dest_path.suffix.lower()
            if suffix == ".csv":
                import pandas as pd
                pd.read_csv(dest_path, nrows=5)
            elif suffix == ".xlsx":
                import pandas as pd
                pd.read_excel(dest_path, nrows=5)
            elif suffix == ".json":
                import pandas as pd
                pd.read_json(dest_path)

            return jsonify({
                "success": True,
                "filename": clean_name,
                "size_bytes": size_bytes,
                "dataset_name": dest_path.stem,
                "message": f"Successfully uploaded '{clean_name}' to landing zone data/raw/.",
            })
        except Exception as e:
            dest_path.unlink(missing_ok=True)
            return jsonify({
                "success": False,
                "error": f"Uploaded file could not be parsed: {sanitize_text(str(e))}",
            }), 400

    @app.route("/api/run-pipeline", methods=["POST"])
    def run_pipeline():
        """
        Execute end-to-end pipeline using existing PipelineOrchestrator.
        Reuses all ETL stages, checkpoints, validation, PostgreSQL loading, and Power BI generation.
        """
        data = request.get_json(silent=True) or {}
        reset_checkpoints = bool(data.get("reset_checkpoints", False))

        try:
            orchestrator = PipelineOrchestrator(
                root_dir=project_root,
                reset_checkpoints=reset_checkpoints,
            )
            summary = orchestrator.execute_pipeline()

            # Clean and mask any potential sensitive strings
            if hasattr(summary, "__dataclass_fields__"):
                summary_dict = asdict(summary)
            elif isinstance(summary, dict):
                summary_dict = summary
            else:
                summary_dict = {
                    "run_id": getattr(summary, "run_id", "unknown"),
                    "pipeline_status": getattr(summary, "pipeline_status", "UNKNOWN"),
                    "duration_seconds": getattr(summary, "duration_seconds", 0.0),
                    "stages": getattr(summary, "stages", []),
                    "datasets": getattr(summary, "datasets", []),
                    "summary": getattr(summary, "summary", {}),
                }

            run_id = getattr(summary, "run_id", "unknown")
            pipeline_status = getattr(summary, "pipeline_status", "UNKNOWN")
            duration_seconds = getattr(summary, "duration_seconds", 0.0)

            return jsonify({
                "success": True,
                "run_id": run_id,
                "pipeline_status": pipeline_status,
                "duration_seconds": duration_seconds,
                "summary": summary_dict,
                "message": f"Pipeline execution completed with status: {pipeline_status}",
            })
        except Exception as e:
            clean_err = sanitize_text(str(e))
            return jsonify({
                "success": False,
                "pipeline_status": "FAILED",
                "error": clean_err,
                "message": f"Pipeline execution encountered error: {clean_err}",
            }), 500

    @app.route("/api/pipeline-report", methods=["GET"])
    def get_pipeline_report():
        """Return the latest pipeline_run_report.json."""
        rep = project_root / "reports" / "pipeline_run_report.json"
        if rep.exists():
            try:
                with open(rep, "r", encoding="utf-8") as f:
                    return jsonify(json.load(f))
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        return jsonify({"error": "No pipeline run report found. Run pipeline first."}), 404

    @app.route("/api/validation-report", methods=["GET"])
    def get_validation_report():
        """Return the latest validation_report.json."""
        rep = project_root / "reports" / "validation_report.json"
        if rep.exists():
            try:
                with open(rep, "r", encoding="utf-8") as f:
                    return jsonify(json.load(f))
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        return jsonify({"error": "No validation report found. Run pipeline first."}), 404

    return app


if __name__ == "__main__":
    cfg = load_dashboard_config()
    application = create_app()
    application.run(
        host=cfg.get("host", "127.0.0.1"),
        port=cfg.get("port", 5000),
        debug=cfg.get("debug", False),
    )
