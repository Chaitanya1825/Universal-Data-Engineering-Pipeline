"""
Universal Data Ingestion & File Detection Module.

Provides domain-agnostic loading of structured data files (CSV, Excel, JSON)
into unified Pandas DataFrames with automatic file discovery, format detection,
error resilience, and ingestion reporting.
"""

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Optional

import pandas as pd


@dataclass
class IngestionResult:
    """Represents the outcome of an attempted file ingestion."""
    file_path: Path
    file_name: str
    file_type: str
    file_size_bytes: int
    status: str  # "SUCCESS", "FAILED", "SKIPPED"
    df: Optional[pd.DataFrame] = None
    row_count: int = 0
    column_count: int = 0
    columns: list[str] = field(default_factory=list)
    error_message: Optional[str] = None


def format_file_size(size_bytes: int) -> str:
    """Format byte count into human-readable size string (B, KB, MB, GB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def discover_input_files(directory_path: Path | str = "data/raw") -> list[Path]:
    """
    Scan the target landing directory and return all discovered file paths.
    
    Ignores directories, hidden files, and version-control tracking files (.gitkeep).
    """
    directory = Path(directory_path)
    if not directory.exists() or not directory.is_dir():
        return []

    discovered: list[Path] = []
    for item in sorted(directory.iterdir()):
        if item.is_file() and not item.name.startswith(".") and item.name != ".gitkeep":
            discovered.append(item)

    return discovered


def detect_file_type(file_path: Path | str) -> str:
    """
    Detect the structured file format based on extension.
    
    Returns 'CSV', 'EXCEL', 'JSON', or 'UNSUPPORTED'.
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return "CSV"
    elif suffix == ".xlsx":
        return "EXCEL"
    elif suffix == ".json":
        return "JSON"
    return "UNSUPPORTED"


def load_csv(file_path: Path | str) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Load a CSV file into a Pandas DataFrame.
    
    Handles character encoding variations, empty files, and malformed CSV syntax.
    Returns (DataFrame, None) on success or (None, error_message) on failure.
    """
    path = Path(file_path)
    if not path.exists():
        return None, f"File does not exist: {path.name}"

    if path.stat().st_size == 0:
        return None, "File is empty (0 bytes)"

    # Attempt standard UTF-8 first, fallback to common alternatives
    encodings_to_try = ["utf-8", "latin1", "cp1252"]
    last_error: Optional[Exception] = None

    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(path, encoding=encoding)
            if df.empty and len(df.columns) == 0:
                return None, "CSV file contains no data or columns"
            return df, None
        except pd.errors.EmptyDataError:
            return None, "CSV file contains no readable data"
        except pd.errors.ParserError as e:
            return None, f"Malformed CSV structure: {e}"
        except UnicodeDecodeError as e:
            last_error = e
            continue
        except Exception as e:
            return None, f"Error reading CSV file: {e}"

    return None, f"Failed to decode CSV with supported encodings: {last_error}"


def load_excel(
    file_path: Path | str, sheet_name: int | str = 0
) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Load an Excel (.xlsx) file into a Pandas DataFrame.
    
    Processes the first worksheet by default.
    Returns (DataFrame, None) on success or (None, error_message) on failure.
    """
    path = Path(file_path)
    if not path.exists():
        return None, f"File does not exist: {path.name}"

    if path.stat().st_size == 0:
        return None, "File is empty (0 bytes)"

    try:
        df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        if df.empty and len(df.columns) == 0:
            return None, "Excel worksheet contains no data or columns"
        return df, None
    except ValueError as e:
        return None, f"Invalid Excel worksheet configuration: {e}"
    except Exception as e:
        return None, f"Failed to read Excel file: {e}"


def load_json(file_path: Path | str) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Load a structured JSON file into a Pandas DataFrame.
    
    Supports tabular-convertible JSON structures:
    - Top-level list of dictionaries (standard record collection).
    - Top-level dictionary containing a record collection under a standard key.
    - Columnar dictionary where values are uniform lists.
    - Single flat object record.
    
    Rejects unstructured or deeply arbitrary nested JSON.
    Returns (DataFrame, None) on success or (None, error_message) on failure.
    """
    path = Path(file_path)
    if not path.exists():
        return None, f"File does not exist: {path.name}"

    if path.stat().st_size == 0:
        return None, "File is empty (0 bytes)"

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON syntax: {e}"
    except UnicodeDecodeError:
        try:
            with open(path, "r", encoding="latin1") as f:
                data = json.load(f)
        except Exception as e:
            return None, f"Error decoding JSON file: {e}"
    except Exception as e:
        return None, f"Error reading JSON file: {e}"

    # Evaluate tabular convertibility
    if isinstance(data, list):
        if len(data) == 0:
            return None, "JSON list is empty"
        if not all(isinstance(item, dict) for item in data):
            return None, "Unsupported JSON structure: list elements must be key-value objects"
        df = pd.DataFrame(data)
        return df, None

    elif isinstance(data, dict):
        if len(data) == 0:
            return None, "JSON object is empty"

        # Check for nested list of records under standard container keys
        for key in ["records", "data", "items", "rows"]:
            if key in data and isinstance(data[key], list):
                nested_list = data[key]
                if len(nested_list) > 0 and all(isinstance(item, dict) for item in nested_list):
                    return pd.DataFrame(nested_list), None

        # Check for columnar dictionary (all values are lists of equal length)
        values = list(data.values())
        if all(isinstance(v, list) for v in values) and len(values) > 0:
            lengths = {len(v) for v in values}
            if len(lengths) == 1:
                return pd.DataFrame(data), None

        # Check for single flat record
        if all(not isinstance(v, (dict, list)) for v in data.values()):
            return pd.DataFrame([data]), None

        return None, "Unsupported JSON structure: nested or non-tabular object hierarchy"

    return None, "Unsupported JSON structure: top-level element must be a list or object"


def load_dataset(file_path: Path | str) -> IngestionResult:
    """
    Inspect, detect, and load any structured dataset file safely.
    
    Orchestrates format detection, loader delegation, and metadata extraction.
    Never modifies raw input files.
    """
    path = Path(file_path)
    file_name = path.name
    file_type = detect_file_type(path)
    file_size = path.stat().st_size if path.exists() else 0

    if file_type == "UNSUPPORTED":
        return IngestionResult(
            file_path=path,
            file_name=file_name,
            file_type="UNSUPPORTED",
            file_size_bytes=file_size,
            status="SKIPPED",
            error_message="Unsupported file format",
        )

    df: Optional[pd.DataFrame] = None
    error_message: Optional[str] = None

    if file_type == "CSV":
        df, error_message = load_csv(path)
    elif file_type == "EXCEL":
        df, error_message = load_excel(path)
    elif file_type == "JSON":
        df, error_message = load_json(path)

    if error_message is not None or df is None:
        return IngestionResult(
            file_path=path,
            file_name=file_name,
            file_type=file_type,
            file_size_bytes=file_size,
            status="FAILED",
            error_message=error_message or "Unknown loading failure",
        )

    return IngestionResult(
        file_path=path,
        file_name=file_name,
        file_type=file_type,
        file_size_bytes=file_size,
        status="SUCCESS",
        df=df,
        row_count=len(df),
        column_count=len(df.columns),
        columns=[str(c) for c in df.columns],
    )


def log_ingestion_event(result: IngestionResult, log_dir: Path | str = "logs") -> None:
    """
    Append an ingestion outcome record to logs/ingestion.log.
    """
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "ingestion.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_label = result.status
    detail = (
        f"Rows: {result.row_count}, Columns: {result.column_count}"
        if result.status == "SUCCESS"
        else f"Reason: {result.error_message}"
    )

    log_entry = (
        f"[{timestamp}] [{status_label}] File: {result.file_name} | "
        f"Type: {result.file_type} | Size: {result.file_size_bytes}B | {detail}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def generate_ingestion_report(results: list[IngestionResult]) -> str:
    """
    Generate a dynamic, human-readable ingestion report from execution results.
    """
    lines: list[str] = [
        "========================================",
        "UNIVERSAL DATA INGESTION",
        "========================================",
    ]

    success_count = 0
    failed_count = 0
    skipped_count = 0

    if not results:
        lines.append("No files discovered in landing directory.")
    else:
        for idx, res in enumerate(results):
            if idx > 0:
                lines.append("---")
            
            lines.append(f"File: {res.file_name}")
            lines.append(f"Type: {res.file_type}")
            
            if res.status == "SUCCESS":
                success_count += 1
                lines.append(f"Size: {format_file_size(res.file_size_bytes)}")
                lines.append(f"Rows: {res.row_count:,}")
                lines.append(f"Columns: {res.column_count}")
                lines.append(f"Status: {res.status}")
                lines.append("\nColumns:")
                for col in res.columns:
                    lines.append(f"* {col}")
            elif res.status == "FAILED":
                failed_count += 1
                lines.append(f"Size: {format_file_size(res.file_size_bytes)}")
                lines.append(f"Status: {res.status}")
                lines.append(f"Reason: {res.error_message}")
            elif res.status == "SKIPPED":
                skipped_count += 1
                lines.append(f"Status: {res.status}")
                lines.append(f"Reason: {res.error_message}")

    lines.extend([
        "========================================",
        "SUMMARY",
        "========================================",
        f"Files discovered: {len(results)}",
        f"Successfully loaded: {success_count}",
        f"Failed: {failed_count}",
        f"Skipped: {skipped_count}",
        "========================================",
    ])

    return "\n".join(lines)


def run_ingestion_pipeline(
    raw_dir: Path | str = "data/raw", log_dir: Path | str = "logs"
) -> tuple[list[IngestionResult], str]:
    """
    Discover all files in raw_dir, ingest each dataset, log outcomes,
    and generate the consolidated report.
    """
    discovered_files = discover_input_files(raw_dir)
    results: list[IngestionResult] = []

    for file_path in discovered_files:
        result = load_dataset(file_path)
        log_ingestion_event(result, log_dir)
        results.append(result)

    report = generate_ingestion_report(results)
    return results, report
