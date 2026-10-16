"""
Universal Incremental Data Processing Engine Module (Stage 8).

Provides:
- Cryptographic file-level hashing (SHA-256)
- Checkpoint persistence and atomic store management
- Deterministic decision engine: NEW -> PROCESS, UNCHANGED -> SKIP, CHANGED -> REPROCESS
- Configurable record-level key hashing and delta detection (new, changed, unchanged, removed)
- Incremental report serialization (JSON and CSV)
- Audit logging without credential exposure
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Optional

import numpy as np
import pandas as pd

# Supported cryptographic hash algorithms
SUPPORTED_HASH_ALGORITHMS = {"sha256", "sha1", "sha512", "md5"}


@dataclass
class IncrementalConfig:
    """Configuration settings for incremental processing."""
    enabled: bool = True
    hash_algorithm: str = "sha256"
    skip_unchanged_files: bool = True
    reprocess_changed_files: bool = True
    checkpoint_file: str = "state/checkpoints.json"
    track_record_changes: bool = True
    delete_missing_records: bool = False
    record_keys: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class DatasetCheckpoint:
    """Historical checkpoint record of a successfully processed dataset."""
    source_path: str
    file_hash: str
    file_size: int
    modified_time: str
    last_processed_at: str
    last_status: str  # "SUCCESS", "FAILED", "DATABASE_UNAVAILABLE"
    database_status: Optional[str] = None
    record_count: Optional[int] = None
    record_hashes: Optional[dict[str, str]] = None  # key -> content_hash


@dataclass
class IncrementalDecision:
    """Evaluation result and delta metrics for an incoming dataset."""
    dataset_name: str
    source_path: str
    current_hash: str
    previous_hash: Optional[str] = None
    decision: str = "NEW"  # "NEW", "UNCHANGED", "CHANGED"
    action: str = "PROCESS"  # "PROCESS", "SKIP", "REPROCESS"
    reason: str = ""
    records_previous: int = 0
    records_current: int = 0
    records_new: int = 0
    records_changed: int = 0
    records_unchanged: int = 0
    records_removed: int = 0
    record_comparison_available: bool = False
    comparison_note: Optional[str] = None
    current_record_hashes: Optional[dict[str, str]] = None


def load_incremental_config(
    config_path: Path | str = "config/incremental.json",
) -> tuple[IncrementalConfig, Optional[str]]:
    """
    Load incremental processing configuration from JSON.
    
    Returns (IncrementalConfig, None) on success or (default_config, error_reason) on error.
    """
    path = Path(config_path)
    if not path.exists():
        return IncrementalConfig(), None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        algo = str(data.get("hash_algorithm", "sha256")).lower().strip()
        if algo not in SUPPORTED_HASH_ALGORITHMS:
            return (
                IncrementalConfig(),
                f"Invalid hash_algorithm '{algo}'. Supported algorithms: {sorted(SUPPORTED_HASH_ALGORITHMS)}",
            )

        cfg = IncrementalConfig(
            enabled=bool(data.get("enabled", True)),
            hash_algorithm=algo,
            skip_unchanged_files=bool(data.get("skip_unchanged_files", True)),
            reprocess_changed_files=bool(data.get("reprocess_changed_files", True)),
            checkpoint_file=str(data.get("checkpoint_file", "state/checkpoints.json")),
            track_record_changes=bool(data.get("track_record_changes", True)),
            delete_missing_records=bool(data.get("delete_missing_records", False)),
            record_keys=dict(data.get("record_keys", {})),
        )
        return cfg, None

    except json.JSONDecodeError as e:
        return IncrementalConfig(), f"Corrupted JSON in {path}: {e}"
    except Exception as e:
        return IncrementalConfig(), f"Error loading {path}: {e}"


def calculate_file_hash(
    file_path: Path | str,
    algorithm: str = "sha256",
    chunk_size: int = 65536,
) -> str:
    """
    Calculate deterministic cryptographic hash of file content in chunks.
    
    Safe for large datasets without reading entire file into memory.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found for hash calculation: {path}")

    algo = algorithm.lower().strip()
    if algo not in SUPPORTED_HASH_ALGORITHMS:
        raise ValueError(f"Unsupported hash algorithm '{algorithm}'. Must be one of {sorted(SUPPORTED_HASH_ALGORITHMS)}")

    hasher = hashlib.new(algo)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)

    return hasher.hexdigest()


def compute_record_hash(
    record: dict[str, Any] | pd.Series,
    key_columns: Optional[list[str]] = None,
    algorithm: str = "sha256",
) -> str:
    """
    Compute deterministic cryptographic hash of a record's contents.
    
    - Handles null values uniformly as '__NULL__'
    - Column names are sorted alphabetically for deterministic ordering
    - Numeric values canonicalized
    """
    data = record.to_dict() if isinstance(record, pd.Series) else dict(record)
    exclude_keys = set(key_columns) if key_columns else set()

    tokens: list[str] = []
    for col in sorted(data.keys()):
        if col in exclude_keys:
            continue
        val = data[col]
        if val is None or pd.isna(val):
            canon_val = "__NULL__"
        elif isinstance(val, (float, np.floating)):
            canon_val = f"{val:.6f}".rstrip("0").rstrip(".") if not np.isnan(val) else "__NULL__"
        elif isinstance(val, (int, np.integer)):
            canon_val = str(int(val))
        elif isinstance(val, (bool, np.bool_)):
            canon_val = "true" if bool(val) else "false"
        else:
            canon_val = str(val).strip()
        tokens.append(f"{col}={canon_val}")

    canonical_repr = "|".join(tokens)
    hasher = hashlib.new(algorithm.lower())
    hasher.update(canonical_repr.encode("utf-8"))
    return hasher.hexdigest()


def extract_record_key(
    record: dict[str, Any] | pd.Series,
    key_columns: list[str],
) -> str:
    """
    Extract a unique string representation of the record key from key columns.
    """
    data = record.to_dict() if isinstance(record, pd.Series) else dict(record)
    parts = []
    for col in key_columns:
        val = data.get(col)
        if val is None or pd.isna(val):
            parts.append("__NULL_KEY__")
        else:
            parts.append(str(val).strip())
    return "::".join(parts)


class CheckpointStore:
    """
    Persistent store for incremental processing state with atomic file writes.
    """

    def __init__(self, checkpoint_path: Path | str = "state/checkpoints.json"):
        self.path = Path(checkpoint_path)
        self.checkpoints: dict[str, DatasetCheckpoint] = {}
        self.load()

    def load(self) -> dict[str, DatasetCheckpoint]:
        """Load checkpoints from disk or initialize empty state."""
        if not self.path.exists():
            self.checkpoints = {}
            return self.checkpoints

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)

            datasets = data.get("datasets", {})
            result: dict[str, DatasetCheckpoint] = {}
            for name, item in datasets.items():
                result[name] = DatasetCheckpoint(
                    source_path=item.get("source_path", ""),
                    file_hash=item.get("file_hash", ""),
                    file_size=int(item.get("file_size", 0)),
                    modified_time=item.get("modified_time", ""),
                    last_processed_at=item.get("last_processed_at", ""),
                    last_status=item.get("last_status", "UNKNOWN"),
                    database_status=item.get("database_status"),
                    record_count=item.get("record_count"),
                    record_hashes=item.get("record_hashes"),
                )
            self.checkpoints = result
            return self.checkpoints

        except Exception:
            # Recover safely from empty or corrupted file
            self.checkpoints = {}
            return self.checkpoints

    def get(self, dataset_name: str) -> Optional[DatasetCheckpoint]:
        """Retrieve checkpoint for a specific dataset filename."""
        return self.checkpoints.get(dataset_name)

    def update(self, dataset_name: str, checkpoint: DatasetCheckpoint) -> None:
        """Update or insert a dataset checkpoint in memory."""
        self.checkpoints[dataset_name] = checkpoint

    def save(self) -> None:
        """
        Atomically write checkpoints to disk using a temporary file.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serializable = {
            "version": "1.0",
            "updated_at": datetime.now().isoformat(),
            "datasets": {name: asdict(cp) for name, cp in self.checkpoints.items()},
        }

        # Write to temporary file in same directory for atomic rename
        temp_file = self.path.with_suffix(".tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, default=str)

        os.replace(temp_file, self.path)

    def clear(self) -> None:
        """Clear all in-memory checkpoints without persisting to disk."""
        self.checkpoints = {}

    def reset(self) -> None:
        """
        Safely reset checkpoint state on disk and in memory.
        Clears incremental checkpoint records while writing a valid,
        empty checkpoint file schema atomically. Does not delete raw or processed data.
        """
        self.checkpoints = {}
        self.save()


def reset_checkpoint_store(
    checkpoint_path: Path | str = "state/checkpoints.json",
) -> CheckpointStore:
    """
    Safely reset the checkpoint store at the given path.
    Atomically writes an empty dataset checkpoint dictionary to disk.
    
    Returns the reset CheckpointStore instance.
    """
    store = CheckpointStore(checkpoint_path)
    store.reset()
    return store


def evaluate_dataset_incremental(
    file_path: Path | str,
    df: Optional[pd.DataFrame],
    config: IncrementalConfig,
    checkpoint_store: CheckpointStore,
) -> IncrementalDecision:
    """
    Evaluate an incoming dataset against historical checkpoints.
    
    Determines decision (NEW, UNCHANGED, CHANGED) and action (PROCESS, SKIP, REPROCESS).
    Computes record-level deltas if record keys are configured.
    """
    path = Path(file_path)
    dataset_name = path.name
    current_hash = calculate_file_hash(path, algorithm=config.hash_algorithm)

    previous = checkpoint_store.get(dataset_name)
    current_row_count = len(df) if df is not None else 0

    # Decision Logic
    if previous is None:
        decision = "NEW"
        action = "PROCESS"
        reason = "New dataset detected (no previous checkpoint found)."
        prev_hash = None
        prev_rows = 0
    elif current_hash == previous.file_hash:
        decision = "UNCHANGED"
        action = "SKIP" if config.skip_unchanged_files else "PROCESS"
        reason = f"File content hash unchanged ({current_hash[:8]}...)."
        prev_hash = previous.file_hash
        prev_rows = previous.record_count or 0
    else:
        decision = "CHANGED"
        action = "REPROCESS" if config.reprocess_changed_files else "SKIP"
        reason = f"File content hash changed ({previous.file_hash[:8]}... -> {current_hash[:8]}...)."
        prev_hash = previous.file_hash
        prev_rows = previous.record_count or 0

    # Record-level change detection (if enabled and DataFrame is available)
    records_new = 0
    records_changed = 0
    records_unchanged = 0
    records_removed = 0
    comparison_available = False
    comp_note = None
    current_rec_hashes: Optional[dict[str, str]] = None

    if config.track_record_changes and df is not None:
        # Check if record keys configured for this dataset
        # Support stem matching or full filename matching
        stem = path.stem
        key_cols = config.record_keys.get(dataset_name) or config.record_keys.get(stem)

        if key_cols and all(col in df.columns for col in key_cols):
            comparison_available = True
            current_rec_hashes = {}
            for _, row in df.iterrows():
                r_key = extract_record_key(row, key_cols)
                r_hash = compute_record_hash(row, key_columns=key_cols, algorithm=config.hash_algorithm)
                current_rec_hashes[r_key] = r_hash

            prev_rec_hashes = (previous.record_hashes or {}) if previous else {}

            curr_keys = set(current_rec_hashes.keys())
            prev_keys = set(prev_rec_hashes.keys())

            new_keys = curr_keys - prev_keys
            removed_keys = prev_keys - curr_keys
            common_keys = curr_keys & prev_keys

            changed_keys = {
                k for k in common_keys if current_rec_hashes[k] != prev_rec_hashes[k]
            }
            unchanged_keys = common_keys - changed_keys

            records_new = len(new_keys)
            records_changed = len(changed_keys)
            records_unchanged = len(unchanged_keys)
            records_removed = len(removed_keys)

            comp_note = f"Record keys configured: {key_cols}"
        else:
            comparison_available = False
            comp_note = "Record-level comparison: NOT_AVAILABLE (Reason: No record key configured)"

    return IncrementalDecision(
        dataset_name=dataset_name,
        source_path=str(path),
        current_hash=current_hash,
        previous_hash=prev_hash,
        decision=decision,
        action=action,
        reason=reason,
        records_previous=prev_rows,
        records_current=current_row_count,
        records_new=records_new,
        records_changed=records_changed,
        records_unchanged=records_unchanged,
        records_removed=records_removed,
        record_comparison_available=comparison_available,
        comparison_note=comp_note,
        current_record_hashes=current_rec_hashes,
    )


def save_incremental_reports(
    decisions: list[IncrementalDecision],
    reports_dir: Path | str = "reports",
    run_id: str = "run_incremental",
) -> tuple[Path, Path]:
    """
    Serialize incremental evaluation metrics to JSON and CSV reports.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "incremental_report.json"
    csv_path = out_dir / "incremental_report.csv"

    # JSON report
    serializable = []
    for d in decisions:
        item = asdict(d)
        # Omit massive hash dictionary from report payload
        item.pop("current_record_hashes", None)
        item["run_id"] = run_id
        serializable.append(item)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, default=str)

    # CSV report
    csv_rows = []
    for d in decisions:
        csv_rows.append({
            "run_id": run_id,
            "dataset_name": d.dataset_name,
            "decision": d.decision,
            "action": d.action,
            "file_hash": d.current_hash,
            "previous_hash": d.previous_hash or "",
            "records_previous": d.records_previous,
            "records_current": d.records_current,
            "records_new": d.records_new,
            "records_changed": d.records_changed,
            "records_unchanged": d.records_unchanged,
            "records_removed": d.records_removed,
            "record_comparison_available": d.record_comparison_available,
            "reason": d.reason,
        })

    df_csv = pd.DataFrame(csv_rows)
    df_csv.to_csv(csv_path, index=False)

    return json_path, csv_path


def log_incremental_event(
    decision: IncrementalDecision,
    log_dir: Path | str = "logs",
    run_id: str = "run_incremental",
) -> None:
    """
    Append incremental evaluation outcome to logs/incremental.log.
    """
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "incremental.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    delta_str = (
        f"Rows: {decision.records_current} (New: {decision.records_new}, Changed: {decision.records_changed}, "
        f"Unchanged: {decision.records_unchanged}, Removed: {decision.records_removed})"
        if decision.record_comparison_available
        else f"Rows: {decision.records_current}"
    )

    log_entry = (
        f"[{timestamp}] [INFO] [{run_id}] Dataset: {decision.dataset_name} | "
        f"Decision: {decision.decision} | Action: {decision.action} | "
        f"Hash: {decision.current_hash[:8]}... | {delta_str} | {decision.reason}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def upsert_records_to_database(
    df: pd.DataFrame,
    source_filename: str,
    key_columns: list[str],
    engine: Optional[Any],
    schema: str = "public",
    delete_missing_records: bool = False,
) -> dict[str, Any]:
    """
    Perform incremental delta loading / upsert into relational database.
    
    Inserts new records and updates changed records based on explicit key_columns.
    Never deletes records unless delete_missing_records=True.
    Returns status dictionary detailing inserted, updated, and deleted counts.
    """
    if engine is None:
        return {
            "status": "DATABASE_UNAVAILABLE",
            "error_message": "Engine is None (DATABASE_URL not configured)",
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
        }

    from pipeline.database import generate_table_name, test_database_connection
    from sqlalchemy import text, inspect

    connected, conn_err = test_database_connection(engine)
    if not connected:
        return {
            "status": "DATABASE_UNAVAILABLE",
            "error_message": f"Database unreachable: {conn_err}",
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
        }

    table_name = generate_table_name(source_filename)
    dialect_name = engine.dialect.name

    try:
        inspector = inspect(engine)
        target_schema = schema if dialect_name == "postgresql" else None
        existing_tables = inspector.get_table_names(schema=target_schema)

        # If table doesn't exist, create it with full df
        if table_name not in existing_tables:
            df.to_sql(table_name, con=engine, schema=target_schema, if_exists="append", index=False)
            return {
                "status": "SUCCESS",
                "table_name": table_name,
                "inserted": len(df),
                "updated": 0,
                "deleted": 0,
            }

        # Query existing data for key comparison
        target_table_ref = f'"{schema}"."{table_name}"' if target_schema else f'"{table_name}"'
        existing_df = pd.read_sql_table(table_name, con=engine, schema=target_schema)

        # Build comparison maps
        existing_keys = {extract_record_key(row, key_columns): row for _, row in existing_df.iterrows()}
        existing_hashes = {k: compute_record_hash(r, key_columns=key_columns) for k, r in existing_keys.items()}

        current_keys = {extract_record_key(row, key_columns): row for _, row in df.iterrows()}
        current_hashes = {k: compute_record_hash(r, key_columns=key_columns) for k, r in current_keys.items()}

        to_insert_keys = set(current_keys.keys()) - set(existing_keys.keys())
        common_keys = set(current_keys.keys()) & set(existing_keys.keys())
        to_update_keys = {k for k in common_keys if current_hashes[k] != existing_hashes[k]}

        # Insert new rows
        inserted_count = 0
        if to_insert_keys:
            insert_rows = [current_keys[k] for k in to_insert_keys]
            insert_df = pd.DataFrame(insert_rows)
            insert_df.to_sql(table_name, con=engine, schema=target_schema, if_exists="append", index=False)
            inserted_count = len(to_insert_keys)

        # Update changed rows
        updated_count = 0
        if to_update_keys:
            with engine.begin() as conn:
                for k in to_update_keys:
                    row_dict = current_keys[k].to_dict()
                    set_clauses = [f'"{col}" = :{col}' for col in row_dict.keys() if col not in key_columns]
                    where_clauses = [f'"{col}" = :{col}' for col in key_columns]
                    if set_clauses:
                        stmt = text(f'UPDATE {target_table_ref} SET {", ".join(set_clauses)} WHERE {" AND ".join(where_clauses)}')
                        conn.execute(stmt, row_dict)
            updated_count = len(to_update_keys)

        # Delete missing rows only if delete_missing_records is explicitly True
        deleted_count = 0
        if delete_missing_records:
            missing_keys = set(existing_keys.keys()) - set(current_keys.keys())
            if missing_keys:
                with engine.begin() as conn:
                    for k in missing_keys:
                        row_dict = existing_keys[k].to_dict()
                        where_clauses = [f'"{col}" = :{col}' for col in key_columns]
                        stmt = text(f'DELETE FROM {target_table_ref} WHERE {" AND ".join(where_clauses)}')
                        conn.execute(stmt, {col: row_dict[col] for col in key_columns})
                deleted_count = len(missing_keys)

        return {
            "status": "SUCCESS",
            "table_name": table_name,
            "inserted": inserted_count,
            "updated": updated_count,
            "deleted": deleted_count,
        }

    except Exception as e:
        return {
            "status": "LOAD_FAILED",
            "error_message": str(e),
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
        }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Incremental Checkpoint State Management")
    parser.add_argument("--reset", action="store_true", help="Safely reset checkpoint state file to empty")
    parser.add_argument("--path", default="state/checkpoints.json", help="Path to checkpoint JSON file")
    cli_args = parser.parse_args()
    if cli_args.reset:
        reset_checkpoint_store(cli_args.path)
        print(f"Incremental checkpoint state safely reset at: {cli_args.path}")
