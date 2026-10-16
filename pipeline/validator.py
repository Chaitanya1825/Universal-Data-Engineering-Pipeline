"""
Universal Data Quality & Validation Engine Module.

Provides domain-agnostic data quality assertions and schema validation for
cleaned datasets. Implements nullability validation, uniqueness assertions,
numeric range checks, regex format validation, logical type verification,
quarantine of invalid records, quality scoring, and audit reporting.
"""

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from pipeline.schema_detector import DatasetProfile


@dataclass
class FailureDetail:
    """Detailed record of an individual validation rule failure on a row."""
    row_index: int
    column: str
    rule: str  # "nullability", "uniqueness", "range", "regex", "type", "schema"
    actual_value: Any
    expected: str
    reason: str


@dataclass
class ValidationConfig:
    """Configurable data quality rules."""
    required_columns: list[str] = field(default_factory=list)
    unique_columns: list[str] = field(default_factory=list)
    range_rules: dict[str, dict[str, float]] = field(default_factory=dict)
    regex_rules: dict[str, str] = field(default_factory=dict)
    type_rules: dict[str, str] = field(default_factory=dict)
    dataset_rules: dict[str, Any] = field(
        default_factory=lambda: {
            "min_row_count": 0,
            "max_missing_percentage": 100.0,
            "allow_unexpected_columns": True,
        }
    )


@dataclass
class DatasetQualityMetrics:
    """Quantitative evaluation metrics for dataset quality."""
    total_rows: int
    valid_rows: int
    invalid_rows: int
    total_failures: int
    quality_score: float  # Formula: (valid_rows / total_rows) * 100
    null_violations: int = 0
    uniqueness_violations: int = 0
    range_violations: int = 0
    regex_violations: int = 0
    type_violations: int = 0
    schema_violations: int = 0


@dataclass
class ValidationResult:
    """Consolidated outcome of dataset validation."""
    source_filename: str
    status: str  # "PASSED", "FAILED"
    metrics: DatasetQualityMetrics
    rules_executed: list[str] = field(default_factory=list)
    failures_by_rule: dict[str, int] = field(default_factory=dict)
    failures_by_column: dict[str, int] = field(default_factory=dict)
    record_failures: list[FailureDetail] = field(default_factory=list)
    validated_output_path: Optional[str] = None
    rejected_output_path: Optional[str] = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def load_validation_config(
    config_path: Path | str = "config/validation.json",
    dataset_name: Optional[str] = None,
) -> ValidationConfig:
    """
    Load validation rules from JSON configuration file.
    
    Supports default global rules and dataset-specific override configurations.
    Returns permissive defaults if configuration file is absent.
    """
    path = Path(config_path)
    if not path.exists():
        return ValidationConfig()

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Check for dataset-specific overrides if provided
        rules_dict = raw_data.get("default", raw_data)
        if dataset_name and "datasets" in raw_data and dataset_name in raw_data["datasets"]:
            dataset_overrides = raw_data["datasets"][dataset_name]
            # Merge default with dataset overrides
            merged = dict(rules_dict)
            merged.update(dataset_overrides)
            rules_dict = merged

        return ValidationConfig(
            required_columns=rules_dict.get("required_columns", []),
            unique_columns=rules_dict.get("unique_columns", []),
            range_rules=rules_dict.get("range_rules", {}),
            regex_rules=rules_dict.get("regex_rules", {}),
            type_rules=rules_dict.get("type_rules", {}),
            dataset_rules=rules_dict.get(
                "dataset_rules",
                {
                    "min_row_count": 0,
                    "max_missing_percentage": 100.0,
                    "allow_unexpected_columns": True,
                },
            ),
        )
    except Exception:
        return ValidationConfig()


def validate_schema(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[list[str], list[str]]:
    """
    Validate dataset column presence, unexpected columns, and duplicates.
    
    Returns (executed_rules, schema_errors).
    """
    executed: list[str] = []
    errors: list[str] = []

    # Check for duplicate column headers
    cols = list(df.columns)
    if len(cols) != len(set(cols)):
        duplicates = [c for c in cols if cols.count(c) > 1]
        errors.append(f"Duplicate column headers detected: {list(set(duplicates))}")

    # Check required column presence
    if config.required_columns:
        executed.append("required_column_presence")
        missing_cols = [c for c in config.required_columns if c not in df.columns]
        if missing_cols:
            errors.append(f"Required column(s) missing from dataset: {missing_cols}")

    # Check unexpected columns if not allowed
    allow_unexpected = config.dataset_rules.get("allow_unexpected_columns", True)
    if not allow_unexpected and config.required_columns:
        executed.append("unexpected_column_check")
        unexpected = [c for c in df.columns if c not in config.required_columns]
        if unexpected:
            errors.append(f"Unexpected column(s) detected: {unexpected}")

    return executed, errors


def validate_nullability(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[dict[int, list[FailureDetail]], int]:
    """
    Validate that configured required columns contain non-null values.
    
    Returns (row_failures_map, total_null_violations).
    """
    failures: dict[int, list[FailureDetail]] = defaultdict(list)
    violation_count = 0

    for col in config.required_columns:
        if col not in df.columns:
            continue

        null_mask = df[col].isna()
        null_indices = df[null_mask].index

        for idx in null_indices:
            failures[int(idx)].append(
                FailureDetail(
                    row_index=int(idx),
                    column=col,
                    rule="nullability",
                    actual_value=None,
                    expected="non-null value",
                    reason=f"Column '{col}' is required but contains a null value",
                )
            )
            violation_count += 1

    return failures, violation_count


def validate_uniqueness(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[dict[int, list[FailureDetail]], int]:
    """
    Validate that configured columns have unique values across the dataset.
    
    Identifies duplicate entries without automatically mutating the dataset.
    Returns (row_failures_map, total_uniqueness_violations).
    """
    failures: dict[int, list[FailureDetail]] = defaultdict(list)
    violation_count = 0

    for col in config.unique_columns:
        if col not in df.columns:
            continue

        # Flag occurrences after the first as duplicate violations
        dup_mask = df.duplicated(subset=[col], keep="first")
        dup_indices = df[dup_mask].index

        for idx in dup_indices:
            val = df.at[idx, col]
            failures[int(idx)].append(
                FailureDetail(
                    row_index=int(idx),
                    column=col,
                    rule="uniqueness",
                    actual_value=str(val),
                    expected="unique value across dataset",
                    reason=f"Column '{col}' violates uniqueness rule (duplicate value: '{val}')",
                )
            )
            violation_count += 1

    return failures, violation_count


def validate_numeric_ranges(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[dict[int, list[FailureDetail]], int]:
    """
    Validate that numeric columns fall within configured minimum and maximum bounds.
    
    Returns (row_failures_map, total_range_violations).
    """
    failures: dict[int, list[FailureDetail]] = defaultdict(list)
    violation_count = 0

    for col, bounds in config.range_rules.items():
        if col not in df.columns:
            continue

        min_val = bounds.get("min")
        max_val = bounds.get("max")
        if min_val is None and max_val is None:
            continue

        series_num = pd.to_numeric(df[col], errors="coerce")

        for idx, val in series_num.items():
            if pd.isna(val):
                continue

            if min_val is not None and val < min_val:
                failures[int(idx)].append(
                    FailureDetail(
                        row_index=int(idx),
                        column=col,
                        rule="range",
                        actual_value=float(val),
                        expected=f">= {min_val}",
                        reason=f"Value {val} in '{col}' is below configured minimum {min_val}",
                    )
                )
                violation_count += 1

            if max_val is not None and val > max_val:
                failures[int(idx)].append(
                    FailureDetail(
                        row_index=int(idx),
                        column=col,
                        rule="range",
                        actual_value=float(val),
                        expected=f"<= {max_val}",
                        reason=f"Value {val} in '{col}' exceeds configured maximum {max_val}",
                    )
                )
                violation_count += 1

    return failures, violation_count


def validate_regex_formats(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[dict[int, list[FailureDetail]], int]:
    """
    Validate string column patterns against configured regular expressions.
    
    Returns (row_failures_map, total_regex_violations).
    """
    failures: dict[int, list[FailureDetail]] = defaultdict(list)
    violation_count = 0

    for col, pattern in config.regex_rules.items():
        if col not in df.columns:
            continue

        try:
            compiled = re.compile(pattern)
        except re.error as e:
            continue

        for idx, val in df[col].items():
            if pd.isna(val):
                continue

            str_val = str(val)
            if not compiled.search(str_val):
                failures[int(idx)].append(
                    FailureDetail(
                        row_index=int(idx),
                        column=col,
                        rule="regex",
                        actual_value=str_val,
                        expected=f"match pattern '{pattern}'",
                        reason=f"Value '{str_val}' in '{col}' does not match required format pattern",
                    )
                )
                violation_count += 1

    return failures, violation_count


def validate_logical_types(
    df: pd.DataFrame, config: ValidationConfig
) -> tuple[dict[int, list[FailureDetail]], int]:
    """
    Validate column value conformity with configured or inferred logical types.
    
    Avoids false failures on valid nullable entries.
    Returns (row_failures_map, total_type_violations).
    """
    failures: dict[int, list[FailureDetail]] = defaultdict(list)
    violation_count = 0

    for col, expected_type in config.type_rules.items():
        if col not in df.columns:
            continue

        for idx, val in df[col].items():
            if pd.isna(val):
                continue

            is_valid = True
            if expected_type == "Integer":
                try:
                    num_val = float(val)
                    if not num_val.is_integer():
                        is_valid = False
                except (ValueError, TypeError):
                    is_valid = False

            elif expected_type == "Float":
                try:
                    float(val)
                except (ValueError, TypeError):
                    is_valid = False

            elif expected_type == "Boolean":
                if not isinstance(val, (bool, np.bool_)):
                    s = str(val).strip().lower()
                    if s not in {"true", "false", "1", "0", "yes", "no", "t", "f", "y", "n"}:
                        is_valid = False

            elif expected_type == "DateTime":
                if not isinstance(val, (datetime, pd.Timestamp)):
                    try:
                        pd.to_datetime(str(val))
                    except Exception:
                        is_valid = False

            if not is_valid:
                failures[int(idx)].append(
                    FailureDetail(
                        row_index=int(idx),
                        column=col,
                        rule="type",
                        actual_value=str(val),
                        expected=f"valid {expected_type}",
                        reason=f"Value '{val}' in '{col}' does not conform to expected type {expected_type}",
                    )
                )
                violation_count += 1

    return failures, violation_count


def validate_dataset(
    df: pd.DataFrame,
    source_filename: str,
    config: Optional[ValidationConfig] = None,
    schema_profile: Optional[DatasetProfile] = None,
) -> tuple[ValidationResult, pd.DataFrame, pd.DataFrame]:
    """
    Perform comprehensive, multi-rule quality validation on a cleaned dataset.
    
    Separates dataset into valid and rejected records without modifying input data.
    Returns (ValidationResult, valid_df, rejected_df).
    """
    cfg = config or load_validation_config(dataset_name=source_filename)
    total_rows = len(df)
    total_cols = len(df.columns)

    rules_executed: list[str] = []
    errors: list[str] = []
    warnings: list[str] = []

    # Map of row_index -> list of FailureDetails
    combined_failures: dict[int, list[FailureDetail]] = defaultdict(list)
    failure_counts_by_rule: dict[str, int] = defaultdict(int)
    failure_counts_by_col: dict[str, int] = defaultdict(int)

    # 1. Schema Validation
    schema_rules, schema_errs = validate_schema(df, cfg)
    rules_executed.extend(schema_rules)
    errors.extend(schema_errs)
    schema_violations = len(schema_errs)
    if schema_violations > 0:
        failure_counts_by_rule["schema"] += schema_violations

    # 2. Nullability Validation
    if cfg.required_columns:
        rules_executed.append("nullability_validation")
        null_fails, null_viols = validate_nullability(df, cfg)
        for idx, fails in null_fails.items():
            combined_failures[idx].extend(fails)
        failure_counts_by_rule["nullability"] = null_viols
    else:
        null_viols = 0

    # 3. Uniqueness Validation
    if cfg.unique_columns:
        rules_executed.append("uniqueness_validation")
        uniq_fails, uniq_viols = validate_uniqueness(df, cfg)
        for idx, fails in uniq_fails.items():
            combined_failures[idx].extend(fails)
        failure_counts_by_rule["uniqueness"] = uniq_viols
    else:
        uniq_viols = 0

    # 4. Numeric Range Validation
    if cfg.range_rules:
        rules_executed.append("numeric_range_validation")
        range_fails, range_viols = validate_numeric_ranges(df, cfg)
        for idx, fails in range_fails.items():
            combined_failures[idx].extend(fails)
        failure_counts_by_rule["range"] = range_viols
    else:
        range_viols = 0

    # 5. Regex / Format Validation
    if cfg.regex_rules:
        rules_executed.append("regex_format_validation")
        regex_fails, regex_viols = validate_regex_formats(df, cfg)
        for idx, fails in regex_fails.items():
            combined_failures[idx].extend(fails)
        failure_counts_by_rule["regex"] = regex_viols
    else:
        regex_viols = 0

    # 6. Logical Type Validation
    if cfg.type_rules:
        rules_executed.append("type_conformity_validation")
        type_fails, type_viols = validate_logical_types(df, cfg)
        for idx, fails in type_fails.items():
            combined_failures[idx].extend(fails)
        failure_counts_by_rule["type"] = type_viols
    else:
        type_viols = 0

    # 7. Dataset-level Threshold Checks
    min_rows = cfg.dataset_rules.get("min_row_count", 0)
    if total_rows < min_rows:
        warnings.append(f"Dataset has {total_rows} row(s), below configured minimum of {min_rows}")

    max_missing_pct = cfg.dataset_rules.get("max_missing_percentage", 100.0)
    if total_rows > 0 and total_cols > 0:
        actual_missing_pct = (df.isna().sum().sum() / (total_rows * total_cols)) * 100
        if actual_missing_pct > max_missing_pct:
            warnings.append(
                f"Missing values ({actual_missing_pct:.1f}%) exceed configured threshold ({max_missing_pct}%)"
            )

    # Flatten all record failure details
    all_record_failures: list[FailureDetail] = []
    for idx, fails in combined_failures.items():
        all_record_failures.extend(fails)
        for f in fails:
            failure_counts_by_col[f.column] += 1

    total_failures = len(all_record_failures)
    invalid_row_indices = set(combined_failures.keys())
    invalid_rows = len(invalid_row_indices)
    valid_rows = max(0, total_rows - invalid_rows)

    # Quality Score: Percentage of records that meet all configured validation rules
    quality_score = round((valid_rows / total_rows * 100), 2) if total_rows > 0 else 100.0

    metrics = DatasetQualityMetrics(
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        total_failures=total_failures,
        quality_score=quality_score,
        null_violations=null_viols,
        uniqueness_violations=uniq_viols,
        range_violations=range_viols,
        regex_violations=regex_viols,
        type_violations=type_viols,
        schema_violations=schema_violations,
    )

    # Partition DataFrame into Valid and Rejected
    if total_rows == 0:
        valid_df = df.copy()
        rejected_df = pd.DataFrame()
    else:
        valid_mask = ~df.index.isin(invalid_row_indices)
        valid_df = df[valid_mask].copy().reset_index(drop=True)

        if invalid_rows > 0:
            rejected_df = df[~valid_mask].copy()
            # Attach audit metadata to rejected records
            reasons_list: list[str] = []
            for idx in rejected_df.index:
                row_fails = combined_failures.get(int(idx), [])
                reason_str = " | ".join(f"{rf.column} ({rf.rule}): {rf.reason}" for rf in row_fails)
                reasons_list.append(reason_str)

            rejected_df.insert(0, "_row_index", rejected_df.index.tolist())
            rejected_df.insert(1, "_rejection_reasons", reasons_list)
            rejected_df = rejected_df.reset_index(drop=True)
        else:
            rejected_df = pd.DataFrame()

    status = "PASSED" if invalid_rows == 0 and len(errors) == 0 else "FAILED"

    result = ValidationResult(
        source_filename=source_filename,
        status=status,
        metrics=metrics,
        rules_executed=rules_executed if rules_executed else ["default_permissive_pass"],
        failures_by_rule=dict(failure_counts_by_rule),
        failures_by_column=dict(failure_counts_by_col),
        record_failures=all_record_failures,
        warnings=warnings,
        errors=errors,
    )

    return result, valid_df, rejected_df


def save_validation_outputs(
    source_filename: str,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    validated_dir: Path | str = "data/validated",
    rejected_dir: Path | str = "data/rejected",
) -> tuple[Path, Optional[Path]]:
    """
    Save validated records to data/validated/ and quarantined records to data/rejected/.
    """
    val_path = Path(validated_dir)
    val_path.mkdir(parents=True, exist_ok=True)
    rej_path = Path(rejected_dir)
    rej_path.mkdir(parents=True, exist_ok=True)

    source_stem = Path(source_filename).stem.replace("_cleaned", "")
    valid_file = val_path / f"{source_stem}_validated.csv"
    valid_df.to_csv(valid_file, index=False)

    rejected_file: Optional[Path] = None
    if len(rejected_df) > 0:
        rejected_file = rej_path / f"{source_stem}_rejected.csv"
        rejected_df.to_csv(rejected_file, index=False)

    return valid_file, rejected_file


def log_validation_event(
    result: ValidationResult, log_dir: Path | str = "logs"
) -> None:
    """
    Append validation outcome to logs/validation.log without exposing sensitive data.
    """
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "validation.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    m = result.metrics
    detail = (
        f"Evaluated: {m.total_rows} | Valid: {m.valid_rows} | Rejected: {m.invalid_rows} | "
        f"Score: {m.quality_score:.1f}% | Rules: {len(result.rules_executed)}"
    )

    log_entry = (
        f"[{timestamp}] [{result.status}] Dataset: {result.source_filename} | {detail}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def save_validation_reports(
    results: list[ValidationResult], reports_dir: Path | str = "reports"
) -> tuple[Path, Path]:
    """
    Serialize validation reports to reports/validation_report.json and reports/validation_report.csv.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "validation_report.json"
    csv_path = out_dir / "validation_report.csv"

    # JSON report
    serializable = []
    for r in results:
        d = asdict(r)
        serializable.append(d)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, default=str)

    # Tabular CSV report
    csv_rows: list[dict[str, Any]] = []
    for r in results:
        csv_rows.append({
            "dataset": r.source_filename,
            "status": r.status,
            "total_rows": r.metrics.total_rows,
            "valid_rows": r.metrics.valid_rows,
            "rejected_rows": r.metrics.invalid_rows,
            "quality_score_pct": r.metrics.quality_score,
            "null_violations": r.metrics.null_violations,
            "uniqueness_violations": r.metrics.uniqueness_violations,
            "range_violations": r.metrics.range_violations,
            "regex_violations": r.metrics.regex_violations,
            "type_violations": r.metrics.type_violations,
            "schema_violations": r.metrics.schema_violations,
            "rules_executed_count": len(r.rules_executed),
            "validated_output": r.validated_output_path or "",
            "rejected_output": r.rejected_output_path or "None",
        })

    csv_df = pd.DataFrame(csv_rows)
    csv_df.to_csv(csv_path, index=False)

    return json_path, csv_path


def format_validation_summary(result: ValidationResult) -> str:
    """
    Format a concise, informative data quality summary for console display.
    """
    m = result.metrics
    lines: list[str] = [
        f"Dataset: {result.source_filename}",
        f"Validation Status: {result.status}",
        f"Quality Score: {m.quality_score:.1f}% ({m.valid_rows:,}/{m.total_rows:,} records valid)",
        f"  * Valid Records Passed: {m.valid_rows:,}",
        f"  * Invalid Records Quarantined: {m.invalid_rows:,}",
        f"  * Rules Executed: {', '.join(result.rules_executed)}",
    ]

    if m.invalid_rows > 0:
        lines.append("  * Failures by Rule Type:")
        for rule, count in result.failures_by_rule.items():
            lines.append(f"      - {rule}: {count} failure(s)")

        lines.append("  * Sample Failure Reasons:")
        for fail in result.record_failures[:3]:
            lines.append(f"      - Row {fail.row_index} | Column '{fail.column}': {fail.reason}")

        if result.rejected_output_path:
            lines.append(f"  * Quarantine File: {result.rejected_output_path}")

    if result.validated_output_path:
        lines.append(f"  * Validated File: {result.validated_output_path}")

    return "\n".join(lines)
