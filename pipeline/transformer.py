"""
Universal Data Cleaning & Transformation Engine Module.

Provides a modular, domain-agnostic transformation pipeline for structured datasets.
Implements column name normalization, whitespace trimming, string case formatting,
type conversion, date standardization, missing-value imputation, duplicate removal,
and before/after transformation reporting without domain-specific assumptions.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from pipeline.schema_detector import DatasetProfile, infer_logical_type


@dataclass
class TransformationConfig:
    """Configuration settings for the transformation engine."""
    normalize_column_names: bool = True
    strip_whitespace: bool = True
    string_case: str = "none"  # "none", "lower", "upper", "title"
    remove_duplicates: bool = True
    duplicate_subset: Optional[list[str]] = None
    handle_missing_values: bool = True
    missing_value_strategies: dict[str, str] = field(
        default_factory=lambda: {
            "numeric": "median",
            "categorical": "mode",
            "text": "constant",
            "text_constant": "Unknown",
            "boolean": "mode",
            "datetime": "ffill",
        }
    )
    convert_types: bool = True
    standardize_dates: bool = True


@dataclass
class TransformationResult:
    """Outcome and audit trail of a dataset transformation."""
    source_filename: str
    output_filename: str
    status: str  # "SUCCESS", "FAILED"
    rows_before: int
    rows_after: int
    columns_before: int
    columns_after: int
    duplicate_rows_detected: int
    duplicate_rows_removed: int
    missing_values_before: int
    missing_values_after: int
    columns_renamed: dict[str, str] = field(default_factory=dict)
    type_conversions: list[str] = field(default_factory=list)
    missing_strategies_applied: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    cleaned_df: Optional[pd.DataFrame] = None


def load_transformation_config(
    config_path: Path | str = "config/transformations.json",
) -> TransformationConfig:
    """
    Load transformation configuration from JSON or return sensible defaults.
    """
    path = Path(config_path)
    if not path.exists():
        return TransformationConfig()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return TransformationConfig(
            normalize_column_names=data.get("normalize_column_names", True),
            strip_whitespace=data.get("strip_whitespace", True),
            string_case=data.get("string_case", "none"),
            remove_duplicates=data.get("remove_duplicates", True),
            duplicate_subset=data.get("duplicate_subset", None),
            handle_missing_values=data.get("handle_missing_values", True),
            missing_value_strategies=data.get(
                "missing_value_strategies",
                {
                    "numeric": "median",
                    "categorical": "mode",
                    "text": "constant",
                    "text_constant": "Unknown",
                    "boolean": "mode",
                    "datetime": "ffill",
                },
            ),
            convert_types=data.get("convert_types", True),
            standardize_dates=data.get("standardize_dates", True),
        )
    except Exception:
        return TransformationConfig()


def normalize_column_name(col: str) -> str:
    """
    Transform an arbitrary column header into a clean, snake_case identifier.
    
    Rules:
    - Trim outer whitespace
    - Convert to lowercase
    - Replace non-alphanumeric characters with underscores
    - Collapse multiple consecutive underscores
    - Strip leading/trailing underscores
    """
    clean = str(col).strip().lower()
    clean = re.sub(r"[^\w]+", "_", clean)
    clean = re.sub(r"_+", "_", clean)
    clean = clean.strip("_")
    return clean if clean else "unnamed_column"


def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Normalize all column names in the DataFrame safely.
    
    Handles name collisions by appending unique integer suffixes.
    Returns (updated_df, rename_mapping).
    """
    rename_map: dict[str, str] = {}
    seen_names: dict[str, int] = {}
    new_columns: list[str] = []

    for orig in df.columns:
        norm = normalize_column_name(orig)
        if norm in seen_names:
            seen_names[norm] += 1
            unique_name = f"{norm}_{seen_names[norm]}"
        else:
            seen_names[norm] = 0
            unique_name = norm

        new_columns.append(unique_name)
        if orig != unique_name:
            rename_map[str(orig)] = unique_name

    df_renamed = df.copy()
    df_renamed.columns = new_columns
    return df_renamed, rename_map


def clean_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim outer whitespace and collapse internal duplicate spaces in text columns.
    
    Does not modify numeric, boolean, or datetime columns.
    """
    df_clean = df.copy()
    for col in df_clean.columns:
        series = df_clean[col]
        # Only process object or string columns
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            # Apply whitespace normalization safely while preserving nulls
            def _clean_str(val: Any) -> Any:
                if pd.isna(val) or not isinstance(val, str):
                    return val
                # Strip leading/trailing and normalize multiple spaces
                return re.sub(r"\s+", " ", val.strip())

            df_clean[col] = series.apply(_clean_str)

    return df_clean


def normalize_string_case(df: pd.DataFrame, case: str = "none") -> pd.DataFrame:
    """
    Apply configurable casing to text columns ('none', 'lower', 'upper', 'title').
    """
    if case == "none" or not case:
        return df

    df_case = df.copy()
    for col in df_case.columns:
        series = df_case[col]
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            if case == "lower":
                df_case[col] = series.apply(lambda x: x.lower() if isinstance(x, str) else x)
            elif case == "upper":
                df_case[col] = series.apply(lambda x: x.upper() if isinstance(x, str) else x)
            elif case == "title":
                df_case[col] = series.apply(lambda x: x.title() if isinstance(x, str) else x)

    return df_case


def handle_missing_values(
    df: pd.DataFrame,
    logical_types: dict[str, str],
    strategies: dict[str, str],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Impute missing values according to configured type-specific strategies.
    
    Returns (imputed_df, applied_operations_audit).
    """
    df_imputed = df.copy()
    applied_ops: list[str] = []

    for col in df_imputed.columns:
        series = df_imputed[col]
        null_count = int(series.isna().sum())
        if null_count == 0:
            continue

        col_type = logical_types.get(col, "String/Text")

        # Determine strategy for this type
        if col_type in ["Integer", "Float"]:
            strat = strategies.get("numeric", "median")
            num_s = pd.to_numeric(series, errors="coerce")
            fill_val = None
            if strat == "median":
                fill_val = num_s.median()
            elif strat == "mean":
                fill_val = num_s.mean()
            elif strat == "constant":
                fill_val = strategies.get("numeric_constant", 0)

            if fill_val is not None and not np.isnan(fill_val):
                if col_type == "Integer":
                    fill_val = int(round(fill_val))
                else:
                    fill_val = round(float(fill_val), 4)
                df_imputed[col] = series.fillna(fill_val)
                applied_ops.append(
                    f"Column '{col}' ({col_type}): filled {null_count} null(s) using {strat} ({fill_val})"
                )

        elif col_type == "Categorical":
            strat = strategies.get("categorical", "mode")
            if strat == "mode":
                mode_vals = series.mode(dropna=True)
                if len(mode_vals) > 0:
                    fill_val = mode_vals.iloc[0]
                    df_imputed[col] = series.fillna(fill_val)
                    applied_ops.append(
                        f"Column '{col}' (Categorical): filled {null_count} null(s) using mode ('{fill_val}')"
                    )
            elif strat == "constant":
                fill_val = strategies.get("categorical_constant", "Unknown")
                df_imputed[col] = series.fillna(fill_val)
                applied_ops.append(
                    f"Column '{col}' (Categorical): filled {null_count} null(s) using constant ('{fill_val}')"
                )

        elif col_type == "Boolean":
            strat = strategies.get("boolean", "mode")
            if strat == "mode":
                mode_vals = series.mode(dropna=True)
                if len(mode_vals) > 0:
                    fill_val = mode_vals.iloc[0]
                    df_imputed[col] = series.fillna(fill_val)
                    applied_ops.append(
                        f"Column '{col}' (Boolean): filled {null_count} null(s) using mode ({fill_val})"
                    )

        elif col_type == "DateTime":
            strat = strategies.get("datetime", "ffill")
            if strat == "ffill":
                df_imputed[col] = series.ffill().bfill()
                applied_ops.append(
                    f"Column '{col}' (DateTime): filled {null_count} null(s) using forward/backward fill"
                )
            elif strat == "bfill":
                df_imputed[col] = series.bfill().ffill()
                applied_ops.append(
                    f"Column '{col}' (DateTime): filled {null_count} null(s) using backward/forward fill"
                )

        else:  # String/Text
            strat = strategies.get("text", "constant")
            if strat == "constant":
                fill_val = strategies.get("text_constant", "Unknown")
                df_imputed[col] = series.fillna(fill_val)
                applied_ops.append(
                    f"Column '{col}' (Text): filled {null_count} null(s) using constant ('{fill_val}')"
                )
            elif strat == "mode":
                mode_vals = series.mode(dropna=True)
                if len(mode_vals) > 0:
                    fill_val = mode_vals.iloc[0]
                    df_imputed[col] = series.fillna(fill_val)
                    applied_ops.append(
                        f"Column '{col}' (Text): filled {null_count} null(s) using mode ('{fill_val}')"
                    )

    return df_imputed, applied_ops


def convert_types_and_dates(
    df: pd.DataFrame,
    logical_types: dict[str, str],
    standardize_dates: bool = True,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Safely cast columns to their inferred logical types and standardize dates.
    
    Returns (converted_df, type_conversions_list, warnings_list).
    """
    df_cast = df.copy()
    conversions: list[str] = []
    warnings: list[str] = []

    for col in df_cast.columns:
        target_type = logical_types.get(col, "String/Text")
        series = df_cast[col]
        non_null_before = int(series.notna().sum())

        try:
            if target_type == "Integer":
                converted = pd.to_numeric(series, errors="coerce")
                non_null_after = int(converted.notna().sum())
                coerced_count = non_null_before - non_null_after
                if coerced_count > 0:
                    warnings.append(
                        f"Column '{col}': {coerced_count} value(s) could not be parsed as Integer and became null"
                    )
                # Use nullable Int64 if nulls exist
                if converted.isna().any():
                    df_cast[col] = converted.round().astype("Int64")
                else:
                    df_cast[col] = converted.round().astype("int64")
                conversions.append(f"Column '{col}' -> Integer ({df_cast[col].dtype})")

            elif target_type == "Float":
                converted = pd.to_numeric(series, errors="coerce")
                non_null_after = int(converted.notna().sum())
                coerced_count = non_null_before - non_null_after
                if coerced_count > 0:
                    warnings.append(
                        f"Column '{col}': {coerced_count} value(s) could not be parsed as Float and became null"
                    )
                df_cast[col] = converted.astype("float64")
                conversions.append(f"Column '{col}' -> Float (float64)")

            elif target_type == "Boolean":
                bool_map = {
                    "true": True, "t": True, "yes": True, "y": True, "1": True, "1.0": True,
                    "false": False, "f": False, "no": False, "n": False, "0": False, "0.0": False,
                }
                def _to_bool(val: Any) -> Any:
                    if pd.isna(val):
                        return val
                    if isinstance(val, (bool, np.bool_)):
                        return bool(val)
                    key = str(val).strip().lower()
                    return bool_map.get(key, np.nan)

                converted = series.apply(_to_bool)
                coerced_count = non_null_before - int(converted.notna().sum())
                if coerced_count > 0:
                    warnings.append(
                        f"Column '{col}': {coerced_count} value(s) could not be parsed as Boolean and became null"
                    )
                df_cast[col] = converted.astype("boolean")
                conversions.append(f"Column '{col}' -> Boolean (boolean)")

            elif target_type == "DateTime":
                converted = pd.to_datetime(series, errors="coerce")
                coerced_count = non_null_before - int(converted.notna().sum())
                if coerced_count > 0:
                    warnings.append(
                        f"Column '{col}': {coerced_count} value(s) could not be parsed as DateTime and became null"
                    )
                if standardize_dates:
                    df_cast[col] = converted
                conversions.append(f"Column '{col}' -> DateTime (datetime64[ns])")

            elif target_type == "Categorical":
                df_cast[col] = series.astype(str).replace("nan", np.nan)
                conversions.append(f"Column '{col}' -> Categorical (string)")

            else:  # String/Text
                df_cast[col] = series.astype(str).replace("nan", np.nan)
                conversions.append(f"Column '{col}' -> String/Text")

        except Exception as e:
            warnings.append(f"Column '{col}': Type casting failed ({e}); original values preserved")

    return df_cast, conversions, warnings


def remove_duplicates(
    df: pd.DataFrame, subset: Optional[list[str]] = None
) -> tuple[pd.DataFrame, int]:
    """
    Identify and remove duplicate rows based on all or configured columns.
    
    Returns (deduplicated_df, duplicates_removed_count).
    """
    # Filter subset to only existing columns
    valid_subset = [c for c in subset if c in df.columns] if subset else None
    duplicate_count = int(df.duplicated(subset=valid_subset).sum())

    if duplicate_count > 0:
        df_dedup = df.drop_duplicates(subset=valid_subset, keep="first").reset_index(drop=True)
    else:
        df_dedup = df.copy()

    return df_dedup, duplicate_count


def transform_dataset(
    df: pd.DataFrame,
    source_filename: str,
    schema_profile: Optional[DatasetProfile] = None,
    config: Optional[TransformationConfig] = None,
) -> TransformationResult:
    """
    Execute the end-to-end cleaning and transformation pipeline on a dataset.
    
    Never modifies raw files or the original input DataFrame in place.
    """
    cfg = config or load_transformation_config()
    source_path = Path(source_filename)
    output_filename = f"{source_path.stem}_cleaned.csv"

    rows_before = len(df)
    columns_before = len(df.columns)
    missing_before = int(df.isna().sum().sum())
    dup_before = int(df.duplicated().sum())

    if rows_before == 0 or columns_before == 0:
        return TransformationResult(
            source_filename=source_filename,
            output_filename=output_filename,
            status="SUCCESS",
            rows_before=rows_before,
            rows_after=0,
            columns_before=columns_before,
            columns_after=0,
            duplicate_rows_detected=0,
            duplicate_rows_removed=0,
            missing_values_before=0,
            missing_values_after=0,
            cleaned_df=df.copy(),
        )

    working_df = df.copy()
    columns_renamed: dict[str, str] = {}
    type_conversions: list[str] = []
    missing_ops: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []

    try:
        # Step 1: Column Name Normalization
        if cfg.normalize_column_names:
            working_df, columns_renamed = normalize_columns(working_df)

        # Build mapping of logical types for current working column names
        logical_types: dict[str, str] = {}
        for col in working_df.columns:
            # Check if this column maps back to a profiled column
            orig_col = None
            for o, n in columns_renamed.items():
                if n == col:
                    orig_col = o
                    break
            lookup_key = orig_col or col

            if schema_profile and lookup_key in schema_profile.columns:
                logical_types[col] = schema_profile.columns[lookup_key].inferred_type
            else:
                logical_types[col] = infer_logical_type(working_df[col], len(working_df))

        # Step 2: Whitespace Cleaning
        if cfg.strip_whitespace:
            working_df = clean_whitespace(working_df)

        # Step 3: String Case Normalization
        if cfg.string_case and cfg.string_case != "none":
            working_df = normalize_string_case(working_df, case=cfg.string_case)

        # Step 4: Missing Value Handling
        if cfg.handle_missing_values:
            working_df, missing_ops = handle_missing_values(
                working_df, logical_types, cfg.missing_value_strategies
            )

        # Step 5: Type Casting & Date Standardization
        if cfg.convert_types:
            working_df, type_conversions, cast_warnings = convert_types_and_dates(
                working_df, logical_types, standardize_dates=cfg.standardize_dates
            )
            warnings.extend(cast_warnings)

        # Step 6: Duplicate Removal
        duplicate_rows_removed = 0
        if cfg.remove_duplicates:
            # If duplicate_subset specified, map column names if renamed
            subset_cols = None
            if cfg.duplicate_subset:
                subset_cols = [
                    columns_renamed.get(c, c) for c in cfg.duplicate_subset
                ]
            working_df, duplicate_rows_removed = remove_duplicates(
                working_df, subset=subset_cols
            )

        rows_after = len(working_df)
        columns_after = len(working_df.columns)
        missing_after = int(working_df.isna().sum().sum())

        return TransformationResult(
            source_filename=source_filename,
            output_filename=output_filename,
            status="SUCCESS",
            rows_before=rows_before,
            rows_after=rows_after,
            columns_before=columns_before,
            columns_after=columns_after,
            duplicate_rows_detected=dup_before,
            duplicate_rows_removed=duplicate_rows_removed,
            missing_values_before=missing_before,
            missing_values_after=missing_after,
            columns_renamed=columns_renamed,
            type_conversions=type_conversions,
            missing_strategies_applied=missing_ops,
            warnings=warnings,
            errors=errors,
            cleaned_df=working_df,
        )

    except Exception as e:
        return TransformationResult(
            source_filename=source_filename,
            output_filename=output_filename,
            status="FAILED",
            rows_before=rows_before,
            rows_after=0,
            columns_before=columns_before,
            columns_after=0,
            duplicate_rows_detected=dup_before,
            duplicate_rows_removed=0,
            missing_values_before=missing_before,
            missing_values_after=0,
            errors=[str(e)],
            cleaned_df=None,
        )


def save_processed_dataset(
    cleaned_df: pd.DataFrame,
    source_filename: str,
    processed_dir: Path | str = "data/processed",
) -> Path:
    """
    Save the transformed clean dataset into data/processed/.
    
    Dynamically generates the output filename (e.g. sample_sales_cleaned.csv).
    Never modifies raw inputs.
    """
    out_dir = Path(processed_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    source_path = Path(source_filename)
    output_filename = f"{source_path.stem}_cleaned.csv"
    output_path = out_dir / output_filename

    # Export cleanly formatted CSV
    cleaned_df.to_csv(output_path, index=False)
    return output_path


def log_transformation_event(
    result: TransformationResult, log_dir: Path | str = "logs"
) -> None:
    """
    Append transformation metrics and operations to logs/transformation.log.
    """
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "transformation.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if result.status == "SUCCESS":
        detail = (
            f"Rows: {result.rows_before} -> {result.rows_after} | "
            f"Dups Removed: {result.duplicate_rows_removed} | "
            f"Nulls: {result.missing_values_before} -> {result.missing_values_after} | "
            f"Renamed: {len(result.columns_renamed)} cols"
        )
    else:
        detail = f"Errors: {', '.join(result.errors)}"

    log_entry = (
        f"[{timestamp}] [{result.status}] Dataset: {result.source_filename} -> "
        f"{result.output_filename} | {detail}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def save_transformation_report(
    results: list[TransformationResult], reports_dir: Path | str = "reports"
) -> Path:
    """
    Serialize all transformation outcomes to reports/transformation_report.json.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_file = out_dir / "transformation_report.json"

    # Convert dataclasses to dict excluding raw dataframe
    serializable = []
    for r in results:
        d = asdict(r)
        d.pop("cleaned_df", None)
        serializable.append(d)

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, default=str)

    return report_file


def format_transformation_summary(result: TransformationResult) -> str:
    """
    Generate a human-readable before-and-after comparison summary for console output.
    """
    if result.status != "SUCCESS":
        return f"Dataset: {result.source_filename}\nStatus: FAILED\nErrors: {', '.join(result.errors)}"

    lines: list[str] = [
        f"Dataset: {result.source_filename} -> {result.output_filename}",
        f"Status: {result.status}",
        "Before vs After Cleaning:",
        f"  * Rows: {result.rows_before:,} -> {result.rows_after:,} ({result.duplicate_rows_removed} duplicate(s) removed)",
        f"  * Columns: {result.columns_before} -> {result.columns_after} ({len(result.columns_renamed)} renamed to snake_case)",
        f"  * Missing Values: {result.missing_values_before:,} -> {result.missing_values_after:,}",
    ]

    if result.columns_renamed:
        lines.append("  * Renamed Columns:")
        for orig, new in list(result.columns_renamed.items())[:5]:
            lines.append(f"      '{orig}' -> '{new}'")

    if result.missing_strategies_applied:
        lines.append("  * Imputations Applied:")
        for op in result.missing_strategies_applied[:5]:
            lines.append(f"      {op}")

    if result.warnings:
        lines.append("  * Conversion Warnings:")
        for warn in result.warnings[:3]:
            lines.append(f"      {warn}")

    return "\n".join(lines)
