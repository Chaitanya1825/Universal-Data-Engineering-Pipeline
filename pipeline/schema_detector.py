"""
Universal Schema Detection & Data Profiling Module.

Provides domain-agnostic inference of logical data types, column-level statistical
profiling, identifier heuristics, categorical detection, date/time detection,
and dataset-level metadata calculation without business domain assumptions.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any, Optional

import numpy as np
import pandas as pd


# Identifier naming pattern heuristic (generic, domain-agnostic)
IDENTIFIER_PATTERN = re.compile(r"(?:^|[_\W])(id|code|key|uuid|pk|num|number)(?:[_\W]|$)", re.IGNORECASE)

# Common date separators/patterns heuristic
DATE_SEPARATORS_PATTERN = re.compile(r"[-/:]")


@dataclass
class IdentifierInfo:
    """Heuristic assessment of whether a column acts as an identifier."""
    likely_identifier: bool
    confidence: float
    reason: str


@dataclass
class NumericStats:
    """Summary statistics for numeric columns."""
    count: int
    mean: Optional[float]
    median: Optional[float]
    std: Optional[float]
    min: Optional[float]
    max: Optional[float]
    q25: Optional[float]
    q50: Optional[float]
    q75: Optional[float]


@dataclass
class TextStats:
    """Summary statistics for text/string columns."""
    min_length: int
    max_length: int
    avg_length: float


@dataclass
class CategoryFrequency:
    """Top value frequency in categorical columns."""
    value: str
    count: int
    percentage: float


@dataclass
class CategoricalStats:
    """Summary statistics for categorical columns."""
    num_categories: int
    top_categories: list[CategoryFrequency] = field(default_factory=list)


@dataclass
class ColumnProfile:
    """Comprehensive profile for an individual column."""
    column_name: str
    pandas_dtype: str
    inferred_type: str  # Integer, Float, Boolean, DateTime, Categorical, String/Text
    total_values: int
    missing_count: int
    missing_percentage: float
    non_null_count: int
    unique_count: int
    distinct_percentage: float
    duplicate_count: int
    is_nullable: bool
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    numeric_stats: Optional[NumericStats] = None
    text_stats: Optional[TextStats] = None
    categorical_stats: Optional[CategoricalStats] = None
    identifier_info: Optional[IdentifierInfo] = None


@dataclass
class DatasetProfile:
    """Comprehensive dataset-level profile."""
    filename: str
    row_count: int
    column_count: int
    total_missing_values: int
    overall_missing_percentage: float
    duplicate_rows: int
    duplicate_row_percentage: float
    memory_usage_bytes: int
    memory_usage_formatted: str
    numeric_columns: int
    text_columns: int
    categorical_columns: int
    datetime_columns: int
    boolean_columns: int
    identifier_columns: int
    columns: dict[str, ColumnProfile] = field(default_factory=dict)


def format_memory_size(bytes_count: int) -> str:
    """Format memory bytes into human-readable string."""
    if bytes_count < 1024:
        return f"{bytes_count} B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f} KB"
    return f"{bytes_count / (1024 * 1024):.1f} MB"


def detect_boolean_type(series: pd.Series) -> tuple[bool, float, str]:
    """
    Check if a series exhibits boolean characteristics.
    
    Recognizes boolean dtypes, as well as string/numeric representations:
    {True, False}, {yes, no}, {y, n}, {1, 0}, {t, f}.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0, "Column has no non-null values"

    if pd.api.types.is_bool_dtype(series):
        return True, 1.0, "Native boolean dtype"

    unique_vals = set(non_null.unique())
    if len(unique_vals) > 2 or len(unique_vals) == 0:
        return False, 0.0, "Contains more than two distinct values"

    # String representations
    str_vals = {str(v).strip().lower() for v in unique_vals}
    boolean_pairs = [
        {"true", "false"},
        {"yes", "no"},
        {"y", "n"},
        {"t", "f"},
        {"1", "0"},
        {"1.0", "0.0"},
    ]

    for pair in boolean_pairs:
        if str_vals.issubset(pair):
            return True, 0.95, f"Values match boolean set: {str_vals}"

    return False, 0.0, "Values do not match boolean patterns"


def detect_datetime_type(series: pd.Series) -> tuple[bool, float, str]:
    """
    Check if a series contains date/time values without falsely classifying pure numbers.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return True, 1.0, "Native datetime dtype"

    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0, "Column has no non-null values"

    # Avoid converting pure numbers (e.g. integers, floats) directly into dates
    if pd.api.types.is_numeric_dtype(series):
        return False, 0.0, "Numeric dtype without date structure"

    # Inspect sample of string representations for date separators
    sample = non_null.head(50).astype(str)
    has_date_separators = sample.apply(lambda s: bool(DATE_SEPARATORS_PATTERN.search(s))).all()
    if not has_date_separators:
        return False, 0.0, "String values lack standard date separators (- / :)"

    try:
        # Attempt conversion on sample
        converted = pd.to_datetime(sample, errors="coerce", format="mixed")
        valid_ratio = converted.notna().mean()
        if valid_ratio >= 0.85:
            # Check reasonable year boundaries (e.g., 1800 to 2200)
            years = converted.dropna().dt.year
            if ((years >= 1800) & (years <= 2200)).all():
                return True, round(float(valid_ratio), 2), f"Parsed {valid_ratio*100:.0f}% sample records as valid dates"
    except Exception:
        pass

    return False, 0.0, "Could not reliably parse as datetime"


def detect_identifier(
    column_name: str,
    series: pd.Series,
    inferred_type: str,
    row_count: int,
) -> IdentifierInfo:
    """
    Heuristic assessment of whether a column is likely an identifier/primary key.
    
    Combines multiple signals:
    - Uniqueness ratio (unique / non-null)
    - Column name naming patterns ('id', 'code', 'key', 'uuid', 'pk', 'num')
    - Low missing/null value ratio
    - Appropriate logical data type (Integer, String/Text)
    """
    if row_count < 2:
        return IdentifierInfo(
            likely_identifier=False,
            confidence=0.0,
            reason="Insufficient rows for identifier evaluation",
        )

    non_null_count = int(series.notna().sum())
    if non_null_count == 0:
        return IdentifierInfo(
            likely_identifier=False,
            confidence=0.0,
            reason="Column is entirely null",
        )

    unique_count = int(series.nunique(dropna=True))
    uniqueness_ratio = unique_count / non_null_count
    missing_ratio = (row_count - non_null_count) / row_count
    has_id_name = bool(IDENTIFIER_PATTERN.search(column_name))
    is_valid_type = inferred_type in ["Integer", "String/Text"]

    # Signal 1: High uniqueness + identifier-like naming pattern (Strongest Signal)
    if has_id_name and is_valid_type:
        if uniqueness_ratio == 1.0 and missing_ratio == 0.0:
            return IdentifierInfo(
                likely_identifier=True,
                confidence=0.98,
                reason="100% uniqueness, no nulls, and identifier naming pattern",
            )
        elif uniqueness_ratio >= 0.90 and missing_ratio <= 0.05:
            return IdentifierInfo(
                likely_identifier=True,
                confidence=0.85,
                reason=f"High uniqueness ({uniqueness_ratio*100:.1f}%) and identifier naming pattern",
            )

    # Signal 2: Perfect uniqueness in large datasets without explicit 'id' naming
    if uniqueness_ratio == 1.0 and missing_ratio == 0.0 and is_valid_type and row_count >= 50:
        return IdentifierInfo(
            likely_identifier=True,
            confidence=0.70,
            reason=f"100% uniqueness across {row_count} rows without nulls",
        )

    return IdentifierInfo(
        likely_identifier=False,
        confidence=0.10,
        reason=(
            f"Uniqueness ({uniqueness_ratio*100:.1f}%) without identifier pattern "
            "does not indicate identifier"
            if not has_id_name
            else f"Low uniqueness ({uniqueness_ratio*100:.1f}%) for identifier candidate"
        ),
    )


def infer_logical_type(series: pd.Series, row_count: int) -> str:
    """
    Infer domain-agnostic logical data type based on values and structure.
    
    Returns one of: 'Integer', 'Float', 'Boolean', 'DateTime', 'Categorical', 'String/Text'.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return "String/Text"

    # 1. Boolean check
    is_bool, _, _ = detect_boolean_type(series)
    if is_bool:
        return "Boolean"

    # 2. DateTime check
    is_dt, _, _ = detect_datetime_type(series)
    if is_dt:
        return "DateTime"

    # 3. Numeric checks
    if pd.api.types.is_integer_dtype(series):
        return "Integer"

    if pd.api.types.is_float_dtype(series):
        # Check if float series actually represents whole integers with nulls
        try:
            is_all_int = (non_null % 1 == 0).all()
            if is_all_int:
                return "Integer"
        except Exception:
            pass
        return "Float"

    # Object/string checking for numeric values
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        # Test numeric parsing
        try:
            num_parsed = pd.to_numeric(non_null, errors="coerce")
            valid_ratio = num_parsed.notna().mean()
            if valid_ratio >= 0.95:
                parsed_non_null = num_parsed.dropna()
                if (parsed_non_null % 1 == 0).all():
                    return "Integer"
                return "Float"
        except Exception:
            pass

    # 4. Categorical check
    unique_count = int(series.nunique(dropna=True))
    cardinality_ratio = unique_count / row_count if row_count > 0 else 1.0

    # Low distinct values, repeated entries, reasonable ratio
    if (
        unique_count >= 2
        and (unique_count <= 25 or cardinality_ratio <= 0.35)
        and len(non_null) > unique_count
    ):
        return "Categorical"

    return "String/Text"


def calculate_numeric_stats(series: pd.Series) -> Optional[NumericStats]:
    """Calculate descriptive statistics for numeric data safely."""
    # Convert safely to numeric
    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric_series) == 0:
        return None

    count = int(len(numeric_series))
    mean_val = float(numeric_series.mean())
    median_val = float(numeric_series.median())
    std_val = float(numeric_series.std()) if count > 1 else 0.0
    min_val = float(numeric_series.min())
    max_val = float(numeric_series.max())
    q25 = float(numeric_series.quantile(0.25))
    q50 = float(numeric_series.quantile(0.50))
    q75 = float(numeric_series.quantile(0.75))

    return NumericStats(
        count=count,
        mean=round(mean_val, 4) if not np.isnan(mean_val) else None,
        median=round(median_val, 4) if not np.isnan(median_val) else None,
        std=round(std_val, 4) if not np.isnan(std_val) else None,
        min=round(min_val, 4) if not np.isnan(min_val) else None,
        max=round(max_val, 4) if not np.isnan(max_val) else None,
        q25=round(q25, 4) if not np.isnan(q25) else None,
        q50=round(q50, 4) if not np.isnan(q50) else None,
        q75=round(q75, 4) if not np.isnan(q75) else None,
    )


def calculate_text_stats(series: pd.Series) -> Optional[TextStats]:
    """Calculate character length metrics for text columns."""
    non_null = series.dropna().astype(str)
    if len(non_null) == 0:
        return None

    lengths = non_null.str.len()
    return TextStats(
        min_length=int(lengths.min()),
        max_length=int(lengths.max()),
        avg_length=round(float(lengths.mean()), 2),
    )


def calculate_categorical_stats(series: pd.Series, top_n: int = 5) -> Optional[CategoricalStats]:
    """Calculate frequency metrics for categorical columns."""
    non_null = series.dropna().astype(str)
    if len(non_null) == 0:
        return None

    total_count = len(non_null)
    value_counts = non_null.value_counts()
    num_categories = len(value_counts)

    top_categories: list[CategoryFrequency] = []
    for val, count in value_counts.head(top_n).items():
        top_categories.append(
            CategoryFrequency(
                value=str(val),
                count=int(count),
                percentage=round((int(count) / total_count) * 100, 2),
            )
        )

    return CategoricalStats(
        num_categories=num_categories,
        top_categories=top_categories,
    )


def profile_column(column_name: str, series: pd.Series, total_rows: int) -> ColumnProfile:
    """Generate a comprehensive profile for a single column."""
    total_values = len(series)
    missing_count = int(series.isna().sum())
    missing_percentage = round((missing_count / total_values * 100), 2) if total_values > 0 else 0.0
    non_null_count = total_values - missing_count
    unique_count = int(series.nunique(dropna=True))
    distinct_percentage = round((unique_count / non_null_count * 100), 2) if non_null_count > 0 else 0.0
    duplicate_count = max(0, non_null_count - unique_count)
    is_nullable = missing_count > 0

    # Inferred logical type
    inferred_type = infer_logical_type(series, total_rows)

    # Identifier heuristic
    identifier_info = detect_identifier(column_name, series, inferred_type, total_rows)

    # Min / Max values
    min_val: Any = None
    max_val: Any = None
    non_null = series.dropna()
    if len(non_null) > 0:
        try:
            if inferred_type in ["Integer", "Float"]:
                num_s = pd.to_numeric(non_null, errors="coerce").dropna()
                if len(num_s) > 0:
                    min_val = round(float(num_s.min()), 4)
                    max_val = round(float(num_s.max()), 4)
            elif inferred_type == "DateTime":
                dt_s = pd.to_datetime(non_null, errors="coerce").dropna()
                if len(dt_s) > 0:
                    min_val = str(dt_s.min())
                    max_val = str(dt_s.max())
            else:
                min_val = str(non_null.min())
                max_val = str(non_null.max())
        except Exception:
            pass

    # Type-specific auxiliary statistics
    numeric_stats: Optional[NumericStats] = None
    text_stats: Optional[TextStats] = None
    categorical_stats: Optional[CategoricalStats] = None

    if inferred_type in ["Integer", "Float"]:
        numeric_stats = calculate_numeric_stats(series)
    elif inferred_type in ["String/Text", "Categorical"]:
        text_stats = calculate_text_stats(series)

    if inferred_type == "Categorical" or (unique_count <= 10 and non_null_count > 0):
        categorical_stats = calculate_categorical_stats(series)

    return ColumnProfile(
        column_name=column_name,
        pandas_dtype=str(series.dtype),
        inferred_type=inferred_type,
        total_values=total_values,
        missing_count=missing_count,
        missing_percentage=missing_percentage,
        non_null_count=non_null_count,
        unique_count=unique_count,
        distinct_percentage=distinct_percentage,
        duplicate_count=duplicate_count,
        is_nullable=is_nullable,
        min_value=min_val,
        max_value=max_val,
        numeric_stats=numeric_stats,
        text_stats=text_stats,
        categorical_stats=categorical_stats,
        identifier_info=identifier_info,
    )


def profile_dataset(df: pd.DataFrame, filename: str) -> DatasetProfile:
    """
    Profile an entire dataset without altering underlying records.
    
    Generates dataset-level metrics and column profiles.
    """
    row_count = len(df)
    column_count = len(df.columns)

    if row_count == 0 or column_count == 0:
        return DatasetProfile(
            filename=filename,
            row_count=row_count,
            column_count=column_count,
            total_missing_values=0,
            overall_missing_percentage=0.0,
            duplicate_rows=0,
            duplicate_row_percentage=0.0,
            memory_usage_bytes=0,
            memory_usage_formatted="0 B",
            numeric_columns=0,
            text_columns=0,
            categorical_columns=0,
            datetime_columns=0,
            boolean_columns=0,
            identifier_columns=0,
            columns={},
        )

    total_cells = row_count * column_count
    total_missing = int(df.isna().sum().sum())
    overall_missing_pct = round((total_missing / total_cells * 100), 2) if total_cells > 0 else 0.0

    duplicate_rows = int(df.duplicated().sum())
    duplicate_row_pct = round((duplicate_rows / row_count * 100), 2) if row_count > 0 else 0.0

    memory_bytes = int(df.memory_usage(deep=True).sum())
    memory_formatted = format_memory_size(memory_bytes)

    # Column-level profiling
    columns_profile: dict[str, ColumnProfile] = {}
    type_counts = {
        "Integer": 0,
        "Float": 0,
        "Boolean": 0,
        "DateTime": 0,
        "Categorical": 0,
        "String/Text": 0,
    }
    identifier_count = 0

    for col_name in df.columns:
        col_prof = profile_column(str(col_name), df[col_name], row_count)
        columns_profile[str(col_name)] = col_prof
        type_counts[col_prof.inferred_type] = type_counts.get(col_prof.inferred_type, 0) + 1
        if col_prof.identifier_info and col_prof.identifier_info.likely_identifier:
            identifier_count += 1

    numeric_cols = type_counts.get("Integer", 0) + type_counts.get("Float", 0)

    return DatasetProfile(
        filename=filename,
        row_count=row_count,
        column_count=column_count,
        total_missing_values=total_missing,
        overall_missing_percentage=overall_missing_pct,
        duplicate_rows=duplicate_rows,
        duplicate_row_percentage=duplicate_row_pct,
        memory_usage_bytes=memory_bytes,
        memory_usage_formatted=memory_formatted,
        numeric_columns=numeric_cols,
        text_columns=type_counts.get("String/Text", 0),
        categorical_columns=type_counts.get("Categorical", 0),
        datetime_columns=type_counts.get("DateTime", 0),
        boolean_columns=type_counts.get("Boolean", 0),
        identifier_columns=identifier_count,
        columns=columns_profile,
    )


def log_profiling_event(
    profile: DatasetProfile,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
    log_dir: Path | str = "logs",
) -> None:
    """Record schema detection and profiling event to logs/profiling.log."""
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "profiling.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "SUCCESS":
        types_summary = ", ".join(
            f"{col}: {p.inferred_type}" for col, p in profile.columns.items()
        )
        detail = (
            f"Rows: {profile.row_count}, Cols: {profile.column_count} | "
            f"Types: [{types_summary}]"
        )
    else:
        detail = f"Error: {error_message}"

    log_entry = (
        f"[{timestamp}] [{status}] File: {profile.filename} | {detail}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def format_profiling_summary(profile: DatasetProfile) -> str:
    """Format a human-readable summary of the dataset profile for console display."""
    lines: list[str] = [
        f"Dataset: {profile.filename}",
        f"Dimensions: {profile.row_count:,} rows x {profile.column_count} columns",
        f"Memory Usage: {profile.memory_usage_formatted}",
        f"Missing Values: {profile.total_missing_values:,} ({profile.overall_missing_percentage}%)",
        f"Duplicate Rows: {profile.duplicate_rows:,} ({profile.duplicate_row_percentage}%)",
        f"Schema Summary: {profile.numeric_columns} Numeric, {profile.text_columns} Text, "
        f"{profile.categorical_columns} Categorical, {profile.datetime_columns} DateTime, "
        f"{profile.boolean_columns} Boolean | {profile.identifier_columns} Likely Identifier(s)",
        "\nColumn Inferences:",
    ]

    for col_name, col_prof in profile.columns.items():
        id_badge = " [IDENTIFIER]" if col_prof.identifier_info and col_prof.identifier_info.likely_identifier else ""
        null_badge = f", {col_prof.missing_count} nulls" if col_prof.is_nullable else ", non-nullable"
        lines.append(
            f"  * {col_name} ({col_prof.pandas_dtype}) -> {col_prof.inferred_type}"
            f"{id_badge} ({col_prof.unique_count} distinct{null_badge})"
        )

    return "\n".join(lines)


def save_profiling_reports(
    profiles: list[DatasetProfile], reports_dir: Path | str = "reports"
) -> tuple[Path, Path]:
    """
    Serialize profiling results to reports/profile_report.json and reports/profile_report.csv.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "profile_report.json"
    csv_path = out_dir / "profile_report.csv"

    # Convert dataclasses to dict
    json_data = [asdict(p) for p in profiles]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=str)

    # Flatten column profiles into CSV summary table
    csv_rows: list[dict[str, Any]] = []
    for p in profiles:
        for col_name, c in p.columns.items():
            csv_rows.append({
                "dataset": p.filename,
                "column_name": col_name,
                "pandas_dtype": c.pandas_dtype,
                "inferred_type": c.inferred_type,
                "total_values": c.total_values,
                "missing_count": c.missing_count,
                "missing_percentage": c.missing_percentage,
                "unique_count": c.unique_count,
                "distinct_percentage": c.distinct_percentage,
                "duplicate_count": c.duplicate_count,
                "is_nullable": c.is_nullable,
                "min_value": c.min_value,
                "max_value": c.max_value,
                "likely_identifier": c.identifier_info.likely_identifier if c.identifier_info else False,
                "identifier_confidence": c.identifier_info.confidence if c.identifier_info else 0.0,
            })

    csv_df = pd.DataFrame(csv_rows)
    csv_df.to_csv(csv_path, index=False)

    return json_path, csv_path
