"""
Universal Dashboard Generator Module.

Dynamically inspects processed datasets (from PostgreSQL or validated file cache),
classifies column roles, computes KPIs, and automatically selects optimal
visualizations (bar charts, line charts, histograms, category distributions, data tables)
aligned with the pipeline's Power BI semantic model and 4-zone dashboard layout.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Optional

import numpy as np
import pandas as pd
from sqlalchemy import inspect as sql_inspect, text
from sqlalchemy.engine import Engine

from pipeline.database import (
    generate_table_name,
    get_database_engine,
    get_database_url,
    load_project_env,
    mask_database_url,
    test_database_connection,
)
from pipeline.powerbi import (
    AnalyticalRole,
    PowerBIIntegrationEngine,
)


@dataclass
class ColumnRoleInfo:
    """Analytical metadata and role for a dataset column."""
    name: str
    data_type: str
    role: str
    is_metric: bool
    is_dimension: bool
    is_date: bool
    is_identifier: bool
    unique_count: int
    null_count: int


@dataclass
class KPICard:
    """Executive KPI card specification."""
    id: str
    title: str
    value: str | int | float
    raw_value: float | int
    subtitle: str
    card_type: str  # "metric", "count", "quality", "rejected"
    status: str = "NORMAL"  # "NORMAL", "HEALTHY", "WARNING", "DANGER"


@dataclass
class ChartSpec:
    """Chart specification for rendering in the web dashboard."""
    chart_id: str
    chart_type: str  # "bar", "line", "histogram", "doughnut", "table"
    title: str
    category_axis: str
    value_axis: str
    labels: list[str]
    datasets: list[dict[str, Any]]
    rationale: str
    confidence: float
    required_columns: list[str]


@dataclass
class DatasetDashboardPayload:
    """Complete structured payload for rendering an interactive dataset dashboard."""
    dataset_name: str
    table_name: str
    source: str  # "postgresql" or "file_cache"
    database_connected: bool
    database_status_message: str
    row_count: int
    column_count: int
    columns: list[dict[str, Any]]
    kpis: list[dict[str, Any]]
    charts: list[dict[str, Any]]
    records: list[dict[str, Any]]
    filters: dict[str, Any]
    quality_metrics: dict[str, Any]
    powerbi_info: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


def format_number(val: Any) -> str:
    """Format numbers into readable strings with commas or decimals."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "N/A"
    if isinstance(val, (int, np.integer)):
        return f"{val:,}"
    if isinstance(val, (float, np.floating)):
        if abs(val) >= 1_000_000:
            return f"{val / 1_000_000:.2f}M"
        if abs(val) >= 1_000:
            return f"{val:,.2f}"
        if val.is_integer():
            return f"{int(val):,}"
        return f"{val:.2f}"
    return str(val)


class DashboardGenerator:
    """
    Engine that inspects processed datasets, evaluates data quality reports,
    and dynamically produces interactive dashboard specifications.
    """

    def __init__(self, root_dir: Optional[Path | str] = None):
        self.root_dir = Path(root_dir).resolve() if root_dir else Path(__file__).resolve().parent.parent
        self.raw_dir = self.root_dir / "data" / "raw"
        self.validated_dir = self.root_dir / "data" / "validated"
        self.processed_dir = self.root_dir / "data" / "processed"
        self.rejected_dir = self.root_dir / "data" / "rejected"
        self.reports_dir = self.root_dir / "reports"
        self.powerbi_dir = self.root_dir / "powerbi"

        load_project_env(self.root_dir / ".env")
        self._engine: Optional[Engine] = None
        self._db_connected: Optional[bool] = None
        self._db_error: Optional[str] = None
        self._bi_engine = PowerBIIntegrationEngine(root_dir=self.root_dir)

    def get_database_status(self) -> tuple[bool, str]:
        """Check PostgreSQL connectivity and return safe status string."""
        if self._engine is None:
            self._engine, err = get_database_engine(dotenv_path=self.root_dir / ".env")
            if not self._engine:
                self._db_connected = False
                masked_url = mask_database_url(get_database_url(dotenv_path=self.root_dir / ".env"))
                self._db_error = f"Database unconfigured or initialization failed ({masked_url})"
                return False, self._db_error

        ok, test_err = test_database_connection(self._engine)
        self._db_connected = ok
        masked_url = mask_database_url(get_database_url(dotenv_path=self.root_dir / ".env"))
        if ok:
            return True, f"Connected to PostgreSQL ({masked_url})"
        self._db_error = f"PostgreSQL unavailable: {test_err or 'Connection refused'} ({masked_url})"
        return False, self._db_error

    def list_available_datasets(self) -> list[dict[str, Any]]:
        """
        Discover all datasets available in the pipeline across PostgreSQL,
        validated data directory, processed directory, and raw landing area.
        """
        datasets: dict[str, dict[str, Any]] = {}
        db_ok, _ = self.get_database_status()

        # 1. Discover PostgreSQL tables
        if db_ok and self._engine:
            try:
                inspector = sql_inspect(self._engine)
                for tbl in inspector.get_table_names(schema="public"):
                    if not tbl.startswith(("pg_", "sql_")):
                        datasets[tbl] = {
                            "name": tbl,
                            "display_name": tbl.replace("_", " ").title(),
                            "source": "postgresql",
                            "has_db": True,
                            "has_validated_file": False,
                        }
            except Exception:
                pass

        # 2. Discover validated CSV files
        if self.validated_dir.exists():
            for f in sorted(self.validated_dir.glob("*_validated.csv")):
                clean_name = f.stem.replace("_validated", "")
                table_name = generate_table_name(clean_name)
                if table_name in datasets:
                    datasets[table_name]["has_validated_file"] = True
                else:
                    datasets[table_name] = {
                        "name": table_name,
                        "display_name": clean_name.replace("_", " ").title(),
                        "source": "file_cache",
                        "has_db": False,
                        "has_validated_file": True,
                    }

        # 3. Discover raw files
        if self.raw_dir.exists():
            for f in sorted(self.raw_dir.iterdir()):
                if f.is_file() and not f.name.startswith(".") and f.name != ".gitkeep":
                    clean_name = f.stem
                    table_name = generate_table_name(clean_name)
                    if table_name not in datasets:
                        datasets[table_name] = {
                            "name": table_name,
                            "display_name": clean_name.replace("_", " ").title(),
                            "source": "raw",
                            "has_db": False,
                            "has_validated_file": False,
                        }

        return list(datasets.values())

    def load_dataset_dataframe(self, dataset_name: str) -> tuple[Optional[pd.DataFrame], str, list[str]]:
        """
        Load dataset into DataFrame from PostgreSQL (primary) or validated file (fallback).
        Returns: (df, source_type, warnings)
        """
        warnings: list[str] = []
        table_name = generate_table_name(dataset_name)
        db_ok, db_msg = self.get_database_status()

        # Try PostgreSQL first
        if db_ok and self._engine:
            try:
                inspector = sql_inspect(self._engine)
                all_tables = inspector.get_table_names(schema="public")
                if table_name in all_tables:
                    query = text(f'SELECT * FROM "public"."{table_name}" LIMIT 5000')
                    with self._engine.connect() as conn:
                        df = pd.read_sql_query(query, conn)
                    return df, "postgresql", warnings
            except Exception as e:
                warnings.append(f"PostgreSQL query for table '{table_name}' failed: {e}")

        # Fallback to validated CSV
        candidate_files = [
            self.validated_dir / f"{table_name}_validated.csv",
            self.validated_dir / f"{dataset_name}_validated.csv",
            self.processed_dir / f"{table_name}_cleaned.csv",
            self.processed_dir / f"{dataset_name}_cleaned.csv",
        ]
        # Check raw files if not yet processed
        for ext in [".csv", ".xlsx", ".json"]:
            candidate_files.append(self.raw_dir / f"{dataset_name}{ext}")
            candidate_files.append(self.raw_dir / f"{table_name}{ext}")

        for path in candidate_files:
            if path.exists() and path.is_file():
                try:
                    if path.suffix == ".csv":
                        df = pd.read_csv(path)
                    elif path.suffix == ".xlsx":
                        df = pd.read_excel(path)
                    elif path.suffix == ".json":
                        df = pd.read_json(path)
                    else:
                        continue

                    if not db_ok:
                        warnings.append("PostgreSQL is unavailable. Displaying data from validated local storage cache.")
                    else:
                        warnings.append(f"Table '{table_name}' not in PostgreSQL yet. Displaying data from file '{path.name}'.")
                    return df, "file_cache", warnings
                except Exception as e:
                    warnings.append(f"Failed to read file '{path.name}': {e}")

        return None, "unavailable", warnings

    def load_validation_metrics(self, dataset_name: str) -> dict[str, Any]:
        """Load quality and validation audit metrics from reports/validation_report.json."""
        default_metrics = {
            "status": "UNKNOWN",
            "quality_score": 100.0,
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "total_failures": 0,
            "null_violations": 0,
            "uniqueness_violations": 0,
            "range_violations": 0,
            "regex_violations": 0,
            "type_violations": 0,
            "schema_violations": 0,
            "rules_executed": [],
            "failures_by_rule": {},
            "failures_by_column": {},
            "record_failures": [],
            "rejected_records_count": 0,
        }

        val_file = self.reports_dir / "validation_report.json"
        if not val_file.exists():
            return default_metrics

        try:
            with open(val_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                target_stem = generate_table_name(dataset_name)
                for item in data:
                    src = item.get("source_filename", "")
                    src_stem = generate_table_name(Path(src).stem)
                    if src_stem == target_stem or src == dataset_name:
                        m = item.get("metrics", {})
                        total = m.get("total_rows", 0)
                        valid = m.get("valid_rows", 0)
                        invalid = m.get("invalid_rows", 0)
                        q_score = m.get("quality_score", 100.0 if total == 0 else (valid / total) * 100.0)

                        # Check rejected file rows if any
                        rej_path = item.get("rejected_output_path")
                        rej_count = invalid
                        if rej_path and Path(rej_path).exists():
                            try:
                                rej_df = pd.read_csv(rej_path)
                                rej_count = len(rej_df)
                            except Exception:
                                pass

                        return {
                            "status": item.get("status", "PASSED"),
                            "quality_score": round(float(q_score), 1),
                            "total_rows": total,
                            "valid_rows": valid,
                            "invalid_rows": invalid,
                            "total_failures": m.get("total_failures", 0),
                            "null_violations": m.get("null_violations", 0),
                            "uniqueness_violations": m.get("uniqueness_violations", 0),
                            "range_violations": m.get("range_violations", 0),
                            "regex_violations": m.get("regex_violations", 0),
                            "type_violations": m.get("type_violations", 0),
                            "schema_violations": m.get("schema_violations", 0),
                            "rules_executed": item.get("rules_executed", []),
                            "failures_by_rule": item.get("failures_by_rule", {}),
                            "failures_by_column": item.get("failures_by_column", {}),
                            "record_failures": item.get("record_failures", [])[:20],
                            "rejected_records_count": rej_count,
                        }
        except Exception:
            pass

        return default_metrics

    def load_powerbi_spec(self, table_name: str) -> dict[str, Any]:
        """Load recommendations and semantic model metadata from powerbi/ directory."""
        result = {
            "is_in_semantic_model": False,
            "table_archetype": "TABULAR",
            "measures": [],
            "recommendations": [],
            "disclaimer": (
                "Visual specifications derived from powerbi/dashboard_spec.json and semantic_model.json. "
                "Note: This is an automatically rendered web dashboard, not a native Microsoft Power BI (.pbix) file."
            ),
        }

        spec_file = self.powerbi_dir / "dashboard_spec.json"
        model_file = self.powerbi_dir / "semantic_model.json"

        if spec_file.exists():
            try:
                with open(spec_file, "r", encoding="utf-8") as f:
                    spec = json.load(f)
                recs = [r for r in spec.get("recommendations", []) if r.get("target_table") == table_name]
                result["recommendations"] = recs
            except Exception:
                pass

        if model_file.exists():
            try:
                with open(model_file, "r", encoding="utf-8") as f:
                    model = json.load(f)
                for tbl in model.get("tables", []):
                    if tbl.get("table_name") == table_name:
                        result["is_in_semantic_model"] = True
                        result["table_archetype"] = tbl.get("table_archetype", "TABULAR")
                        break
            except Exception:
                pass

        return result

    def classify_columns(self, df: pd.DataFrame) -> list[ColumnRoleInfo]:
        """Classify each DataFrame column into its analytical role."""
        classified: list[ColumnRoleInfo] = []

        for col in df.columns:
            series = df[col]
            clean_name = str(col).lower().strip()
            total_len = len(series)
            null_count = int(series.isna().sum())
            unique_count = int(series.nunique(dropna=True))

            # Attempt datetime inference if object or string
            is_date = False
            if pd.api.types.is_datetime64_any_dtype(series):
                is_date = True
            elif (series.dtype == object or pd.api.types.is_string_dtype(series) or str(series.dtype) in ["str", "string", "object"]) and total_len > 0:
                if any(k in clean_name for k in ["date", "time", "timestamp", "created_at", "updated_at", "recorded_at"]):
                    try:
                        pd.to_datetime(series.dropna().head(20), errors="raise")
                        is_date = True
                    except Exception:
                        is_date = False

            # Role classification using powerbi classification rules
            is_numeric = pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series) and not is_date
            is_bool = pd.api.types.is_bool_dtype(series)

            role = self._bi_engine.classify_column_role(
                col_name=str(col),
                col_type="TIMESTAMP" if is_date else str(series.dtype),
                is_pk=False,
                is_nullable=null_count > 0,
                unique_ratio=(unique_count / total_len) if total_len > 0 else 0.0,
            )

            is_date_final = is_date or role == AnalyticalRole.DATETIME_DIMENSION.value
            is_metric = role == AnalyticalRole.NUMERIC_METRIC.value
            is_dim = (role in [
                AnalyticalRole.CATEGORICAL_DIMENSION.value,
                AnalyticalRole.BOOLEAN_DIMENSION.value,
            ]) and not is_date_final
            is_id = role == AnalyticalRole.IDENTIFIER.value

            classified.append(
                ColumnRoleInfo(
                    name=str(col),
                    data_type=str(series.dtype),
                    role=role,
                    is_metric=is_metric,
                    is_dimension=is_dim,
                    is_date=is_date_final,
                    is_identifier=is_id,
                    unique_count=unique_count,
                    null_count=null_count,
                )
            )

        return classified

    def generate_kpi_cards(
        self,
        df: pd.DataFrame,
        cols: list[ColumnRoleInfo],
        val_metrics: dict[str, Any],
    ) -> list[KPICard]:
        """Generate executive KPI summary cards from actual dataset records."""
        kpis: list[KPICard] = []
        row_count = len(df)

        # 1. Total Records
        kpis.append(
            KPICard(
                id="total_records",
                title="Total Records",
                value=format_number(row_count),
                raw_value=row_count,
                subtitle="Active verified rows in dataset",
                card_type="count",
                status="NORMAL",
            )
        )

        # 2. Data Quality Score
        q_score = val_metrics.get("quality_score", 100.0)
        q_status = "HEALTHY" if q_score >= 95 else "WARNING" if q_score >= 80 else "DANGER"
        kpis.append(
            KPICard(
                id="quality_score",
                title="Data Quality Score",
                value=f"{q_score}%",
                raw_value=q_score,
                subtitle=f"{val_metrics.get('total_failures', 0)} rule violations detected",
                card_type="quality",
                status=q_status,
            )
        )

        # 3. Quarantined / Rejected Count
        rej_count = val_metrics.get("rejected_records_count", 0)
        rej_status = "HEALTHY" if rej_count == 0 else "WARNING"
        kpis.append(
            KPICard(
                id="quarantined_records",
                title="Quarantined Records",
                value=format_number(rej_count),
                raw_value=rej_count,
                subtitle="Quarantined into data/rejected/" if rej_count > 0 else "Zero invalid records quarantined",
                card_type="rejected",
                status=rej_status,
            )
        )

        # 4. Metric KPIs for numeric columns (up to 3 key numeric metrics)
        metrics = [c for c in cols if c.is_metric]
        for m in metrics[:3]:
            series = pd.to_numeric(df[m.name], errors="coerce").dropna()
            if not series.empty:
                s_sum = float(series.sum())
                s_avg = float(series.mean())
                s_min = float(series.min())
                s_max = float(series.max())
                clean_title = m.name.replace("_", " ").title()

                kpis.append(
                    KPICard(
                        id=f"kpi_{m.name}_sum",
                        title=f"Total {clean_title}",
                        value=format_number(s_sum),
                        raw_value=s_sum,
                        subtitle=f"Avg: {format_number(s_avg)} | Range: {format_number(s_min)} – {format_number(s_max)}",
                        card_type="metric",
                        status="NORMAL",
                    )
                )

        # 5. If no numeric metric, show distinct count of primary categorical dimension or identifier
        if not metrics:
            dims = [c for c in cols if c.is_dimension or c.is_identifier]
            for d in dims[:2]:
                u_cnt = d.unique_count
                clean_title = d.name.replace("_", " ").title()
                kpis.append(
                    KPICard(
                        id=f"kpi_{d.name}_distinct",
                        title=f"Distinct {clean_title}",
                        value=format_number(u_cnt),
                        raw_value=u_cnt,
                        subtitle=f"Unique values in {clean_title} dimension",
                        card_type="count",
                        status="NORMAL",
                    )
                )

        return kpis

    def generate_charts(
        self,
        df: pd.DataFrame,
        cols: list[ColumnRoleInfo],
        table_name: str,
    ) -> list[ChartSpec]:
        """
        Automatically select charts based on column roles:
        - categorical + numeric -> bar chart
        - date + numeric -> line chart
        - numeric distributions -> histogram
        - categorical columns -> category distribution (doughnut)
        - otherwise fallback gracefully
        """
        charts: list[ChartSpec] = []
        if df.empty:
            return charts

        metrics = [c for c in cols if c.is_metric]
        categories = [c for c in cols if c.is_dimension]
        dates = [c for c in cols if c.is_date]

        # 1. Bar Chart (Categorical Dimension + Numeric Metric)
        if categories and metrics:
            cat = categories[0]
            met = metrics[0]
            try:
                grouped = (
                    df[[cat.name, met.name]]
                    .dropna()
                    .groupby(cat.name)[met.name]
                    .sum()
                    .sort_values(ascending=False)
                    .head(15)
                )
                if not grouped.empty:
                    c_title = f"{met.name.replace('_', ' ').title()} by {cat.name.replace('_', ' ').title()}"
                    charts.append(
                        ChartSpec(
                            chart_id=f"chart_bar_{cat.name}_{met.name}",
                            chart_type="bar",
                            title=c_title,
                            category_axis=cat.name,
                            value_axis=met.name,
                            labels=[str(k) for k in grouped.index],
                            datasets=[
                                {
                                    "label": met.name.replace("_", " ").title(),
                                    "data": [float(v) for v in grouped.values],
                                    "backgroundColor": "rgba(99, 102, 241, 0.75)",
                                    "borderColor": "rgba(99, 102, 241, 1.0)",
                                    "borderWidth": 1.5,
                                }
                            ],
                            rationale=f"Auto-selected: Categorical Dimension ({cat.name}) × Numeric Metric ({met.name})",
                            confidence=0.92,
                            required_columns=[cat.name, met.name],
                        )
                    )
            except Exception:
                pass

        # 2. Line Chart (Date Dimension + Numeric Metric)
        if dates and metrics:
            d_col = dates[0]
            met = metrics[0]
            try:
                temp_df = df[[d_col.name, met.name]].dropna().copy()
                temp_df["parsed_date"] = pd.to_datetime(temp_df[d_col.name], errors="coerce")
                temp_df = temp_df.dropna(subset=["parsed_date"]).sort_values("parsed_date")

                if not temp_df.empty:
                    # Group by formatted date
                    temp_df["date_label"] = temp_df["parsed_date"].dt.strftime("%Y-%m-%d")
                    grouped = temp_df.groupby("date_label")[met.name].sum()

                    c_title = f"{met.name.replace('_', ' ').title()} Trend over {d_col.name.replace('_', ' ').title()}"
                    charts.append(
                        ChartSpec(
                            chart_id=f"chart_line_{d_col.name}_{met.name}",
                            chart_type="line",
                            title=c_title,
                            category_axis=d_col.name,
                            value_axis=met.name,
                            labels=[str(k) for k in grouped.index],
                            datasets=[
                                {
                                    "label": met.name.replace("_", " ").title(),
                                    "data": [float(v) for v in grouped.values],
                                    "fill": True,
                                    "backgroundColor": "rgba(16, 185, 129, 0.15)",
                                    "borderColor": "rgba(16, 185, 129, 1.0)",
                                    "tension": 0.3,
                                    "borderWidth": 2,
                                }
                            ],
                            rationale=f"Auto-selected: Chronological Trend ({d_col.name}) × Numeric Metric ({met.name})",
                            confidence=0.95,
                            required_columns=[d_col.name, met.name],
                        )
                    )
            except Exception:
                pass

        # 3. Numeric Distribution Histogram (Continuous Metric)
        if metrics:
            hist_met = metrics[0]
            try:
                num_vals = pd.to_numeric(df[hist_met.name], errors="coerce").dropna().values
                if len(num_vals) > 0:
                    num_bins = min(8, max(3, len(set(num_vals))))
                    counts, bin_edges = np.histogram(num_vals, bins=num_bins)
                    bin_labels = [
                        f"{format_number(bin_edges[i])} - {format_number(bin_edges[i+1])}"
                        for i in range(len(counts))
                    ]

                    c_title = f"{hist_met.name.replace('_', ' ').title()} Frequency Distribution"
                    charts.append(
                        ChartSpec(
                            chart_id=f"chart_hist_{hist_met.name}",
                            chart_type="histogram",
                            title=c_title,
                            category_axis="bin_range",
                            value_axis="frequency",
                            labels=bin_labels,
                            datasets=[
                                {
                                    "label": f"{hist_met.name.replace('_', ' ').title()} Frequency",
                                    "data": [int(c) for c in counts],
                                    "backgroundColor": "rgba(245, 158, 11, 0.75)",
                                    "borderColor": "rgba(245, 158, 11, 1.0)",
                                    "borderWidth": 1.5,
                                }
                            ],
                            rationale=f"Auto-selected: Continuous Numeric Distribution ({hist_met.name})",
                            confidence=0.88,
                            required_columns=[hist_met.name],
                        )
                    )
            except Exception:
                pass

        # 4. Categorical Proportions (Doughnut / Pie Chart)
        if categories:
            cat = categories[0]
            try:
                val_counts = df[cat.name].value_counts().head(8)
                if not val_counts.empty:
                    palette = [
                        "rgba(99, 102, 241, 0.8)",
                        "rgba(16, 185, 129, 0.8)",
                        "rgba(245, 158, 11, 0.8)",
                        "rgba(239, 68, 68, 0.8)",
                        "rgba(168, 85, 247, 0.8)",
                        "rgba(14, 165, 233, 0.8)",
                        "rgba(236, 72, 153, 0.8)",
                        "rgba(107, 114, 128, 0.8)",
                    ]
                    c_title = f"{cat.name.replace('_', ' ').title()} Composition"
                    charts.append(
                        ChartSpec(
                            chart_id=f"chart_cat_{cat.name}",
                            chart_type="doughnut",
                            title=c_title,
                            category_axis=cat.name,
                            value_axis="count",
                            labels=[str(k) for k in val_counts.index],
                            datasets=[
                                {
                                    "label": "Record Count",
                                    "data": [int(v) for v in val_counts.values],
                                    "backgroundColor": palette[: len(val_counts)],
                                    "borderWidth": 2,
                                }
                            ],
                            rationale=f"Auto-selected: Categorical Proportions ({cat.name})",
                            confidence=0.85,
                            required_columns=[cat.name],
                        )
                    )
            except Exception:
                pass

        return charts

    def generate_filter_options(
        self, df: pd.DataFrame, cols: list[ColumnRoleInfo]
    ) -> dict[str, Any]:
        """Extract unique values for categorical filters and bounds for date filters."""
        filters: dict[str, Any] = {
            "categories": {},
            "dates": {},
        }
        if df.empty:
            return filters

        # Categorical columns with cardinality between 1 and 50
        for c in cols:
            if c.is_dimension and 1 <= c.unique_count <= 50:
                unique_vals = sorted([str(x) for x in df[c.name].dropna().unique().tolist()])
                filters["categories"][c.name] = unique_vals

        # Date columns
        for c in cols:
            if c.is_date:
                try:
                    s_date = pd.to_datetime(df[c.name], errors="coerce").dropna()
                    if not s_date.empty:
                        filters["dates"][c.name] = {
                            "min": s_date.min().strftime("%Y-%m-%d"),
                            "max": s_date.max().strftime("%Y-%m-%d"),
                        }
                except Exception:
                    pass

        return filters

    def build_dashboard_payload(self, dataset_name: str) -> DatasetDashboardPayload:
        """
        Build the complete interactive dashboard payload for the specified dataset.
        Handles empty data, invalid schemas, missing columns, and database unavailability.
        """
        table_name = generate_table_name(dataset_name)
        db_ok, db_msg = self.get_database_status()

        # Load dataset
        df, source_type, warnings = self.load_dataset_dataframe(dataset_name)

        if df is None:
            # Dataset could not be loaded at all
            warnings.append(f"No processed data found for dataset '{dataset_name}'. Please run the pipeline.")
            val_metrics = self.load_validation_metrics(dataset_name)
            pbi_spec = self.load_powerbi_spec(table_name)
            return DatasetDashboardPayload(
                dataset_name=dataset_name,
                table_name=table_name,
                source="unavailable",
                database_connected=db_ok,
                database_status_message=db_msg,
                row_count=0,
                column_count=0,
                columns=[],
                kpis=[
                    asdict(KPICard("total_records", "Total Records", 0, 0, "No records loaded", "count", "WARNING")),
                    asdict(KPICard("quality_score", "Quality Score", "N/A", 0, "No data to validate", "quality", "WARNING")),
                    asdict(KPICard("quarantined", "Quarantined", 0, 0, "No quarantine data", "rejected", "NORMAL")),
                ],
                charts=[],
                records=[],
                filters={"categories": {}, "dates": {}},
                quality_metrics=val_metrics,
                powerbi_info=pbi_spec,
                warnings=warnings,
            )

        # Dataset loaded (can be empty)
        row_count = len(df)
        col_count = len(df.columns)
        if row_count == 0:
            warnings.append("Dataset contains 0 records.")

        # Classify columns
        cols = self.classify_columns(df)
        val_metrics = self.load_validation_metrics(dataset_name)
        pbi_spec = self.load_powerbi_spec(table_name)

        # Generate KPIs, Charts, Filters
        kpi_cards = self.generate_kpi_cards(df, cols, val_metrics)
        chart_specs = self.generate_charts(df, cols, table_name)
        filter_options = self.generate_filter_options(df, cols)

        # Format records for data table (limit to 1000 for web responsiveness)
        clean_df = df.head(1000).replace({np.nan: None})
        records_list = clean_df.to_dict(orient="records")

        # Serialized columns
        cols_serialized = [
            {
                "name": c.name,
                "data_type": c.data_type,
                "role": c.role,
                "is_metric": c.is_metric,
                "is_dimension": c.is_dimension,
                "is_date": c.is_date,
                "is_identifier": c.is_identifier,
                "unique_count": c.unique_count,
                "null_count": c.null_count,
            }
            for c in cols
        ]

        return DatasetDashboardPayload(
            dataset_name=dataset_name,
            table_name=table_name,
            source=source_type,
            database_connected=db_ok,
            database_status_message=db_msg,
            row_count=row_count,
            column_count=col_count,
            columns=cols_serialized,
            kpis=[asdict(k) for k in kpi_cards],
            charts=[asdict(c) for c in chart_specs],
            records=records_list,
            filters=filter_options,
            quality_metrics=val_metrics,
            powerbi_info=pbi_spec,
            warnings=warnings,
        )
