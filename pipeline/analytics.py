"""
Universal SQL Analytics Layer Module (Stage 9).

Provides reusable, domain-agnostic analytical query execution against dynamically
modeled relational database tables (PostgreSQL and SQLAlchemy-compatible engines).

Key Capabilities:
- Table metadata inspection & dynamic discovery
- Basic summary (row counts, column counts)
- Numeric aggregations (COUNT, SUM, AVG, MIN, MAX)
- Group-by & categorical frequency distributions
- Parameterized filtering (bound parameters, zero SQL injection)
- Time-series and date trend analysis (day, week, month, quarter, year)
- Window functions (RANK, DENSE_RANK, ROW_NUMBER)
- Cumulative running totals
- Top-N ranking analysis
- SQL-level data quality audits (null percentages, duplicates, distinct counts)
- Reusable SQL template execution from sql/analytics/
- Result serialization and export to reports/analytics/ (JSON and CSV)
- Analytics summary reports (reports/analytics_report.json, .csv)
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Optional

import pandas as pd
from sqlalchemy import Engine, inspect, text

from pipeline.database import (
    get_database_engine,
    get_database_url,
    inspect_table_schema,
    load_project_env,
    mask_database_url,
    test_database_connection,
)

# Regex enforcing safe SQL identifiers (tables, columns, alias names)
IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_IDENTIFIER_LENGTH = 63


@dataclass
class AnalyticsConfig:
    """Configuration settings for SQL Analytics Layer."""
    enabled: bool = True
    default_limit: int = 1000
    max_limit: int = 10000
    export_formats: list[str] = field(default_factory=lambda: ["json", "csv"])
    allowed_aggregations: list[str] = field(
        default_factory=lambda: ["COUNT", "SUM", "AVG", "MIN", "MAX"]
    )
    allowed_grains: list[str] = field(
        default_factory=lambda: ["day", "week", "month", "quarter", "year"]
    )
    allowed_ranking_methods: list[str] = field(
        default_factory=lambda: ["RANK", "DENSE_RANK", "ROW_NUMBER"]
    )


@dataclass
class QueryResult:
    """Structured execution outcome of an analytical SQL query."""
    query_name: str
    table_name: str
    status: str  # "SUCCESS", "FAILED", "DATABASE_UNAVAILABLE", "INVALID_IDENTIFIER", "CONFIG_ERROR"
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    execution_time_seconds: float = 0.0
    sql: Optional[str] = None
    output_files: dict[str, str] = field(default_factory=dict)
    error_message: Optional[str] = None

    def to_dataframe(self) -> pd.DataFrame:
        """Convert rows to a Pandas DataFrame."""
        if not self.rows:
            return pd.DataFrame(columns=self.columns)
        return pd.DataFrame(self.rows)


@dataclass
class AnalyticsExecutionRecord:
    """Audit summary record for an individual analytical query."""
    run_id: str
    query_name: str
    table: str
    status: str
    execution_time: float
    result_rows: int
    output_files: list[str] = field(default_factory=list)
    error_message: Optional[str] = None


def is_safe_identifier(name: str) -> bool:
    """Verify that an identifier contains only alphanumeric characters and underscores."""
    if not isinstance(name, str):
        return False
    name = name.strip()
    if not name or len(name) > MAX_IDENTIFIER_LENGTH:
        return False
    return bool(IDENTIFIER_REGEX.match(name))


def validate_identifier(name: str, label: str = "identifier") -> str:
    """
    Validate identifier string against safe pattern.
    
    Raises ValueError on unsafe or malicious inputs.
    """
    if not is_safe_identifier(name):
        raise ValueError(
            f"Invalid or unsafe SQL {label}: '{name}'. Must match ^[A-Za-z_][A-Za-z0-9_]*$ (max {MAX_IDENTIFIER_LENGTH} chars)."
        )
    return name.strip()


def quote_identifier(name: str) -> str:
    """Wrap a validated identifier in double quotes for SQL standard escaping."""
    clean = validate_identifier(name)
    return f'"{clean}"'


def sanitize_text(text_val: Optional[str]) -> Optional[str]:
    """Sanitize strings to mask passwords and secrets."""
    if not text_val:
        return text_val
    clean = re.sub(r"://([^:@\s]+):([^@\s]+)@", r"://\1:***@", str(text_val))
    return clean.strip()


def load_analytics_config(
    config_path: Path | str = "config/analytics.json",
) -> tuple[AnalyticsConfig, Optional[str]]:
    """
    Load analytics settings from JSON configuration.
    
    Returns (AnalyticsConfig, None) on success or (default_config, error_reason).
    """
    path = Path(config_path)
    if not path.exists():
        return AnalyticsConfig(), None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        allowed_aggs = [
            str(a).upper().strip()
            for a in data.get("allowed_aggregations", ["COUNT", "SUM", "AVG", "MIN", "MAX"])
        ]
        allowed_grains = [
            str(g).lower().strip()
            for g in data.get("allowed_grains", ["day", "week", "month", "quarter", "year"])
        ]
        allowed_rankings = [
            str(r).upper().strip()
            for r in data.get("allowed_ranking_methods", ["RANK", "DENSE_RANK", "ROW_NUMBER"])
        ]

        cfg = AnalyticsConfig(
            enabled=bool(data.get("enabled", True)),
            default_limit=int(data.get("default_limit", 1000)),
            max_limit=int(data.get("max_limit", 10000)),
            export_formats=list(data.get("export_formats", ["json", "csv"])),
            allowed_aggregations=allowed_aggs,
            allowed_grains=allowed_grains,
            allowed_ranking_methods=allowed_rankings,
        )
        return cfg, None

    except json.JSONDecodeError as e:
        return AnalyticsConfig(), f"Corrupted JSON in {path}: {e}"
    except Exception as e:
        return AnalyticsConfig(), f"Error loading {path}: {e}"


class SQLAnalyticsEngine:
    """
    Reusable, domain-agnostic SQL analytical query engine.
    
    Executes parameterized aggregations, window functions, time-series analysis,
    and data-quality audits against relational database tables.
    """

    def __init__(
        self,
        engine: Optional[Engine] = None,
        config: Optional[AnalyticsConfig] = None,
        queries_dir: Path | str = "sql/analytics",
        schema: str = "public",
        root_dir: Optional[Path | str] = None,
    ):
        self.engine = engine
        self.config = config or AnalyticsConfig()
        self.queries_dir = Path(queries_dir)
        self.schema = schema
        self.root_dir = Path(root_dir).resolve() if root_dir else self.queries_dir.resolve().parent.parent
        self._connected: Optional[bool] = None
        self._connection_error: Optional[str] = None

    def connect(self) -> tuple[bool, Optional[str]]:
        """
        Verify database engine connectivity.
        
        Returns (is_connected, error_message).
        """
        if self.engine is None:
            dotenv_path = self.root_dir / ".env" if self.root_dir else None
            self.engine, err = get_database_engine(dotenv_path=dotenv_path)
            if self.engine is None:
                self._connected = False
                configured = get_database_url(dotenv_path=dotenv_path) is not None
                if configured:
                    self._connection_error = err or "Failed to initialize database engine"
                else:
                    self._connection_error = "DATABASE_URL not configured in environment"
                return False, self._connection_error

        connected, conn_err = test_database_connection(self.engine)
        self._connected = connected
        self._connection_error = conn_err
        return connected, conn_err

    def list_tables(self) -> list[str]:
        """Retrieve list of existing table names in current schema."""
        if not self.engine:
            conn, _ = self.connect()
            if not conn:
                return []

        try:
            inspector = inspect(self.engine)
            target_schema = self.schema if self.engine.dialect.name == "postgresql" else None
            return inspector.get_table_names(schema=target_schema)
        except Exception:
            return []

    def _check_connection_or_return_unavailable(self, query_name: str, table_name: str) -> Optional[QueryResult]:
        """Check if database is connected; if not, return QueryResult with DATABASE_UNAVAILABLE."""
        conn, err = self.connect()
        if not conn:
            return QueryResult(
                query_name=query_name,
                table_name=table_name,
                status="DATABASE_UNAVAILABLE",
                error_message=sanitize_text(f"Database connection unavailable: {err}"),
            )
        return None

    def validate_table_name(self, table_name: str) -> bool:
        """Validate table name identifier syntax and database existence."""
        validate_identifier(table_name, "table_name")
        conn, _ = self.connect()
        if conn and self.engine:
            schema_info = self.get_table_schema(table_name)
            if not schema_info.get("exists", False):
                raise ValueError(f"No such table '{table_name}'")
        return True

    def validate_columns(self, table_name: str, columns: list[str]) -> bool:
        """Validate column identifier syntax and existence within target table."""
        self.validate_table_name(table_name)
        conn, _ = self.connect()
        if conn and self.engine:
            schema_info = self.get_table_schema(table_name)
            existing_cols = {c["name"] for c in schema_info.get("columns", [])}
            for col in columns:
                validate_identifier(col, "column")
                if col not in existing_cols:
                    raise ValueError(f"No such column '{col}' in table '{table_name}'")
        else:
            for col in columns:
                validate_identifier(col, "column")
        return True

    def execute_parameterized_query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        query_name: str = "parameterized_query",
        table_name: Optional[str] = None,
    ) -> QueryResult:
        """Execute parameterized query safely via SQLAlchemy text binding."""
        return self.execute_query(sql=sql, params=params, query_name=query_name, table_name=table_name)

    def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """
        Inspect table structural metadata and row counts.
        """
        validate_identifier(table_name, "table_name")
        if not self.engine:
            conn, _ = self.connect()
            if not conn:
                return {
                    "table_name": table_name,
                    "exists": False,
                    "columns": [],
                    "primary_keys": [],
                    "row_count": 0,
                }

        return inspect_table_schema(self.engine, table_name, schema=self.schema)

    def get_table_row_count(self, table_name: str) -> int:
        """Retrieve exact row count for a table."""
        validate_identifier(table_name, "table_name")
        res = self.execute_query(
            sql=f"SELECT COUNT(*) AS total_rows FROM {self._format_table_ref(table_name)}",
            query_name="row_count",
            table_name=table_name,
        )
        if res.status == "SUCCESS" and res.rows:
            return int(res.rows[0].get("total_rows", 0))
        return 0

    def _format_table_ref(self, table_name: str) -> str:
        """Format table reference with optional schema qualification."""
        clean_tbl = quote_identifier(table_name)
        if self.engine and self.engine.dialect.name == "postgresql" and self.schema:
            clean_schema = quote_identifier(self.schema)
            return f"{clean_schema}.{clean_tbl}"
        return clean_tbl

    def _get_date_trunc_expression(self, date_col: str, grain: str) -> str:
        """
        Generate dialect-specific SQL expression for date truncation / bucketing.
        
        Supports PostgreSQL and SQLite.
        """
        col_quoted = quote_identifier(date_col)
        grain_clean = grain.lower().strip()
        dialect = self.engine.dialect.name if self.engine else "postgresql"

        if dialect == "postgresql":
            # PostgreSQL: DATE_TRUNC('month', col)::DATE
            return f"CAST(DATE_TRUNC('{grain_clean}', CAST({col_quoted} AS TIMESTAMP)) AS DATE)"
        else:
            # SQLite fallback expressions
            if grain_clean == "day":
                return f"strftime('%Y-%m-%d', {col_quoted})"
            elif grain_clean == "week":
                return f"strftime('%Y-W%W', {col_quoted})"
            elif grain_clean == "month":
                return f"strftime('%Y-%m', {col_quoted})"
            elif grain_clean == "year":
                return f"strftime('%Y', {col_quoted})"
            elif grain_clean == "quarter":
                return (
                    f"strftime('%Y-', {col_quoted}) || 'Q' || "
                    f"((CAST(strftime('%m', {col_quoted}) AS INTEGER) + 2) / 3)"
                )
            else:
                return f"strftime('%Y-%m-%d', {col_quoted})"

    def execute_query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        query_name: str = "custom_query",
        table_name: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute parameterized SQL query safely via SQLAlchemy text binding.
        
        Returns structured QueryResult with timing, columns, and rows.
        """
        t_start = time.monotonic()
        tbl_label = table_name or "unknown_table"

        # Verify connectivity
        conn, conn_err = self.connect()
        if not conn:
            duration = round(time.monotonic() - t_start, 4)
            return QueryResult(
                query_name=query_name,
                table_name=tbl_label,
                status="DATABASE_UNAVAILABLE",
                execution_time_seconds=duration,
                sql=sql,
                error_message=sanitize_text(f"Database connection unavailable: {conn_err}"),
            )

        bound_params = params or {}

        try:
            with self.engine.connect() as connection:
                statement = text(sql)
                cursor = connection.execute(statement, bound_params)

                columns = list(cursor.keys()) if cursor.returns_rows else []
                raw_rows = cursor.fetchall() if cursor.returns_rows else []

                # Convert rows to serializable dictionary format
                serialized_rows = []
                for row in raw_rows:
                    row_dict = {}
                    for col_name, val in zip(columns, row):
                        if isinstance(val, (datetime, pd.Timestamp)):
                            row_dict[col_name] = val.isoformat()
                        elif hasattr(val, "as_uuid"):
                            row_dict[col_name] = str(val)
                        elif isinstance(val, float) and (pd.isna(val) or val != val):
                            row_dict[col_name] = None
                        else:
                            row_dict[col_name] = val
                    serialized_rows.append(row_dict)

                duration = round(time.monotonic() - t_start, 4)
                return QueryResult(
                    query_name=query_name,
                    table_name=tbl_label,
                    status="SUCCESS",
                    columns=columns,
                    rows=serialized_rows,
                    row_count=len(serialized_rows),
                    execution_time_seconds=duration,
                    sql=sql,
                )

        except Exception as e:
            duration = round(time.monotonic() - t_start, 4)
            clean_err = sanitize_text(str(e))
            return QueryResult(
                query_name=query_name,
                table_name=tbl_label,
                status="FAILED",
                execution_time_seconds=duration,
                sql=sql,
                error_message=clean_err,
            )

    def aggregate(
        self,
        table_name: str,
        column: str,
        functions: Optional[list[str]] = None,
        filter_condition: Optional[str] = None,
        filter_params: Optional[dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Execute numeric aggregation on a specified column.
        
        Supported functions: COUNT, SUM, AVG, MIN, MAX.
        """
        validate_identifier(table_name, "table_name")
        validate_identifier(column, "column")

        funcs = functions or ["COUNT", "SUM", "AVG", "MIN", "MAX"]
        for fn in funcs:
            fn_upper = fn.upper().strip()
            if fn_upper not in self.config.allowed_aggregations:
                raise ValueError(
                    f"Unsupported aggregation '{fn}'. Must be one of {self.config.allowed_aggregations}"
                )

        conn_check = self._check_connection_or_return_unavailable("numeric_aggregation", table_name)
        if conn_check:
            return conn_check

        try:
            self.validate_columns(table_name, [column])
        except ValueError as err:
            return QueryResult(
                query_name="numeric_aggregation",
                table_name=table_name,
                status="FAILED",
                error_message=str(err),
            )

        agg_exprs = []
        for fn in funcs:
            fn_upper = fn.upper().strip()
            alias = f"{fn_upper.lower()}_{column}"
            col_q = quote_identifier(column)
            if fn_upper in ["AVG", "SUM"]:
                agg_exprs.append(f"ROUND(CAST({fn_upper}({col_q}) AS NUMERIC), 4) AS {quote_identifier(alias)}")
            else:
                agg_exprs.append(f"{fn_upper}({col_q}) AS {quote_identifier(alias)}")

        table_ref = self._format_table_ref(table_name)
        sql = f"SELECT\n    " + ",\n    ".join(agg_exprs) + f"\nFROM {table_ref}"

        params: dict[str, Any] = {}
        if filter_condition:
            sql += f"\nWHERE {filter_condition}"
            if filter_params:
                params.update(filter_params)

        return self.execute_query(sql, params=params, query_name="numeric_aggregation", table_name=table_name)

    def group_by(
        self,
        table_name: str,
        group_column: str,
        metric_column: Optional[str] = None,
        aggregation: str = "COUNT",
        filter_condition: Optional[str] = None,
        filter_params: Optional[dict[str, Any]] = None,
        order_desc: bool = True,
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Perform grouped aggregation with ordering and limit.
        """
        validate_identifier(table_name, "table_name")
        validate_identifier(group_column, "group_column")

        agg_upper = aggregation.upper().strip()
        if agg_upper not in self.config.allowed_aggregations:
            raise ValueError(
                f"Unsupported aggregation '{aggregation}'. Allowed: {self.config.allowed_aggregations}"
            )

        conn_check = self._check_connection_or_return_unavailable("grouped_aggregation", table_name)
        if conn_check:
            return conn_check

        cols_to_check = [group_column]
        if metric_column:
            validate_identifier(metric_column, "metric_column")
            cols_to_check.append(metric_column)

        try:
            self.validate_columns(table_name, cols_to_check)
        except ValueError as err:
            return QueryResult(
                query_name="grouped_aggregation",
                table_name=table_name,
                status="FAILED",
                error_message=str(err),
            )

        grp_q = quote_identifier(group_column)
        if metric_column:
            met_q = quote_identifier(metric_column)
            metric_expr = f"{agg_upper}({met_q})"
            metric_alias = f"{agg_upper.lower()}_{metric_column}"
        else:
            metric_expr = "COUNT(*)"
            metric_alias = "record_count"

        table_ref = self._format_table_ref(table_name)
        sql = (
            f"SELECT\n"
            f"    {grp_q} AS {quote_identifier(group_column)},\n"
            f"    {metric_expr} AS {quote_identifier(metric_alias)}\n"
            f"FROM {table_ref}"
        )

        params: dict[str, Any] = {}
        if filter_condition:
            sql += f"\nWHERE {filter_condition}"
            if filter_params:
                params.update(filter_params)

        order_dir = "DESC" if order_desc else "ASC"
        sql += f"\nGROUP BY {grp_q}\nORDER BY {quote_identifier(metric_alias)} {order_dir}"

        eff_limit = min(limit or self.config.default_limit, self.config.max_limit)
        sql += "\nLIMIT :limit_n"
        params["limit_n"] = eff_limit

        return self.execute_query(sql, params=params, query_name="grouped_aggregation", table_name=table_name)

    def date_trend(
        self,
        table_name: str,
        date_column: str,
        metric_column: Optional[str] = None,
        aggregation: str = "COUNT",
        grain: str = "month",
        filter_condition: Optional[str] = None,
        filter_params: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Perform time-series trend analysis bucketed by temporal grain (day, month, year, etc.).
        """
        validate_identifier(table_name, "table_name")
        validate_identifier(date_column, "date_column")

        grain_clean = grain.lower().strip()
        if grain_clean not in self.config.allowed_grains:
            raise ValueError(f"Unsupported temporal grain '{grain}'. Allowed: {self.config.allowed_grains}")

        agg_upper = aggregation.upper().strip()
        if agg_upper not in self.config.allowed_aggregations:
            raise ValueError(f"Unsupported aggregation '{aggregation}'. Allowed: {self.config.allowed_aggregations}")

        conn_check = self._check_connection_or_return_unavailable("date_trend", table_name)
        if conn_check:
            return conn_check

        cols_to_check = [date_column]
        if metric_column:
            validate_identifier(metric_column, "metric_column")
            cols_to_check.append(metric_column)

        try:
            self.validate_columns(table_name, cols_to_check)
        except ValueError as err:
            return QueryResult(
                query_name="date_trend",
                table_name=table_name,
                status="FAILED",
                error_message=str(err),
            )

        date_trunc_expr = self._get_date_trunc_expression(date_column, grain_clean)

        if metric_column:
            met_q = quote_identifier(metric_column)
            metric_select = f"{agg_upper}({met_q}) AS {quote_identifier('metric_value')}"
        else:
            metric_select = "COUNT(*) AS record_count"

        table_ref = self._format_table_ref(table_name)
        date_q = quote_identifier(date_column)

        sql = (
            f"SELECT\n"
            f"    {date_trunc_expr} AS time_bucket,\n"
            f"    {metric_select},\n"
            f"    COUNT(*) AS total_records\n"
            f"FROM {table_ref}\n"
            f"WHERE {date_q} IS NOT NULL"
        )

        params: dict[str, Any] = {}
        if filter_condition:
            sql += f" AND ({filter_condition})"
            if filter_params:
                params.update(filter_params)

        sql += f"\nGROUP BY {date_trunc_expr}\nORDER BY time_bucket ASC"

        eff_limit = min(limit or self.config.default_limit, self.config.max_limit)
        sql += "\nLIMIT :limit_n"
        params["limit_n"] = eff_limit

        return self.execute_query(sql, params=params, query_name="date_trend", table_name=table_name)

    def rank_rows(
        self,
        table_name: str,
        order_by_column: str,
        partition_by_column: Optional[str] = None,
        method: str = "RANK",
        direction: str = "DESC",
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute window function ranking (RANK, DENSE_RANK, ROW_NUMBER).
        """
        validate_identifier(table_name, "table_name")
        validate_identifier(order_by_column, "order_by_column")

        method_upper = method.upper().strip()
        if method_upper not in self.config.allowed_ranking_methods:
            raise ValueError(
                f"Unsupported ranking method '{method}'. Allowed: {self.config.allowed_ranking_methods}"
            )

        conn_check = self._check_connection_or_return_unavailable(f"{method_upper.lower()}_ranking", table_name)
        if conn_check:
            return conn_check

        cols_to_check = [order_by_column]
        if partition_by_column:
            validate_identifier(partition_by_column, "partition_by_column")
            cols_to_check.append(partition_by_column)

        try:
            self.validate_columns(table_name, cols_to_check)
        except ValueError as err:
            return QueryResult(
                query_name=f"{method_upper.lower()}_ranking",
                table_name=table_name,
                status="FAILED",
                error_message=str(err),
            )

        dir_clean = "DESC" if direction.upper().strip() == "DESC" else "ASC"

        partition_clause = ""
        if partition_by_column:
            partition_clause = f"PARTITION BY {quote_identifier(partition_by_column)}"

        order_clause = f"ORDER BY {quote_identifier(order_by_column)} {dir_clean}"
        table_ref = self._format_table_ref(table_name)

        sql = (
            f"SELECT\n"
            f"    *,\n"
            f"    {method_upper}() OVER (\n"
            f"        {partition_clause}\n"
            f"        {order_clause}\n"
            f"    ) AS calculated_rank\n"
            f"FROM {table_ref}\n"
            f"ORDER BY calculated_rank ASC"
        )

        eff_limit = min(limit or self.config.default_limit, self.config.max_limit)
        sql += "\nLIMIT :limit_n"
        params = {"limit_n": eff_limit}

        return self.execute_query(sql, params=params, query_name=f"{method_upper.lower()}_ranking", table_name=table_name)

    def running_total(
        self,
        table_name: str,
        order_by_column: str,
        metric_column: str,
        partition_by_column: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute cumulative running total using SUM() OVER (ORDER BY ...).
        """
        validate_identifier(table_name, "table_name")
        validate_identifier(order_by_column, "order_by_column")
        validate_identifier(metric_column, "metric_column")

        conn_check = self._check_connection_or_return_unavailable("running_total", table_name)
        if conn_check:
            return conn_check

        cols_to_check = [order_by_column, metric_column]
        if partition_by_column:
            validate_identifier(partition_by_column, "partition_by_column")
            cols_to_check.append(partition_by_column)

        try:
            self.validate_columns(table_name, cols_to_check)
        except ValueError as err:
            return QueryResult(
                query_name="running_total",
                table_name=table_name,
                status="FAILED",
                error_message=str(err),
            )

        partition_clause = ""
        if partition_by_column:
            partition_clause = f"PARTITION BY {quote_identifier(partition_by_column)}"

        table_ref = self._format_table_ref(table_name)
        ord_q = quote_identifier(order_by_column)
        met_q = quote_identifier(metric_column)

        sql = (
            f"SELECT\n"
            f"    {ord_q},\n"
            f"    {met_q},\n"
            f"    SUM({met_q}) OVER (\n"
            f"        {partition_clause}\n"
            f"        ORDER BY {ord_q} ASC\n"
            f"        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
            f"    ) AS running_total\n"
            f"FROM {table_ref}\n"
            f"WHERE {met_q} IS NOT NULL\n"
            f"ORDER BY {ord_q} ASC"
        )

        eff_limit = min(limit or self.config.default_limit, self.config.max_limit)
        sql += "\nLIMIT :limit_n"
        params = {"limit_n": eff_limit}

        return self.execute_query(sql, params=params, query_name="running_total", table_name=table_name)

    def top_n(
        self,
        table_name: str,
        group_column: str,
        metric_column: Optional[str] = None,
        aggregation: str = "SUM",
        n: int = 10,
    ) -> QueryResult:
        """
        Execute Top-N ranking aggregation with bound limit parameter.
        """
        eff_n = max(1, min(int(n), self.config.max_limit))
        res = self.group_by(
            table_name=table_name,
            group_column=group_column,
            metric_column=metric_column,
            aggregation=aggregation,
            order_desc=True,
            limit=eff_n,
        )
        res.query_name = f"top_{eff_n}_{group_column}"
        return res

    def null_summary(self, table_name: str) -> QueryResult:
        """
        Data quality analysis: Compute null count and null percentage for all columns in table.
        """
        validate_identifier(table_name, "table_name")
        conn_check = self._check_connection_or_return_unavailable("null_summary", table_name)
        if conn_check:
            return conn_check

        schema_info = self.get_table_schema(table_name)
        cols = [c["name"] for c in schema_info.get("columns", [])]

        if not cols:
            return QueryResult(
                query_name="null_summary",
                table_name=table_name,
                status="FAILED",
                error_message=f"Table '{table_name}' has no columns or does not exist.",
            )

        exprs = ["COUNT(*) AS total_rows"]
        for c in cols:
            c_q = quote_identifier(c)
            exprs.append(f"COUNT(*) - COUNT({c_q}) AS {quote_identifier(f'null_{c}')}")

        table_ref = self._format_table_ref(table_name)
        sql = "SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM {table_ref}"

        res = self.execute_query(sql, query_name="null_summary_raw", table_name=table_name)
        if res.status != "SUCCESS" or not res.rows:
            return res

        raw = res.rows[0]
        tot = int(raw.get("total_rows", 0))

        summary_rows = []
        for c in cols:
            n_count = int(raw.get(f"null_{c}", 0))
            pct = round((n_count * 100.0 / tot), 2) if tot > 0 else 0.0
            summary_rows.append({
                "column_name": c,
                "total_rows": tot,
                "null_count": n_count,
                "null_percentage": pct,
            })

        return QueryResult(
            query_name="null_summary",
            table_name=table_name,
            status="SUCCESS",
            columns=["column_name", "total_rows", "null_count", "null_percentage"],
            rows=summary_rows,
            row_count=len(summary_rows),
            execution_time_seconds=res.execution_time_seconds,
            sql=sql,
        )

    def distinct_summary(self, table_name: str) -> QueryResult:
        """
        Data quality analysis: Compute distinct counts and distinct percentages for all columns.
        """
        validate_identifier(table_name, "table_name")
        conn_check = self._check_connection_or_return_unavailable("distinct_summary", table_name)
        if conn_check:
            return conn_check

        schema_info = self.get_table_schema(table_name)
        cols = [c["name"] for c in schema_info.get("columns", [])]

        if not cols:
            return QueryResult(
                query_name="distinct_summary",
                table_name=table_name,
                status="FAILED",
                error_message=f"Table '{table_name}' has no columns or does not exist.",
            )

        exprs = ["COUNT(*) AS total_rows"]
        for c in cols:
            c_q = quote_identifier(c)
            exprs.append(f"COUNT(DISTINCT {c_q}) AS {quote_identifier(f'dist_{c}')}")

        table_ref = self._format_table_ref(table_name)
        sql = "SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM {table_ref}"

        res = self.execute_query(sql, query_name="distinct_summary_raw", table_name=table_name)
        if res.status != "SUCCESS" or not res.rows:
            return res

        raw = res.rows[0]
        tot = int(raw.get("total_rows", 0))

        summary_rows = []
        for c in cols:
            d_count = int(raw.get(f"dist_{c}", 0))
            pct = round((d_count * 100.0 / tot), 2) if tot > 0 else 0.0
            summary_rows.append({
                "column_name": c,
                "total_rows": tot,
                "distinct_count": d_count,
                "distinct_percentage": pct,
            })

        return QueryResult(
            query_name="distinct_summary",
            table_name=table_name,
            status="SUCCESS",
            columns=["column_name", "total_rows", "distinct_count", "distinct_percentage"],
            rows=summary_rows,
            row_count=len(summary_rows),
            execution_time_seconds=res.execution_time_seconds,
            sql=sql,
        )

    def execute_saved_query(
        self,
        template_name: str,
        table_name: str,
        replacements: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Load and execute a saved SQL query template from sql/analytics/.
        
        Validates all replacement identifiers before substituting into template.
        """
        validate_identifier(table_name, "table_name")
        clean_tpl = Path(template_name).stem
        sql_file = self.queries_dir / f"{clean_tpl}.sql"

        if not sql_file.exists():
            return QueryResult(
                query_name=clean_tpl,
                table_name=table_name,
                status="FAILED",
                error_message=f"Saved query template '{sql_file}' not found.",
            )

        template_content = sql_file.read_text(encoding="utf-8")

        # Prepare safe replacements
        subs = {"table_name": self._format_table_ref(table_name)}
        if replacements:
            for k, v in replacements.items():
                # Allow safe expression strings or validated identifiers
                if is_safe_identifier(v):
                    subs[k] = quote_identifier(v)
                else:
                    subs[k] = v

        try:
            rendered_sql = template_content.format(**subs)
        except KeyError as e:
            return QueryResult(
                query_name=clean_tpl,
                table_name=table_name,
                status="CONFIG_ERROR",
                error_message=f"Missing template parameter: {e}",
            )

        bound_params = params or {}
        if "limit_n" not in bound_params:
            bound_params["limit_n"] = self.config.default_limit

        return self.execute_query(
            sql=rendered_sql,
            params=bound_params,
            query_name=clean_tpl,
            table_name=table_name,
        )

    def export_result(
        self,
        result: QueryResult,
        reports_dir: Path | str = "reports/analytics",
    ) -> dict[str, str]:
        """
        Serialize QueryResult to JSON and CSV formats without leaking credentials.
        """
        out_dir = Path(reports_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        safe_name = re.sub(r"[^A-Za-z0-9_]", "_", result.query_name).lower()
        json_path = out_dir / f"{safe_name}.json"
        csv_path = out_dir / f"{safe_name}.csv"

        created_files = {}

        if "json" in self.config.export_formats:
            payload = {
                "query_name": result.query_name,
                "table_name": result.table_name,
                "status": result.status,
                "row_count": result.row_count,
                "execution_time_seconds": result.execution_time_seconds,
                "columns": result.columns,
                "data": result.rows,
            }
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
            created_files["json"] = str(json_path)

        if "csv" in self.config.export_formats:
            df = result.to_dataframe()
            df.to_csv(csv_path, index=False)
            created_files["csv"] = str(csv_path)

        result.output_files = created_files
        return created_files

    def run_automated_analytics_suite(
        self,
        table_name: str,
        reports_dir: Path | str = "reports/analytics",
    ) -> list[QueryResult]:
        """
        Intelligently discover columns and execute full analytical battery on a table.
        """
        validate_identifier(table_name, "table_name")
        schema_info = self.get_table_schema(table_name)
        if not schema_info.get("exists", False):
            return [
                QueryResult(
                    query_name="table_check",
                    table_name=table_name,
                    status="INVALID_TABLE",
                    error_message=f"Table '{table_name}' does not exist in database.",
                )
            ]

        columns_meta = schema_info.get("columns", [])
        results: list[QueryResult] = []

        # 1. Basic Summary
        q_basic = self.execute_query(
            f"SELECT COUNT(*) AS total_rows, {len(columns_meta)} AS column_count FROM {self._format_table_ref(table_name)}",
            query_name="basic_summary",
            table_name=table_name,
        )
        self.export_result(q_basic, reports_dir)
        results.append(q_basic)

        # 2. Null Summary
        q_null = self.null_summary(table_name)
        self.export_result(q_null, reports_dir)
        results.append(q_null)

        # 3. Distinct Summary
        q_dist = self.distinct_summary(table_name)
        self.export_result(q_dist, reports_dir)
        results.append(q_dist)

        # Identify numeric, date, and categorical columns dynamically
        numeric_cols = []
        date_cols = []
        text_cols = []

        for col in columns_meta:
            c_name = col["name"]
            c_type = col["type"].upper()
            if any(t in c_type for t in ["INT", "FLOAT", "NUMERIC", "DOUBLE", "DECIMAL", "REAL"]):
                numeric_cols.append(c_name)
            elif any(t in c_type for t in ["DATE", "TIME"]):
                date_cols.append(c_name)
            else:
                text_cols.append(c_name)

        # 4. Numeric Aggregation (first numeric column)
        if numeric_cols:
            num_col = numeric_cols[0]
            q_num = self.aggregate(table_name, num_col)
            q_num.query_name = f"numeric_summary_{num_col}"
            self.export_result(q_num, reports_dir)
            results.append(q_num)

        # 5. Categorical Distribution (first text column)
        if text_cols:
            cat_col = text_cols[0]
            q_cat = self.top_n(table_name, group_column=cat_col, n=10)
            q_cat.query_name = f"category_distribution_{cat_col}"
            self.export_result(q_cat, reports_dir)
            results.append(q_cat)

        # 6. Date Trend (if date column exists)
        if date_cols:
            d_col = date_cols[0]
            num_metric = numeric_cols[0] if numeric_cols else None
            q_trend = self.date_trend(
                table_name=table_name,
                date_column=d_col,
                metric_column=num_metric,
                aggregation="AVG" if num_metric else "COUNT",
                grain="month",
            )
            q_trend.query_name = f"date_trend_{d_col}"
            self.export_result(q_trend, reports_dir)
            results.append(q_trend)

        # 7. Window Functions (Ranking and Running Total if numeric column exists)
        if numeric_cols:
            n_col = numeric_cols[0]
            q_rank = self.rank_rows(
                table_name=table_name,
                order_by_column=n_col,
                method="RANK",
                direction="DESC",
                limit=10,
            )
            q_rank.query_name = f"ranking_{n_col}"
            self.export_result(q_rank, reports_dir)
            results.append(q_rank)

            order_col = date_cols[0] if date_cols else n_col
            q_rt = self.running_total(
                table_name=table_name,
                order_by_column=order_col,
                metric_column=n_col,
                limit=20,
            )
            q_rt.query_name = f"running_total_{n_col}"
            self.export_result(q_rt, reports_dir)
            results.append(q_rt)

        return results


def save_analytics_reports(
    records: list[AnalyticsExecutionRecord],
    reports_dir: Path | str = "reports",
    run_id: str = "run_analytics",
) -> tuple[Path, Path]:
    """
    Serialize overall analytics execution audit records to JSON and CSV reports.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "analytics_report.json"
    csv_path = out_dir / "analytics_report.csv"

    # JSON report
    serializable = [asdict(r) for r in records]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, default=str)

    # CSV report
    csv_rows = []
    for r in records:
        csv_rows.append({
            "run_id": r.run_id,
            "query_name": r.query_name,
            "table": r.table,
            "status": r.status,
            "execution_time": r.execution_time,
            "result_rows": r.result_rows,
            "output_files": "; ".join(r.output_files) if r.output_files else "",
            "error_message": r.error_message or "",
        })

    df_csv = pd.DataFrame(csv_rows)
    df_csv.to_csv(csv_path, index=False)

    return json_path, csv_path


def log_analytics_event(
    record: AnalyticsExecutionRecord,
    log_dir: Path | str = "logs",
) -> None:
    """Append structured analytics query execution entry to logs/analytics.log."""
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "analytics.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = (
        f"[{timestamp}] [INFO] [{record.run_id}] Query: '{record.query_name}' | "
        f"Table: '{record.table}' | Status: {record.status} | "
        f"Rows: {record.result_rows} | Time: {record.execution_time:.4f}s"
    )
    if record.error_message:
        log_entry += f" | Error: {record.error_message}"
    log_entry += "\n"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)
