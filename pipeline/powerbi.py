"""
Universal Power BI Integration & Business Intelligence Module (Stage 10).

Provides domain-agnostic discovery, dimensional classification, semantic model
generation, visualization recommendations, DAX formula generation, and readiness reporting
for PostgreSQL analytical datasets.
"""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from pipeline.analytics import (
    is_safe_identifier,
    quote_identifier,
    sanitize_text,
    validate_identifier,
)
from pipeline.database import (
    get_database_engine,
    get_database_url,
    load_project_env,
    test_database_connection,
)


class AnalyticalRole(str, Enum):
    """Domain-agnostic analytical classification roles for database columns."""
    IDENTIFIER = "IDENTIFIER"
    NUMERIC_METRIC = "NUMERIC_METRIC"
    CATEGORICAL_DIMENSION = "CATEGORICAL_DIMENSION"
    DATETIME_DIMENSION = "DATETIME_DIMENSION"
    BOOLEAN_DIMENSION = "BOOLEAN_DIMENSION"
    TEXT_ATTRIBUTE = "TEXT_ATTRIBUTE"


@dataclass
class PowerBIConfig:
    """Configuration settings for Power BI integration layer."""
    enabled: bool = True
    schema: str = "public"
    connection_mode: str = "import"
    supported_connection_modes: list[str] = field(default_factory=lambda: ["import", "directquery"])
    max_tables: int = 50
    generate_views: bool = True
    generate_metadata: bool = True
    generate_dax_templates: bool = True
    generate_visualization_recommendations: bool = True
    generate_refresh_guidance: bool = True
    generate_rls_guidance: bool = True
    output_formats: list[str] = field(default_factory=lambda: ["json", "csv"])


@dataclass
class ColumnMetadata:
    """Metadata and analytical classification for a single database column."""
    name: str
    data_type: str
    nullable: bool
    primary_key: bool
    analytical_role: str
    candidate_measure: bool
    candidate_dimension: bool


@dataclass
class TableMetadata:
    """Structural and semantic metadata for a single database table."""
    table_name: str
    schema: str
    row_count: int
    table_type: str  # "FACT", "DIMENSION", or "TABULAR"
    columns: list[ColumnMetadata] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RelationshipMetadata:
    """Discovered or candidate relational link between two database entities."""
    relationship_id: str
    parent_table: str
    parent_column: str
    child_table: str
    child_column: str
    relationship_type: str = "1-to-many"
    status: str = "CONFIRMED"  # "CONFIRMED" or "CANDIDATE"
    cross_filter_direction: str = "Single"


@dataclass
class VisualizationRecommendation:
    """Automated visual recommendation based on discovered schema characteristics."""
    visualization_id: str
    visualization_type: str  # "kpi_card", "line_chart", "column_chart", "bar_chart", "matrix", "table", "date_slicer"
    title: str
    target_table: str
    fields: dict[str, list[str]]
    rationale: str
    confidence: float
    required_columns: list[str]


@dataclass
class PowerBIExecutionRecord:
    """Audit summary tracking execution status and metrics for Stage 10."""
    run_id: str
    status: str  # "SUCCESS", "DATABASE_UNAVAILABLE", "CONFIG_ERROR", "FAILED"
    database_status: str
    tables_discovered: int = 0
    columns_discovered: int = 0
    relationships_discovered: int = 0
    measures_generated: int = 0
    visualizations_recommended: int = 0
    semantic_model_generated: bool = False
    documentation_generated: bool = False
    execution_time: float = 0.0
    errors: Optional[str] = None


def load_powerbi_config(
    config_path: Path | str = "config/powerbi.json",
) -> tuple[PowerBIConfig, Optional[str]]:
    """
    Load and validate Power BI integration configuration.
    
    Returns (PowerBIConfig, None) on success or (default_config, error_message).
    """
    path = Path(config_path)
    if not path.exists():
        return PowerBIConfig(), None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return PowerBIConfig(), f"Invalid JSON in {path}: expected JSON object root."

        # Validate connection mode
        mode = str(data.get("connection_mode", "import")).lower().strip()
        supported = [m.lower().strip() for m in data.get("supported_connection_modes", ["import", "directquery"])]
        if mode not in supported:
            return (
                PowerBIConfig(),
                f"Unsupported connection_mode '{mode}' in {path}. Must be one of {supported}",
            )

        # Validate max_tables
        max_tbls = data.get("max_tables", 50)
        if not isinstance(max_tbls, int) or max_tbls <= 0:
            return PowerBIConfig(), f"Invalid max_tables '{max_tbls}' in {path}: must be positive integer."

        # Validate schema identifier
        schema_name = str(data.get("schema", "public")).strip()
        if not is_safe_identifier(schema_name):
            return PowerBIConfig(), f"Invalid schema name '{schema_name}' in {path}: unsafe SQL identifier."

        cfg = PowerBIConfig(
            enabled=bool(data.get("enabled", True)),
            schema=schema_name,
            connection_mode=mode,
            supported_connection_modes=supported,
            max_tables=max_tbls,
            generate_views=bool(data.get("generate_views", True)),
            generate_metadata=bool(data.get("generate_metadata", True)),
            generate_dax_templates=bool(data.get("generate_dax_templates", True)),
            generate_visualization_recommendations=bool(data.get("generate_visualization_recommendations", True)),
            generate_refresh_guidance=bool(data.get("generate_refresh_guidance", True)),
            generate_rls_guidance=bool(data.get("generate_rls_guidance", True)),
            output_formats=list(data.get("output_formats", ["json", "csv"])),
        )
        return cfg, None

    except json.JSONDecodeError as e:
        return PowerBIConfig(), f"Corrupted JSON in {path}: {e}"
    except Exception as e:
        return PowerBIConfig(), f"Error loading {path}: {e}"


class PowerBIIntegrationEngine:
    """
    Domain-agnostic engine that inspects relational database schemas, discovers
    analytical roles, produces semantic models, recommends visual types, and generates DAX.
    """

    def __init__(
        self,
        engine: Optional[Engine] = None,
        config: Optional[PowerBIConfig] = None,
        schema: str = "public",
        powerbi_dir: Path | str = "powerbi",
        root_dir: Optional[Path | str] = None,
    ):
        self.engine = engine
        self.config = config or PowerBIConfig()
        self.schema = self.config.schema or schema
        self.powerbi_dir = Path(powerbi_dir)
        self.root_dir = Path(root_dir).resolve() if root_dir else self.powerbi_dir.resolve().parent
        self._connected: Optional[bool] = None
        self._connection_error: Optional[str] = None

    def connect(self) -> tuple[bool, Optional[str]]:
        """Verify database engine connectivity."""
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

    def discover_tables(self) -> list[str]:
        """
        Dynamically discover available table names in the configured schema.
        Respects max_tables limit.
        """
        conn, _ = self.connect()
        if not conn or not self.engine:
            return []

        try:
            inspector = inspect(self.engine)
            target_schema = self.schema if self.engine.dialect.name == "postgresql" else None
            all_tables = inspector.get_table_names(schema=target_schema)

            # Filter for safe identifiers and apply max_tables limit
            safe_tables = [t for t in all_tables if is_safe_identifier(t)]
            return safe_tables[: self.config.max_tables]
        except Exception:
            return []

    def classify_column_role(
        self,
        col_name: str,
        col_type: str,
        is_pk: bool = False,
        is_nullable: bool = True,
        unique_ratio: Optional[float] = None,
    ) -> str:
        """
        Classify a database column into a domain-agnostic analytical role.
        """
        clean_name = col_name.lower().strip()
        clean_type = col_type.upper().strip()

        # 1. Primary keys are always Identifiers
        if is_pk:
            return AnalyticalRole.IDENTIFIER.value

        # 2. Boolean types or clear binary flags
        if any(b in clean_type for b in ["BOOL", "BOOLEAN"]):
            return AnalyticalRole.BOOLEAN_DIMENSION.value
        if clean_name in ["active", "is_active", "enabled", "is_enabled", "deleted", "flag"]:
            return AnalyticalRole.BOOLEAN_DIMENSION.value

        # 3. Datetime / Temporal types
        if any(t in clean_type for t in ["TIME", "DATE", "TIMESTAMP", "TIMESTAMPTZ"]):
            return AnalyticalRole.DATETIME_DIMENSION.value

        # 4. Identifier naming heuristic (e.g. customer_id, device_uuid, transaction_code)
        is_id_name = clean_name.endswith(("_id", "_key", "_uuid", "_code", "_pk", "_fk")) or clean_name in [
            "id", "uuid", "code", "key"
        ]
        is_not_metric_name = not any(
            m in clean_name for m in ["count", "amount", "total", "sum", "avg", "score", "rate", "pct"]
        )

        if is_id_name and is_not_metric_name:
            return AnalyticalRole.IDENTIFIER.value

        # 5. Numeric types
        is_numeric_type = any(
            n in clean_type
            for n in ["INT", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL", "REAL", "BIGINT", "SMALLINT"]
        )
        if is_numeric_type:
            # If 100% unique integer without metric keywords, likely an identifier
            if unique_ratio is not None and unique_ratio >= 0.99 and not is_nullable and is_id_name:
                return AnalyticalRole.IDENTIFIER.value
            return AnalyticalRole.NUMERIC_METRIC.value

        # 6. Text / String types
        is_string_type = any(s in clean_type for s in ["CHAR", "TEXT", "VARCHAR", "STRING"])
        if is_string_type:
            freeform_keywords = ["comment", "description", "note", "notes", "bio", "details", "remark", "remarks", "message", "body"]
            if any(k in clean_name for k in freeform_keywords):
                return AnalyticalRole.TEXT_ATTRIBUTE.value

            categorical_keywords = [
                "category", "status", "type", "department", "name", "gender",
                "level", "group", "class", "state", "city", "country", "role",
                "segment", "channel", "brand", "model", "code", "tier", "priority"
            ]
            if any(k in clean_name for k in categorical_keywords):
                return AnalyticalRole.CATEGORICAL_DIMENSION.value

            if "VARCHAR" in clean_type or "CHAR" in clean_type:
                return AnalyticalRole.CATEGORICAL_DIMENSION.value

            return AnalyticalRole.CATEGORICAL_DIMENSION.value

        # Fallback default
        return AnalyticalRole.CATEGORICAL_DIMENSION.value

    def inspect_table(self, table_name: str) -> TableMetadata:
        """
        Inspect table structural properties, column roles, and row count.
        """
        validate_identifier(table_name, "table_name")
        conn, _ = self.connect()
        if not conn or not self.engine:
            return TableMetadata(
                table_name=table_name,
                schema=self.schema,
                row_count=0,
                table_type="TABULAR",
            )

        inspector = inspect(self.engine)
        target_schema = self.schema if self.engine.dialect.name == "postgresql" else None

        raw_columns = inspector.get_columns(table_name, schema=target_schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=target_schema)
        pks = pk_constraint.get("constrained_columns", []) if pk_constraint else []

        # Foreign keys
        try:
            raw_fks = inspector.get_foreign_keys(table_name, schema=target_schema) or []
        except Exception:
            raw_fks = []

        # Count rows
        row_count = 0
        try:
            tbl_ref = f'"{target_schema}"."{table_name}"' if target_schema else f'"{table_name}"'
            with self.engine.connect() as connection:
                res = connection.execute(text(f"SELECT COUNT(*) FROM {tbl_ref}")).scalar()
                row_count = int(res) if res is not None else 0
        except Exception:
            row_count = 0

        columns_meta: list[ColumnMetadata] = []
        for col in raw_columns:
            c_name = col["name"]
            c_type = str(col["type"])
            c_null = col.get("nullable", True)
            c_pk = c_name in pks

            role = self.classify_column_role(col_name=c_name, col_type=c_type, is_pk=c_pk, is_nullable=c_null)
            is_measure = role == AnalyticalRole.NUMERIC_METRIC.value
            is_dim = role in [
                AnalyticalRole.CATEGORICAL_DIMENSION.value,
                AnalyticalRole.DATETIME_DIMENSION.value,
                AnalyticalRole.BOOLEAN_DIMENSION.value,
                AnalyticalRole.IDENTIFIER.value,
            ]

            columns_meta.append(
                ColumnMetadata(
                    name=c_name,
                    data_type=c_type,
                    nullable=c_null,
                    primary_key=c_pk,
                    analytical_role=role,
                    candidate_measure=is_measure,
                    candidate_dimension=is_dim,
                )
            )

        # Classify candidate table archetype
        metric_count = sum(1 for c in columns_meta if c.analytical_role == AnalyticalRole.NUMERIC_METRIC.value)
        dim_count = sum(
            1 for c in columns_meta if c.analytical_role in [
                AnalyticalRole.CATEGORICAL_DIMENSION.value,
                AnalyticalRole.TEXT_ATTRIBUTE.value,
            ]
        )
        date_count = sum(1 for c in columns_meta if c.analytical_role == AnalyticalRole.DATETIME_DIMENSION.value)

        if metric_count >= 1 and (date_count >= 1 or len(columns_meta) >= 3):
            archetype = "FACT"
        elif dim_count >= metric_count and len(columns_meta) > 0:
            archetype = "DIMENSION"
        else:
            archetype = "TABULAR"

        return TableMetadata(
            table_name=table_name,
            schema=self.schema,
            row_count=row_count,
            table_type=archetype,
            columns=columns_meta,
            primary_keys=pks,
            foreign_keys=raw_fks,
        )

    def discover_relationships(self, tables: list[TableMetadata]) -> list[RelationshipMetadata]:
        """
        Discover explicit database foreign keys across analyzed tables.
        Never fabricates relationships based on coincidental column names alone.
        """
        relationships: list[RelationshipMetadata] = []
        table_map = {t.table_name: t for t in tables}

        for tbl in tables:
            for fk in tbl.foreign_keys:
                ref_tbl = fk.get("referred_table")
                ref_cols = fk.get("referred_columns", [])
                const_cols = fk.get("constrained_columns", [])

                if ref_tbl and ref_cols and const_cols and ref_tbl in table_map:
                    rel_id = f"rel_{tbl.table_name}_{const_cols[0]}_to_{ref_tbl}_{ref_cols[0]}"
                    relationships.append(
                        RelationshipMetadata(
                            relationship_id=rel_id,
                            parent_table=ref_tbl,
                            parent_column=ref_cols[0],
                            child_table=tbl.table_name,
                            child_column=const_cols[0],
                            relationship_type="1-to-many",
                            status="CONFIRMED",
                            cross_filter_direction="Single",
                        )
                    )

        return relationships

    def generate_dax_templates_for_table(self, table: TableMetadata) -> list[dict[str, str]]:
        """
        Generate ready-to-use DAX measures customized for the discovered table and columns.
        """
        measures: list[dict[str, str]] = []
        tbl_q = f"'{table.table_name}'"

        # 1. Total Rows
        measures.append({
            "name": f"Total {table.table_name} Rows",
            "dax": f"Total {table.table_name} Rows = COUNTROWS({tbl_q})",
            "table": table.table_name,
            "type": "row_count",
        })

        # 2. Aggregations for each numeric metric
        for col in table.columns:
            if col.analytical_role == AnalyticalRole.NUMERIC_METRIC.value:
                col_q = f"{tbl_q}[{col.name}]"
                c_clean = col.name.replace("_", " ").title()

                measures.append({
                    "name": f"Total {c_clean}",
                    "dax": f"Total {c_clean} = SUM({col_q})",
                    "table": table.table_name,
                    "type": "sum",
                })
                measures.append({
                    "name": f"Avg {c_clean}",
                    "dax": f"Avg {c_clean} = AVERAGE({col_q})",
                    "table": table.table_name,
                    "type": "average",
                })

            elif col.analytical_role == AnalyticalRole.IDENTIFIER.value:
                col_q = f"{tbl_q}[{col.name}]"
                c_clean = col.name.replace("_", " ").title()
                measures.append({
                    "name": f"Distinct {c_clean} Count",
                    "dax": f"Distinct {c_clean} Count = DISTINCTCOUNT({col_q})",
                    "table": table.table_name,
                    "type": "distinct_count",
                })

        return measures

    def generate_visualization_recommendations(
        self, tables: list[TableMetadata]
    ) -> list[VisualizationRecommendation]:
        """
        Recommend Power BI visualization types by inspecting discovered column analytical roles.
        Guarantees that recommended visuals only reference existing columns.
        """
        recommendations: list[VisualizationRecommendation] = []

        for tbl in tables:
            col_names = {c.name for c in tbl.columns}
            metrics = [c for c in tbl.columns if c.analytical_role == AnalyticalRole.NUMERIC_METRIC.value]
            categories = [c for c in tbl.columns if c.analytical_role == AnalyticalRole.CATEGORICAL_DIMENSION.value]
            dates = [c for c in tbl.columns if c.analytical_role == AnalyticalRole.DATETIME_DIMENSION.value]
            ids = [c for c in tbl.columns if c.analytical_role == AnalyticalRole.IDENTIFIER.value]

            # 1. KPI Cards for each numeric metric
            for m in metrics:
                if m.name in col_names:
                    rec_id = f"kpi_{tbl.table_name}_{m.name}"
                    m_label = m.name.replace("_", " ").title()
                    recommendations.append(
                        VisualizationRecommendation(
                            visualization_id=rec_id,
                            visualization_type="kpi_card",
                            title=f"Total {m_label}",
                            target_table=tbl.table_name,
                            fields={"measure": [m.name]},
                            rationale=f"Executive KPI snapshot displaying total aggregated {m.name} for {tbl.table_name}.",
                            confidence=0.95,
                            required_columns=[m.name],
                        )
                    )

            # 2. Time Trend Line Chart (Date + Metric)
            if dates and metrics:
                primary_date = dates[0]
                for m in metrics[:2]:  # Limit to top 2 metrics
                    if primary_date.name in col_names and m.name in col_names:
                        rec_id = f"trend_{tbl.table_name}_{primary_date.name}_{m.name}"
                        m_label = m.name.replace("_", " ").title()
                        d_label = primary_date.name.replace("_", " ").title()
                        recommendations.append(
                            VisualizationRecommendation(
                                visualization_id=rec_id,
                                visualization_type="line_chart",
                                title=f"{m_label} Trend over {d_label}",
                                target_table=tbl.table_name,
                                fields={"axis": [primary_date.name], "values": [m.name]},
                                rationale=f"Chronological line trend tracking {m.name} along continuous timeline {primary_date.name}.",
                                confidence=0.90,
                                required_columns=[primary_date.name, m.name],
                            )
                        )

            # 3. Categorical Distribution Column/Bar Chart (Category + Metric)
            if categories and metrics:
                primary_cat = categories[0]
                primary_metric = metrics[0]
                if primary_cat.name in col_names and primary_metric.name in col_names:
                    rec_id = f"dist_{tbl.table_name}_{primary_cat.name}_{primary_metric.name}"
                    c_label = primary_cat.name.replace("_", " ").title()
                    m_label = primary_metric.name.replace("_", " ").title()
                    recommendations.append(
                        VisualizationRecommendation(
                            visualization_id=rec_id,
                            visualization_type="column_chart",
                            title=f"{m_label} by {c_label}",
                            target_table=tbl.table_name,
                            fields={"category": [primary_cat.name], "values": [primary_metric.name]},
                            rationale=f"Categorical bar comparison illustrating {primary_metric.name} distributed across {primary_cat.name}.",
                            confidence=0.88,
                            required_columns=[primary_cat.name, primary_metric.name],
                        )
                    )

            # 4. Multi-Dimensional Matrix (Category 1 + Category 2 + Metric)
            if len(categories) >= 2 and metrics:
                cat1, cat2 = categories[0], categories[1]
                primary_metric = metrics[0]
                if cat1.name in col_names and cat2.name in col_names and primary_metric.name in col_names:
                    rec_id = f"matrix_{tbl.table_name}_{cat1.name}_{cat2.name}"
                    recommendations.append(
                        VisualizationRecommendation(
                            visualization_id=rec_id,
                            visualization_type="matrix",
                            title=f"{primary_metric.name.replace('_', ' ').title()} Matrix ({cat1.name} × {cat2.name})",
                            target_table=tbl.table_name,
                            fields={"rows": [cat1.name], "columns": [cat2.name], "values": [primary_metric.name]},
                            rationale=f"Cross-tabulated matrix visual analyzing {primary_metric.name} by dual categories {cat1.name} and {cat2.name}.",
                            confidence=0.80,
                            required_columns=[cat1.name, cat2.name, primary_metric.name],
                        )
                    )

            # 5. Date Range Slicer
            if dates:
                d_col = dates[0]
                if d_col.name in col_names:
                    rec_id = f"slicer_{tbl.table_name}_{d_col.name}"
                    recommendations.append(
                        VisualizationRecommendation(
                            visualization_id=rec_id,
                            visualization_type="date_slicer",
                            title=f"Date Filter ({d_col.name.replace('_', ' ').title()})",
                            target_table=tbl.table_name,
                            fields={"date": [d_col.name]},
                            rationale=f"Interactive date slider allowing end-users to dynamically filter report pages by {d_col.name}.",
                            confidence=0.92,
                            required_columns=[d_col.name],
                        )
                    )

            # 6. Granular Records Table
            rep_cols = [c.name for c in (ids[:2] + categories[:2] + metrics[:2] + dates[:1])]
            valid_rep_cols = [c for c in rep_cols if c in col_names]
            if valid_rep_cols:
                rec_id = f"table_{tbl.table_name}_records"
                recommendations.append(
                    VisualizationRecommendation(
                        visualization_id=rec_id,
                        visualization_type="table",
                        title=f"{tbl.table_name.replace('_', ' ').title()} Detailed Records",
                        target_table=tbl.table_name,
                        fields={"columns": valid_rep_cols},
                        rationale=f"Granular audit table supporting sorting, multi-column drillthrough, and raw data inspection.",
                        confidence=0.85,
                        required_columns=valid_rep_cols,
                    )
                )

        return recommendations

    def build_semantic_model(
        self,
        tables: list[TableMetadata],
        relationships: list[RelationshipMetadata],
        status: str = "SUCCESS",
    ) -> dict[str, Any]:
        """
        Compile structured semantic model dictionary matching powerbi/semantic_model.json schema.
        """
        all_measures: list[dict[str, Any]] = []
        all_dimensions: list[dict[str, Any]] = []
        serialized_tables: list[dict[str, Any]] = []

        for tbl in tables:
            tbl_measures = self.generate_dax_templates_for_table(tbl)
            all_measures.extend(tbl_measures)

            serialized_cols = []
            for col in tbl.columns:
                serialized_cols.append({
                    "name": col.name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                    "primary_key": col.primary_key,
                    "role": col.analytical_role,
                    "candidate_measure": col.candidate_measure,
                    "candidate_dimension": col.candidate_dimension,
                })
                if col.candidate_dimension:
                    all_dimensions.append({
                        "table": tbl.table_name,
                        "column": col.name,
                        "role": col.analytical_role,
                    })

            serialized_tables.append({
                "table_name": tbl.table_name,
                "schema": tbl.schema,
                "row_count": tbl.row_count,
                "table_archetype": tbl.table_type,
                "primary_keys": tbl.primary_keys,
                "columns": serialized_cols,
            })

        serialized_relationships = []
        for rel in relationships:
            serialized_relationships.append(asdict(rel))

        rel_note = (
            f"Discovered {len(relationships)} foreign-key relationships."
            if relationships
            else "No explicit PostgreSQL foreign-key relationships discovered."
        )

        return {
            "model_type": "dynamic",
            "status": status,
            "source": {
                "type": "postgresql",
                "schema": self.schema,
                "connection_mode": self.config.connection_mode,
            },
            "tables": serialized_tables,
            "relationships": serialized_relationships,
            "relationship_note": rel_note,
            "measures": all_measures,
            "dimensions": all_dimensions,
        }

    def generate_generic_views_sql(self, tables: list[TableMetadata]) -> list[dict[str, str]]:
        """
        Generate idempotent SQL definition for Power BI-ready database views.
        """
        views: list[dict[str, str]] = []
        if not self.config.generate_views or not tables:
            return views

        schema_prefix = f'"{self.schema}".' if self.schema else ""

        # 1. Dataset overview view
        select_parts = []
        for t in tables:
            t_ref = f"{schema_prefix}\"{t.table_name}\""
            select_parts.append(f"SELECT '{t.table_name}' AS table_name, COUNT(*) AS row_count FROM {t_ref}")

        if select_parts:
            overview_sql = (
                f"CREATE OR REPLACE VIEW {schema_prefix}\"vw_powerbi_dataset_overview\" AS\n"
                + "\nUNION ALL\n".join(select_parts)
                + ";"
            )
            views.append({
                "view_name": "vw_powerbi_dataset_overview",
                "sql": overview_sql,
                "purpose": "Consolidated table row-count overview for Power BI administrative dashboards",
            })

        return views

    def run_integration_suite(
        self,
        output_dir: Optional[Path | str] = None,
        run_id: str = "run_default",
    ) -> tuple[PowerBIExecutionRecord, dict[str, Any], dict[str, Any]]:
        """
        Execute end-to-end Power BI integration suite:
        Discovers tables, inspects metadata, builds semantic model, generates recommendations,
        and saves JSON specs to powerbi/ and reports/powerbi/.
        """
        t0 = time.monotonic()
        target_dir = Path(output_dir) if output_dir else self.powerbi_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        # 1. Check database connectivity
        conn, conn_err = self.connect()
        if not conn:
            duration = round(time.monotonic() - t0, 3)
            err_msg = sanitize_text(f"Database connection unavailable: {conn_err}")
            unavail_semantic = {
                "model_type": "dynamic",
                "status": "DATABASE_UNAVAILABLE",
                "source": {"type": "postgresql", "schema": self.schema},
                "tables": [],
                "relationships": [],
                "relationship_note": "PostgreSQL database is currently offline or unconfigured.",
                "measures": [],
                "dimensions": [],
            }
            unavail_dash = {
                "dashboard_title": "Universal Analytical Dashboard",
                "status": "DATABASE_UNAVAILABLE",
                "layout": "4-zone-executive",
                "recommendations": [],
                "note": "No recommendations generated: database is offline or unconfigured.",
            }

            # Write fallback valid JSON files
            (target_dir / "semantic_model.json").write_text(json.dumps(unavail_semantic, indent=2), encoding="utf-8")
            (target_dir / "dashboard_spec.json").write_text(json.dumps(unavail_dash, indent=2), encoding="utf-8")

            record = PowerBIExecutionRecord(
                run_id=run_id,
                status="DATABASE_UNAVAILABLE",
                database_status="DATABASE_UNAVAILABLE",
                tables_discovered=0,
                columns_discovered=0,
                relationships_discovered=0,
                measures_generated=0,
                visualizations_recommended=0,
                semantic_model_generated=False,
                documentation_generated=True,
                execution_time=duration,
                errors=err_msg,
            )
            return record, unavail_semantic, unavail_dash

        # 2. Discover tables
        table_names = self.discover_tables()
        tables_meta: list[TableMetadata] = []
        total_cols = 0

        for t_name in table_names:
            t_meta = self.inspect_table(t_name)
            tables_meta.append(t_meta)
            total_cols += len(t_meta.columns)

        # 3. Discover relationships
        relationships = self.discover_relationships(tables_meta)

        # 4. Generate Semantic Model
        semantic_model = self.build_semantic_model(tables_meta, relationships, status="SUCCESS")
        (target_dir / "semantic_model.json").write_text(
            json.dumps(semantic_model, indent=2, default=str), encoding="utf-8"
        )

        # 5. Generate Visual Recommendations
        visual_recs = self.generate_visualization_recommendations(tables_meta)
        dash_spec = {
            "dashboard_title": "Universal Analytical Dashboard",
            "status": "SUCCESS",
            "layout": "4-zone-executive",
            "total_recommendations": len(visual_recs),
            "recommendations": [asdict(r) for r in visual_recs],
        }
        (target_dir / "dashboard_spec.json").write_text(
            json.dumps(dash_spec, indent=2, default=str), encoding="utf-8"
        )

        # 6. Generate concrete DAX export
        if self.config.generate_dax_templates:
            dax_dir = target_dir / "dax"
            dax_dir.mkdir(parents=True, exist_ok=True)
            dax_lines = [
                "// Auto-Generated Concrete DAX Measures",
                f"// Generated at: {datetime.now().isoformat()}",
                "// ==============================================================================",
            ]
            for m in semantic_model.get("measures", []):
                dax_lines.append(f"\n// Table: {m.get('table')}")
                dax_lines.append(m.get("dax", ""))
            (dax_dir / "concrete_measures.dax").write_text("\n".join(dax_lines), encoding="utf-8")

        duration = round(time.monotonic() - t0, 3)
        record = PowerBIExecutionRecord(
            run_id=run_id,
            status="SUCCESS",
            database_status="CONNECTED",
            tables_discovered=len(tables_meta),
            columns_discovered=total_cols,
            relationships_discovered=len(relationships),
            measures_generated=len(semantic_model.get("measures", [])),
            visualizations_recommended=len(visual_recs),
            semantic_model_generated=True,
            documentation_generated=True,
            execution_time=duration,
            errors=None,
        )

        return record, semantic_model, dash_spec


def save_powerbi_reports(
    record: PowerBIExecutionRecord,
    reports_dir: Path | str = "reports/powerbi",
    run_id: str = "run_default",
) -> tuple[Path, Path]:
    """
    Save Power BI readiness audit reports in JSON and CSV formats.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "powerbi_report.json"
    csv_path = out_dir / "powerbi_report.csv"

    payload = asdict(record)

    # 1. JSON Report
    json_path.write_text(json.dumps([payload], indent=2, default=str), encoding="utf-8")

    # 2. CSV Report
    fieldnames = list(payload.keys())
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(payload)

    return json_path, csv_path


def log_powerbi_event(
    record: PowerBIExecutionRecord,
    log_dir: Path | str = "logs",
) -> None:
    """
    Append sanitized Power BI integration execution record to logs/powerbi.log.
    """
    target_dir = Path(log_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    log_file = target_dir / "powerbi.log"

    timestamp = datetime.now().isoformat()
    clean_err = sanitize_text(record.errors) if record.errors else "None"
    entry = (
        f"[{timestamp}] [RUN:{record.run_id}] [STATUS:{record.status}] "
        f"[TABLES:{record.tables_discovered}] [COLS:{record.columns_discovered}] "
        f"[RELS:{record.relationships_discovered}] [MEASURES:{record.measures_generated}] "
        f"[VISUALS:{record.visualizations_recommended}] [ERRORS:{clean_err}]\n"
    )
    with log_file.open("a", encoding="utf-8") as f:
        f.write(entry)
