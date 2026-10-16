"""
Universal PostgreSQL Database & Dynamic Data Modeling Module.

Provides domain-agnostic dynamic table creation, schema inspection, type mapping,
and batch loading of validated datasets into PostgreSQL using SQLAlchemy.
Safely handles unconfigured or unavailable database instances without crashing.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import (
    BIGINT,
    BOOLEAN,
    FLOAT,
    TEXT,
    TIMESTAMP,
    Column,
    MetaData,
    Table,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.types import TypeEngine

from pipeline.schema_detector import DatasetProfile

# PostgreSQL identifier length constraint (standard NAMEDATALEN limit is 63 bytes)
POSTGRES_MAX_IDENTIFIER_LENGTH = 63


@dataclass
class DatabaseConfig:
    """Configuration settings for database operations."""
    if_exists: str = "append"  # "append", "replace", "fail"
    schema: str = "public"
    create_tables: bool = True
    auto_primary_key: bool = False
    batch_size: int = 1000
    primary_keys: dict[str, str] = field(default_factory=dict)


@dataclass
class DatabaseLoadResult:
    """Outcome and audit trail of a dataset database loading operation."""
    source_dataset: str
    target_table: str
    database_status: str  # "SUCCESS", "LOAD_FAILED", "DATABASE_UNAVAILABLE", "CONFIG_MISSING"
    connection_status: str  # "CONNECTED", "FAILED", "UNCONFIGURED"
    rows_attempted: int
    rows_inserted: int
    rows_failed: int
    columns_created: list[str] = field(default_factory=list)
    sql_types: dict[str, str] = field(default_factory=dict)
    load_mode: str = "append"
    duration_seconds: float = 0.0
    error_message: Optional[str] = None


def load_database_config(
    config_path: Path | str = "config/database.json",
) -> DatabaseConfig:
    """Load database configuration from JSON or return safe defaults."""
    path = Path(config_path)
    if not path.exists():
        return DatabaseConfig()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return DatabaseConfig(
            if_exists=data.get("if_exists", "append"),
            schema=data.get("schema", "public"),
            create_tables=data.get("create_tables", True),
            auto_primary_key=data.get("auto_primary_key", False),
            batch_size=data.get("batch_size", 1000),
            primary_keys=data.get("primary_keys", {}),
        )
    except Exception:
        return DatabaseConfig()


def load_project_env(dotenv_path: Optional[Path | str] = None) -> bool:
    """
    Consistently load environment variables from the project root .env file.
    
    If dotenv_path is provided, loads from that specific path.
    Otherwise, checks the project root .env (anchored relative to this file).
    Falls back to python-dotenv's default search if no file is found.
    Returns True if a dotenv file was successfully located and loaded.
    """
    if dotenv_path is not None:
        p = Path(dotenv_path)
        if p.is_file():
            return load_dotenv(dotenv_path=p, override=False)
        return False

    project_root = Path(__file__).resolve().parent.parent
    root_env = project_root / ".env"
    if root_env.is_file():
        return load_dotenv(dotenv_path=root_env, override=False)

    return load_dotenv()


def normalize_database_url(url: Optional[str]) -> Optional[str]:
    """
    Normalize database connection URL to ensure compatibility with installed drivers.
    
    SQLAlchemy 2.0 defaults to psycopg (v3) for unqualified 'postgresql://'.
    Because this project uses psycopg2-binary, unqualified postgresql:// and legacy postgres://
    URIs are normalized to postgresql+psycopg2:// so SQLAlchemy uses the installed psycopg2 driver.
    """
    if not url:
        return None
    url_clean = url.strip()
    if url_clean.startswith("postgres://"):
        return "postgresql+psycopg2://" + url_clean[len("postgres://"):]
    if url_clean.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url_clean[len("postgresql://"):]
    return url_clean


def get_database_url(dotenv_path: Optional[Path | str] = None) -> Optional[str]:
    """
    Retrieve PostgreSQL connection URL from environment variables.
    
    Reads from .env using python-dotenv anchored to the project root. Evaluates
    DATABASE_URL first (normalizing to postgresql+psycopg2 if needed),
    falling back to individual parameters if present.
    Returns None if unconfigured.
    """
    if dotenv_path is not None:
        p = Path(dotenv_path)
        if p.is_file():
            load_dotenv(dotenv_path=p, override=False)
    else:
        load_project_env()

    url = os.getenv("DATABASE_URL")
    if url and url.strip():
        return normalize_database_url(url)

    # Fallback to component parameters if all required elements exist
    host = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT", "5432")
    dbname = os.getenv("DATABASE_NAME")
    user = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD", "")

    if host and dbname and user:
        pwd_part = f":{password}" if password else ""
        return f"postgresql+psycopg2://{user}{pwd_part}@{host}:{port}/{dbname}"

    return None


def mask_database_url(url: Optional[str]) -> str:
    """
    Mask password in database URL for safe logging and reporting.
    
    Example: postgresql+psycopg2://user:secret@host:5432/db -> postgresql+psycopg2://user:***@host:5432/db
    """
    if not url:
        return "[UNCONFIGURED]"

    try:
        parsed = urlparse(url)
        if parsed.password:
            netloc = f"{parsed.username}:***@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            masked = urlunparse((
                parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            ))
            return masked
        return url
    except Exception:
        return "[MASKED_URL]"


def get_database_engine(
    database_url: Optional[str] = None,
    timeout_seconds: int = 5,
    dotenv_path: Optional[Path | str] = None,
) -> tuple[Optional[Engine], Optional[str]]:
    """
    Initialize SQLAlchemy Engine with connection timeout.
    
    Returns (engine, None) on success or (None, error_reason) on failure.
    """
    if database_url is not None:
        raw_url = database_url.strip() if database_url else ""
    else:
        raw_url = get_database_url(dotenv_path=dotenv_path) or ""

    if not raw_url:
        return None, "DATABASE_URL is not set in environment or .env file"

    url = normalize_database_url(raw_url)
    if not url:
        return None, "DATABASE_URL is not set in environment or .env file"

    try:
        engine = create_engine(
            url,
            connect_args={"connect_timeout": timeout_seconds} if "postgresql" in url else {},
            pool_pre_ping=True,
        )
        return engine, None
    except Exception as e:
        clean_err = re.sub(r"://[^@]+@", "://***:***@", str(e))
        return None, f"Failed to initialize SQLAlchemy engine: {clean_err}"


def test_database_connection(engine: Optional[Engine]) -> tuple[bool, Optional[str]]:
    """
    Execute a connectivity probe (SELECT 1).
    
    Returns (True, None) if reachable, or (False, error_message) if unavailable.
    """
    if engine is None:
        return False, "Database engine is not initialized"

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        # Strip credentials from error string if present
        clean_err = re.sub(r"://[^@]+@", "://***:***@", str(e))
        return False, clean_err


def generate_table_name(source_filename: str) -> str:
    """
    Generate a safe, standardized PostgreSQL table name from a dataset filename.
    
    Rules:
    - Strip directory paths and file extensions
    - Remove pipeline stage suffixes (_validated, _cleaned, _processed, _raw)
    - Convert to lowercase
    - Replace spaces and non-alphanumeric characters with underscores
    - Collapse multiple underscores and trim outer underscores
    - Truncate to PostgreSQL identifier limit (63 characters)
    """
    stem = Path(source_filename).stem
    # Remove common pipeline stage suffixes
    clean = re.sub(r"(_validated|_cleaned|_processed|_raw)$", "", stem, flags=re.IGNORECASE)
    clean = clean.lower()
    clean = re.sub(r"[^\w]+", "_", clean)
    clean = re.sub(r"_+", "_", clean)
    clean = clean.strip("_")

    if not clean:
        clean = "universal_table"

    # Enforce PostgreSQL 63-character limit
    return clean[:POSTGRES_MAX_IDENTIFIER_LENGTH]


def map_logical_type_to_sql(logical_type: str, dialect_name: str = "postgresql") -> TypeEngine:
    """
    Map logical data types to SQLAlchemy SQL column types.
    
    Mappings:
    - Integer     -> BIGINT
    - Float       -> FLOAT (PostgreSQL DOUBLE PRECISION)
    - Boolean     -> BOOLEAN
    - DateTime    -> TIMESTAMP
    - Categorical -> TEXT
    - String/Text -> TEXT
    """
    type_key = logical_type.strip().lower()

    if "int" in type_key:
        return BIGINT()
    elif "float" in type_key or "numeric" in type_key or "double" in type_key:
        return FLOAT()
    elif "bool" in type_key:
        return BOOLEAN()
    elif "date" in type_key or "time" in type_key:
        return TIMESTAMP()
    elif "cat" in type_key:
        return TEXT()
    else:
        return TEXT()


def inspect_table_schema(
    engine: Engine, table_name: str, schema: str = "public"
) -> dict[str, Any]:
    """
    Inspect an existing database table and return structural metadata and row count.
    """
    inspector = inspect(engine)
    target_schema = schema if engine.dialect.name == "postgresql" else None

    # Check existence
    tables = inspector.get_table_names(schema=target_schema)
    if table_name not in tables:
        return {
            "table_name": table_name,
            "schema": schema,
            "exists": False,
            "columns": [],
            "primary_keys": [],
            "row_count": 0,
        }

    raw_columns = inspector.get_columns(table_name, schema=target_schema)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=target_schema)
    pks = pk_constraint.get("constrained_columns", []) if pk_constraint else []

    columns_info = []
    for col in raw_columns:
        columns_info.append({
            "name": col["name"],
            "type": str(col["type"]),
            "nullable": col.get("nullable", True),
            "primary_key": col["name"] in pks,
        })

    # Retrieve row count
    row_count = 0
    try:
        schema_prefix = f'"{schema}".' if target_schema else ""
        query = text(f'SELECT COUNT(*) FROM {schema_prefix}"{table_name}"')
        with engine.connect() as conn:
            res = conn.execute(query).scalar()
            row_count = int(res) if res is not None else 0
    except Exception:
        pass

    return {
        "table_name": table_name,
        "schema": schema,
        "exists": True,
        "columns": columns_info,
        "primary_keys": pks,
        "row_count": row_count,
    }


def load_validated_dataset(
    df: pd.DataFrame,
    source_filename: str,
    engine: Optional[Engine],
    config: Optional[DatabaseConfig] = None,
    schema_profile: Optional[DatasetProfile] = None,
) -> DatabaseLoadResult:
    """
    Dynamically create table structure and batch load validated records into database.
    
    Never loads raw, processed, or rejected records.
    """
    cfg = config or load_database_config()
    table_name = generate_table_name(source_filename)
    rows_attempted = len(df)

    if engine is None:
        return DatabaseLoadResult(
            source_dataset=source_filename,
            target_table=table_name,
            database_status="CONFIG_MISSING",
            connection_status="UNCONFIGURED",
            rows_attempted=rows_attempted,
            rows_inserted=0,
            rows_failed=rows_attempted,
            error_message="Database engine not configured (DATABASE_URL missing)",
        )

    # Test connectivity first
    connected, conn_err = test_database_connection(engine)
    if not connected:
        return DatabaseLoadResult(
            source_dataset=source_filename,
            target_table=table_name,
            database_status="DATABASE_UNAVAILABLE",
            connection_status="FAILED",
            rows_attempted=rows_attempted,
            rows_inserted=0,
            rows_failed=rows_attempted,
            error_message=f"Database connection failed: {conn_err}",
        )

    # Build SQL type mapping dynamically
    sql_types_dict: dict[str, Any] = {}
    sql_type_labels: dict[str, str] = {}
    dialect = engine.dialect.name

    for col in df.columns:
        col_type = "String/Text"
        if schema_profile and col in schema_profile.columns:
            col_type = schema_profile.columns[col].inferred_type
        sql_t = map_logical_type_to_sql(col_type, dialect_name=dialect)
        sql_types_dict[col] = sql_t
        sql_type_labels[col] = str(sql_t)

    # Determine primary key
    primary_key_col: Optional[str] = None
    if table_name in cfg.primary_keys:
        primary_key_col = cfg.primary_keys[table_name]
    elif cfg.auto_primary_key and schema_profile:
        for col, col_prof in schema_profile.columns.items():
            if (
                col_prof.identifier_info
                and col_prof.identifier_info.likely_identifier
                and col_prof.identifier_info.confidence >= 0.95
                and not col_prof.is_nullable
                and col in df.columns
            ):
                primary_key_col = col
                break

    start_time = time.time()
    try:
        target_schema = cfg.schema if dialect == "postgresql" else None

        # Handle table creation if enabled
        if cfg.create_tables:
            meta = MetaData(schema=target_schema)
            columns_list = []
            for col_name, sql_t in sql_types_dict.items():
                is_pk = (col_name == primary_key_col)
                columns_list.append(Column(col_name, sql_t, primary_key=is_pk))

            Table(table_name, meta, *columns_list, extend_existing=True)
            meta.create_all(engine)

        load_df = df.copy()
        for col, sql_t in sql_types_dict.items():
            if isinstance(sql_t, TIMESTAMP):
                if not pd.api.types.is_datetime64_any_dtype(load_df[col]):
                    load_df[col] = pd.to_datetime(load_df[col], errors="coerce")

        # Batch load data using Pandas to_sql with multi-row batching
        load_df.to_sql(
            name=table_name,
            con=engine,
            schema=target_schema,
            if_exists=cfg.if_exists,
            index=False,
            chunksize=cfg.batch_size,
            method="multi" if dialect == "postgresql" else None,
            dtype=sql_types_dict,
        )

        duration = round(time.time() - start_time, 3)

        return DatabaseLoadResult(
            source_dataset=source_filename,
            target_table=table_name,
            database_status="SUCCESS",
            connection_status="CONNECTED",
            rows_attempted=rows_attempted,
            rows_inserted=rows_attempted,
            rows_failed=0,
            columns_created=list(df.columns),
            sql_types=sql_type_labels,
            load_mode=cfg.if_exists,
            duration_seconds=duration,
        )

    except Exception as e:
        duration = round(time.time() - start_time, 3)
        clean_err = re.sub(r"://[^@]+@", "://***:***@", str(e))
        return DatabaseLoadResult(
            source_dataset=source_filename,
            target_table=table_name,
            database_status="LOAD_FAILED",
            connection_status="CONNECTED",
            rows_attempted=rows_attempted,
            rows_inserted=0,
            rows_failed=rows_attempted,
            columns_created=list(df.columns),
            sql_types=sql_type_labels,
            load_mode=cfg.if_exists,
            duration_seconds=duration,
            error_message=clean_err,
        )


def save_database_load_reports(
    results: list[DatabaseLoadResult], reports_dir: Path | str = "reports"
) -> tuple[Path, Path]:
    """
    Serialize database loading outcomes to JSON and CSV reports.
    
    Guarantees no passwords or credentials are included.
    """
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "database_load_report.json"
    csv_path = out_dir / "database_load_report.csv"

    # JSON report
    serializable = [asdict(r) for r in results]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, default=str)

    # CSV report
    csv_rows = []
    for r in results:
        csv_rows.append({
            "source_dataset": r.source_dataset,
            "target_table": r.target_table,
            "database_status": r.database_status,
            "connection_status": r.connection_status,
            "rows_attempted": r.rows_attempted,
            "rows_inserted": r.rows_inserted,
            "rows_failed": r.rows_failed,
            "columns_count": len(r.columns_created),
            "load_mode": r.load_mode,
            "duration_seconds": r.duration_seconds,
            "error_message": r.error_message or "",
        })

    csv_df = pd.DataFrame(csv_rows)
    csv_df.to_csv(csv_path, index=False)

    return json_path, csv_path


def log_database_event(
    result: DatabaseLoadResult, log_dir: Path | str = "logs"
) -> None:
    """
    Append database operation outcomes to logs/database.log without exposing credentials.
    """
    logs_path = Path(log_dir)
    logs_path.mkdir(parents=True, exist_ok=True)
    log_file = logs_path / "database.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clean_err = re.sub(r"\s+", " ", (result.error_message or "").strip())
    detail = (
        f"Rows: {result.rows_inserted}/{result.rows_attempted} inserted | "
        f"Mode: {result.load_mode} | Duration: {result.duration_seconds}s"
        if result.database_status == "SUCCESS"
        else f"Reason: {clean_err}"
    )

    log_entry = (
        f"[{timestamp}] [{result.database_status}] Dataset: {result.source_dataset} -> "
        f"Table: {result.target_table} | {detail}\n"
    )

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)


def format_database_summary(result: DatabaseLoadResult) -> str:
    """
    Format human-readable summary of database loading for terminal display.
    """
    lines: list[str] = [
        f"Dataset: {result.source_dataset} -> Target Table: '{result.target_table}'",
        f"Database Status: {result.database_status} (Connection: {result.connection_status})",
    ]

    if result.database_status == "SUCCESS":
        lines.extend([
            f"  * Rows Inserted: {result.rows_inserted:,} / {result.rows_attempted:,}",
            f"  * Load Mode: {result.load_mode}",
            f"  * Duration: {result.duration_seconds}s",
            f"  * Schema Columns: {', '.join(result.columns_created)}",
        ])
    elif result.database_status in ["DATABASE_UNAVAILABLE", "CONFIG_MISSING"]:
        lines.append(f"  * Note: {result.error_message}")
    else:
        lines.extend([
            f"  * Rows Failed: {result.rows_failed:,}",
            f"  * Error: {result.error_message}",
        ])

    return "\n".join(lines)
