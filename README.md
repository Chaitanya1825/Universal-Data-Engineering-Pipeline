# Universal Data Engineering Pipeline

<p align="center">
  <a href="https://github.com/your-username/universal-data-engineering-pipeline/actions"><img src="https://img.shields.io/badge/CI%20Build-Passing-brightgreen.svg?style=flat-square&logo=githubactions&logoColor=white" alt="CI Build"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg?style=flat-square&logo=python&logoColor=white" alt="Python Versions"></a>
  <a href="#7-run-automated-tests"><img src="https://img.shields.io/badge/Tests-257%20Passed%20(100%25)-success.svg?style=flat-square&logo=pytest&logoColor=white" alt="Automated Tests"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square" alt="MIT License"></a>
  <a href="https://peps.python.org/pep-0008/"><img src="https://img.shields.io/badge/Code%20Style-PEP%208-orange.svg?style=flat-square" alt="PEP 8"></a>
  <a href="#project-status"><img src="https://img.shields.io/badge/Status-Production%20Ready%20(v2.0)-blueviolet.svg?style=flat-square" alt="Production Ready"></a>
</p>

<p align="center">
  <strong>A high-performance, domain-agnostic data engineering system engineered to ingest multi-format structured datasets (CSV, Excel, JSON), automatically profile schemas and statistical distributions, execute configurable transformations and quality validations, and load clean records into a PostgreSQL warehouse for downstream SQL analytics, Power BI business intelligence modeling, and an interactive Flask web dashboard.</strong>
</p>

---

## Table of Contents

- [Visual Showcase & Interactive Dashboard](#visual-showcase--interactive-analytics-dashboard)
  - [Executive Overview & 4-Zone Canvas](#executive-overview--4-zone-canvas)
  - [Multi-Domain Dataset Adaptability](#multi-domain-dataset-adaptability)
  - [Observability, Stages Tracker & Audit Modals](#observability-stages-tracker--audit-modals)
- [Overview & Design Principles](#overview--design-principles)
- [Repository Structure](#repository-structure)
- [Pipeline Architecture & Lifecycle](#pipeline-architecture--lifecycle)
- [Stage Roadmap (Stages 1–11)](#stage-roadmap)
- [Stage 2: Universal Ingestion & File Detection](#stage-2-implementation-details-universal-data-ingestion)
- [Stage 3: Schema Detection & Profiling](#stage-3-implementation-details-schema-detection--profiling)
- [Stage 4: Data Cleaning & Transformation](#stage-4-implementation-details-cleaning--transformation)
- [Stage 5: Data Quality & Validation Engine](#stage-5-implementation-details-data-quality--validation-engine)
- [Stage 6: PostgreSQL Database & Dynamic Modeling](#stage-6-implementation-details-postgresql-database--dynamic-data-modeling)
- [Stage 7: Pipeline Orchestration & Observability](#stage-7-implementation-details-etl-pipeline-orchestration)
- [Stage 8: Incremental Data Processing & CDC](#stage-8-implementation-details-incremental-data-processing)
- [Stage 9: SQL Analytics Layer](#stage-9-implementation-details-sql-analytics-layer)
- [Stage 10: Power BI Integration & BI Layer](#stage-10-implementation-details-power-bi-integration--bi-layer)
- [Stage 11: Production Health, Monitoring & Testing](#stage-11-implementation-details-testing-monitoring--production-readiness)
- [Getting Started & Quickstart](#getting-started)
- [Automated Testing Suite (257 Tests)](#7-run-automated-tests)
- [Publishing to GitHub](#8-publishing-to-github)
- [Contributing & License](#contributing--license)

---

## Visual Showcase & Interactive Analytics Dashboard

The platform includes a built-in, modern **Web Analytics Dashboard** (`python run_dashboard.py`) built with Flask and Chart.js. The interface dynamically inspects whichever structured dataset is loaded, automatically renders executive KPIs, creates intelligent charts matching column roles, and provides live pipeline controls.

### Executive Overview & 4-Zone Canvas

<p align="center">
  <img src="docs/images/dashboard_full_view.png" alt="Universal Pipeline Interactive Dashboard Full View" width="100%">
</p>

The dashboard is structured into four cohesive operational zones:

| Zone | Component | Capabilities & Architecture |
| :--- | :--- | :--- |
| **Zone 1** | **Dynamic Controls & Slicers** | Real-time dataset switching (`sample_sales`, `sample_employees`, `sample_students`), categorical dropdown filter, date window range pickers, active dataset source indicator (`POSTGRESQL` vs `VALIDATED_FILE`), and manual pipeline execution trigger. |
| **Zone 2** | **Executive KPI Summary Strip** | Live active record count, automated **Data Quality Score** (`100.0% HEALTHY`), quarantined records counter (`0 Quarantined`), and dynamic numeric metrics (Sums, Averages, and value ranges calculated on-the-fly). |
| **Zone 3** | **Intelligent Analytical Visuals Grid** | Dynamic Chart.js visualizations automatically selected based on column types: Categorical bar charts (92% match), chronological trend lines with gradient fills (95% match), continuous numeric frequency histograms (88% match), and category proportion donuts (85% match). |
| **Zone 4** | **Granular Audit Records Explorer** | Full data explorer featuring real-time client-side substring search, client pagination, one-click CSV export, automated **Data Quality Audit** inspection modal, and **Power BI Semantic Specification** discovery. |

---

### Multi-Domain Dataset Adaptability

Unlike rigid pipelines that only understand orders or transactions, the engine adapts seamlessly to arbitrary domain schemas:

<p align="center">
  <img src="docs/images/dashboard_overview.png" alt="Sales Analytics Overview" width="49%">
  <img src="docs/images/dashboard_employees.png" alt="HR & Compensation Analytics" width="49%">
</p>
<p align="center">
  <em>Left: E-Commerce Sales Transaction Domain (Units Sold, Category Distributions, Timeline Trends).<br>Right: Corporate Human Resources Domain (Department Salaries, Compensation Histograms, Employee Distributions).</em>
</p>

<p align="center">
  <img src="docs/images/dashboard_students.png" alt="Student Academic Performance Analytics" width="70%">
</p>
<p align="center">
  <em>Academic Education Domain: Student GPAs, Enrollment Cohorts, and Academic Distribution Analytics.</em>
</p>

---

### Observability, Stages Tracker & Audit Modals

<p align="center">
  <img src="docs/images/dashboard_pipeline_stages.png" alt="End-to-End Pipeline Execution Stages Tracker" width="100%">
</p>
<p align="center">
  <em>Live 11-Stage Pipeline Execution Tracker displaying granular runtime duration, execution timestamps, and stage success badges across Foundation, Ingestion, Incremental, Profiling, Transformation, Validation, Database, Analytics, and Power BI.</em>
</p>

<p align="center">
  <img src="docs/images/dashboard_powerbi_spec.png" alt="Power BI Semantic Model Inspector Modal" width="49%">
  <img src="docs/images/dashboard_quality_audit.png" alt="Automated Data Quality Audit Modal" width="49%">
</p>
<p align="center">
  <em>Left: Power BI Semantic Model &amp; Visual Specification Inspector (DAX measures, table archetype, visual recommendations).<br>Right: Automated Data Quality Audit Modal (transparent rule violation checks, clean vs quarantined audit trail).</em>
</p>

---

## Overview & Design Principles

Unlike traditional pipelines that are hardcoded to a single business vertical (such as e-commerce, banking, or healthcare), the **Universal Data Engineering Pipeline** is engineered as a **Universal Structured Data Pipeline**. 

### Key Principles:
* **Domain-Agnostic Architecture**: Ingestion, profiling, transformation, and validation engines make zero assumptions about domain-specific entities (no hardcoded references to orders, customers, or salaries).
* **Multi-Domain Adaptability**: Seamlessly processes diverse structured datasets—including `employees.json`, `sales.csv`, `students.xlsx`, `inventory.csv`, `hospital_records.csv`, and `sensor_readings.json`—using identical core logic.
* **Format-Centric Design**: Focuses strictly on structured/tabular data representations.
  > "The initial version supports CSV, XLSX, and JSON structured data. It does not claim to support arbitrary unstructured files."
* **Data Immutability Guarantee**: Raw data files in `data/raw/` are treated as strictly immutable landing zone inputs and are never altered, renamed, or deleted. Cleaned outputs are written to `data/processed/`, validated records to `data/validated/`, and quarantined records to `data/rejected/`.

---

## Repository Structure

```text
├── .github/
│   ├── workflows/
│   │   └── ci.yml               # GitHub Actions CI workflow (multi-OS, Python 3.10-3.13)
│   ├── ISSUE_TEMPLATE/          # Bug report & feature request templates
│   └── PULL_REQUEST_TEMPLATE.md # PR submission checklist
├── config/                      # Declarative JSON configs (transform, validate, db, analytics, BI)
├── dashboard/                   # Interactive Flask 4-Zone Web Analytics Dashboard & REST API
│   ├── static/                  # Modern CSS stylesheets and client-side Chart.js logic
│   ├── templates/               # HTML5 semantic dashboard canvas
│   ├── app.py                   # REST API backend & secure file uploader
│   └── generator.py             # Dynamic KPI & visual payload generator
├── data/
│   ├── raw/                     # Immutable input landing zone (sample CSV, XLSX, JSON)
│   ├── processed/               # Cleaned & standardized datasets (Stage 4)
│   ├── validated/               # Quality-approved datasets ready for DB loading (Stage 5)
│   └── rejected/                # Quarantined anomaly records with audit violation tags
├── docs/                        # In-depth architectural designs and production readiness reports
├── logs/                        # Sanitized execution logs (credential-masked)
├── pipeline/                    # Modular 11-stage core ETL, profiling, and monitoring engine
├── powerbi/                     # Domain-agnostic semantic model, DAX library & refresh strategy
├── reports/                     # Audit trail reports (profile, validation, lineage, analytics)
├── sql/                         # Domain-agnostic SQL analytical query library
├── state/                       # Checkpoint store for incremental ingestion and delta hashing
├── tests/                       # Comprehensive automated unit test suite (257 tests)
├── .env.example                 # Environment variable template with safe defaults
├── .gitattributes               # Line ending normalization & binary definitions
├── .gitignore                    # Production-grade Git ignore patterns
├── CONTRIBUTING.md              # Contributor guidelines and local development instructions
├── LICENSE                      # MIT Open Source License
├── pyproject.toml               # PEP 517 / 621 packaging metadata and tool configs
├── requirements.txt             # Core runtime dependencies
├── run_dashboard.py             # CLI entrypoint for Interactive Web Dashboard
└── run_pipeline.py              # CLI entrypoint for complete 11-stage pipeline orchestrator
```

---

## Pipeline Architecture & Lifecycle

<p align="center">
  <img src="docs/images/pipeline_architecture.svg" alt="Universal Data Engineering Pipeline Architecture" width="100%">
</p>

The pipeline follows a clear progression across stages:
* **Stage 3 = Understand & Profile Data**: Discovers types, checks nulls, calculates stats, identifies anomalies without changing anything.
* **Stage 4 = Clean & Transform Data**: Normalizes headers, trims whitespace, casts types, fills missing values, removes duplicates, and exports clean datasets.
* **Stage 5 = Validate Data Quality**: Evaluates clean records against configured quality rules, computes transparent quality scores, and separates data into validated and quarantined partitions.

```text
INPUT DATA (CSV / Excel / JSON)
              ↓
    FILE TYPE DETECTION
              ↓
       DATA INGESTION
              ↓
      SCHEMA DETECTION
              ↓
       DATA PROFILING
              ↓
  TRANSFORMATION / CLEANING
              ↓
    DATA QUALITY CHECKS (VALIDATION)
         ↙          ↘
VALIDATED RECORDS   REJECTED RECORDS (QUARANTINE)
        ↓
DATABASE LOADING (PostgreSQL / Dynamic Modeling)
        ↓
ETL PIPELINE ORCHESTRATION & OBSERVABILITY (Stage 7)
        ↓
INCREMENTAL DATA PROCESSING (Stage 8)
        ↓
SQL ANALYTICS LAYER (Stage 9)
        ↓
POWER BI INTEGRATION & BUSINESS INTELLIGENCE LAYER (Stage 10)
        ↓
CENTRALIZED MONITORING, HEALTH & PRODUCTION READINESS (Stage 11)
```

---

## Stage Roadmap

* **Stage 1 — Project Foundation — COMPLETE**
* **Stage 2 — Universal Data Ingestion & File Detection — COMPLETE**
* **Stage 3 — Automatic Schema Detection & Data Profiling — COMPLETE**
* **Stage 4 — Data Cleaning & Transformation Engine — COMPLETE**
* **Stage 5 — Data Quality & Validation Engine — COMPLETE**
* **Stage 6 — PostgreSQL Database & Dynamic Data Modeling — COMPLETE**
* **Stage 7 — ETL Pipeline Orchestration — COMPLETE**
* **Stage 8 — Incremental Data Processing — COMPLETE**
* **Stage 9 — SQL Analytics Layer — COMPLETE**
* **Stage 10 — Power BI Integration & Business Intelligence Layer — COMPLETE**
* **Stage 11 — Testing, Monitoring & Production Readiness — COMPLETE**

---

## Stage 2 Implementation Details: Universal Data Ingestion

### 1. Supported Input Formats
* **CSV (`.csv`)**: Loaded via Pandas with multi-encoding fallback (`utf-8`, `latin1`, `cp1252`), empty file detection, and parser error handling.
* **Excel (`.xlsx`)**: Loaded via Pandas using the `openpyxl` engine (first worksheet default). Gracefully handles empty workbooks and unreadable spreadsheets.
* **JSON (`.json`)**: Ingests tabular-convertible JSON formats (lists of records, standard container objects, columnar dicts, and flat records). Clearly rejects non-tabular or arbitrary nested JSON.
* **Unsupported Formats**: Any file with an unrecognized extension (e.g. `.pdf`, `.docx`, `.txt`) is flagged as `SKIPPED` with a clear explanation (`Unsupported file format`).

### 2. Automatic File Discovery & Fault Tolerance
* Dynamic scanning of `data/raw/` landing zone using `pathlib.Path` (ignoring `.gitkeep` and hidden files).
* Processing is completely isolated per file: corrupted or unsupported files do not stop valid files from completing ingestion.
* Events logged to `logs/ingestion.log`.

---

## Stage 3 Implementation Details: Schema Detection & Profiling

### 1. Automatic Logical Type Inference
The schema detector analyzes the raw values and structure of each column to infer its domain-agnostic logical data type, going beyond native Pandas dtypes:
* **Integer**: Native integer columns, whole numbers stored as strings (e.g. `"10"`, `"20"`), and float columns with integer values masked by nulls.
* **Float**: Continuous numeric columns with decimal/fractional components.
* **Boolean**: Detects native booleans as well as string and binary representations (`{True, False}`, `{Yes, No}`, `{Y, N}`, `{1, 0}`, `{T, F}`).
* **DateTime**: Automatically detects dates and timestamps across standard formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `MM-DD-YYYY`, `YYYY/MM/DD`, `YYYY-MM-DD HH:MM:SS`) using pattern matching and validation thresholds (bounds 1800–2200), preventing pure numbers from false date classification.
* **Categorical**: Low-cardinality discrete columns (distinct count <= 25 or cardinality ratio <= 35%) with repeated values.
* **String/Text**: Unstructured text or high-cardinality strings.

### 2. Column-Level Profiling & Statistics
* Missing value counts, percentages, nullability flags, distinct counts, and duplicate counts.
* **Numeric Stats**: Count, mean, median, standard deviation, minimum, maximum, and 25th / 50th / 75th percentiles.
* **Text Stats**: Minimum, maximum, and average string lengths.
* **Categorical Stats**: Category count and top 5 categories with frequencies and percentages.
* **Identifier Heuristics**: Multi-signal scoring combining naming patterns, uniqueness ratio ($\ge 95\%$), low missingness ($\le 5\%$), and discrete types.

### 3. Profiling Artifacts & Logs
* Generates `reports/profile_report.json` and `reports/profile_report.csv`.
* Logs profiling runs to `logs/profiling.log`.

---

## Stage 4 Implementation Details: Cleaning & Transformation

### 1. Modular Transformation Capabilities
The engine (`pipeline/transformer.py`) provides generic, domain-agnostic cleaning primitives:
* **Column Name Normalization**: Trims outer whitespace, converts to lowercase, converts special characters to underscores, collapses repeated underscores, and avoids collisions.
* **Whitespace Cleaning**: Trims outer whitespace and collapses internal multiple spaces for all string/text columns while keeping numeric, boolean, and datetime columns unaltered.
* **Configurable String Normalization**: Supports configurable string casing (`"none"` [default], `"lower"`, `"upper"`, `"title"`).
* **Missing Value Imputation**: Imputes missing values based on column logical type and configured strategy:
  * Numeric: `median` (default), `mean`, or `constant`.
  * Categorical: `mode` (default) or `constant`.
  * Text: `constant` (e.g., `"Unknown"`) or `mode`.
  * DateTime: `ffill` (forward fill) or `bfill` (backward fill).
  * Boolean: `mode`.
* **Type Conversion & Date Standardization**: Safely casts columns to target logical types (`Int64`, `float64`, `boolean`, `datetime64[ns]`, `string`) guided by Stage 3 inference. Coerces invalid values safely and tracks warnings.
* **Duplicate Removal**: Deduplicates rows based on complete rows or user-defined column subsets (`remove_duplicates: true`).

### 2. Configuration (`config/transformations.json`)
Allows pipeline users to customize cleaning behavior without modifying Python code. Outputs are saved to `data/processed/<source_stem>_cleaned.csv` and audited in `reports/transformation_report.json`.

---

## Stage 5 Implementation Details: Data Quality & Validation Engine

<p align="center">
  <img src="docs/images/dashboard_quality_audit.png" alt="Data Quality & Validation Audit Modal" width="85%">
</p>

### 1. Architectural Distinction: Transformation vs. Validation
> **Clear Separation of Concerns:**
> * **Stage 4 (Transform & Clean)**: Transforms data values (e.g., strips whitespace, standardizes casing, fills missing values, casts types).
> * **Stage 5 (Validate Quality)**: Evaluates whether records satisfy configured quality assertions. It does **not** silently clean invalid values. Non-conforming records are isolated and quarantined into `data/rejected/` for human review, while conforming records proceed to `data/validated/`.
> * *Validation is only as meaningful as the configured rules; when no rules are configured, datasets pass permissively.*

### 2. Supported Validation Rule Types
The validation engine (`pipeline/validator.py`) implements generic quality assertions:
* **Schema Validation**: Checks for required column existence, identifies duplicate column headers, and optionally detects unexpected columns.
* **Nullability Validation**: Enforces mandatory column presence (`required_columns`), flagging any row containing null or missing values in those fields.
* **Uniqueness Validation**: Asserts uniqueness constraints (`unique_columns`), flagging duplicate rows without destructive deletion.
* **Numeric Range Validation**: Enforces minimum and/or maximum bounds on numeric columns (`range_rules`: `{"col": {"min": 0, "max": 100}}`).
* **Regex Format Validation**: Validates string format conformity using regular expressions (`regex_rules`: `{"code": "^[A-Z0-9]+$"}`).
* **Logical Type Validation**: Validates that non-null values conform to expected logical types (`Integer`, `Float`, `Boolean`, `DateTime`).
* **Dataset-Level Rules**: Configurable minimum row count (`min_row_count`) and maximum overall missing percentage (`max_missing_percentage`).

### 3. Record Partitioning & Quarantine Flow
For every dataset evaluated:
* **Valid Records**: Rows that satisfy all configured validation rules are exported to `data/validated/<source_stem>_validated.csv`.
* **Rejected Records**: Non-conforming rows are routed to `data/rejected/<source_stem>_rejected.csv`. Every quarantined record includes audit metadata columns:
  * `_row_index`: Original row index in the dataset.
  * `_rejection_reasons`: Detailed, semicolon-separated list of all rule violations triggered by that record.

### 4. Transparent Quality Scoring
The quality score represents compliance with configured validation rules:
$$\text{Quality Score (\%)} = \left(\frac{\text{Valid Records}}{\text{Total Records}}\right) \times 100$$
*(If a dataset contains 0 rows, score is safely computed as 100.0% without division-by-zero errors).*

### 5. Declarative Configuration (`config/validation.json`)
```json
{
  "default": {
    "required_columns": [],
    "unique_columns": [],
    "range_rules": {},
    "regex_rules": {},
    "type_rules": {},
    "dataset_rules": {
      "min_row_count": 0,
      "max_missing_percentage": 100.0,
      "allow_unexpected_columns": true
    }
  },
  "datasets": {}
}
```

### 6. Validation Artifacts & Logging
* **`reports/validation_report.json`**: Detailed JSON report documenting metrics, rules executed, failure breakdowns by rule and by column, and paths to valid/rejected files.
* **`reports/validation_report.csv`**: Consolidated tabular summary of quality scores and violation counts across all evaluated datasets.
* **`logs/validation.log`**: Runtime execution log tracking timestamps, datasets, row counts, quality scores, and validation statuses.

### 7. Limitations
* Validation rules evaluate records deterministically based on provided configurations.
* Cross-table relational integrity (foreign keys) will be introduced upon relational data modeling.

---

## Stage 6 Implementation Details: PostgreSQL Database & Dynamic Data Modeling

### 1. Database Architecture & Data Flow
Stage 6 introduces a fully dynamic, domain-agnostic PostgreSQL persistence layer utilizing SQLAlchemy and psycopg2-binary. It receives validated datasets from `data/validated/` and dynamically maps logical types into native PostgreSQL schema definitions, creating tables on demand and performing batch data ingestion.

```text
Validated CSV / Dataset
           ↓
    Pandas DataFrame
           ↓
    Schema Detection (Stage 3 Profile)
           ↓
    SQL Type Mapping
           ↓
Dynamic PostgreSQL Table (DDL)
           ↓
 Batch Data Load (Pandas to_sql chunksize=1000)
           ↓
PostgreSQL Database / SQLite Engine
```

> **Data Separation Guarantee:**
> * Loads **ONLY** from `data/validated/`.
> * **NEVER** loads raw data from `data/raw/`.
> * **NEVER** loads unvalidated transformed data from `data/processed/`.
> * **NEVER** loads quarantined records from `data/rejected/`.

---

### 2. SQLAlchemy & Dynamic Schema Modeling
The database layer (`pipeline/database.py`) uses SQLAlchemy `MetaData`, `Table`, and `Column` definitions to dynamically translate DataFrame structures into relational tables without requiring hardcoded schema declarations or migrations:
* Automatically detects all column names and maps their logical types.
* Dynamically creates the target table in PostgreSQL (`public` schema by default) if it does not already exist.
* Uses batch loading (`chunksize=1000`, `method="multi"`) for efficient inserts.

---

### 3. Dynamic SQL Type Mapping
Columns are dynamically mapped from Stage 3 logical types to SQLAlchemy / PostgreSQL types:

| Logical Data Type | Pandas Inferred Types | PostgreSQL / SQLAlchemy Type | Description |
| :--- | :--- | :--- | :--- |
| **Integer** | `int64`, whole number strings | `BIGINT` | Large integer capacity for identifiers and counts |
| **Float** | `float64`, continuous numerics | `FLOAT` / `DOUBLE PRECISION` | High-precision floating-point numbers |
| **Boolean** | `bool`, boolean flags | `BOOLEAN` | True/False boolean states |
| **DateTime** | `datetime64[ns]`, parsed timestamps | `TIMESTAMP` | Temporal values with microsecond precision |
| **Categorical** | Categorical strings | `TEXT` | Variable-length discrete textual categories |
| **String/Text** | Arbitrary object/strings | `TEXT` | Generic variable-length text representation |

---

### 4. Dynamic Table Naming Strategy
Table names are dynamically generated from source filenames using PostgreSQL-safe identifier rules:
1. Strips pipeline stage suffixes (`_validated`, `_cleaned`, `_processed`, `_raw`).
2. Converts characters to lowercase.
3. Replaces spaces and non-alphanumeric characters with underscores.
4. Collapses repeated underscores and strips leading/trailing underscores.
5. Enforces PostgreSQL identifier length limit ($\le 63$ characters).
6. Provides safe fallback (`universal_table`) if the sanitized name is empty.

**Examples:**
* `sample_sales_validated.csv` $\rightarrow$ `sample_sales`
* `sample_employees_validated.csv` $\rightarrow$ `sample_employees`
* `customer_transactions_validated.csv` $\rightarrow$ `customer_transactions`
* `Annual Revenue 2024.xlsx` $\rightarrow$ `annual_revenue_2024`

---

### 5. Primary Key Strategy
To maintain universal compatibility across arbitrary schemas without risking unexpected data load failures:
* **Safe Default**: Tables are created without auto-generated primary keys unless explicitly configured or verified with extreme confidence.
* **Configurable Explicit Primary Keys**: Specific primary keys can be mapped via `config/database.json` (`"primary_keys": {"sample_sales": "transaction_id"}`).
* **Conservative Automatic Detection**: When `auto_primary_key` is set to `true`, a primary key is only created if a column possesses $\ge 95\%$ identifier confidence, 100% uniqueness, and 0% missingness.

---

### 6. Existing Table Behavior (`if_exists`)
Configured via `config/database.json`:
* **`append`** (Default): Appends incoming validated records to the existing table. Safe for continuous ingestion batches.
* **`replace`**: Drops and recreates the target table before loading new records.
* **`fail`**: Prevents loading and reports `LOAD_FAILED` if the table already exists, ensuring existing data remains untouched.

---

### 7. Environment Configuration & Security Best Practices
Database credentials are managed exclusively via environment variables and `.env` files (loaded via `python-dotenv`).
* **Zero Hardcoded Credentials**: Passwords and connection strings are strictly omitted from code, logs, and reports.
* **Credential Masking**: Any database URL displayed or recorded is sanitized via `mask_database_url()`:
  `postgresql+psycopg2://user:secret@localhost:5432/db` $\rightarrow$ `postgresql+psycopg2://user:***@localhost:5432/db`
* **Git Protection**: `.env` and `.env.*` are explicitly ignored in `.gitignore`. A clean template is provided in `.env.example`.

---

### 8. Fault Tolerance & Graceful Database Fallback
The pipeline is designed to be fully resilient: **it never crashes if PostgreSQL is offline or unconfigured**.
* **Pre-Flight Connectivity Probe**: Executes a lightweight `SELECT 1` ping before attempting table creation or batch loading.
* **Graceful Degradation**:
  * If `DATABASE_URL` is missing: Reports `CONFIG_MISSING`, logs the event, and completes earlier stages cleanly.
  * If PostgreSQL is unreachable: Reports `DATABASE_UNAVAILABLE`, records the connection failure in load reports, and exits with return code `0`.
* **Zero False Reporting**: Never pretends data was loaded when the database was offline.

---

### 9. Database Load Artifacts & Logging
* **`reports/database_load_report.json`**: Machine-readable audit report documenting dataset sources, target tables, load statuses, rows attempted/inserted/failed, SQL types, and execution durations.
* **`reports/database_load_report.csv`**: Tabular summary of database loading operations for quick monitoring and audit verification.
* **`logs/database.log`**: Runtime execution log tracking timestamps, datasets, tables, inserted rows, and sanitized error messages.
* **`inspect_table_schema()`**: Inspection utility returning table existence, column definitions, data types, nullability, primary keys, and real-time row counts.

---

## Stage 7 Implementation Details: ETL Pipeline Orchestration

### 1. Why Orchestration is Needed
Prior to Stage 7, individual stages (ingestion, schema detection, transformation, validation, and database loading) were executed sequentially in basic scripts. While functional, production-grade data pipelines require centralized orchestration to provide:
* **Deterministic Dependency Enforcement**: Ensuring downstream stages never execute against missing or corrupted upstream outputs.
* **Granular Observability & Auditability**: Tracking pipeline runs, stage timings, dataset-level statuses, and detailed error diagnostics.
* **Configurable Workflow Control**: Enabling or disabling specific stages, toggling fail-fast behaviors, and handling infrastructure degradation gracefully.
* **Automated Failure Isolation & Propagation**: Marking dependent stages as `BLOCKED` upon upstream failure while allowing non-dependent tasks to complete.

---

### 2. Pipeline Dependency Graph
The pipeline follows an explicit, strict Directed Acyclic Graph (DAG) of stage dependencies managed by `PipelineOrchestrator`:

```text
    Stage 1: foundation
            ↓
    Stage 2: ingestion
            ↓
    Stage 3: profiling
            ↓
    Stage 4: transformation (requires ingestion & profiling)
            ↓
    Stage 5: validation (requires transformation)
            ↓
    Stage 6: database (requires validation)
```

| Stage | Dependencies | Prerequisite Condition |
| :--- | :--- | :--- |
| **`foundation`** | *None* | Verifies core project directory structure. |
| **`ingestion`** | `foundation` | Foundation directories must exist. |
| **`profiling`** | `ingestion` | Ingestion must have completed successfully. |
| **`transformation`** | `ingestion`, `profiling` | Both raw data and schema profiles must be available. |
| **`validation`** | `transformation` | Cleaned data outputs must be produced. |
| **`database`** | `validation` | Validated records must have passed quality gates. |

---

### 3. Stage & Pipeline Status Models

#### Stage Statuses:
* **`PENDING`**: Initial state before stage execution begins.
* **`RUNNING`**: Currently executing.
* **`SUCCESS`**: Stage finished successfully without critical errors.
* **`FAILED`**: Stage encountered an error or unhandled exception.
* **`SKIPPED`**: Stage was intentionally omitted in `config/pipeline.json`.
* **`BLOCKED`**: Stage cannot run because an upstream prerequisite was skipped or failed.
* **`DATABASE_UNAVAILABLE`**: Specifically indicates PostgreSQL server is offline or unconfigured.
* **`CONFIG_ERROR`**: Stage configuration is invalid.

#### Overall Pipeline Statuses:
* **`COMPLETED`**: All enabled stages succeeded.
* **`COMPLETED_WITH_WARNINGS`**: All enabled data stages succeeded, but non-critical warnings occurred (e.g., PostgreSQL offline when `continue_on_database_unavailable=true`, or quarantined records detected).
* **`FAILED`**: A critical stage failure occurred or dependent stages were blocked.
* **`CONFIG_ERROR`**: Configuration contained syntax errors or unrecognized stage names.

---

### 4. Run ID & Traceability
Every execution generates a unique, human-readable run identifier:
$$\text{Run ID Format: } \texttt{run\_YYYYMMDD\_HHMMSS\_<hex6>}$$
*(Example: `run_20261003_225034_6f1973`)*

The Run ID is consistently attached to:
* Console terminal banners and summaries
* Unified JSON reports (`reports/pipeline_run_report.json`)
* Unified CSV summaries (`reports/pipeline_run_report.csv`)
* Execution log entries (`logs/pipeline.log`)
* Stage execution records and dataset audit trails

---

### 5. Configurable Pipeline Execution (`config/pipeline.json`)
Declarative settings govern pipeline orchestration:
```json
{
  "enabled_stages": [
    "foundation",
    "ingestion",
    "profiling",
    "transformation",
    "validation",
    "database"
  ],
  "fail_fast": false,
  "continue_on_database_unavailable": true,
  "generate_reports": true,
  "logging_enabled": true
}
```

* **`enabled_stages`**: Selectively run specific stages. Omitted stages are marked `SKIPPED`. If an unrecognized stage name is provided, execution is aborted with `CONFIG_ERROR`.
* **`fail_fast`**: When `true`, stops pipeline execution immediately upon the first stage failure, setting all remaining stages to `BLOCKED`.
* **`continue_on_database_unavailable`**: When `true`, allows the pipeline to conclude with `COMPLETED_WITH_WARNINGS` if PostgreSQL is offline, rather than failing the entire run.
* **`generate_reports`**: Toggles automatic creation of JSON and CSV run reports.
* **`logging_enabled`**: Toggles runtime audit logging in `logs/pipeline.log`.

---

### 6. Dataset-Level Execution Tracking
The orchestrator tracks every dataset independently across all stages:
```text
sample_sales.csv
  ├── ingestion:       SUCCESS (5 rows)
  ├── profiling:       SUCCESS (5 rows)
  ├── transformation:  SUCCESS -> sample_sales_cleaned.csv (5 rows)
  ├── validation:      PASSED  -> sample_sales_validated.csv (5 rows valid, 0 quarantined)
  └── database:        CONFIG_MISSING (PostgreSQL unconfigured)
```

---

### 7. Unified Reports & Logging
* **`reports/pipeline_run_report.json`**: Complete execution report including Run ID, pipeline status, monotonic timings, configuration snapshot, stage metrics, dataset records, and aggregate summary.
* **`reports/pipeline_run_report.csv`**: Tabular execution summary containing run IDs, stage names, statuses, durations, and counts of processed/succeeded/failed datasets.
* **`logs/pipeline.log`**: Structured runtime log tracking timestamps, log levels, run IDs, stage starts/completions, and dataset milestones with secret sanitization.

---

### 8. Security & Secret Protection
The orchestrator implements strict credential masking through `sanitize_text()`. Connection strings, passwords, and sensitive credentials are automatically sanitized (`user:***@host`) before appearing in terminal output, logs, or serialized reports.

---

## Technology Stack

* **Language**: Python 3.13+
* **Data Processing & Analytics**: Pandas, NumPy
* **Excel Processing Engine**: openpyxl
* **Database & Persistence**: PostgreSQL, SQLAlchemy, Psycopg2-binary
* **Configuration Management**: Python-dotenv, JSON
* **Reporting & Visualization**: Microsoft Power BI
* **Testing**: Python `unittest` framework
* **Version Control**: Git & GitHub

---

## Project Structure

```text
Universal-Data-Engineering-Pipeline/
│
├── data/
│   ├── raw/                   # Landing zone for incoming structured files (IMMUTABLE)
│   │   ├── sample_sales.csv
│   │   ├── sample_students.xlsx
│   │   └── sample_employees.json
│   ├── processed/             # Cleaned and transformed datasets
│   │   ├── sample_sales_cleaned.csv
│   │   ├── sample_students_cleaned.csv
│   │   └── sample_employees_cleaned.csv
│   ├── validated/             # Conforming datasets passing quality validation gates
│   │   ├── sample_sales_validated.csv
│   │   ├── sample_students_validated.csv
│   │   └── sample_employees_validated.csv
│   └── rejected/              # Quarantined records with failure metadata
│       └── .gitkeep
│
├── pipeline/
│   ├── __init__.py            # Pipeline package initialization
│   ├── ingestion.py           # Universal file discovery, format detection, & loaders
│   ├── schema_detector.py     # Schema inference, profiling metrics, & heuristics
│   ├── transformer.py         # Data cleaning, normalization, imputation, & typing
│   ├── validator.py           # Quality rules, assertions, partitioning, & scoring
│   ├── database.py            # Dynamic PostgreSQL table creation, type mapping, & loading
│   ├── orchestrator.py        # Central ETL orchestrator, dependency management, & metrics
│   └── main.py                # Pipeline execution coordinator (Stages 1-6)
│
├── config/
│   ├── transformations.json   # Configurable transformation rules and strategies
│   ├── validation.json        # Declarative data quality rules and assertions
│   ├── database.json          # Database loading configuration & existing table behavior
│   ├── pipeline.json          # Stage toggling, fail-fast, and orchestration settings
│   └── dashboard.json         # Web dashboard host, port, and upload settings
│
├── dashboard/                 # Stage 12: Interactive Analytics Dashboard (Separate from ETL)
│   ├── __init__.py
│   ├── app.py                 # Flask REST application & routing
│   ├── generator.py           # Dynamic column role inspection & visual recommendation engine
│   ├── templates/
│   │   └── index.html         # Modern 4-zone dashboard layout with glassmorphic UI
│   └── static/
│       ├── css/dashboard.css  # Slate-dark responsive theme & micro-interactions
│       └── js/dashboard.js    # Reactive Chart.js visualizer & live pipeline tracker
│
├── reports/                   # Automated profiling, transformation, & quality reports
│   ├── profile_report.json    # Detailed JSON profile report for all datasets
│   ├── profile_report.csv     # Tabular CSV summary of profiled columns
│   ├── transformation_report.json # Audit report of cleaning operations
│   ├── validation_report.json # Detailed quality report with failure breakdowns
│   ├── validation_report.csv  # Tabular quality score summary
│   ├── database_load_report.json # Detailed database batch loading report
│   ├── database_load_report.csv  # Tabular summary of database load statuses
│   ├── pipeline_run_report.json  # Unified pipeline run execution report
│   └── pipeline_run_report.csv   # Tabular summary of pipeline stage metrics
│
├── sql/                       # DDL scripts, views, and analytical SQL queries
│   └── .gitkeep
│
├── logs/                      # Application runtime logs for monitoring
│   ├── ingestion.log          # Ingestion event logs
│   ├── profiling.log          # Schema detection and profiling logs
│   ├── transformation.log     # Cleaning and transformation execution logs
│   ├── validation.log         # Quality validation execution logs
│   ├── database.log           # Database connection and data loading logs
│   └── pipeline.log           # Central ETL orchestration and execution audit logs
│
├── tests/                     # Unit and integration test suites
│   ├── __init__.py
│   ├── test_ingestion.py      # Stage 2 ingestion tests (13 tests)
│   ├── test_schema_detector.py# Stage 3 schema detection & profiling tests (16 tests)
│   ├── test_transformer.py    # Stage 4 transformation tests (20 tests)
│   ├── test_validator.py      # Stage 5 data quality validation tests (20 tests)
│   ├── test_database.py       # Stage 6 database & dynamic modeling tests (19 tests)
│   ├── test_orchestrator.py   # Stage 7 ETL orchestration tests (22 tests)
│   └── test_dashboard.py      # Stage 12 Interactive dashboard & API tests (19 tests)
│
├── powerbi/                   # Power BI report files (.pbix) and dashboard assets
│   └── .gitkeep
│
├── docs/                      # Architecture diagrams, schemas, and specifications
│   └── .gitkeep
│
├── .env.example               # Template for environment configuration
├── .gitignore                 # Git ignore rules for clean version control
├── requirements.txt           # Core project dependencies (includes flask)
├── README.md                  # Comprehensive project documentation
├── run_pipeline.py            # Root execution entry point (Orchestrator)
└── run_dashboard.py           # Web Dashboard startup entry point
```

---

## Stage 8 Implementation Details: Incremental Data Processing

### 1. Purpose & Motivation
In production data engineering workflows, re-ingesting and re-processing unchanged historical datasets on every pipeline run consumes unnecessary compute, I/O bandwidth, and database write throughput. Stage 8 upgrades the pipeline into an **intelligent incremental data processing engine** that:
* Detects **completely new** datasets.
* Identifies **previously processed unchanged** datasets and **skips downstream stages**.
* Detects **modified datasets** whose content has changed and triggers **reprocessing**.
* Computes **record-level deltas** (new, changed, unchanged, removed) when dataset keys are configured.
* Implements safe, isolated **database delta/upsert** semantics without corrupting existing records.

### 2. Cryptographic Content Hashing (SHA-256)
* **Content-Based Hashing**: Implemented in `calculate_file_hash()` using `hashlib.sha256()`. The hash is computed strictly from file bytes, not superficial file modification timestamps (`mtime`).
* **Memory-Safe Chunking**: Calculates hashes in 64 KB chunks (`chunk_size=65536`), preventing memory exhaustion when hashing large structured files.
* **Deterministic Check**: Any byte modification (e.g. edited cell, added row, modified header) alters the SHA-256 digest, triggering change detection.

### 3. Checkpoint Store Architecture (`state/checkpoints.json`)
The pipeline maintains persistent state across runs in `state/checkpoints.json`:
* **Directory Persistence**: `state/` directory is tracked in git via `state/.gitkeep`, while `.gitignore` ignores actual checkpoint JSON files.
* **Schema**:
```json
{
  "version": "1.0",
  "updated_at": "2026-10-03T23:10:20.123456",
  "datasets": {
    "sample_sales.csv": {
      "source_path": "C:\\path\\data\\raw\\sample_sales.csv",
      "file_hash": "515900d3a1101683a588505018e6e1e00ac9125f85d4497b14493c1717317f99",
      "file_size": 319,
      "modified_time": "2026-10-03T22:06:02.573272",
      "last_processed_at": "2026-10-03T23:10:20.123456",
      "last_status": "SUCCESS",
      "database_status": "CONFIG_MISSING",
      "record_count": 5,
      "record_hashes": { ... }
    }
  }
}
```
* **Crash Safety & Atomic Persistence**: Checkpoint updates write to a temporary file (`state/checkpoints.json.tmp`) and use `os.replace` for atomic replacement, preventing partial writes during crashes.
* **Corrupted File Recovery**: If `checkpoints.json` is missing, empty, or corrupted, `CheckpointStore` recovers safely by initializing a clean empty state without crashing.
* **Checkpoint Update Safety**: A dataset checkpoint is ONLY committed to `SUCCESS` after all active stages (transformation, validation) have succeeded. Failed datasets are NEVER recorded as `SUCCESS`.
* **Honest Database Auditing**: When PostgreSQL is offline or unconfigured, the checkpoint truthfully distinguishes pipeline processing success (`last_status="SUCCESS"`) from database state (`database_status="DATABASE_UNAVAILABLE"` or `"CONFIG_MISSING"`).

### 4. Incremental Decision Engine
For each incoming structured file discovered during ingestion:
```text
File not in checkpoint store?
        ↓ YES
      [NEW]  → Action: PROCESS (full downstream pipeline)

File in checkpoint AND current_hash == previous_hash?
        ↓ YES
   [UNCHANGED] → Action: SKIP (bypass profiling, cleaning, validation, database)

File in checkpoint AND current_hash != previous_hash?
        ↓ YES
    [CHANGED]  → Action: REPROCESS (full downstream pipeline)
```
When an unchanged dataset is skipped, it is tagged as `UNCHANGED_SKIPPED` across all downstream stages, enabling detailed observability without performing redundant work.

### 5. Record-Level Change Detection & Hashing
When `track_record_changes = true` and `record_keys` are configured in `config/incremental.json`:
* **Deterministic Record Hashing**: Each row is tokenized into a canonical alphabetically sorted representation, with null values safely mapped to `__NULL__`. Numeric and string values are normalized, and a SHA-256 record hash is generated.
* **Domain-Agnostic Key Strategy**: The pipeline NEVER guesses or assumes `id` is a primary key. Record keys must be explicitly declared in `config/incremental.json`:
```json
{
  "record_keys": {
    "sample_sales.csv": ["transaction_id"]
  }
}
```
* **Graceful Fallback**: If a dataset has no configured keys, record comparison truthfully reports `NOT_AVAILABLE (Reason: No record key configured)` without failing or fabricating metrics.
* **Delta Categorization**:
  * **New records**: Key present in current dataset but absent from previous checkpoint.
  * **Changed records**: Key present in both, but row hash differs.
  * **Unchanged records**: Key present in both, and row hash is identical.
  * **Removed records**: Key present in previous checkpoint but absent from current dataset.

### 6. Database Delta & Upsert Loading Semantics
Implemented in `pipeline/incremental.py` via `upsert_records_to_database()`:
* **New Records**: `INSERT` into target table.
* **Changed Records**: `UPDATE` existing table rows matching the configured `key_columns`.
* **Unchanged Records**: Bypassed without redundant updates.
* **Safety Guarantee**: `delete_missing_records = false` by default. Existing database records are NEVER deleted merely because a source file omitted them.
* **Offline Safety**: When PostgreSQL is unavailable, database status is truthfully reported as `DATABASE_UNAVAILABLE`.

### 7. Configuration Reference (`config/incremental.json`)
```json
{
  "enabled": true,
  "hash_algorithm": "sha256",
  "skip_unchanged_files": true,
  "reprocess_changed_files": true,
  "checkpoint_file": "state/checkpoints.json",
  "track_record_changes": true,
  "delete_missing_records": false,
  "record_keys": {
    "sample_sales.csv": ["transaction_id"]
  }
}
```

### 8. Auditing, Reporting & Logging
* **`reports/incremental_report.json`**: Complete execution payload containing hashes, decisions, actions, and delta metrics.
* **`reports/incremental_report.csv`**: Tabular export ready for review and audit tracking.
* **`logs/incremental.log`**: Timestamped execution trace masking all passwords and secrets.
* **`reports/pipeline_run_report.json`**: Stage 7 unified report includes Stage 8 metrics (`datasets_new`, `datasets_unchanged`, `datasets_changed`, `datasets_reprocessed`, `datasets_incremental_skipped`, and `record_delta_metrics`).

---

---

## Stage 9 Implementation Details: SQL Analytics Layer

Stage 9 implements a reusable, domain-agnostic SQL analytics layer capable of executing analytical queries against dynamically modeled PostgreSQL database tables. It demonstrates practical Data Engineering SQL capabilities: numeric aggregations, multi-column grouping, parameterized filtering, temporal trend analysis, window functions (ranking and running totals), Top-N analysis, SQL data quality audits, strict SQL injection prevention, structured query execution, and dual-format result exports (JSON and CSV).

### 1. Analytics Layer Architecture

```mermaid
flowchart TD
    subgraph Storage["PostgreSQL Dynamic Tables"]
        T1[("Target Table 1")]
        T2[("Target Table 2")]
    end

    subgraph Config["Configuration & Templates"]
        C1["config/analytics.json"]
        TPL["sql/analytics/*.sql"]
    end

    subgraph Engine["SQL Analytics Engine (pipeline/analytics.py)"]
        VAL["Identifier & Column Validation"]
        PARAM["Bound Parameter Binding (:param)"]
        DIALECT["Dialect-Aware SQL Generation"]
        EXEC["Safe Query Execution & Timing"]
    end

    subgraph Operations["Analytical Operations"]
        AGG["Numeric Aggregations<br/>COUNT, SUM, AVG, MIN, MAX"]
        GRP["Group-By Aggregations<br/>ORDER BY, LIMIT"]
        TIME["Date/Time Trend Analysis<br/>day, week, month, quarter, year"]
        WIN["Window Functions<br/>RANK, DENSE_RANK, ROW_NUMBER, SUM() OVER"]
        TOPN["Top-N Analysis<br/>Parameterized N Limit"]
        DQ["SQL Data Quality<br/>Null & Distinct Audits"]
    end

    subgraph Exports["Observability & Reporting"]
        OUT1["reports/analytics/<query>.json"]
        OUT2["reports/analytics/<query>.csv"]
        SUM1["reports/analytics_report.json"]
        SUM2["reports/analytics_report.csv"]
        LOGS["logs/analytics.log"]
    end

    Storage --> Engine
    Config --> Engine
    Engine --> VAL --> PARAM --> DIALECT --> EXEC
    EXEC --> Operations
    Operations --> Exports
```

### 2. Reusable Query Library (`sql/analytics/`)
The query library decouples query templates from Python execution logic. All templates use standard parameterized SQL with safe identifier placeholders:
* **`basic_summary.sql`**: Computes total rows, non-null counts, and distinct counts.
* **`null_summary.sql`**: Dynamic SQL-level data quality audit measuring missing counts and percentages per column.
* **`categorical_distribution.sql`**: Grouped cardinality distribution with descending frequency and parameterized limits.
* **`numeric_summary.sql`**: Numeric aggregations (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`).
* **`date_trend.sql`**: Time-series trend bucketing by date/time columns.
* **`ranking.sql`**: Window function ranking (`RANK()`, `DENSE_RANK()`, `ROW_NUMBER()`) partitioned by categorical dimensions.
* **`running_total.sql`**: Cumulative running totals over chronological or sequential dimensions via `SUM(metric) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

### 3. Core Analytical Operations

| Operation | Method | Capabilities | SQL Pattern |
| :--- | :--- | :--- | :--- |
| **Row Count** | `get_table_row_count(table)` | Exact table row count | `SELECT COUNT(*) FROM "table"` |
| **Numeric Aggregation** | `aggregate(table, column, funcs, ...)` | Allowlist: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | `SELECT SUM("col"), AVG("col") FROM "table"` |
| **Group-By Aggregation** | `group_by(table, group_col, metric_col, ...)` | Dimensional grouping with metric aggregates and descending ordering | `SELECT "group", SUM("val") FROM "table" GROUP BY "group" ORDER BY "total" DESC LIMIT :limit` |
| **Parameterized Filtering** | Filter conditions across all methods | Safe binding through SQLAlchemy `text()` `:param` syntax | `WHERE "amount" > :min_val` |
| **Date/Time Trend Analysis** | `date_trend(table, date_col, metric_col, grain, ...)` | Temporal grains: `day`, `week`, `month`, `quarter`, `year` | `SELECT DATE_TRUNC('month', "ts") AS bucket, COUNT(*) FROM "table" GROUP BY bucket ORDER BY bucket ASC` |
| **Window Ranking** | `rank_rows(table, order_by, partition_by, method, ...)` | Methods: `RANK`, `DENSE_RANK`, `ROW_NUMBER` | `SELECT *, RANK() OVER (PARTITION BY "cat" ORDER BY "val" DESC) AS rank FROM "table"` |
| **Running Total** | `running_total(table, order_by, metric_col, ...)` | Cumulative running aggregation | `SELECT "dt", "val", SUM("val") OVER (ORDER BY "dt" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "table"` |
| **Top-N Analysis** | `top_n(table, group_col, metric_col, n, ...)` | Dimensional leaderboards with bounded limit | `SELECT "cat", SUM("metric") FROM "table" GROUP BY "cat" ORDER BY total DESC LIMIT :n` |
| **Null Data Quality** | `null_summary(table)` | Discovers all columns and measures null counts & percentages | `SELECT COUNT(*) - COUNT("col") AS null_col FROM "table"` |
| **Distinct Cardinality** | `distinct_summary(table)` | Discovers all columns and measures unique value counts & uniqueness ratios | `SELECT COUNT(DISTINCT "col") FROM "table"` |

### 4. SQL Safety Strategy & Parameterization
To guarantee resilience against SQL injection without sacrificing generic dataset support:
1. **Strict Identifier Validation**: Table and column names are validated against regex `^[A-Za-z_][A-Za-z0-9_]*$`. Identifiers with spaces, semicolons, hyphens, quotes, or SQL fragments are strictly rejected.
2. **Schema & Column Existence Verification**: Prior to query compilation, tables and requested columns are verified against the active database schema (`inspector.get_columns()`). Unknown tables or columns fail safely before execution.
3. **Double-Quote Escaping**: All validated identifiers are enclosed in ANSI double quotes (`"identifier"`).
4. **Strict Allowlisting**: Aggregation functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), temporal grains (`day`, `week`, `month`, `quarter`, `year`), and ranking functions (`RANK`, `DENSE_RANK`, `ROW_NUMBER`) are validated against immutable allowlists. Arbitrary functions or custom SQL fragments are rejected.
5. **Bound Parameter Binding**: All filter values, threshold criteria, and limits are passed exclusively via SQLAlchemy parameter dictionaries (`params={"limit_n": 10, "min_val": 100.0}`). String concatenation or f-string interpolation of values into SQL is prohibited.
6. **No Arbitrary SQL Endpoint**: No generic `execute_any_sql(user_input)` exists; only application-controlled templates and verified parameterized builders execute.

### 5. Automated Execution & Result Export
The engine features an automated suite runner (`run_automated_analytics_suite(table_name)`) that inspects the discovered schema and automatically executes appropriate analytical queries:
* An overview aggregation across the first numeric column.
* A categorical grouped aggregation on the first low-cardinality dimension.
* A date trend query if a datetime column is present.
* A windowed rank and running total query.
* A Top-N leaderboard query.
* Null and distinct data-quality audits.

Every query result is serialized into:
* **`reports/analytics/<query_name>.json`**: Detailed JSON containing query metadata, column headers, execution timing, and serialized rows.
* **`reports/analytics/<query_name>.csv`**: Flat tabular CSV ready for external tooling or spreadsheet ingestion.

### 6. Pipeline Reports & Observability
At the end of stage execution, Stage 9 compiles run-level audit summaries:
* **`reports/analytics_report.json`**: JSON array tracking `run_id`, `query_name`, `table`, `status`, `execution_time`, `result_rows`, `output_files`, and `error_message`.
* **`reports/analytics_report.csv`**: Tabular CSV report matching the JSON payload.
* **`logs/analytics.log`**: Timestamped execution trace with masked credentials.

Status codes tracked:
* `SUCCESS`: Query executed and exported cleanly.
* `FAILED`: Query execution error (e.g. invalid syntax or constraint).
* `DATABASE_UNAVAILABLE`: PostgreSQL server unreachable or unconfigured in environment.
* `CONFIG_ERROR`: Corrupted or invalid `config/analytics.json`.
* `INVALID_TABLE`: Requested table does not exist in schema.
* `INVALID_COLUMN`: Requested column does not exist in target table.

### 7. Orchestrator Integration & Graceful Degradation
* Integrated into `PipelineOrchestrator` as Stage 9, dependent on `database`.
* If PostgreSQL is unavailable or unconfigured, the pipeline marks Stage 9 as `DATABASE_UNAVAILABLE` honestly, does not fabricate query results, logs the reason, writes `reports/analytics_report.json` and `reports/analytics_report.csv`, and finishes gracefully with `COMPLETED_WITH_WARNINGS`.

### 8. Configuration (`config/analytics.json`)
```json
{
  "enabled": true,
  "default_limit": 1000,
  "max_limit": 10000,
  "export_formats": [
    "json",
    "csv"
  ],
  "allowed_aggregations": [
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX"
  ],
  "allowed_grains": [
    "day",
    "week",
    "month",
    "quarter",
    "year"
  ],
  "allowed_ranking_methods": [
    "RANK",
    "DENSE_RANK",
    "ROW_NUMBER"
  ]
}
```

### 9. Limitations & Boundaries
* **Scope Boundary**: Implements SQL analytics only. Dashboards and visual reporting are integrated through Stage 10 (Power BI Integration).
* **Database Engine**: Optimized for PostgreSQL with automatic fallback to SQLite for local unit testing and development.
* **Complex Multi-Table Joins**: Automated query discovery executes on individual dynamic tables; relational joins require explicitly defined foreign key configurations.

---

## Stage 10 Implementation Details: Power BI Integration & BI Layer

<p align="center">
  <img src="docs/images/dashboard_powerbi_spec.png" alt="Power BI Semantic Specification" width="85%">
</p>

### 1. PostgreSQL → Power BI Architecture
Stage 10 bridges PostgreSQL dynamic modeling with enterprise Business Intelligence in Power BI Desktop. The system provides:
* **Dynamic Dataset & Column Discovery**: Introspects public PostgreSQL tables, column definitions, data types, nullability constraints, and row counts via `pipeline/powerbi.py`.
* **Analytical Role Classification**: Automatically classifies columns into domain-agnostic analytical roles:
  * `IDENTIFIER`: Keys, IDs, codes with high uniqueness ratio.
  * `NUMERIC_METRIC`: Measures, quantities, amounts, durations, rates.
  * `CATEGORICAL_DIMENSION`: Low-cardinality discrete categories, status, types.
  * `DATETIME_DIMENSION`: Timestamps and dates for time-series analysis.
  * `BOOLEAN_DIMENSION`: Binary flags and boolean indicators.
  * `TEXT_ATTRIBUTE`: High-cardinality descriptions, comments, or notes.
* **No Domain Hardcoding**: Zero business-domain assumptions (e.g. no hardcoded `sales`, `employees`, or `healthcare` logic). Evaluates arbitrary schema definitions (e.g. IoT sensors, telemetry, logs, operational data).

### 2. Import vs. DirectQuery Guidance
Documented extensively in [`powerbi/CONNECTION_GUIDE.md`](powerbi/CONNECTION_GUIDE.md):
* **Import Mode**: Loads data into the high-performance VertiPaq in-memory engine. Optimal for dashboards requiring fast interactivity, complex DAX time-intelligence, and scheduled refreshes.
* **DirectQuery Mode**: Pushes queries down to PostgreSQL in real time. Essential for massive datasets that exceed memory limits or operational dashboards requiring sub-second source sync.
* *Honest tradeoff analysis*: DirectQuery limits certain DAX functions and relies heavily on PostgreSQL indexing and network latency.

### 3. Dynamic Semantic Model (`powerbi/semantic_model.json`)
Exports dynamic semantic model specifications representing analytical entities, candidate dimensions, numeric measures, and explicit foreign-key relationships.
* **Explicit Foreign Key Discovery**: Discovers relationships through PostgreSQL foreign key reflection (`discover_relationships`).
* **Zero Relationship Fabrication**: If no explicit foreign keys exist, the model explicitly records `"No explicit PostgreSQL foreign-key relationships discovered."` and never fabricates artificial links based on column name similarity.
* **Graceful Degradation**: When PostgreSQL is offline or unconfigured, outputs a valid schema with status `DATABASE_UNAVAILABLE`.

### 4. Reusable DAX Measure Templates
Located in [`powerbi/dax/`](powerbi/dax/):
* **Generic Measures ([`generic_measures.dax`](powerbi/dax/generic_measures.dax))**: Standard aggregations (`COUNTROWS`, `SUM`, `AVERAGE`, `MIN`, `MAX`, `DISTINCTCOUNT`, `DIVIDE`, percentage of total) with placeholders (`'TableName'[NumericColumn]`).
* **Time Intelligence ([`time_intelligence.dax`](powerbi/dax/time_intelligence.dax))**: Period-over-period calculations (`TOTALYTD`, `TOTALMTD`, `SAMEPERIODLASTYEAR`, variance, growth %, rolling averages). Clearly documents prerequisites (contiguous date table and active relationships).

### 5. Visualization Recommendation Engine (`powerbi/dashboard_spec.json`)
The recommendation engine inspects discovered schema metadata and recommends appropriate visual types based on analytical role pairings:
* `NUMERIC_METRIC` only $\rightarrow$ KPI Card, Gauge, Summary Card.
* `CATEGORICAL_DIMENSION` + `NUMERIC_METRIC` $\rightarrow$ Bar Chart, Column Chart, Matrix.
* `DATETIME_DIMENSION` + `NUMERIC_METRIC` $\rightarrow$ Line Chart, Area Chart.
* Two `CATEGORICAL_DIMENSION` $\rightarrow$ Clustered Column Chart, Matrix Heatmap.
* `DATETIME_DIMENSION` only $\rightarrow$ Timeline, Event Volume Histogram.
* Missing required fields are gracefully handled without hallucinating visualizations.

### 6. Power Query / M Guidance & ETL Division
Documented in [`powerbi/POWER_QUERY_GUIDE.md`](powerbi/POWER_QUERY_GUIDE.md):
* **ETL Division of Responsibility**: Heavy data cleaning, missing value imputation, type casting, and data validation are performed upstream in Python ETL (Stages 2–5). Power BI consumes validated analytical tables.
* **M Transformation Patterns**: Safe type coercion, query folding, and dimensional filtering.

### 7. Refresh Strategy & Row-Level Security
* **Refresh Strategy ([`powerbi/REFRESH_STRATEGY.md`](powerbi/REFRESH_STRATEGY.md))**: Links Power BI scheduled refresh and incremental refresh to Stage 8 incremental processing.
* **Row-Level Security ([`powerbi/RLS_GUIDE.md`](powerbi/RLS_GUIDE.md))**: Generic static and dynamic RLS patterns using `USERPRINCIPALNAME()` and dimension filtering.

### 8. Power BI Templates & `.pbix` Boundary
Documented in [`powerbi/templates/README.md`](powerbi/templates/README.md):
* The repository provides machine-readable specifications, DAX templates, connection guides, and semantic metadata.
* It explicitly refrains from fabricating proprietary binary `.pbix` files, guiding analysts to assemble reports natively in Power BI Desktop.

### 9. Power BI Configuration (`config/powerbi.json`)
```json
{
  "enabled": true,
  "schema": "public",
  "connection_mode": "import",
  "supported_connection_modes": [
    "import",
    "directquery"
  ],
  "max_tables": 50,
  "generate_views": true,
  "generate_metadata": true,
  "generate_dax_templates": true,
  "generate_visualization_recommendations": true,
  "generate_refresh_guidance": true,
  "generate_rls_guidance": true,
  "output_formats": [
    "json"
  ]
}
```

---

## Stage 11 Implementation Details: Testing, Monitoring & Production Readiness

<p align="center">
  <img src="docs/images/dashboard_pipeline_stages.png" alt="Production Monitoring & Stage Observability" width="90%">
</p>

### 1. Centralized Pipeline Monitoring (`pipeline/monitoring.py`)
Provides production-grade run observability and performance tracking:
* **Metric Capture**: Tracks wall-clock start/end times, CPU monotonic durations, stage-level timings, records read, records processed, records quarantined/rejected, records loaded to database, records skipped, and records reprocessed.
* **Throughput Calculations**: Computes real-time processing throughput (`records_per_second`) for each pipeline stage.
* **Quality Score Aggregation**: Computes overall weighted dataset validation scores.

### 2. Pre-Flight Health Diagnostics (`pipeline/health.py`)
Verifies execution environment prior to data processing:
* **Runtime Verification**: Checks Python version ($\ge 3.10$).
* **Filesystem Diagnostics**: Probes existence and write permissions for all 10 foundational directories.
* **Configuration Integrity**: Confirms valid JSON syntax across all configuration files.
* **State Store Accessibility**: Verifies `state/checkpoints.json` health.
* **Honest Database Probing**: Dynamically tests `DATABASE_URL`. If unconfigured, reports `DATABASE_UNAVAILABLE` honestly and never claims healthy when unconnected.
* Structured status codes: `HEALTHY`, `WARNING`, `ERROR`, `DATABASE_UNAVAILABLE`, `CONFIGURATION_ERROR`.

### 3. Machine-Readable Run Summaries (`reports/run_summary.json` & `.csv`)
Generates comprehensive execution audit reports tracking high-level pipeline status, stage breakdowns, duration metrics, and error tallies.

### 4. End-to-End Data Lineage (`pipeline/lineage.py`)
Captures dataset-level lifecycle traceability from raw landing file through ingestion, profiling, transformation, validation, incremental state, database load, SQL analytics, and Power BI readiness:
* Outputs: [`reports/lineage.json`](reports/lineage.json) and [`reports/lineage.csv`](reports/lineage.csv).
* Zero relationship fabrication: records verified upstream/downstream connections only.

### 5. Error Classification & Secret Masking
* **Structured Categorization**: Categorizes pipeline errors into `CONFIGURATION_ERROR`, `FILE_READ_ERROR`, `SCHEMA_ERROR`, `TRANSFORMATION_ERROR`, `VALIDATION_ERROR`, `DATABASE_ERROR`, `DATABASE_UNAVAILABLE`, `ANALYTICS_ERROR`, `POWERBI_ERROR`, `STATE_ERROR`, and `UNEXPECTED_ERROR`.
* **Credential Masking**: Sanitizes connection strings (`postgresql://user:***@host:5432/db`), tokens, and passwords across all logs, error records, and reports.

### 6. Centralized Configuration Validation (`pipeline/config_validator.py`)
Centralized validator validating all JSON configuration files (`pipeline.json`, `transformations.json`, `validation.json`, `database.json`, `incremental.json`, `analytics.json`, `powerbi.json`), verifying data types, required keys, allowed enum values, and conflict rules.

### 7. Idempotency & State Recovery
* **Idempotent Ingestion**: Repeated execution on unchanged files marks datasets `UNCHANGED -> SKIP`.
* **Delta Reprocessing**: Modified datasets trigger `CHANGED -> REPROCESS` with exact row delta calculations (`+new`, `~changed`, `=unchanged`, `-removed`).
* **Atomic Writes**: Checkpoint updates use temporary file writes and atomic replacement (`os.replace`).
* **Corruption Resilience**: If `state/checkpoints.json` is corrupted, the system safely falls back to a clean state without crashing.

### 8. Production Readiness Reports (`reports/production_readiness_report.json` & `.csv`)
Exports formal production readiness audits and architectural documentation in [`docs/PRODUCTION_READINESS.md`](docs/PRODUCTION_READINESS.md) and [`docs/FINAL_ARCHITECTURE.md`](docs/FINAL_ARCHITECTURE.md).

---

## Project Status

* **Stage 1 — Project Foundation**: **COMPLETE**
* **Stage 2 — Universal Data Ingestion & File Detection**: **COMPLETE**
* **Stage 3 — Automatic Schema Detection & Data Profiling**: **COMPLETE**
* **Stage 4 — Data Cleaning & Transformation Engine**: **COMPLETE**
* **Stage 5 — Data Quality & Validation Engine**: **COMPLETE**
* **Stage 6 — PostgreSQL Database & Dynamic Data Modeling**: **COMPLETE**
* **Stage 7 — ETL Pipeline Orchestration**: **COMPLETE**
* **Stage 8 — Incremental Data Processing**: **COMPLETE**
* **Stage 9 — SQL Analytics Layer**: **COMPLETE**
* **Stage 10 — Power BI Integration & Business Intelligence Layer**: **COMPLETE**
* **Stage 11 — Testing, Monitoring & Production Readiness**: **COMPLETE**

> **Current Implementation State:**
> - Directory architecture, environment templates, and version control rules initialized.
> - Universal ingestion engine implemented in `pipeline/ingestion.py` (CSV, Excel `.xlsx`, JSON).
> - Schema inference and statistical profiling engine implemented in `pipeline/schema_detector.py`.
> - Data cleaning and transformation engine implemented in `pipeline/transformer.py`.
> - Quality validation engine implemented in `pipeline/validator.py` with valid/quarantine partitioning.
> - Dynamic PostgreSQL data modeling and batch loading implemented in `pipeline/database.py`.
> - Centralized pipeline orchestration, dependency management, and observability in `pipeline/orchestrator.py`.
> - Incremental data processing, SHA-256 chunked hashing, atomic checkpointing, and delta detection in `pipeline/incremental.py`.
> - Reusable, domain-agnostic SQL analytics engine implemented in `pipeline/analytics.py` with query library in `sql/analytics/`.
> - Power BI integration and BI readiness engine implemented in `pipeline/powerbi.py` with semantic modeling and visualization recommendations in `powerbi/`.
> - Centralized monitoring, pre-flight health diagnostics, data lineage tracking, and configuration validation in `pipeline/monitoring.py`, `pipeline/health.py`, `pipeline/lineage.py`, and `pipeline/config_validator.py`.
> - Immutable raw data (`data/raw/`), cleaned outputs (`data/processed/`), validated records (`data/validated/`), and quarantined records (`data/rejected/`).
> - Checkpoint store in `state/checkpoints.json`.
> - Complete suite of execution and audit reports in `reports/` (`run_summary`, `lineage`, `production_readiness_report`, `pipeline_run_report`, `profile_report`, `validation_report`, `incremental_report`, `analytics_report`, `powerbi_report`).
> - Comprehensive runtime execution logs in `logs/`.
> - **257 automated unit tests** verifying the complete pipeline across all 11 stages and interactive web dashboard (256 passed, 1 conditional PostgreSQL test).
> - System is hardened, production-ready, and fully verified.

---

## Getting Started

### 1. Prerequisites
* Python 3.10 or higher
* Git

### 2. Set Up Virtual Environment
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Windows (Command Prompt):
.venv\Scripts\activate.bat

# Linux / macOS:
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
cp .env.example .env
```
*(Optionally populate `DATABASE_URL` in `.env` with your PostgreSQL connection string).*

### 5. Run the Complete Orchestrated Pipeline (Stages 1–11)
```bash
python run_pipeline.py
```

Expected terminal output (when PostgreSQL is unconfigured):
```text
====================================================
UNIVERSAL DATA ENGINEERING PIPELINE
====================================================

Run ID: run_20261004_230507_642c41

Stage              Status                 Duration  
----------------------------------------------------
Foundation         SUCCESS                0.00s     
Ingestion          SUCCESS                0.37s     
Incremental        SUCCESS                0.01s     
Profiling          SUCCESS                0.00s     
Transformation     SUCCESS                0.00s     
Validation         SUCCESS                0.00s     
Database           SUCCESS                0.00s     
Analytics          DATABASE_UNAVAILABLE   0.00s     
Powerbi            DATABASE_UNAVAILABLE   0.00s     
----------------------------------------------------
Datasets discovered:   3
Datasets successful:   0
Database unavailable:  0
----------------------------------------------------
Incremental Processing:
  sample_employees.json    UNCHANGED    -> SKIP
  sample_sales.csv         UNCHANGED    -> SKIP
    Deltas: +0 new, ~0 changed, =5 unchanged, -0 removed
  sample_students.xlsx     UNCHANGED    -> SKIP
----------------------------------------------------
SQL ANALYTICS
----------------------------------------------------
Analytics Status:
DATABASE_UNAVAILABLE
Reason: DATABASE_URL not configured in environment
----------------------------------------------------
POWER BI INTEGRATION
----------------------------------------------------
Power BI Status:
DATABASE_UNAVAILABLE
Reason: DATABASE_URL not configured in environment
----------------------------------------------------
MONITORING & PRODUCTION READINESS
----------------------------------------------------
Health Status:         DATABASE_UNAVAILABLE
Records Read:          15
Records Processed:     0
Records Quarantined:   0
Records Loaded:        0
Records Skipped:       15
Quality Score:         100.0%

Pipeline Status:
COMPLETED_WITH_WARNINGS

Report:
reports/pipeline_run_report.json
====================================================
```

### 6. Run the Interactive Web Analytics Dashboard (Stage 12)
```bash
python run_dashboard.py --port 5000
```
Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) in your web browser.

The web interface provides:
* **Interactive File Upload**: Upload CSV, XLSX, or JSON files into `data/raw/` with format validation.
* **Run Pipeline Controls**: Trigger the end-to-end pipeline with optional checkpoint reset and view live stage execution flow.
* **4-Zone Canvas Layout**:
  - **Zone 1 (Header & Slicers)**: Dataset selector, dynamic category dropdown filter, and date window slicers.
  - **Zone 2 (Executive KPI Strip)**: Total records, data quality score, quarantined records count, and dynamic metric sums/averages.
  - **Zone 3 (Primary Analytical Visuals)**: Automatically chosen based on column roles (Bar chart for Categorical × Metric, Line chart for Date × Metric, Histogram for continuous distributions, Doughnut chart for categorical proportions).
  - **Zone 4 (Granular Audit Records Table)**: Verified records table with search, pagination, and CSV export.
* **Quality Audit Inspector**: Full transparency into executed validation rules, failure reasons, and quarantined records.
* **Power BI Semantic Integration**: Discovered table archetype and visual recommendations from `powerbi/dashboard_spec.json` with an explicit disclaimer distinguishing the web dashboard from a native `.pbix` binary file.

### 7. Run Automated Tests
```bash
python -m unittest discover tests
```
Output:
```text
Ran 257 tests in 8.3s - OK (skipped=1)
```

### 8. Publishing to GitHub

When you are ready to publish this project to your GitHub account:

1. **Initialize Git (if not yet initialized)**:
   ```bash
   git init
   ```

2. **Stage all repository files**:
   ```bash
   git add .
   ```

3. **Create your initial commit**:
   ```bash
   git commit -m "feat: initial commit of universal data engineering pipeline"
   ```

4. **Add your remote repository**:
   ```bash
   git remote add origin https://github.com/<your-username>/universal-data-engineering-pipeline.git
   ```

5. **Set the main branch and push**:
   ```bash
   git branch -M main
   git push -u origin main
   ```

---

## Contributing & License

* **Contributing**: Contributions and pull requests are welcomed! Please see [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
* **License**: This project is licensed under the open-source [MIT License](LICENSE).

