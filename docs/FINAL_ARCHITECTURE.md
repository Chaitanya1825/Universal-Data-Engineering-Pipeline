# Universal Data Engineering Pipeline — Final Architecture

**Complete Technical Architecture Specification**  
**Engineering Stages: 1 through 11**  
**Platform:** Python 3.10+ | Pandas | SQLAlchemy | PostgreSQL | Power BI Desktop

---

## 1. High-Level Architectural Diagram

```text
========================================================================================
                          UNIVERSAL DATA ENGINEERING PIPELINE
========================================================================================

    [ RAW DATA SOURCES ]
      ├── CSV   (.csv)
      ├── Excel (.xlsx)
      └── JSON  (.json)
           │
           ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 2: Universal Data Ingestion & File Detection     │
    │ - Multi-encoding auto-fallback (utf-8, latin1, cp1252) │
    │ - Schema format detection & empty file guards          │
    │ - Landing zone: data/raw/                              │
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 8: Incremental Data Processing                   │
    │ - Chunked SHA-256 content hashing                      │
    │ - Atomic state checkpointing (state/checkpoints.json)  │
    │ - Actions: PROCESS (new), SKIP (unchanged), REPROCESS  │
    │ - Delta detection: +new, ~changed, =unchanged, -removed│
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 3: Automatic Schema Detection & Profiling        │
    │ - Inferred types: Int, Float, Bool, DateTime, Cat, Text│
    │ - Nullability, cardinality, uniqueness ratio scoring   │
    │ - Statistical distributions (mean, median, percentiles)│
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 4: Data Cleaning & Transformation Engine         │
    │ - Header normalization & collision prevention          │
    │ - Whitespace trimming & casing standardization         │
    │ - Missing value imputation (mode, median, constant)    │
    │ - Type coercion guided by inferred schema              │
    │ - Target partition: data/processed/                    │
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 5: Data Quality & Validation Engine              │
    │ - Quality assertions: nullability, uniqueness, ranges  │
    │ - Format conformity & regex validation                 │
    │ - Deterministic quality scoring (0% - 100%)            │
    └───────────────┬────────────────────────┬───────────────┘
                    │                        │
         [ Validated Records ]     [ Quarantined Records ]
                    ▼                        ▼
             data/validated/          data/rejected/
                    │
                    ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 6: PostgreSQL Database & Dynamic Modeling        │
    │ - Dynamic DDL generation with inferred SQL types       │
    │ - Parameterized batch loading & schema reflection      │
    │ - Graceful offline fallback: DATABASE_UNAVAILABLE      │
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 9: SQL Analytics Layer                           │
    │ - Aggregations, GROUP BY, date truncations             │
    │ - Window functions: RANK, DENSE_RANK, running totals   │
    │ - Parameterized SQL statements in sql/analytics/       │
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
    ┌────────────────────────────────────────────────────────┐
    │ STAGE 10: Power BI Integration & BI Readiness          │
    │ - Dynamic semantic model: powerbi/semantic_model.json  │
    │ - Visualization engine: powerbi/dashboard_spec.json    │
    │ - Reusable DAX libraries: powerbi/dax/*.dax            │
    │ - Architectural connection & refresh guides            │
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼
========================================================================================
             STAGE 11: OBSERVABILITY, MONITORING, HEALTH & LINEAGE
========================================================================================

    ┌──────────────────────────────────────────────────────────────────────────┐
    │ Observability Layer (PipelineMonitor, HealthChecker, LineageTracker)     │
    │                                                                          │
    │ ├── Health Check:        pipeline/health.py                              │
    │ ├── Monitoring:          pipeline/monitoring.py                          │
    │ ├── Data Lineage:        pipeline/lineage.py                             │
    │ ├── Config Validation:   pipeline/config_validator.py                    │
    │                                                                          │
    │ ├── Reports:                                                             │
    │ │   ├── reports/run_summary.json & .csv                                  │
    │ │   ├── reports/lineage.json & .csv                                      │
    │ │   ├── reports/production_readiness_report.json & .csv                  │
    │ │   └── reports/pipeline_run_report.json & .csv                          │
    │ └── Logs:                                                                │
    │     └── logs/*.log (Masked passwords & credentials)                      │
    └──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Specifications

### 1. Ingestion Engine (`pipeline/ingestion.py`)
* Discovers and parses `.csv`, `.xlsx`, and `.json` data files.
* Implements multi-encoding fallbacks (`utf-8`, `latin1`, `cp1252`) to prevent parsing crashes.
* Verifies tabular structure; gracefully rejects malformed or non-tabular structures.

### 2. Incremental Processing (`pipeline/incremental.py`)
* Computes chunked SHA-256 file hashes to detect state without loading full files into memory.
* Evaluates records for deltas (`+new`, `~changed`, `=unchanged`, `-removed`).
* Persists state to `state/checkpoints.json` using atomic temporary file renames.

### 3. Profiling & Inference Engine (`pipeline/schema_detector.py`)
* Infers domain-agnostic logical types: `Integer`, `Float`, `Boolean`, `DateTime`, `Categorical`, and `String/Text`.
* Computes complete statistical profiles (null percentages, unique counts, quantiles, and category frequencies).

### 4. Transformation Engine (`pipeline/transformer.py`)
* Normalizes column names into snake_case while preventing duplicate collisions.
* Imputes missing values based on column logical type and configured strategy.
* Normalizes whitespace without altering numeric, datetime, or boolean values.

### 5. Quality Validation Engine (`pipeline/validator.py`)
* Separates data into `data/validated/` and quarantined `data/rejected/`.
* Enforces nullability, uniqueness, range constraints, and regex formatting.
* Computes deterministic quality scores based on validated record proportions.

### 6. Database Dynamic Modeler (`pipeline/database.py`)
* Maps inferred logical types into native PostgreSQL types (`INTEGER`, `DOUBLE PRECISION`, `BOOLEAN`, `TIMESTAMP`, `TEXT`).
* Creates idempotent table structures and executes batched loads with transaction rollbacks on failure.
* Operates in graceful `DATABASE_UNAVAILABLE` fallback mode when offline.

### 7. SQL Analytics Layer (`pipeline/analytics.py`)
* Generates and executes parameterized analytical queries:
  * Table row counting & schema reflection
  * Metric aggregations (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
  * Date/time grain analysis (`day`, `week`, `month`, `quarter`, `year`)
  * Window ranking (`RANK`, `DENSE_RANK`, `ROW_NUMBER`)
  * Running totals and rolling metric windows
* Exports results to `reports/analytics/`.

### 8. Power BI Integration Engine (`pipeline/powerbi.py`)
* Classifies columns into analytical roles (`IDENTIFIER`, `NUMERIC_METRIC`, `CATEGORICAL_DIMENSION`, `DATETIME_DIMENSION`, `BOOLEAN_DIMENSION`, `TEXT_ATTRIBUTE`).
* Discovers foreign-key relationships without artificial fabrication.
* Emits dynamic semantic models (`powerbi/semantic_model.json`), visualization recommendations (`powerbi/dashboard_spec.json`), and DAX measures.

### 9. Monitoring & Observability (`pipeline/monitoring.py`, `pipeline/health.py`, `pipeline/lineage.py`)
* **PipelineMonitor**: Tracks precise CPU and wall-clock execution timings, records read/processed/rejected/loaded, and error categorization.
* **PipelineHealthChecker**: Pre-flight system diagnostic verifying runtime, directories, configurations, state store, and database connectivity.
* **PipelineLineageTracker**: End-to-end dataset trace capturing source file $\rightarrow$ transformation $\rightarrow$ validation $\rightarrow$ database $\rightarrow$ analytics $\rightarrow$ Power BI.

---

## 3. Security Architecture

1. **Credential Sanitization**:
   All database connection strings and passwords are automatically masked before writing to log files, terminal outputs, or report files:
   `postgresql://user:***@host:5432/database`
2. **SQL Injection Defense**:
   SQL analytics and dynamic table creation utilize parameterized queries (`sqlalchemy.text` with bound parameters) and strict whitelist identifier regex validation (`^[a-zA-Z_][a-zA-Z0-9_]*$`). Arbitrary dynamic table or column interpolation is prevented.
3. **Zero Secrets in Repository**:
   Environment variables (`DATABASE_URL`) are read via `.env` (guarded by `.gitignore`).
