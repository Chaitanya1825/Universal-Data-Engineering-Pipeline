"""
Main entry module for the Universal Data Engineering Pipeline.

Orchestrates:
- Stage 1: Foundation Verification
- Stage 2: Universal Data Ingestion & File Detection
- Stage 3: Automatic Schema Detection & Data Profiling
- Stage 4: Data Cleaning & Transformation Engine
- Stage 5: Data Quality & Validation Engine
- Stage 6: PostgreSQL Database & Dynamic Data Modeling
"""

from pathlib import Path
import sys
from typing import Optional

# Ensure project root is in sys.path for direct invocation
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import pandas as pd

from pipeline.database import (
    DatabaseLoadResult,
    format_database_summary,
    generate_table_name,
    get_database_engine,
    load_database_config,
    load_project_env,
    load_validated_dataset,
    log_database_event,
    mask_database_url,
    save_database_load_reports,
    test_database_connection,
)
from pipeline.ingestion import IngestionResult, run_ingestion_pipeline
from pipeline.schema_detector import (
    DatasetProfile,
    format_profiling_summary,
    log_profiling_event,
    profile_dataset,
    save_profiling_reports,
)
from pipeline.transformer import (
    TransformationResult,
    format_transformation_summary,
    load_transformation_config,
    log_transformation_event,
    save_processed_dataset,
    save_transformation_report,
    transform_dataset,
)
from pipeline.validator import (
    ValidationResult,
    format_validation_summary,
    load_validation_config,
    log_validation_event,
    save_validation_outputs,
    save_validation_reports,
    validate_dataset,
)


def check_project_structure() -> bool:
    """
    Verify that the foundational directories for the data engineering
    pipeline exist in the project root.
    """
    root_dir = Path(__file__).resolve().parent.parent
    required_directories = [
        root_dir / "data" / "raw",
        root_dir / "data" / "processed",
        root_dir / "data" / "rejected",
        root_dir / "data" / "validated",
        root_dir / "pipeline",
        root_dir / "sql",
        root_dir / "config",
        root_dir / "logs",
        root_dir / "tests",
        root_dir / "powerbi",
        root_dir / "docs",
    ]

    return all(dir_path.exists() for dir_path in required_directories)


def print_stage1_banner() -> None:
    """Display Stage 1 verification status."""
    banner = (
        "========================================\n"
        "STAGE 1: PROJECT FOUNDATION VERIFICATION\n"
        "========================================\n"
        "Status: Environment initialized successfully\n"
        "========================================"
    )
    print(banner)


def run_stage2(raw_dir: Path, log_dir: Path) -> tuple[list[IngestionResult], int]:
    """Execute Stage 2 universal file discovery, detection, and ingestion."""
    print("\n" + "=" * 40)
    print("STAGE 2: UNIVERSAL DATA INGESTION & DETECTION")
    print("=" * 40)

    try:
        results, report = run_ingestion_pipeline(raw_dir=raw_dir, log_dir=log_dir)
        print(report)
        return results, 0
    except Exception as e:
        print(f"Error during Stage 2 execution: {e}")
        return [], 1


def run_stage3(
    ingestion_results: list[IngestionResult],
    reports_dir: Path,
    log_dir: Path,
) -> tuple[list[DatasetProfile], int]:
    """Execute Stage 3 automatic schema detection and profiling on ingested datasets."""
    print("\n" + "=" * 40)
    print("STAGE 3: AUTOMATIC SCHEMA DETECTION & PROFILING")
    print("=" * 40)

    successful_datasets = [
        res for res in ingestion_results if res.status == "SUCCESS" and res.df is not None
    ]

    if not successful_datasets:
        print("No successfully ingested datasets available for profiling.")
        return [], 0

    profiles: list[DatasetProfile] = []

    for idx, res in enumerate(successful_datasets):
        if idx > 0:
            print("\n" + "-" * 40)

        try:
            profile = profile_dataset(res.df.copy(), filename=res.file_name)
            log_profiling_event(profile, status="SUCCESS", log_dir=log_dir)
            profiles.append(profile)
            print(format_profiling_summary(profile))

        except Exception as e:
            print(f"Error profiling dataset '{res.file_name}': {e}")
            dummy_prof = DatasetProfile(
                filename=res.file_name,
                row_count=len(res.df) if res.df is not None else 0,
                column_count=len(res.df.columns) if res.df is not None else 0,
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
            log_profiling_event(dummy_prof, status="FAILED", error_message=str(e), log_dir=log_dir)

    if profiles:
        json_path, csv_path = save_profiling_reports(profiles, reports_dir=reports_dir)
        print("\n" + "=" * 40)
        print("PROFILING REPORTS GENERATED")
        print("=" * 40)
        print(f"JSON Report: {json_path}")
        print(f"CSV Summary: {csv_path}")

    return profiles, 0


def run_stage4(
    ingestion_results: list[IngestionResult],
    profiles: list[DatasetProfile],
    processed_dir: Path,
    reports_dir: Path,
    log_dir: Path,
    config_path: Path,
) -> tuple[list[TransformationResult], int]:
    """Execute Stage 4 cleaning and transformation on ingested datasets."""
    print("\n" + "=" * 40)
    print("STAGE 4: DATA CLEANING & TRANSFORMATION")
    print("=" * 40)

    successful_datasets = [
        res for res in ingestion_results if res.status == "SUCCESS" and res.df is not None
    ]

    if not successful_datasets:
        print("No successfully ingested datasets available for transformation.")
        return [], 0

    cfg = load_transformation_config(config_path)
    profile_map = {p.filename: p for p in profiles}
    transformation_results: list[TransformationResult] = []

    for idx, res in enumerate(successful_datasets):
        if idx > 0:
            print("\n" + "-" * 40)

        schema_prof = profile_map.get(res.file_name)
        trans_res = transform_dataset(
            df=res.df.copy(),
            source_filename=res.file_name,
            schema_profile=schema_prof,
            config=cfg,
        )

        if trans_res.status == "SUCCESS" and trans_res.cleaned_df is not None:
            out_file = save_processed_dataset(
                cleaned_df=trans_res.cleaned_df,
                source_filename=res.file_name,
                processed_dir=processed_dir,
            )
            trans_res.output_filename = out_file.name

        log_transformation_event(trans_res, log_dir=log_dir)
        transformation_results.append(trans_res)
        print(format_transformation_summary(trans_res))

    if transformation_results:
        rep_file = save_transformation_report(transformation_results, reports_dir=reports_dir)
        print("\n" + "=" * 40)
        print("TRANSFORMATION REPORT GENERATED")
        print("=" * 40)
        print(f"Transformation Report: {rep_file}")

    return transformation_results, 0


def run_stage5(
    transformation_results: list[TransformationResult],
    profiles: list[DatasetProfile],
    validated_dir: Path,
    rejected_dir: Path,
    reports_dir: Path,
    log_dir: Path,
    config_path: Path,
) -> tuple[list[ValidationResult], dict[str, pd.DataFrame], int]:
    """Execute Stage 5 data quality validation on cleaned datasets."""
    print("\n" + "=" * 40)
    print("STAGE 5: DATA QUALITY & VALIDATION ENGINE")
    print("=" * 40)

    successful_transformations = [
        t for t in transformation_results if t.status == "SUCCESS" and t.cleaned_df is not None
    ]

    if not successful_transformations:
        print("No successfully transformed datasets available for validation.")
        return [], {}, 0

    profile_map = {p.filename: p for p in profiles}
    validation_results: list[ValidationResult] = []
    validated_dfs: dict[str, pd.DataFrame] = {}

    for idx, trans in enumerate(successful_transformations):
        if idx > 0:
            print("\n" + "-" * 40)

        schema_prof = profile_map.get(trans.source_filename)
        val_cfg = load_validation_config(config_path, dataset_name=trans.source_filename)

        val_res, valid_df, rejected_df = validate_dataset(
            df=trans.cleaned_df,
            source_filename=trans.source_filename,
            config=val_cfg,
            schema_profile=schema_prof,
        )

        val_file, rej_file = save_validation_outputs(
            source_filename=trans.source_filename,
            valid_df=valid_df,
            rejected_df=rejected_df,
            validated_dir=validated_dir,
            rejected_dir=rejected_dir,
        )

        val_res.validated_output_path = str(val_file)
        if rej_file:
            val_res.rejected_output_path = str(rej_file)

        log_validation_event(val_res, log_dir=log_dir)
        validation_results.append(val_res)
        validated_dfs[trans.source_filename] = valid_df
        print(format_validation_summary(val_res))

    if validation_results:
        json_path, csv_path = save_validation_reports(validation_results, reports_dir=reports_dir)
        print("\n" + "=" * 40)
        print("VALIDATION REPORTS GENERATED")
        print("=" * 40)
        print(f"JSON Validation Report: {json_path}")
        print(f"CSV Validation Summary: {csv_path}")

    return validation_results, validated_dfs, 0


def run_stage6(
    validated_dfs: dict[str, pd.DataFrame],
    profiles: list[DatasetProfile],
    reports_dir: Path,
    log_dir: Path,
    config_path: Path,
) -> int:
    """
    Execute Stage 6 dynamic PostgreSQL table creation and batch loading.
    
    Loads ONLY validated datasets. Exits gracefully if PostgreSQL is unavailable.
    """
    print("\n" + "=" * 40)
    print("STAGE 6: POSTGRESQL DATABASE & DYNAMIC DATA MODELING")
    print("=" * 40)

    if not validated_dfs:
        print("No validated datasets available for database loading.")
        return 0

    cfg = load_database_config(config_path)
    profile_map = {p.filename: p for p in profiles}

    # Initialize Engine and probe connectivity
    engine, engine_err = get_database_engine()
    connected = False
    conn_err: Optional[str] = None

    if engine is not None:
        connected, conn_err = test_database_connection(engine)
        if connected:
            print("Database Connection: SUCCESS (PostgreSQL reachable)")
        else:
            print("Database Connection: DATABASE_UNAVAILABLE")
            print(f"Reason: {conn_err}")
    else:
        print("Database Connection: CONFIG_MISSING")
        print(f"Reason: {engine_err}")

    db_results: list[DatabaseLoadResult] = []

    for idx, (source_name, valid_df) in enumerate(validated_dfs.items()):
        if idx > 0:
            print("\n" + "-" * 40)

        schema_prof = profile_map.get(source_name)

        if not connected:
            status_label = "DATABASE_UNAVAILABLE" if engine is not None else "CONFIG_MISSING"
            reason_text = (
                f"PostgreSQL server unreachable: {conn_err}"
                if engine is not None
                else "DATABASE_URL not configured in environment"
            )
            res = DatabaseLoadResult(
                source_dataset=source_name,
                target_table=generate_table_name(source_name),
                database_status=status_label,
                connection_status="FAILED" if engine is not None else "UNCONFIGURED",
                rows_attempted=len(valid_df),
                rows_inserted=0,
                rows_failed=len(valid_df),
                error_message=reason_text,
            )
        else:
            res = load_validated_dataset(
                df=valid_df,
                source_filename=source_name,
                engine=engine,
                config=cfg,
                schema_profile=schema_prof,
            )

        log_database_event(res, log_dir=log_dir)
        db_results.append(res)
        print(format_database_summary(res))

    # Serialize database load reports
    if db_results:
        json_path, csv_path = save_database_load_reports(db_results, reports_dir=reports_dir)
        print("\n" + "=" * 40)
        print("DATABASE LOAD REPORTS GENERATED")
        print("=" * 40)
        print(f"JSON Load Report: {json_path}")
        print(f"CSV Load Summary: {csv_path}")

    return 0


def main() -> int:
    """
    Execute pipeline workflow: Stage 1 -> Stage 2 -> Stage 3 -> Stage 4 -> Stage 5 -> Stage 6.
    """
    root_dir = Path(__file__).resolve().parent.parent
    load_project_env(root_dir / ".env")
    raw_dir = root_dir / "data" / "raw"
    processed_dir = root_dir / "data" / "processed"
    validated_dir = root_dir / "data" / "validated"
    rejected_dir = root_dir / "data" / "rejected"
    log_dir = root_dir / "logs"
    reports_dir = root_dir / "reports"
    trans_config_path = root_dir / "config" / "transformations.json"
    val_config_path = root_dir / "config" / "validation.json"
    db_config_path = root_dir / "config" / "database.json"

    # Stage 1: Foundation Verification
    if not check_project_structure():
        print("Error: Missing required project directories. Please verify setup.")
        return 1

    print_stage1_banner()

    # Stage 2: Ingestion
    ingestion_results, stage2_exit = run_stage2(raw_dir, log_dir)
    if stage2_exit != 0:
        return stage2_exit

    # Stage 3: Schema Detection & Profiling
    profiles, stage3_exit = run_stage3(ingestion_results, reports_dir, log_dir)
    if stage3_exit != 0:
        return stage3_exit

    # Stage 4: Cleaning & Transformation
    transformation_results, stage4_exit = run_stage4(
        ingestion_results,
        profiles,
        processed_dir,
        reports_dir,
        log_dir,
        trans_config_path,
    )
    if stage4_exit != 0:
        return stage4_exit

    # Stage 5: Data Quality & Validation
    validation_results, validated_dfs, stage5_exit = run_stage5(
        transformation_results,
        profiles,
        validated_dir,
        rejected_dir,
        reports_dir,
        log_dir,
        val_config_path,
    )
    if stage5_exit != 0:
        return stage5_exit

    # Stage 6: PostgreSQL Database & Dynamic Modeling
    stage6_exit = run_stage6(
        validated_dfs,
        profiles,
        reports_dir,
        log_dir,
        db_config_path,
    )
    return stage6_exit


if __name__ == "__main__":
    sys.exit(main())
