"""
Data Lineage Tracking & Audit Module (Stage 11).

Provides end-to-end dataset-level traceability:
Raw File -> Ingestion -> Profiling -> Transformation -> Validation -> Incremental Processing -> Database -> Analytics -> Power BI

Serializes audit records to:
- reports/lineage.json
- reports/lineage.csv
"""

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Optional

import pandas as pd


@dataclass
class DatasetLineageRecord:
    """End-to-end trace of a single dataset through all pipeline transformations."""
    run_id: str
    dataset_identifier: str
    source_filename: str
    source_format: str
    timestamp: str
    input_row_count: int = 0
    transformed_row_count: int = 0
    valid_row_count: int = 0
    rejected_row_count: int = 0
    loaded_row_count: int = 0
    target_table_name: str = ""
    incremental_decision: str = "NOT_EVALUATED"
    incremental_action: str = "PROCESS"
    checkpoint_hash: Optional[str] = None
    quarantine_path: Optional[str] = None
    database_status: str = "NOT_RUN"
    analytics_execution_status: str = "NOT_RUN"
    powerbi_status: str = "NOT_RUN"
    lineage_verified: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PipelineLineageTracker:
    """
    Coordinates and persists dataset-level lineage records for the pipeline.
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.lineage_records: dict[str, DatasetLineageRecord] = {}

    def get_or_create_record(
        self,
        dataset_name: str,
        source_format: str = "UNKNOWN",
    ) -> DatasetLineageRecord:
        """Retrieve existing lineage record or create a fresh tracking entry."""
        if dataset_name not in self.lineage_records:
            self.lineage_records[dataset_name] = DatasetLineageRecord(
                run_id=self.run_id,
                dataset_identifier=dataset_name,
                source_filename=dataset_name,
                source_format=source_format,
                timestamp=datetime.now().isoformat(),
            )
        return self.lineage_records[dataset_name]

    def record_ingestion(self, dataset_name: str, source_format: str, row_count: int) -> None:
        """Capture raw input row count and format."""
        rec = self.get_or_create_record(dataset_name, source_format)
        rec.input_row_count = row_count
        rec.source_format = source_format

    def record_transformation(self, dataset_name: str, transformed_rows: int) -> None:
        """Capture row count after cleaning and normalization."""
        rec = self.get_or_create_record(dataset_name)
        rec.transformed_row_count = transformed_rows

    def record_validation(
        self,
        dataset_name: str,
        valid_rows: int,
        rejected_rows: int,
        quarantine_path: Optional[str] = None,
    ) -> None:
        """Capture partition counts between validated and quarantined records."""
        rec = self.get_or_create_record(dataset_name)
        rec.valid_row_count = valid_rows
        rec.rejected_row_count = rejected_rows
        rec.quarantine_path = quarantine_path

    def record_incremental(
        self,
        dataset_name: str,
        decision: str,
        action: str,
        file_hash: Optional[str] = None,
    ) -> None:
        """Capture incremental state decision and content hash."""
        rec = self.get_or_create_record(dataset_name)
        rec.incremental_decision = decision
        rec.incremental_action = action
        rec.checkpoint_hash = file_hash

    def record_database_load(
        self,
        dataset_name: str,
        target_table: str,
        loaded_rows: int,
        status: str,
    ) -> None:
        """Capture database target table and successfully inserted rows."""
        rec = self.get_or_create_record(dataset_name)
        rec.target_table_name = target_table
        rec.loaded_row_count = loaded_rows
        rec.database_status = status

    def record_downstream_status(
        self,
        dataset_name: str,
        analytics_status: str,
        powerbi_status: str,
    ) -> None:
        """Capture downstream SQL analytics and Power BI readiness statuses."""
        rec = self.get_or_create_record(dataset_name)
        rec.analytics_execution_status = analytics_status
        rec.powerbi_status = powerbi_status

    def save_lineage_reports(self, reports_dir: Path | str = "reports") -> tuple[Path, Path]:
        """
        Serialize lineage records to:
        - reports/lineage.json
        - reports/lineage.csv
        """
        out_dir = Path(reports_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "lineage.json"
        csv_path = out_dir / "lineage.csv"

        records_list = [r.to_dict() for r in self.lineage_records.values()]

        # Write JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records_list, f, indent=2, default=str)

        # Write CSV
        if records_list:
            df = pd.DataFrame(records_list)
            df.to_csv(csv_path, index=False)
        else:
            # Empty schema fallback
            columns = list(DatasetLineageRecord.__annotations__.keys())
            pd.DataFrame(columns=columns).to_csv(csv_path, index=False)

        return json_path, csv_path
