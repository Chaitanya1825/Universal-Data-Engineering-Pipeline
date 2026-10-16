"""
Root entry point to run the Universal Data Engineering Pipeline.

Initializes the PipelineOrchestrator and executes end-to-end ETL stages
with observability, reporting, and graceful failure handling.
"""

import argparse
from pathlib import Path
import sys
from typing import Optional
from pipeline.database import load_project_env
from pipeline.orchestrator import PipelineOrchestrator


def main(argv: Optional[list[str]] = None) -> int:
    """Run orchestrated pipeline and return process exit code."""
    parser = argparse.ArgumentParser(description="Universal Data Engineering Pipeline")
    parser.add_argument(
        "--reset-checkpoints",
        action="store_true",
        help="Safely clear incremental checkpoint state to force fresh processing without deleting raw or processed data.",
    )
    args = parser.parse_args(argv)

    load_project_env(Path(__file__).resolve().parent / ".env")
    orchestrator = PipelineOrchestrator(reset_checkpoints=args.reset_checkpoints)
    summary = orchestrator.execute_pipeline()
    return 0 if summary.pipeline_status in ["COMPLETED", "COMPLETED_WITH_WARNINGS"] else 1


if __name__ == "__main__":
    sys.exit(main())
