"""
feeder.py - BICC Intake Controller & Auto-DDL Supervisor CLI Entrypoint

Thin CLI entrypoint delegating intake scanning, FIFO scheduling, Schema Guard
introspection, raw manifest vaulting, and Orchestrator invocation to
`lib.intake_service.IntakeControllerService` and `dataloader_core`.
"""

from __future__ import annotations

import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMMON_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "COMMON"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from dataloader_core.config import expand_env_vars
from dataloader_core.db import get_teradata_connection
from dataloader_core.exceptions import DataLoaderError
from dataloader_core.logging_utils import setup_feeder_logger as setup_logger
from lib.intake_service import IntakeControllerService, select_header_chunk_file


def main() -> None:
    parser = argparse.ArgumentParser(description="BICC Intake Controller - Over-Arching Oracle Fusion Feeder")
    parser.add_argument("--config", type=str, required=True, help="Path to controller configuration JSON file")
    parser.add_argument(
        "--skip-post-tasks",
        action="store_true",
        default=False,
        help="Skip execution of downstream post-ingestion tasks (e.g. dbt)",
    )
    parser.add_argument("--source", type=str, default=None, help="Explicit data source system code override")
    args = parser.parse_args()

    try:
        service = IntakeControllerService(
            config_path=args.config,
            source_override=args.source,
            skip_post_tasks=args.skip_post_tasks,
        )
        sys.exit(service.run())
    except DataLoaderError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
