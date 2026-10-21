"""
feeder.py - BICC Intake Controller CLI Entrypoint (Pure Producer)

Thin CLI entrypoint delegating DROP_ZONE scanning, FIFO scheduling, in-memory
header inspection, MD5 calculation, atomic Batch Capsule staging in LANDING_ZONE,
and optional Orchestrator trigger to `lib.intake_service.IntakeControllerService`.
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
from dataloader_core.exceptions import DataLoaderError
from dataloader_core.logging_utils import setup_feeder_logger as setup_logger
from dataloader_core.schema_reader import select_header_chunk_file
from lib.intake_service import IntakeControllerService


def main() -> None:
    parser = argparse.ArgumentParser(description="BICC Intake Controller - Stateless Batch Capsule Producer")
    parser.add_argument("--config", type=str, required=True, help="Path to controller configuration JSON file")
    parser.add_argument(
        "--skip-post-tasks",
        action="store_true",
        default=False,
        help="Skip execution of downstream post-ingestion tasks (e.g. dbt)",
    )
    parser.add_argument("--source", type=str, default=None, help="Explicit data source system code override")
    parser.add_argument(
        "--enqueue-only",
        action="store_true",
        default=False,
        help="Stage and publish Batch Capsules in LANDING_ZONE without invoking the Orchestrator",
    )
    args = parser.parse_args()

    try:
        service = IntakeControllerService(
            config_path=args.config,
            source_override=args.source,
            skip_post_tasks=args.skip_post_tasks,
            enqueue_only=args.enqueue_only,
        )
        sys.exit(service.run())
    except DataLoaderError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
