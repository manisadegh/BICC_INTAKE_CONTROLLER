"""
lib/intake_service.py - Stateless BICC Intake Producer Service

Encapsulates the decoupled BICC Intake Controller lifecycle (`IntakeControllerService`)
built upon `dataloader_core.intake.BaseIntakeController` and `SourceIntakePlugin`:
  - Scans DROP_ZONE/<SOURCE> and sorts arrived batches in chronological FIFO order.
  - Verifies payload upload stability (>0 bytes, settling window).
  - Extracts CSV/PECSV/ZIP/GZ header columns in memory (`extract_csv_columns`),
    maps files to target entities via `EntityMapper`, and computes MD5 hashes once.
  - Stages and atomically publishes self-contained Batch Capsules into
    `LANDING_ZONE/<SOURCE>/BATCH_<EXTRACT_TS>_<BATCH_ID>/` (including `RAW/`,
    `RECEIPT_*.json`, `MANIFEST_*.MF`, `batch_descriptor.json`, and payload files).
  - Operates with zero Teradata database dependency and zero Orchestrator state
    inspection; optionally triggers `orchestrator.py` once after queuing all batches
    when `trigger_orchestrator` is enabled (and `--enqueue-only` is not set).
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional, Union

_SCRIPT_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
_COMMON_DIR = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from dataloader_core.config import ConfigLoader
from dataloader_core.exceptions import (
    BatchValidationError,
    ConfigurationError,
    DataLoaderError,
    OrchestratorExecutionError,
)
from dataloader_core.intake import (
    BaseIntakeController,
    PluginRegistry,
    RawBatchCandidate,
)
from dataloader_core.locking import ProcessLock
from dataloader_core.logging_utils import LoggingFactory
from dataloader_core.mapping import EntityMapper
from dataloader_core.schema_reader import extract_csv_columns, select_header_chunk_file

from lib.manifest_factory import (
    compute_file_md5,
    stage_batch_capsule_to_landing_zone,
)
from lib.orchestrator_bridge import invoke_orchestrator
from lib.scanner import (
    scan_drop_zone,
    sort_batches_fifo,
    verify_payload_stability,
)


class IntakeControllerService(BaseIntakeController):
    """
    Stateless Intake Producer service scanning DROP_ZONE and publishing queued
    Batch Capsules to LANDING_ZONE, built on BaseIntakeController.
    """

    def __init__(
        self,
        config_path: str,
        source_override: Optional[str] = None,
        skip_post_tasks: bool = False,
        enqueue_only: bool = False,
    ) -> None:
        super().__init__(
            config_path=config_path,
            source_override=source_override,
            skip_post_tasks=skip_post_tasks,
            enqueue_only=enqueue_only,
        )

    def process_single_batch(
        self,
        batch_info: Union[RawBatchCandidate, Dict[str, Any]],
        mapper_or_conn: Any = None,
        maybe_mapper: Optional[EntityMapper] = None,
    ) -> str:
        """
        Validates and enqueues a single batch into LANDING_ZONE/<SOURCE>/BATCH_<TS>_<ID>/.
        Maintains backward compatibility with earlier 2-argument calls.
        """
        mapper: Optional[EntityMapper] = maybe_mapper if maybe_mapper is not None else mapper_or_conn
        if mapper is not None and isinstance(mapper, EntityMapper):
            self.mapper = mapper
        return self.stage_and_publish_batch(batch_info)

    def invoke_orchestration_stage(self) -> int:
        """Launches DATA_INGESTION_ORCHESTRATOR using invoke_orchestrator helper."""
        extra_env = os.environ.copy()
        if "auto_migrate_ddl" in self.schema_cfg:
            extra_env["DL_AUTO_MIGRATE_DDL"] = "true" if self.schema_cfg.get("auto_migrate_ddl") else "false"

        post_tasks_file = self._write_post_tasks_file()

        rc = invoke_orchestrator(
            orchestrator_dir=self.orch_dir,
            config_file=self.orch_config,
            python_bin=self.python_bin,
            env=extra_env,
            skip_post_tasks=self.skip_post_tasks,
            source=self.default_source,
            post_tasks_file=post_tasks_file,
        )
        if rc != 0:
            raise OrchestratorExecutionError(f"Orchestrator failed with non-zero exit code {rc} under FAIL_STOP.")
        return 0

    process_batch = process_single_batch
    enqueue_batch_capsule = process_single_batch
