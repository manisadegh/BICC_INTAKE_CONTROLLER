"""
lib/intake_service.py - Stateless BICC Intake Producer Service

Encapsulates the decoupled BICC Intake Controller lifecycle (`IntakeControllerService`)
backed by `dataloader_core`:
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
from typing import Any, Dict, List, Optional

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


class IntakeControllerService:
    """Stateless Intake Producer service scanning DROP_ZONE and publishing queued Batch Capsules to LANDING_ZONE."""

    def __init__(
        self,
        config_path: str,
        source_override: Optional[str] = None,
        skip_post_tasks: bool = False,
        enqueue_only: bool = False,
    ) -> None:
        self.config_path = config_path
        self.config = ConfigLoader.load_json_config(config_path)
        self.base_dir = os.path.dirname(os.path.abspath(config_path))
        self.skip_post_tasks = skip_post_tasks
        self.enqueue_only = enqueue_only

        self.feeder_cfg = self.config.get("feeder", {})
        self.schema_cfg = self.config.get("schema_guard", {})
        self.orch_bridge_cfg = self.config.get("orchestrator", {})

        self.settling_sec = self.feeder_cfg.get("file_stability_window_seconds", 5)
        self.default_source = source_override or self.feeder_cfg.get("source_system_code", "ORACLE_FUSION")

        concurrency_cfg = self.feeder_cfg.get("concurrency", {})
        self.poll_interval_sec = concurrency_cfg.get("poll_interval_seconds", 5)
        self.lock_timeout_sec = concurrency_cfg.get("lock_timeout_seconds", 7200)

        drop_zone_cfg = self.feeder_cfg.get("drop_zone")
        if not drop_zone_cfg:
            raise ConfigurationError("Configuration error: 'feeder.drop_zone' is required.")
        self.drop_zone = ConfigLoader.resolve_path(
            self.base_dir, drop_zone_cfg, source=self.default_source, ensure_exists=True
        )

        logs_dir_cfg = self.feeder_cfg.get("logs_dir") or os.path.join(_SCRIPT_DIR, "logs")
        self.logs_dir = ConfigLoader.resolve_path(
            self.base_dir, logs_dir_cfg, source=self.default_source, ensure_exists=True
        )

        mapping_cfg = (
            self.feeder_cfg.get("entity_mapping_file")
            or self.schema_cfg.get("entity_mapping_file")
            or "entity_mapping.csv"
        )
        self.mapping_file = (
            mapping_cfg
            if os.path.isabs(mapping_cfg)
            else os.path.normpath(os.path.join(self.base_dir, mapping_cfg))
        )

        orch_dir_cfg = self.orch_bridge_cfg.get("orchestrator_dir", "../DATA_INGESTION_ORCHESTRATOR")
        self.orch_dir = (
            orch_dir_cfg
            if os.path.isabs(orch_dir_cfg)
            else os.path.normpath(os.path.join(self.base_dir, orch_dir_cfg))
        )
        self.orch_config = self.orch_bridge_cfg.get("config_file", "config.json")

        landing_cfg = self.orch_bridge_cfg.get("landing_zone")
        if landing_cfg:
            self.landing_zone_base = (
                landing_cfg
                if os.path.isabs(landing_cfg)
                else os.path.normpath(os.path.join(self.base_dir, landing_cfg))
            )
        else:
            self.landing_zone_base = os.path.normpath(os.path.join(self.orch_dir, "../LANDING_ZONE"))

        self.python_bin = self.orch_bridge_cfg.get("python_executable", "python3")
        self.trigger_orchestrator = (
            False
            if self.enqueue_only
            else bool(self.orch_bridge_cfg.get("trigger_orchestrator", True))
        )

        self.logger = LoggingFactory.setup_feeder_logger(self.logs_dir, source=self.default_source)
        self.logger.info("=" * 70)
        self.logger.info(f"STARTING BICC INTAKE CONTROLLER - PRODUCER RUN (Source: {self.default_source})")
        self.logger.info("=" * 70)
        self.logger.info(f"Drop Zone: {self.drop_zone}")
        self.logger.info(f"Orchestrator Directory: {self.orch_dir}")
        self.logger.info(f"Trigger Orchestrator: {self.trigger_orchestrator}")

    def acquire_concurrency_guards(self) -> ProcessLock:
        """Returns a configured ProcessLock for `.feeder_<source>.pid` in `drop_zone`."""
        feeder_lock_file = os.path.join(self.drop_zone, f".feeder_{self.default_source}.pid")
        return ProcessLock(
            lock_file=feeder_lock_file,
            poll_interval_sec=self.poll_interval_sec,
            timeout_sec=self.lock_timeout_sec,
            logger=self.logger,
            label="BICC Intake Controller",
        )

    def discover_and_sort_batches(self) -> List[Dict[str, Any]]:
        """Scans DROP_ZONE for raw manifests and sorts them in FIFO chronological order."""
        raw_manifests = scan_drop_zone(self.drop_zone, self.default_source)
        if not raw_manifests:
            return []
        return sort_batches_fifo(raw_manifests)

    def process_single_batch(
        self,
        batch_info: Dict[str, Any],
        mapper_or_conn: Any,
        maybe_mapper: Optional[EntityMapper] = None,
    ) -> str:
        """
        Validates and enqueues a single batch into `LANDING_ZONE/<SOURCE>/BATCH_<TS>_<ID>/`:
          1. Verifies payload file stability (>0 bytes, settling window).
          2. Matches files to target entities via `EntityMapper`.
          3. Extracts CSV/PECSV/ZIP/GZ header columns in memory (`extract_csv_columns`).
          4. Computes MD5 checksums and file sizes in a single pass.
          5. Stages `.staging_BATCH_<TS>_<ID>/` (with `RAW/`, `RECEIPT_*.json`, `MANIFEST_*.MF`,
             `batch_descriptor.json`, and payload files) and atomically renames to `BATCH_<TS>_<ID>/`.
        Returns the published queue capsule directory path.
        """
        mapper: EntityMapper = maybe_mapper if maybe_mapper is not None else mapper_or_conn

        batch_id = str(batch_info["batch_id"])
        source = str(batch_info["source"])
        extract_ts = str(batch_info["extract_timestamp"])
        raw_manifest_path = batch_info["manifest_path"]
        declared_files = list(batch_info["declared_files"])

        # 1. Stability Sentry
        stable, reason = verify_payload_stability(self.drop_zone, declared_files, self.settling_sec)
        if not stable:
            self.logger.critical(f"Payload stability check failed for batch {batch_id}: {reason}")
            self.logger.critical("Halting pipeline under FAIL_STOP policy to prevent corrupt ingestion.")
            raise BatchValidationError(f"Payload stability check failed for batch {batch_id}: {reason}")

        # 2. Group files by target entity & retain matched rules
        entity_groups: Dict[str, List[str]] = {}
        entity_rules: Dict[str, Any] = {}
        file_to_table: Dict[str, str] = {}

        for fname in declared_files:
            matched_rule = mapper.match_file(fname, file_dir=self.drop_zone, source=self.default_source)
            if not matched_rule:
                msg = f"FATAL: File '{fname}' does not match any recognized entity in '{self.mapping_file}'."
                self.logger.critical(msg)
                raise BatchValidationError(msg)
            target_table = matched_rule.target_table
            file_to_table[fname] = target_table
            entity_rules[target_table] = matched_rule
            entity_groups.setdefault(target_table, []).append(fname)

        self.logger.info(
            f"Batch {batch_id} targets {len(entity_groups)} distinct staging table(s): {list(entity_groups.keys())}"
        )

        # 3. Extract CSV/PECSV/ZIP/GZ Header Columns in Memory (Zero DB Dependency)
        entity_headers: Dict[str, List[str]] = {}
        for target_table, files in entity_groups.items():
            header_file = select_header_chunk_file(files)
            header_full_path = os.path.join(self.drop_zone, header_file)
            try:
                cols = extract_csv_columns(header_full_path)
                entity_headers[target_table] = cols
                self.logger.info(
                    f"[{target_table}] Extracted {len(cols)} header column(s) in memory from '{header_file}'."
                )
            except Exception as exc:
                msg = f"FATAL: Header extraction failed for {target_table} ('{header_file}'): {exc}"
                self.logger.critical(msg)
                raise BatchValidationError(msg) from exc

        # 4. Single-Pass MD5 Calculation & Descriptor Assembly
        file_checksum_pairs = []
        receipt_files = []
        entities_descriptor: Dict[str, Dict[str, Any]] = {}

        for target_table, files in entity_groups.items():
            rule = entity_rules[target_table]
            entities_descriptor[target_table] = {
                "target_table": target_table,
                "signature": rule.signature,
                "extension": rule.raw_extension,
                "inner_extension": rule.inner_extension,
                "header_columns": entity_headers[target_table],
                "files": [],
            }

        for fname in declared_files:
            fpath = os.path.join(self.drop_zone, fname)
            fsize = os.path.getsize(fpath)
            md5_hash = compute_file_md5(fpath)
            file_checksum_pairs.append((fname, md5_hash))
            tbl = file_to_table[fname]
            file_entry = {
                "filename": fname,
                "filesize_bytes": fsize,
                "md5": md5_hash,
            }
            entities_descriptor[tbl]["files"].append(file_entry)
            receipt_files.append({
                "filename": fname,
                "filesize_bytes": fsize,
                "md5": md5_hash,
                "target_table": tbl,
                "schema_action": "DEFERRED_TO_ORCHESTRATOR",
            })

        # 5. Stage & Atomically Publish Batch Capsule in LANDING_ZONE/<SOURCE>/BATCH_<TS>_<ID>/
        target_landing_zone = ConfigLoader.resolve_path(
            self.base_dir, self.landing_zone_base, source=source, ensure_exists=True
        )
        capsule_dir, _ = stage_batch_capsule_to_landing_zone(
            landing_zone_dir=target_landing_zone,
            drop_zone_dir=self.drop_zone,
            raw_manifest_path=raw_manifest_path,
            source=source,
            extract_timestamp=extract_ts,
            batch_id=batch_id,
            entities_descriptor=entities_descriptor,
            file_checksum_pairs=file_checksum_pairs,
            receipt_payload=receipt_files,
            declared_files=declared_files,
        )
        self.logger.info(f"[ENQUEUED] Batch {batch_id} staged and published at '{capsule_dir}'.")
        return capsule_dir

    process_batch = process_single_batch
    enqueue_batch_capsule = process_single_batch

    def _delegate_to_orchestrator(self, source: str) -> None:
        """Invokes DATA_INGESTION_ORCHESTRATOR once to drain the LANDING_ZONE queue under FAIL_STOP semantics."""
        extra_env: Optional[Dict[str, str]] = None
        if "auto_migrate_ddl" in self.schema_cfg and self.schema_cfg.get("auto_migrate_ddl") is False:
            extra_env = {"DL_AUTO_MIGRATE_DDL": "false"}

        self.logger.info(f"Delegating queue execution for source '{source}' to DATA_INGESTION_ORCHESTRATOR...")
        if extra_env:
            orch_rc = invoke_orchestrator(
                orchestrator_dir=self.orch_dir,
                config_file=self.orch_config,
                python_bin=self.python_bin,
                env=extra_env,
                skip_post_tasks=self.skip_post_tasks,
                source=source,
            )
        else:
            orch_rc = invoke_orchestrator(
                orchestrator_dir=self.orch_dir,
                config_file=self.orch_config,
                python_bin=self.python_bin,
                skip_post_tasks=self.skip_post_tasks,
                source=source,
            )
        if orch_rc != 0:
            self.logger.critical(
                f"FATAL: DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} for source '{source}'!"
            )
            self.logger.critical("Applying FAIL_STOP policy: Halting feeder immediately to preserve data lineage.")
            raise OrchestratorExecutionError(
                f"DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} for source '{source}'"
            )

    def run(
        self,
        skip_post_tasks: Optional[bool] = None,
        enqueue_only: Optional[bool] = None,
    ) -> int:
        """Executes the Intake Controller producer workflow and returns exit code (0 or 1)."""
        if skip_post_tasks is not None:
            self.skip_post_tasks = skip_post_tasks
        if enqueue_only is not None:
            self.enqueue_only = enqueue_only
            if self.enqueue_only:
                self.trigger_orchestrator = False

        try:
            with self.acquire_concurrency_guards():
                fifo_queue = self.discover_and_sort_batches()
                if not fifo_queue:
                    self.logger.info("Drop zone is empty. No new batches discovered in DROP_ZONE.")
                    if self.trigger_orchestrator:
                        self._delegate_to_orchestrator(self.default_source)
                    return 0

                self.logger.info(
                    f"Discovered {len(fifo_queue)} batch(es) in drop zone. "
                    "Enqueueing in FIFO order (oldest to newest):"
                )
                for idx, b in enumerate(fifo_queue, 1):
                    self.logger.info(
                        f"  [{idx}/{len(fifo_queue)}] Batch ID: {b['batch_id']}, "
                        f"Extract TS: {b['extract_timestamp']}, Files: {len(b['declared_files'])}"
                    )

                if not os.path.exists(self.mapping_file):
                    self.logger.critical(f"FATAL: Entity mapping file not found at: '{self.mapping_file}'")
                    return 1

                mapper = EntityMapper(mapping_file=self.mapping_file, default_source=self.default_source)
                rules = mapper.load_rules(source=self.default_source)
                self.logger.info(f"Loaded {len(rules)} entity mapping rules for source '{self.default_source}'.")

                for batch_idx, batch_info in enumerate(fifo_queue, 1):
                    self.logger.info("-" * 70)
                    self.logger.info(
                        f"ENQUEUEING BATCH {batch_idx}/{len(fifo_queue)}: "
                        f"ID={batch_info['batch_id']} (Source={batch_info['source']}, TS={batch_info['extract_timestamp']})"
                    )
                    self.logger.info("-" * 70)
                    self.process_single_batch(batch_info, mapper)

                if self.trigger_orchestrator:
                    self._delegate_to_orchestrator(self.default_source)
                else:
                    self.logger.info(
                        f"Enqueue-only mode active (trigger_orchestrator=False). "
                        f"All {len(fifo_queue)} batch(es) staged in LANDING_ZONE."
                    )

                self.logger.info("=" * 70)
                self.logger.info(f"ALL {len(fifo_queue)} BATCHES PROCESSED WITH 100% SUCCESS!")
                self.logger.info("BICC INTAKE CONTROLLER TERMINATING NORMALLY (RC 0).")
                self.logger.info("=" * 70)
                return 0

        except DataLoaderError:
            return 1
        except Exception as exc:
            self.logger.critical(f"FATAL: Unhandled exception in BICC Intake Controller: {exc}")
            return 1
