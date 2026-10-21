"""
lib/intake_service.py - Cohesive BICC Intake Controller Service

Encapsulates the full BICC Intake Controller lifecycle in a modular, unit-testable
service class (`IntakeControllerService`) backed by `dataloader_core`.
Methods raise typed `DataLoaderError` subclasses instead of invoking `sys.exit()`
inside business logic.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional

_SCRIPT_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
_COMMON_DIR = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from dataloader_core.capsule import CapsulePathResolver
from dataloader_core.config import ConfigLoader
from dataloader_core.db import TeradataConnectionManager, validate_sql_identifier
from dataloader_core.exceptions import (
    BatchValidationError,
    ConfigurationError,
    DataLoaderError,
    OrchestratorExecutionError,
    SchemaSyncError,
)
from dataloader_core.locking import ProcessLock, is_pid_alive, read_pid_file, wait_for_pid_completion
from dataloader_core.logging_utils import LoggingFactory
from dataloader_core.mapping import EntityMapper

from lib.manifest_factory import (
    compute_file_md5,
    create_standardized_manifest,
    transfer_batch_to_landing_zone,
    vault_raw_manifest,
)
from lib.orchestrator_bridge import invoke_orchestrator
from lib.scanner import (
    parse_manifest_filename,
    scan_drop_zone,
    sort_batches_fifo,
    verify_payload_stability,
)
from lib.schema_guard import sync_entity_schema_if_drifted


def select_header_chunk_file(file_list: List[str]) -> str:
    """
    Among a list of chunk files for an entity, returns the file containing the header.
    In BICC multi-chunk conventions, part1.csv or the first alphabetical chunk contains line 1 header.
    """
    return sorted(file_list)[0]


class IntakeControllerService:
    """Service orchestrating BICC intake scanning, Schema Guard DDL, vaulting, and delegation."""

    def __init__(
        self,
        config_path: str,
        source_override: Optional[str] = None,
        skip_post_tasks: bool = False,
    ) -> None:
        self.config_path = config_path
        self.config = ConfigLoader.load_json_config(config_path)
        self.base_dir = os.path.dirname(os.path.abspath(config_path))
        self.skip_post_tasks = skip_post_tasks

        self.feeder_cfg = self.config.get("feeder", {})
        self.schema_cfg = self.config.get("schema_guard", {})
        self.orch_bridge_cfg = self.config.get("orchestrator", {})
        self.td_cfg = self.config.get("teradata", {})

        self.settling_sec = self.feeder_cfg.get("file_stability_window_seconds", 5)
        self.default_source = source_override or self.feeder_cfg.get("source_system_code", "ORACLE_FUSION")
        self.source_post_tasks = self.feeder_cfg.get("post_ingestion_tasks")

        concurrency_cfg = self.feeder_cfg.get("concurrency", {})
        self.poll_interval_sec = concurrency_cfg.get("poll_interval_seconds", 5)
        self.lock_timeout_sec = concurrency_cfg.get("lock_timeout_seconds", 7200)

        drop_zone_cfg = self.feeder_cfg.get("drop_zone")
        if not drop_zone_cfg:
            raise ConfigurationError("Configuration error: 'feeder.drop_zone' is required.")
        self.drop_zone = ConfigLoader.resolve_path(
            self.base_dir, drop_zone_cfg, source=self.default_source, ensure_exists=True
        )

        raw_archive_cfg = self.feeder_cfg.get("raw_manifest_archive")
        if raw_archive_cfg:
            self.raw_archive = (
                raw_archive_cfg
                if os.path.isabs(raw_archive_cfg)
                else os.path.normpath(os.path.join(self.base_dir, raw_archive_cfg))
            )
        else:
            self.raw_archive = os.path.join(self.drop_zone, "archive")

        logs_dir_cfg = self.feeder_cfg.get("logs_dir") or os.path.join(_SCRIPT_DIR, "logs")
        self.logs_dir = ConfigLoader.resolve_path(
            self.base_dir, logs_dir_cfg, source=self.default_source, ensure_exists=True
        )

        self.target_db = self.schema_cfg.get("target_database") or f"{os.environ.get('TDENV', '')}SIDFINSTG"
        mapping_cfg = self.schema_cfg.get("entity_mapping_file", "entity_mapping.csv")
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

        self.loading_zone = ConfigLoader.resolve_path(
            self.base_dir,
            os.path.normpath(os.path.join(self.landing_zone_base, "../LOADING_ZONE")),
            source=self.default_source,
            ensure_exists=True,
        )

        self.python_bin = self.orch_bridge_cfg.get("python_executable", "python3")

        self.logger = LoggingFactory.setup_feeder_logger(self.logs_dir, source=self.default_source)
        self.logger.info("=" * 70)
        self.logger.info(f"STARTING BICC INTAKE CONTROLLER - SUPERVISOR RUN (Source: {self.default_source})")
        self.logger.info("=" * 70)
        self.logger.info(f"Drop Zone: {self.drop_zone}")
        self.logger.info(f"Target Database: {self.target_db}")
        self.logger.info(f"Orchestrator Directory: {self.orch_dir}")

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

    def _write_post_tasks_file(self, source: str, extract_ts: str, batch_id: str) -> Optional[str]:
        """Writes source-specific post-ingestion tasks JSON inside the Batch Log Capsule."""
        if not self.source_post_tasks:
            return None
        clean_ts = CapsulePathResolver.normalize_timestamp_token(extract_ts)
        capsule_log_dir = CapsulePathResolver.get_log_capsule_dir(
            self.logs_dir, source, batch_id, clean_ts, ensure_exists=True
        )
        post_tasks_file = os.path.join(capsule_log_dir, f"POST_TASKS_{source}_{clean_ts}_{batch_id}.json")
        try:
            with open(post_tasks_file, "w", encoding="utf-8") as ptf:
                json.dump(self.source_post_tasks, ptf, indent=2)
            self.logger.info(f"Generated source-specific post-tasks file for Orchestrator: {post_tasks_file}")
            return post_tasks_file
        except Exception as exc:
            self.logger.warning(f"Could not write post-tasks file {post_tasks_file}: {exc}")
            return None

    def recover_inflight_batch_if_present(
        self,
        conn_mgr: Optional[TeradataConnectionManager] = None,
    ) -> None:
        """
        Checks for active Orchestrator runs in BATCH_RUNS and `.orchestrator.pid`,
        waits if active, and delegates in-flight crash recovery if an uncompleted
        manifest is present in LOADING_ZONE.
        """
        if not self.loading_zone:
            return

        # 1. Pre-flight check of Teradata BATCH_RUNS for active batch & PROCESS_PID
        own_mgr = False
        try:
            tech_db = self.td_cfg.get("technical_database") or f"{os.environ.get('TDENV', '')}SIDFINTCH"
            validate_sql_identifier(tech_db)
            active_mgr = conn_mgr
            if active_mgr is None:
                active_mgr = TeradataConnectionManager(self.td_cfg, max_connections=1)
                own_mgr = True
            with active_mgr.cursor() as cur:
                cur.execute(
                    f"SELECT BATCH_ID, SOURCE, START_TIME, PROCESS_PID "
                    f"FROM {tech_db}.BATCH_RUNS WHERE STATUS = 'RUNNING' AND SOURCE = ?",
                    [self.default_source],
                )
                row = cur.fetchone()
                if row and row[3]:
                    db_pid = int(row[3])
                    if is_pid_alive(db_pid) and db_pid != os.getpid():
                        wait_for_pid_completion(
                            pid=db_pid,
                            poll_interval_sec=self.poll_interval_sec,
                            timeout_sec=self.lock_timeout_sec,
                            logger=self.logger,
                            context_label=f"batch {row[0]}",
                            initial_message=(
                                f"Active batch {row[0]} (Source: {self.default_source}) detected in BATCH_RUNS "
                                f"with running PID {db_pid}. Entering active waiting loop..."
                            ),
                            poll_message=f"Batch {row[0]} (PID {db_pid}) still active in BATCH_RUNS. Waiting {self.poll_interval_sec}s...",
                            completion_message=f"Active batch {row[0]} (PID {db_pid}) finished execution.",
                        )
        except DataLoaderError:
            raise
        except Exception as exc:
            self.logger.debug(f"Pre-flight Teradata BATCH_RUNS check skipped or non-blocking: {exc}")
        finally:
            if own_mgr and active_mgr is not None:
                active_mgr.close_all()

        # 2. Check `.orchestrator.pid` in LOADING_ZONE
        orch_pid_file = os.path.join(self.loading_zone, ".orchestrator.pid")
        if os.path.exists(orch_pid_file):
            orch_pid = read_pid_file(orch_pid_file)
            if is_pid_alive(orch_pid) and orch_pid != os.getpid():
                wait_for_pid_completion(
                    pid=orch_pid,
                    poll_interval_sec=self.poll_interval_sec,
                    timeout_sec=self.lock_timeout_sec,
                    logger=self.logger,
                    context_label="Orchestrator",
                    initial_message=(
                        f"An Orchestrator run is actively in progress in LOADING_ZONE (PID {orch_pid}). "
                        "Entering active waiting loop..."
                    ),
                    poll_message=f"Orchestrator PID {orch_pid} still active in LOADING_ZONE. Waiting {self.poll_interval_sec}s...",
                    completion_message=f"Active Orchestrator (PID {orch_pid}) finished execution.",
                )
            if os.path.exists(orch_pid_file) and not is_pid_alive(read_pid_file(orch_pid_file)):
                try:
                    os.remove(orch_pid_file)
                except OSError:
                    pass

        # 3. Check for uncompleted in-flight manifest in LOADING_ZONE
        in_flight_mf = [f for f in os.listdir(self.loading_zone) if f.upper().endswith(".MF")]
        if in_flight_mf:
            self.logger.info(
                f"In-flight batch detected in LOADING_ZONE ({in_flight_mf[0]}). "
                "Resuming Orchestrator delegation (Crash Recovery)..."
            )
            mf_meta = parse_manifest_filename(in_flight_mf[0], self.default_source)
            inf_src = mf_meta.get("source", self.default_source)
            inf_ts = str(mf_meta.get("extract_timestamp", ""))
            inf_bid = mf_meta.get("batch_id", "INFLIGHT")
            post_tasks_file = self._write_post_tasks_file(inf_src, inf_ts, inf_bid)

            rc = invoke_orchestrator(
                self.orch_dir,
                self.orch_config,
                self.python_bin,
                skip_post_tasks=self.skip_post_tasks,
                source=self.default_source,
                post_tasks_file=post_tasks_file,
            )
            if rc != 0:
                msg = "FATAL: In-flight batch recovery failed under FAIL_STOP policy."
                self.logger.critical(msg)
                raise OrchestratorExecutionError(msg)
            self.logger.info("In-flight batch completed and archived successfully.")

    check_and_recover_inflight = recover_inflight_batch_if_present

    def discover_and_sort_batches(self) -> List[Dict[str, Any]]:
        """Scans DROP_ZONE for raw manifests and sorts them in FIFO chronological order."""
        raw_manifests = scan_drop_zone(self.drop_zone, self.default_source)
        if not raw_manifests:
            return []
        return sort_batches_fifo(raw_manifests)

    def process_single_batch(
        self,
        batch_info: Dict[str, Any],
        conn_mgr: TeradataConnectionManager,
        mapper: EntityMapper,
    ) -> None:
        """
        Processes a single batch: stability check, entity mapping, Schema Guard DDL sync,
        MD5 hashing, raw manifest vaulting, standardized manifest creation, LANDING_ZONE
        transfer, and Orchestrator execution under FAIL_STOP semantics.
        """
        batch_id = batch_info["batch_id"]
        source = batch_info["source"]
        extract_ts = batch_info["extract_timestamp"]
        raw_manifest_path = batch_info["manifest_path"]
        declared_files = batch_info["declared_files"]

        # 1. Stability Sentry
        stable, reason = verify_payload_stability(self.drop_zone, declared_files, self.settling_sec)
        if not stable:
            self.logger.critical(f"Payload stability check failed for batch {batch_id}: {reason}")
            self.logger.critical("Halting pipeline under FAIL_STOP policy to prevent corrupt ingestion.")
            raise BatchValidationError(f"Payload stability check failed for batch {batch_id}: {reason}")

        # 2. Group files by target entity
        entity_groups: Dict[str, List[str]] = {}
        file_to_table: Dict[str, str] = {}

        for fname in declared_files:
            matched_rule = mapper.match_file(fname, file_dir=self.drop_zone, source=self.default_source)
            if not matched_rule:
                msg = f"FATAL: File '{fname}' does not match any recognized entity in '{self.mapping_file}'."
                self.logger.critical(msg)
                raise BatchValidationError(msg)
            target_table = matched_rule.target_table
            file_to_table[fname] = target_table
            entity_groups.setdefault(target_table, []).append(fname)

        self.logger.info(
            f"Batch {batch_id} targets {len(entity_groups)} distinct staging table(s): {list(entity_groups.keys())}"
        )

        # 3. Auto-DDL Schema Synchronization
        schema_statuses: Dict[str, str] = {}
        with conn_mgr.acquire_connection() as db_conn:
            for target_table, files in entity_groups.items():
                header_file = select_header_chunk_file(files)
                header_full_path = os.path.join(self.drop_zone, header_file)
                try:
                    _, action, _ = sync_entity_schema_if_drifted(
                        connection=db_conn,
                        target_db=self.target_db,
                        target_table=target_table,
                        sample_file_path=header_full_path,
                        default_type=self.schema_cfg.get("default_column_type", "VARCHAR(4000)"),
                        character_set=self.schema_cfg.get("character_set", "UNICODE"),
                        auto_migrate_ddl=self.schema_cfg.get("auto_migrate_ddl", True),
                        load_mode=self.schema_cfg.get("default_load_mode", "replace"),
                    )
                    schema_statuses[target_table] = action
                except Exception as exc:
                    msg = f"FATAL: Schema synchronization failed for {target_table}: {exc}"
                    self.logger.critical(msg)
                    raise SchemaSyncError(msg) from exc

        # 4. Compute MD5 Checksums for all data files
        file_checksum_pairs = []
        receipt_files = []
        for fname in declared_files:
            fpath = os.path.join(self.drop_zone, fname)
            md5_hash = compute_file_md5(fpath)
            file_checksum_pairs.append((fname, md5_hash))
            tbl = file_to_table[fname]
            receipt_files.append({
                "filename": fname,
                "filesize_bytes": os.path.getsize(fpath),
                "md5": md5_hash,
                "target_table": tbl,
                "schema_action": schema_statuses.get(tbl, "UNKNOWN"),
            })

        # 5. Vault Raw BICC Manifest + Intake Receipt
        vault_raw_manifest(
            raw_manifest_path=raw_manifest_path,
            archive_base_dir=self.raw_archive,
            source=source,
            extract_timestamp=extract_ts,
            batch_id=batch_id,
            receipt_payload=receipt_files,
        )

        # 6. Synthesize Standardized Manifest in Orchestrator's LANDING_ZONE/<SOURCE>
        target_landing_zone = ConfigLoader.resolve_path(
            self.base_dir, self.landing_zone_base, source=source, ensure_exists=True
        )
        create_standardized_manifest(
            output_dir=target_landing_zone,
            source=source,
            extract_timestamp=extract_ts,
            batch_id=batch_id,
            file_checksum_pairs=file_checksum_pairs,
        )

        # 7. Transfer Data Files to LANDING_ZONE
        transfer_batch_to_landing_zone(
            drop_zone_dir=self.drop_zone,
            landing_zone_dir=target_landing_zone,
            data_files=declared_files,
        )

        # 8. Prepare source-specific post-tasks file if configured
        post_tasks_file = self._write_post_tasks_file(source, extract_ts, batch_id)

        # 9. Launch Orchestrator and Enforce FAIL_STOP Barrier
        self.logger.info(f"Delegating execution of batch {batch_id} to DATA_INGESTION_ORCHESTRATOR...")
        orch_rc = invoke_orchestrator(
            orchestrator_dir=self.orch_dir,
            config_file=self.orch_config,
            python_bin=self.python_bin,
            skip_post_tasks=self.skip_post_tasks,
            source=source,
            post_tasks_file=post_tasks_file,
        )
        if orch_rc != 0:
            self.logger.critical(
                f"FATAL: DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} on Batch {batch_id}!"
            )
            self.logger.critical("Applying FAIL_STOP policy: Halting feeder immediately to preserve data lineage.")
            raise OrchestratorExecutionError(
                f"DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} on Batch {batch_id}"
            )

        self.logger.info(f"[SUCCESS] Batch {batch_id} ingested, verified and transformed successfully.")

    process_batch = process_single_batch

    def run(self, skip_post_tasks: Optional[bool] = None) -> int:
        """Executes the full Intake Controller supervisor workflow and returns exit code (0 or 1)."""
        if skip_post_tasks is not None:
            self.skip_post_tasks = skip_post_tasks

        conn_mgr: Optional[TeradataConnectionManager] = None
        try:
            with self.acquire_concurrency_guards():
                self.recover_inflight_batch_if_present()

                fifo_queue = self.discover_and_sort_batches()
                if not fifo_queue:
                    self.logger.info("Drop zone is empty. No batches discovered. Exiting gracefully with RC 0.")
                    return 0

                self.logger.info(
                    f"Discovered {len(fifo_queue)} batch(es) in drop zone. "
                    "Processing queue in FIFO order (oldest to newest):"
                )
                for idx, b in enumerate(fifo_queue, 1):
                    self.logger.info(
                        f"  [{idx}/{len(fifo_queue)}] Batch ID: {b['batch_id']}, "
                        f"Extract TS: {b['extract_timestamp']}, Files: {len(b['declared_files'])}"
                    )

                try:
                    conn_mgr = TeradataConnectionManager(self.td_cfg, max_connections=2)
                    with conn_mgr.acquire_connection():
                        pass
                    self.logger.info("Connected successfully to Teradata Vantage for schema introspection.")
                except Exception as exc:
                    self.logger.critical(f"FATAL: Database connection failed: {exc}")
                    return 1

                if not os.path.exists(self.mapping_file):
                    self.logger.critical(f"FATAL: Entity mapping file not found at: '{self.mapping_file}'")
                    return 1

                mapper = EntityMapper(mapping_file=self.mapping_file, default_source=self.default_source)
                rules = mapper.load_rules(source=self.default_source)
                self.logger.info(f"Loaded {len(rules)} entity mapping rules for source '{self.default_source}'.")

                for batch_idx, batch_info in enumerate(fifo_queue, 1):
                    self.logger.info("-" * 70)
                    self.logger.info(
                        f"PROCESSING BATCH {batch_idx}/{len(fifo_queue)}: "
                        f"ID={batch_info['batch_id']} (Source={batch_info['source']}, TS={batch_info['extract_timestamp']})"
                    )
                    self.logger.info("-" * 70)
                    self.process_single_batch(batch_info, conn_mgr, mapper)

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
        finally:
            if conn_mgr is not None:
                conn_mgr.close_all()
