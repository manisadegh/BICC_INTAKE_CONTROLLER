"""
feeder.py - BICC Intake Controller & Auto-DDL Supervisor

Master entrypoint for processing asynchronous multi-batch Oracle Fusion Cloud BICC
exports. Enforces strict FIFO chronological ordering, upload settling validation,
in-memory schema drift detection against DBC.ColumnsV, automated NOPI VARCHAR(4000)
table creation, cryptographic manifest standardizing, raw manifest vaulting, and
delegation to DATA_INGESTION_ORCHESTRATOR under strict FAIL_STOP semantics.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
import atexit
import time
import teradatasql

def is_pid_alive(pid: int) -> bool:
    """Checks if a given process ID is actively running."""
    if pid <= 0:
        return False
    if os.name == 'nt':
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            SYNCHRONIZE = 0x00100000
            process = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
            if process != 0:
                kernel32.CloseHandle(process)
                return True
            return False
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False
        except PermissionError:
            return True

# Ensure local lib/ modules are importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lib.scanner import scan_drop_zone, sort_batches_fifo, verify_payload_stability
from lib.schema_guard import (
    load_entity_mappings,
    match_target_table_for_file,
    sync_entity_schema_if_drifted
)
from lib.manifest_factory import (
    compute_file_md5,
    create_standardized_manifest,
    vault_raw_manifest,
    transfer_batch_to_landing_zone
)
from lib.orchestrator_bridge import invoke_orchestrator


def setup_logger(logs_dir: str, source: Optional[str] = None) -> logging.Logger:
    """
    Sets up dual stream and dedicated file logging in logs_dir:
      LOGS/[<SOURCE>/]LOG_FEEDER_<SOURCE>_<YYYYMMDD_HHMMSS>.log
    """
    os.makedirs(logs_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    src_tag = f"_{source}" if source else ""
    log_file = os.path.join(logs_dir, f"LOG_FEEDER{src_tag}_{ts}.log")

    logger = logging.getLogger("BICC_INTAKE_CONTROLLER")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File Handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Initialized BICC Intake Controller log file: '{log_file}'")
    return logger


def get_teradata_connection(td_cfg: dict) -> teradatasql.TeradataConnection:
    """
    Establishes connection to Teradata Vantage.
    Supports config keys ('host', 'server', 'tdpid'), environment variables (TDPID, TDENV, SID_ADM_USER),
    and stored encrypted passwords via Key/Pass property files.
    """
    if td_cfg is None:
        td_cfg = {}

    host = td_cfg.get("host") or td_cfg.get("server") or td_cfg.get("tdpid") or os.environ.get("TDPID")
    if not host:
        raise ValueError("Teradata host/server not configured. Provide 'host' in config.json or export TDPID.")

    env_user = f"{os.environ.get('TDENV', '')}{os.environ.get('SID_ADM_USER', '')}" if os.environ.get("SID_ADM_USER") else os.environ.get("USER")
    user = td_cfg.get("user") or td_cfg.get("username") or env_user
    if not user:
        raise ValueError("Teradata user not configured. Provide 'user' in config.json or export TDENV and SID_ADM_USER.")

    kwargs = {
        "host": host,
        "user": user,
        "logmech": td_cfg.get("logmech", "TD2")
    }

    # 1. Direct key_file and pass_file configuration
    if "key_file" in td_cfg and "pass_file" in td_cfg:
        kwargs["password"] = f"ENCRYPTED_PASSWORD(file:{td_cfg['key_file']},file:{td_cfg['pass_file']})"
    elif "password" in td_cfg and td_cfg["password"]:
        kwargs["password"] = td_cfg["password"]
    else:
        # 2. Check for matching property files in ~/.dbt or $DBT_PROFILE_DIR
        dbt_dir = os.environ.get("DBT_PROFILE_DIR") or os.path.expanduser("~/.dbt")
        auto_key = os.path.join(dbt_dir, f"{user}_Key.properties")
        auto_pass = os.path.join(dbt_dir, f"{user}_Pass.properties")
        if os.path.exists(auto_key) and os.path.exists(auto_pass):
            kwargs["password"] = f"ENCRYPTED_PASSWORD(file:{auto_key},file:{auto_pass})"
        elif os.environ.get("TD_PASSWORD"):
            kwargs["password"] = os.environ.get("TD_PASSWORD")

    return teradatasql.connect(**kwargs)


def select_header_chunk_file(file_list: List[str]) -> str:
    """
    Among a list of chunk files for an entity, returns the file containing the header.
    In BICC multi-chunk conventions, part1.csv or the first alphabetical chunk contains line 1 header.
    """
    return sorted(file_list)[0]


def expand_env_vars(data):
    """Recursively expands ${VAR} or $VAR environment variables in configuration strings."""
    if isinstance(data, str):
        return os.path.expandvars(data)
    if isinstance(data, dict):
        return {k: expand_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [expand_env_vars(v) for v in data]
    return data


def main():
    parser = argparse.ArgumentParser(description="BICC Intake Controller - Over-Arching Oracle Fusion Feeder")
    parser.add_argument("--config", type=str, required=True, help="Path to controller configuration JSON file")
    parser.add_argument("--skip-post-tasks", action="store_true", default=False, help="Skip execution of downstream post-ingestion tasks (e.g. dbt)")
    parser.add_argument("--source", type=str, default=None, help="Explicit data source system code override")
    args = parser.parse_args()

    # 1. Load Configuration
    if not os.path.exists(args.config):
        print(f"FATAL: Configuration file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    base_dir = os.path.dirname(os.path.abspath(args.config))

    with open(args.config, "r", encoding="utf-8") as f:
        config = expand_env_vars(json.load(f))

    feeder_cfg = config.get("feeder", {})
    schema_cfg = config.get("schema_guard", {})
    orch_bridge_cfg = config.get("orchestrator", {})
    td_cfg = config.get("teradata", {})

    settling_sec = feeder_cfg.get("file_stability_window_seconds", 5)
    default_source = args.source or feeder_cfg.get("source_system_code", "ORACLE_FUSION")

    drop_zone = feeder_cfg.get("drop_zone")
    if drop_zone and not os.path.isabs(drop_zone):
        drop_zone = os.path.normpath(os.path.join(base_dir, drop_zone))

    # Multi-Source Directory Support
    if drop_zone and os.path.isdir(os.path.join(drop_zone, default_source)):
        drop_zone = os.path.join(drop_zone, default_source)

    raw_archive = feeder_cfg.get("raw_manifest_archive")
    if raw_archive and not os.path.isabs(raw_archive):
        raw_archive = os.path.normpath(os.path.join(base_dir, raw_archive))
    elif not raw_archive and drop_zone:
        raw_archive = os.path.join(drop_zone, "archive")

    # Multi-source support for raw manifest vaulting
    if default_source and raw_archive and "ORACLE_FUSION_MANIFESTS" in raw_archive and default_source != "ORACLE_FUSION":
        raw_archive = raw_archive.replace("ORACLE_FUSION_MANIFESTS", f"{default_source}_MANIFESTS")

    logs_dir = feeder_cfg.get("logs_dir")
    if logs_dir and not os.path.isabs(logs_dir):
        logs_dir = os.path.normpath(os.path.join(base_dir, logs_dir))
    elif not logs_dir:
        logs_dir = os.path.join(SCRIPT_DIR, "logs")

    # Always partition logs by source
    if default_source and os.path.basename(logs_dir).lower() != default_source.lower():
        logs_dir = os.path.join(logs_dir, default_source)
    os.makedirs(logs_dir, exist_ok=True)

    # Support partitioned DROP_ZONE/<source> if populated
    src_drop = os.path.join(drop_zone, default_source) if drop_zone else ""
    if src_drop and os.path.isdir(src_drop):
        try:
            if any(not f.startswith(".") for f in os.listdir(src_drop)):
                drop_zone = src_drop
        except Exception:
            pass

    target_db = schema_cfg.get("target_database") or f"{os.environ.get('TDENV', '')}SIDFINSTG"
    mapping_file = schema_cfg.get("entity_mapping_file", "entity_mapping.csv")
    if not os.path.isabs(mapping_file):
        mapping_file = os.path.normpath(os.path.join(base_dir, mapping_file))

    orch_dir = orch_bridge_cfg.get("orchestrator_dir")
    if orch_dir and not os.path.isabs(orch_dir):
        orch_dir = os.path.normpath(os.path.join(base_dir, orch_dir))

    orch_config = orch_bridge_cfg.get("config_file", "config.json")
    landing_zone = orch_bridge_cfg.get("landing_zone")
    if landing_zone and not os.path.isabs(landing_zone):
        landing_zone = os.path.normpath(os.path.join(base_dir, landing_zone))

    python_bin = orch_bridge_cfg.get("python_executable", "python3")

    # 2. Bootstrap Logger
    logger = setup_logger(logs_dir, source=default_source)
    logger.info("=" * 70)
    logger.info(f"STARTING BICC INTAKE CONTROLLER - SUPERVISOR RUN (Source: {default_source})")
    logger.info("=" * 70)
    logger.info(f"Drop Zone: {drop_zone}")
    logger.info(f"Target Database: {target_db}")
    logger.info(f"Orchestrator Directory: {orch_dir}")

    # 2.3 Feeder Concurrency Guard (Mutual Exclusion per Source)
    concurrency_cfg = feeder_cfg.get("concurrency", {})
    poll_interval_sec = concurrency_cfg.get("poll_interval_seconds", 5)
    lock_timeout_sec = concurrency_cfg.get("lock_timeout_seconds", 7200)

    if drop_zone:
        os.makedirs(drop_zone, exist_ok=True)
        feeder_lock_file = os.path.join(drop_zone, f".feeder_{default_source}.pid")
        if os.path.exists(feeder_lock_file):
            try:
                with open(feeder_lock_file, "r") as pf:
                    active_fpid = int(pf.read().strip())
            except Exception:
                active_fpid = -1
            if is_pid_alive(active_fpid) and active_fpid != os.getpid():
                logger.info(f"Another BICC Intake Controller is currently active (PID {active_fpid}). Entering active waiting loop (polling every {poll_interval_sec}s, timeout: {lock_timeout_sec}s)...")
                start_w = time.time()
                while is_pid_alive(active_fpid):
                    if time.time() - start_w > lock_timeout_sec:
                        logger.critical(f"FATAL: Timeout ({lock_timeout_sec}s) exceeded waiting for active feeder (PID {active_fpid}).")
                        sys.exit(1)
                    time.sleep(poll_interval_sec)
                    logger.info(f"Feeder PID {active_fpid} still running. Waiting {poll_interval_sec}s...")
                logger.info(f"Active feeder PID {active_fpid} has completed. Resuming execution.")
            if os.path.exists(feeder_lock_file):
                try:
                    os.remove(feeder_lock_file)
                except Exception:
                    pass

        try:
            with open(feeder_lock_file, "w") as pf:
                pf.write(str(os.getpid()))
        except Exception as e:
            logger.warning(f"Could not write feeder PID lock file {feeder_lock_file}: {e}")

        def cleanup_feeder_pid():
            if os.path.exists(feeder_lock_file):
                try:
                    with open(feeder_lock_file, "r") as pf:
                        spid = int(pf.read().strip())
                    if spid == os.getpid():
                        os.remove(feeder_lock_file)
                except Exception:
                    pass
        atexit.register(cleanup_feeder_pid)

    # 2.5 Check for Active Orchestrator Run or In-Flight Batch in LOADING_ZONE (Crash Recovery)
    loading_zone = os.path.normpath(os.path.join(landing_zone, "../LOADING_ZONE")) if landing_zone else None
    if loading_zone and os.path.exists(loading_zone):
        # 2.5.0 Query Teradata BATCH_RUNS for active batch & PROCESS_PID
        try:
            tech_db = td_cfg.get("technical_database") or f"{os.environ.get('TDENV', '')}SIDFINTCH"
            conn_chk = get_teradata_connection(td_cfg)
            with conn_chk.cursor() as cur:
                cur.execute(f"SELECT BATCH_ID, SOURCE, START_TIME, PROCESS_PID FROM {tech_db}.BATCH_RUNS WHERE STATUS = 'RUNNING'")
                row = cur.fetchone()
                if row and row[3]:
                    db_pid = int(row[3])
                    if is_pid_alive(db_pid) and db_pid != os.getpid():
                        logger.info(f"Active batch {row[0]} detected in BATCH_RUNS with running PID {db_pid}. Entering active waiting loop...")
                        start_w = time.time()
                        while is_pid_alive(db_pid):
                            if time.time() - start_w > lock_timeout_sec:
                                logger.critical(f"FATAL: Timeout ({lock_timeout_sec}s) exceeded waiting for active batch {row[0]} (PID {db_pid}).")
                                sys.exit(1)
                            time.sleep(poll_interval_sec)
                            logger.info(f"Batch {row[0]} (PID {db_pid}) still active in BATCH_RUNS. Waiting {poll_interval_sec}s...")
                        logger.info(f"Active batch {row[0]} (PID {db_pid}) finished execution.")
            conn_chk.close()
        except Exception as e:
            logger.debug(f"Pre-flight Teradata BATCH_RUNS check skipped or non-blocking: {e}")

        orch_pid_file = os.path.join(loading_zone, ".orchestrator.pid")
        if os.path.exists(orch_pid_file):
            try:
                with open(orch_pid_file, "r") as pf:
                    orch_pid = int(pf.read().strip())
            except Exception:
                orch_pid = -1
            if is_pid_alive(orch_pid):
                logger.info(f"An Orchestrator run is actively in progress in LOADING_ZONE (PID {orch_pid}). Entering active wait loop...")
                start_w = time.time()
                while is_pid_alive(orch_pid):
                    if time.time() - start_w > lock_timeout_sec:
                        logger.critical(f"FATAL: Timeout ({lock_timeout_sec}s) exceeded waiting for active Orchestrator (PID {orch_pid}).")
                        sys.exit(1)
                    time.sleep(poll_interval_sec)
                    logger.info(f"Orchestrator PID {orch_pid} still active in LOADING_ZONE. Waiting {poll_interval_sec}s...")
                logger.info(f"Active Orchestrator (PID {orch_pid}) finished execution.")
                if os.path.exists(orch_pid_file):
                    try:
                        os.remove(orch_pid_file)
                    except Exception:
                        pass

        in_flight_mf = [f for f in os.listdir(loading_zone) if f.upper().endswith(".MF")]
        if in_flight_mf:
            logger.info(f"In-flight batch detected in LOADING_ZONE ({in_flight_mf[0]}). Resuming Orchestrator delegation (Crash Recovery)...")
            source_post_tasks = feeder_cfg.get("post_ingestion_tasks")
            post_tasks_file = None
            if source_post_tasks:
                post_tasks_file = os.path.join(logs_dir, f"post_tasks_{default_source}_inflight.json")
                try:
                    with open(post_tasks_file, "w", encoding="utf-8") as ptf:
                        json.dump(source_post_tasks, ptf, indent=2)
                except Exception:
                    post_tasks_file = None

            rc = invoke_orchestrator(
                orch_dir, orch_config, python_bin,
                skip_post_tasks=args.skip_post_tasks,
                source=default_source,
                post_tasks_file=post_tasks_file
            )
            if rc != 0:
                logger.critical("FATAL: In-flight batch recovery failed under FAIL_STOP policy.")
                sys.exit(rc)
            logger.info("In-flight batch completed and archived successfully.")

    # 3. Scan Drop Zone
    raw_manifests = scan_drop_zone(drop_zone, default_source)
    if not raw_manifests:
        logger.info("Drop zone is empty. No batches discovered. Exiting gracefully with RC 0.")
        sys.exit(0)

    # 4. Sort Queue in Strict FIFO Chronological Order
    fifo_queue = sort_batches_fifo(raw_manifests)
    logger.info(f"Discovered {len(fifo_queue)} batch(es) in drop zone. Processing queue in FIFO order (oldest to newest):")
    for idx, b in enumerate(fifo_queue, 1):
        logger.info(f"  [{idx}/{len(fifo_queue)}] Batch ID: {b['batch_id']}, Extract TS: {b['extract_timestamp']}, Files: {len(b['declared_files'])}")

    # 5. Connect to Teradata for Schema Guard Inspections
    try:
        db_conn = get_teradata_connection(td_cfg)
        logger.info("Connected successfully to Teradata Vantage for schema introspection.")
    except Exception as e:
        logger.critical(f"FATAL: Database connection failed: {e}")
        sys.exit(1)

    try:
        # Load entity mapping rules
        if not os.path.exists(mapping_file):
            logger.critical(f"FATAL: Entity mapping file not found at: '{mapping_file}'")
            sys.exit(1)
        mapping_rules = load_entity_mappings(mapping_file, filter_source=default_source)
        logger.info(f"Loaded {len(mapping_rules)} entity mapping rules for source '{default_source}'.")

        # 6. Process Queue in FIFO Order
        for batch_idx, batch_info in enumerate(fifo_queue, 1):
            batch_id = batch_info["batch_id"]
            source = batch_info["source"]
            extract_ts = batch_info["extract_timestamp"]
            raw_manifest_path = batch_info["manifest_path"]
            declared_files = batch_info["declared_files"]

            logger.info("-" * 70)
            logger.info(f"PROCESSING BATCH {batch_idx}/{len(fifo_queue)}: ID={batch_id} (Source={source}, TS={extract_ts})")
            logger.info("-" * 70)

            # 6.1 Stability Sentry
            stable, reason = verify_payload_stability(drop_zone, declared_files, settling_sec)
            if not stable:
                logger.critical(f"Payload stability check failed for batch {batch_id}: {reason}")
                logger.critical("Halting pipeline under FAIL_STOP policy to prevent corrupt ingestion.")
                sys.exit(1)

            # 6.2 Group files by target entity and inspect schemas
            entity_groups = {}
            file_to_table = {}

            for fname in declared_files:
                target_table = match_target_table_for_file(fname, mapping_rules, file_dir=drop_zone)
                if not target_table:
                    logger.critical(f"FATAL: File '{fname}' does not match any recognized entity in '{mapping_file}'.")
                    sys.exit(1)
                file_to_table[fname] = target_table
                entity_groups.setdefault(target_table, []).append(fname)

            logger.info(f"Batch {batch_id} targets {len(entity_groups)} distinct staging table(s): {list(entity_groups.keys())}")

            # 6.3 Auto-DDL Schema Synchronization
            schema_statuses = {}
            for target_table, files in entity_groups.items():
                header_file = select_header_chunk_file(files)
                header_full_path = os.path.join(drop_zone, header_file)

                try:
                    _, action, _ = sync_entity_schema_if_drifted(
                        connection=db_conn,
                        target_db=target_db,
                        target_table=target_table,
                        sample_file_path=header_full_path,
                        default_type=schema_cfg.get("default_column_type", "VARCHAR(4000)")
                    )
                    schema_statuses[target_table] = action
                except Exception as e:
                    logger.critical(f"FATAL: Schema synchronization failed for {target_table}: {e}")
                    sys.exit(1)

            # 6.4 Compute MD5 Checksums for all data files
            file_checksum_pairs = []
            receipt_files = []
            for fname in declared_files:
                fpath = os.path.join(drop_zone, fname)
                md5_hash = compute_file_md5(fpath)
                file_checksum_pairs.append((fname, md5_hash))
                tbl = file_to_table[fname]
                receipt_files.append({
                    "filename": fname,
                    "filesize_bytes": os.path.getsize(fpath),
                    "md5": md5_hash,
                    "target_table": tbl,
                    "schema_action": schema_statuses.get(tbl, "UNKNOWN")
                })

            # 6.5 Vault Raw BICC Manifest + Intake Receipt
            vault_raw_manifest(
                raw_manifest_path=raw_manifest_path,
                archive_base_dir=raw_archive,
                source=source,
                extract_timestamp=extract_ts,
                batch_id=batch_id,
                receipt_payload=receipt_files
            )

            # 6.6 Synthesize Standardized Manifest in Orchestrator's LANDING_ZONE/<SOURCE>
            target_landing_zone = landing_zone or os.path.join(orch_dir, "../LANDING_ZONE")
            if source and os.path.basename(target_landing_zone).lower() != source.lower():
                target_landing_zone = os.path.join(target_landing_zone, source)
            os.makedirs(target_landing_zone, exist_ok=True)

            create_standardized_manifest(
                output_dir=target_landing_zone,
                source=source,
                extract_timestamp=extract_ts,
                batch_id=batch_id,
                file_checksum_pairs=file_checksum_pairs
            )

            # 6.7 Transfer Data Files to LANDING_ZONE
            transfer_batch_to_landing_zone(
                drop_zone_dir=drop_zone,
                landing_zone_dir=target_landing_zone,
                data_files=declared_files
            )

            # 6.7.1 Prepare source-specific post-tasks file if configured in intake controller
            source_post_tasks = feeder_cfg.get("post_ingestion_tasks")
            post_tasks_file = None
            if source_post_tasks:
                clean_ts = str(extract_ts).replace(":", "").replace("-", "").replace(" ", "_")
                year_month = f"{clean_ts[:4]}-{clean_ts[4:6]}" if len(clean_ts) >= 6 else "UNKNOWN_DATE"
                capsule_folder = f"BATCH_{batch_id}_{clean_ts}"
                capsule_log_dir = os.path.join(logs_dir, year_month, capsule_folder)
                os.makedirs(capsule_log_dir, exist_ok=True)

                post_tasks_file = os.path.join(capsule_log_dir, f"POST_TASKS_{source}_{clean_ts}_{batch_id}.json")
                legacy_pt_file = os.path.join(logs_dir, f"post_tasks_{source}_{batch_id}.json")
                try:
                    with open(post_tasks_file, "w", encoding="utf-8") as ptf:
                        json.dump(source_post_tasks, ptf, indent=2)
                    with open(legacy_pt_file, "w", encoding="utf-8") as lptf:
                        json.dump(source_post_tasks, lptf, indent=2)
                    logger.info(f"Generated source-specific post-tasks file for Orchestrator: {post_tasks_file}")
                except Exception as e:
                    logger.warning(f"Could not write post-tasks file {post_tasks_file}: {e}")
                    post_tasks_file = None

            # 6.8 Launch Orchestrator and Enforce Barrier
            logger.info(f"Delegating execution of batch {batch_id} to DATA_INGESTION_ORCHESTRATOR...")
            orch_rc = invoke_orchestrator(
                orchestrator_dir=orch_dir,
                config_file=orch_config,
                python_bin=python_bin,
                skip_post_tasks=args.skip_post_tasks,
                source=source,
                post_tasks_file=post_tasks_file
            )

            # 6.9 Evaluate Outcome (FAIL_STOP)
            if orch_rc != 0:
                logger.critical(f"FATAL: DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} on Batch {batch_id}!")
                logger.critical("Applying FAIL_STOP policy: Halting feeder immediately to preserve data lineage.")
                sys.exit(1)

            logger.info(f"[SUCCESS] Batch {batch_id} ingested, verified and transformed successfully.")

        # 7. Final Wrap-up
        logger.info("=" * 70)
        logger.info(f"ALL {len(fifo_queue)} BATCHES PROCESSED WITH 100% SUCCESS!")
        logger.info("BICC INTAKE CONTROLLER TERMINATING NORMALLY (RC 0).")
        logger.info("=" * 70)
        sys.exit(0)

    finally:
        try:
            db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
