import os
import sys
import json
import logging
import argparse
from datetime import datetime
import teradatasql

# Import local modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from lib.scanner import scan_drop_zone, sort_batches_fifo, verify_payload_stability
from lib.schema_guard import load_entity_mappings, match_target_table_for_file, sync_entity_schema_if_drifted
from lib.manifest_factory import compute_file_md5, create_standardized_manifest, vault_raw_manifest, transfer_batch_to_landing_zone
from lib.orchestrator_bridge import invoke_orchestrator

def setup_logger(logs_dir):
    """
    Sets up dual stream and dedicated file logging in logs_dir:
      logs/feeder_<YYYYMMDD_HHMMSS>.log
    """
    os.makedirs(logs_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"feeder_{ts}.log").replace('\\', '/')

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
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Initialized BICC Intake Controller log file: '{log_file}'")
    return logger

def get_teradata_connection(td_cfg):
    """
    Establishes connection to Teradata Vantage.
    Supports config keys ('host', 'server', 'tdpid'), environment variables (TDPID, TDENV, SID_ADM_USER),
    and stored encrypted passwords via Key/Pass property files.
    """
    if td_cfg is None:
        td_cfg = {}

    host = td_cfg.get("host") or td_cfg.get("server") or td_cfg.get("tdpid") or os.environ.get("TDPID")
    if not host:
        raise ValueError("Teradata host/server not configured. Provide 'host' (or 'server') in config.json or export TDPID.")

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
    elif "pass_file" in td_cfg and "key_file" in td_cfg:
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

def select_header_chunk_file(file_list):
    """
    Among a list of chunk files for an entity, returns the file that contains the header.
    In BICC multi-chunk conventions, part1.csv or the first alphabetical chunk has the header.
    """
    sorted_files = sorted(file_list)
    return sorted_files[0]

def main():
    parser = argparse.ArgumentParser(description="BICC Intake Controller - Over-Arching Oracle Fusion Feeder")
    parser.add_argument("--config", type=str, required=True, help="Path to controller configuration JSON file")
    args = parser.parse_args()

    # 1. Load Configuration
    if not os.path.exists(args.config):
        print(f"FATAL: Configuration file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    feeder_cfg = config.get("feeder", {})
    schema_cfg = config.get("schema_guard", {})
    orch_bridge_cfg = config.get("orchestrator", {})
    td_cfg = config.get("teradata", {})

    drop_zone = feeder_cfg.get("drop_zone")
    raw_archive = feeder_cfg.get("raw_manifest_archive", os.path.join(drop_zone, "archive"))
    logs_dir = feeder_cfg.get("logs_dir", os.path.join(SCRIPT_DIR, "logs"))
    settling_sec = feeder_cfg.get("file_stability_window_seconds", 5)
    default_source = feeder_cfg.get("source_system_code", "ORACLE_FUSION")

    target_db = schema_cfg.get("target_database", "globalmart_stg")
    mapping_file = schema_cfg.get("entity_mapping_file", os.path.join(SCRIPT_DIR, "entity_mapping.csv"))
    if not os.path.isabs(mapping_file):
        mapping_file = os.path.join(os.path.dirname(os.path.abspath(args.config)), mapping_file)

    orch_dir = orch_bridge_cfg.get("orchestrator_dir")
    orch_config = orch_bridge_cfg.get("config_file", "config.json")
    landing_zone = orch_bridge_cfg.get("landing_zone")
    python_bin = orch_bridge_cfg.get("python_executable", sys.executable)

    # 2. Bootstrap Logger
    logger = setup_logger(logs_dir)
    logger.info("=" * 70)
    logger.info("STARTING BICC INTAKE CONTROLLER - SUPERVISOR RUN")
    logger.info("=" * 70)
    logger.info(f"Drop Zone: {drop_zone}")
    logger.info(f"Target Database: {target_db}")
    logger.info(f"Orchestrator Directory: {orch_dir}")

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
        entity_groups = {}  # {TARGET_TABLE: [file1, file2]}
        receipt_files = []

        for fname in declared_files:
            target_table = match_target_table_for_file(fname, mapping_rules)
            if not target_table:
                logger.critical(f"FATAL: File '{fname}' does not match any recognized entity in '{mapping_file}'.")
                sys.exit(1)
            if target_table not in entity_groups:
                entity_groups[target_table] = []
            entity_groups[target_table].append(fname)

        logger.info(f"Batch {batch_id} targets {len(entity_groups)} distinct staging table(s): {list(entity_groups.keys())}")

        # 6.3 Auto-DDL Schema Synchronization
        schema_statuses = {}
        for target_table, files in entity_groups.items():
            header_file = select_header_chunk_file(files)
            header_full_path = os.path.join(drop_zone, header_file)
            
            try:
                is_identical, action, cols = sync_entity_schema_if_drifted(
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
        for fname in declared_files:
            fpath = os.path.join(drop_zone, fname)
            md5_hash = compute_file_md5(fpath)
            file_checksum_pairs.append((fname, md5_hash))
            receipt_files.append({
                "filename": fname,
                "filesize_bytes": os.path.getsize(fpath),
                "md5": md5_hash,
                "target_table": match_target_table_for_file(fname, mapping_rules),
                "schema_action": schema_statuses.get(match_target_table_for_file(fname, mapping_rules), "UNKNOWN")
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

        # 6.6 Synthesize Standardized Manifest in Orchestrator's LANDING_ZONE
        target_landing_zone = landing_zone or os.path.join(orch_dir, "../LANDING_ZONE")
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

        # 6.8 Launch Orchestrator and Enforce Barrier
        logger.info(f"Delegating execution of batch {batch_id} to DATA_INGESTION_ORCHESTRATOR...")
        orch_rc = invoke_orchestrator(
            orchestrator_dir=orch_dir,
            config_file=orch_config,
            python_bin=python_bin
        )

        # 6.9 Evaluate Outcome (FAIL_STOP)
        if orch_rc != 0:
            logger.critical(f"FATAL: DATA_INGESTION_ORCHESTRATOR failed with exit code {orch_rc} on Batch {batch_id}!")
            logger.critical("Applying FAIL_STOP policy: Halting feeder immediately to preserve data lineage.")
            db_conn.close()
            sys.exit(1)

        logger.info(f"[SUCCESS] Batch {batch_id} ingested, verified and transformed successfully.")

    # 7. Final Wrap-up
    db_conn.close()
    logger.info("=" * 70)
    logger.info(f"ALL {len(fifo_queue)} BATCHES PROCESSED WITH 100% SUCCESS!")
    logger.info("BICC INTAKE CONTROLLER TERMINATING NORMALLY (RC 0).")
    logger.info("=" * 70)
    sys.exit(0)

if __name__ == "__main__":
    main()
