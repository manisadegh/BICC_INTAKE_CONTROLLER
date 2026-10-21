# BICC_INTAKE_CONTROLLER - Technical Manual & Function Inventory

## 1. System Context & Architecture

The **BICC_INTAKE_CONTROLLER** (`feeder.py`) is deployed on the Linux application server at:
```
/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/BICC_INTAKE_CONTROLLER/
```
Under the **Phase 03 Decoupled Architecture**, it operates as a **pure flat-file intake producer with zero Teradata database dependency**. It supervises raw asynchronous ingress from `DROP_ZONE/<SOURCE>/`, extracts column headers in-memory via `dataloader_core.schema_reader`, calculates MD5 checksums, stages complete self-contained Batch Capsules into `LANDING_ZONE/<SOURCE>/BATCH_<TS>_<ID>/`, vaults raw manifests and intake receipts, and delegates FIFO queue execution to `DATA_INGESTION_ORCHESTRATOR`.

```
BICC_INTAKE_CONTROLLER/
├── feeder.py                  # Thin CLI entrypoint (<80 lines) delegating to IntakeControllerService
├── config.json                # Deployment configuration file
├── config_template.json       # Clean template with defaults & concurrency
├── entity_mapping.csv         # Signatures to target table mapping
├── docs/                      # Comprehensive technical documentation suite
└── lib/
    ├── intake_service.py      # IntakeControllerService lifecycle orchestrator (imports dataloader_core)
    ├── scanner.py             # Manifest discovery, FIFO sorting, stability sentry
    ├── manifest_factory.py    # MD5 calculation, Batch Capsule assembly & atomic publishing
    └── orchestrator_bridge.py # Subprocess delegation & real-time log streamer
```

---

## 2. Module Inventory & Code Signatures

### 2.1 CLI Entrypoint (`feeder.py`) & Service (`lib/intake_service.py`)
- **Paths**: `feeder.py`, `lib/intake_service.py`
- **Role**: Command-line interface (`feeder.py`, strictly $<80$ lines) and cohesive service class (`IntakeControllerService`) inheriting from `dataloader_core.intake.BaseIntakeController` and backed by `COMMON/dataloader_core` (`ConfigLoader`, `ProcessLock`, `EntityMapper`, `CapsulePathResolver`, `LoggingFactory`, `BICCIntakePlugin`).

#### Class `IntakeControllerService(BaseIntakeController)` (`lib/intake_service.py`):
1. `__init__(config_path: str, source_override: Optional[str] = None, skip_post_tasks: bool = False, enqueue_only: bool = False)`
   - **Description**: Loads configuration via `dataloader_core.config.ConfigLoader`, resolves source-partitioned zone paths (`DROP_ZONE`, `LANDING_ZONE`, `LOGS`), and configures `"intake_plugin": "bicc"` (or configured alternative).
   - **Database**: Zero Teradata connection or driver import.

2. `acquire_concurrency_guards() -> ProcessLock`
   - **Description**: Returns a POSIX advisory lock (`dataloader_core.locking.ProcessLock`) on `DROP_ZONE/<SOURCE>/.feeder_<SOURCE>.pid` combining `fcntl.flock` with PID liveness polling (`poll_interval_seconds`, `lock_timeout_seconds`).

3. `discover_and_sort_batches() -> List[Dict]`
   - **Description**: Scans `DROP_ZONE/<SOURCE>/` via plugin discovery (`BICCIntakePlugin`) and sorts batches in ascending FIFO chronological order.

4. `process_single_batch(batch_info: Dict, mapper: EntityMapper) -> str`
   - **Description**:
     1. Executes stability sentry check (`verify_payload_stability`) over settling window.
     2. Matches declared files to target entities via `EntityMapper`.
     3. For each entity, selects header chunk file and extracts sanitized column names in-memory via `dataloader_core.schema_reader.extract_csv_columns()`.
     4. Computes single-pass MD5 checksums for all declared files.
     5. Packages the Batch Capsule into `LANDING_ZONE/<SOURCE>/.staging_BATCH_<CLEAN_TS>_<BATCH_ID>/` containing `RAW/` (vaulted raw manifest + `RECEIPT_*.json`), standardized `MANIFEST_*.MF`, `batch_descriptor.json`, and payload data files.
     6. Atomically renames `.staging_BATCH_*` -> `BATCH_*`.
   - **Returns**: Path to the published capsule directory.

5. `run(skip_post_tasks: Optional[bool] = None, enqueue_only: Optional[bool] = None) -> int`
   - **Description**: Delegates template method intake loop to `BaseIntakeController.run()`. Enqueues all batches in `DROP_ZONE` in strict FIFO order. If `enqueue_only` is False and `trigger_orchestrator` is True, invokes `DATA_INGESTION_ORCHESTRATOR` once to drain the queue. Returns exit code (`0` or `1`).

---

### 2.2 Module: `lib/scanner.py`
- **Path**: `lib/scanner.py`
- **Role**: Drop Zone discovery, manifest parsing, FIFO queue sorting, and upload stability sentry.

#### Functions in `lib/scanner.py`:
1. `extract_batch_id_from_filenames(declared_files: List[str]) -> Optional[str]`
   - **Description**: Scans payload names using regex `r"-batch([A-Za-z0-9_-]+?)-"` to recover missing batch IDs when omitted from the manifest title.

2. `parse_manifest_filename(filename: str, default_source: str = "ORACLE_FUSION") -> Dict`
   - **Description**: Tests filename against `MANIFEST_PATTERNS` regex suite. Extracts `source`, `extract_timestamp`, `batch_id`, and builds a composite `sort_key = (timestamp_float, batch_id)`.

3. `read_declared_files_from_manifest(manifest_path: str) -> List[str]`
   - **Description**: Parses content of raw manifest file. Handles commas, semicolons, and tabs. Skips comment lines (`#`), headers (`filename`), and configuration keys (`key=val`).

4. `scan_drop_zone(drop_zone_dir: str, default_source: str = "ORACLE_FUSION") -> List[Dict]`
   - **Description**: Discovers all `*.MF` files in the source partitioned drop zone `DROP_ZONE/<SOURCE>`. Files placed at the root of `DROP_ZONE/` are strictly ignored to prevent cross-source contamination. Reads declared files, refines batch ID and timestamps, and returns a list of batch metadata dictionaries.

5. `sort_batches_fifo(batches: List[Dict]) -> List[Dict]`
   - **Description**: Sorts batch dictionaries by `sort_key` in strict ascending chronological order.

6. `verify_payload_stability(drop_zone_dir: str, declared_files: List[str], settling_window_sec: int = 5) -> Tuple[bool, str]`
   - **Description**: Verifies that every manifested file exists, is non-zero in size, and records initial byte sizes. Sleeps for `settling_window_sec` seconds and checks that file sizes have not changed.
   - **Return**: `(is_stable: bool, message: str)`.

---

### 2.3 Module: `lib/manifest_factory.py`
- **Path**: `lib/manifest_factory.py`
- **Role**: Checksum generation, Batch Capsule packaging, receipt generation, and atomic publishing.

#### Functions in `lib/manifest_factory.py`:
1. `compute_file_md5(file_path: str) -> str`
   - **Description**: Computes MD5 checksum streaming in 64 KB blocks (`read(65536)`). Memory-safe for multi-gigabyte files.

2. `create_standardized_manifest(output_dir: str, source: str, extract_timestamp: str, batch_id: str, file_checksum_pairs: List[Tuple[str, str]]) -> str`
   - **Description**: Generates `MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF` containing CSV header `filename,checksum`.

3. `stage_batch_capsule_to_landing_zone(...) -> Tuple[str, BatchDescriptor]`
   - **Description**: Delegates to `dataloader_core.capsule.CapsulePathResolver.stage_and_publish_queue_capsule` to stage `.staging_BATCH_<CLEAN_TS>_<BATCH_ID>/` containing `RAW/` vault, standardized manifest, `batch_descriptor.json`, and payload files, and atomically renames to `BATCH_<CLEAN_TS>_<BATCH_ID>`.

4. `vault_raw_manifest(raw_manifest_path: str, archive_base_dir: str, source: str, extract_timestamp: str, batch_id: str, receipt_payload: List[Dict]) -> str`
   - **Description**: Backward-compatible vault helper preserving raw manifests and canonical receipts.

---

### 2.4 Module: `lib/orchestrator_bridge.py`
- **Path**: `lib/orchestrator_bridge.py`
- **Role**: Subprocess delegation and unbuffered real-time log streaming.

#### Functions in `lib/orchestrator_bridge.py`:
1. `invoke_orchestrator(orchestrator_dir: str, config_file: str = "config.json", python_bin: Optional[str] = None, env: Optional[Dict] = None, skip_post_tasks: bool = False, source: Optional[str] = None, post_tasks_file: Optional[str] = None) -> int`
   - **Description**: Spawns `<python_bin> orchestrator.py --config <config_file> [--skip-post-tasks] [--source <source>] [--post-tasks-file <file>]` using `subprocess.Popen(bufsize=1, stdout=PIPE, stderr=STDOUT, text=True)`. Iterates over `readline` to relay output in real-time to the controller logger. Returns subprocess exit code. Enforces `FAIL_STOP`.

---

## 3. Core SDK Dependencies (`COMMON/dataloader_core`)

`BICC_INTAKE_CONTROLLER` relies on the shared SDK for consistent enterprise behavior:

| Module | Features Consumed |
|---|---|
| `dataloader_core.schema_reader` | `extract_csv_columns`, `clean_column_name`, `select_header_chunk_file` (in-memory header extraction from CSV, ZIP, GZ, PECSV). |
| `dataloader_core.capsule` | `BatchDescriptor`, `CapsulePathResolver.stage_and_publish_queue_capsule` (Batch Capsule packaging and atomic renaming). |
| `dataloader_core.mapping` | `EntityMapper` (matches file patterns and signatures to staging table names). |
| `dataloader_core.locking` | `ProcessLock`, `is_pid_alive`, `read_pid_file` (POSIX advisory locking). |
| `dataloader_core.config` | `ConfigLoader`, `expand_env_vars`, `resolve_zone_path`. |
| `dataloader_core.logging_utils`| `setup_feeder_logger`, `LoggingFactory`. |

---

## 4. Exit Codes & Process Termination Matrix

The controller adheres to strict POSIX exit semantics:

| Exit Code | Classification | Cause / Trigger | Operational Impact |
|:---:|:---|:---|:---|
| **`0`** | `SUCCESS / IDLE` | All batches in FIFO queue staged (and drained if chained) successfully, or `DROP_ZONE/<SOURCE>` is empty. | Pipeline ready for next cycle. |
| **`1`** | `FATAL / FAIL_STOP` | - Unhandled configuration error.<br/>- Payload stability check failed (file size changing / upload in progress).<br/>- Unmapped entity signature in `entity_mapping.csv`.<br/>- Process lock wait timeout (`lock_timeout_seconds`) exceeded.<br/>- Downstream Orchestrator failed with a fatal error. | Pipeline halted immediately under `FAIL_STOP`. Ingress queue frozen to protect data lineage. |
| **`>1`** | `SUBPROCESS_PROPAGATION` | Propagated exit code from `orchestrator.py` during chained queue draining. | Controller halts with the exact return code of the child process. |
