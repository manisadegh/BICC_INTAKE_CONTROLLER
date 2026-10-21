# BICC_INTAKE_CONTROLLER - Technical Manual & Function Inventory

## 1. System Context & Architecture

The **BICC_INTAKE_CONTROLLER** (`feeder.py`) is deployed on the Linux application server at:
```
/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/BICC_INTAKE_CONTROLLER/
```
It supervises raw asynchronous ingress, orchestrates Auto-DDL schema migrations against Teradata Vantage, and delegates verified single batches to `DATA_INGESTION_ORCHESTRATOR` via POSIX subprocess execution.

```
BICC_INTAKE_CONTROLLER/
├── feeder.py                  # Thin CLI entrypoint (<80 lines) delegating to IntakeControllerService
├── config.json                # Deployment configuration file
├── config_template.json       # Clean template with defaults & concurrency
├── entity_mapping.csv         # Signatures to Teradata target table mapping
├── docs/                      # Comprehensive technical documentation suite
└── lib/
    ├── intake_service.py      # IntakeControllerService lifecycle orchestrator (imports dataloader_core)
    ├── scanner.py             # Manifest discovery, FIFO sorting, stability sentry
    ├── schema_guard.py        # In-memory header streaming & DBC.ColumnsV Auto-DDL
    ├── manifest_factory.py    # MD5 calculation, manifest synthesis & raw vaulting
    └── orchestrator_bridge.py # Subprocess delegation & real-time log streamer
```

---

## 2. Module Inventory & Code Signatures

### 2.1 CLI Entrypoint (`feeder.py`) & Service (`lib/intake_service.py`)
- **Paths**: `feeder.py`, `lib/intake_service.py`
- **Role**: Command-line interface (`feeder.py`) and cohesive service class (`IntakeControllerService`) backed by `COMMON/dataloader_core` (`ConfigLoader`, `ProcessLock`, `TeradataConnectionManager`, `EntityMapper`, `CapsulePathResolver`, `LoggingFactory`).

#### Class `IntakeControllerService` (`lib/intake_service.py`):
1. `__init__(config_path: str, source_override: Optional[str] = None, skip_post_tasks: bool = False)`
   - **Description**: Loads configuration via `dataloader_core.config.ConfigLoader`, resolves source-partitioned zone paths (`DROP_ZONE`, `ARCHIVE_ZONE`, `LOADING_ZONE`, `LOGS`), and initializes the feeder logger via `LoggingFactory.setup_feeder_logger()`.

2. `acquire_concurrency_guards() -> ProcessLock`
   - **Description**: Returns a POSIX advisory lock (`dataloader_core.locking.ProcessLock`) on `DROP_ZONE/<SOURCE>/.feeder_<SOURCE>.pid` combining `fcntl.flock` with PID liveness polling (`poll_interval_seconds`, `lock_timeout_seconds`).

3. `recover_inflight_batch_if_present(conn_mgr: Optional[TeradataConnectionManager] = None) -> None`
   - **Description**: Queries `${TDENV}SIDFINTCH.BATCH_RUNS` and checks `LOADING_ZONE/<SOURCE>/.orchestrator.pid` via `wait_for_pid_completion()`. If an uncompleted `.MF` exists in `LOADING_ZONE/<SOURCE>` with a dead PID, writes source post-tasks inside the Batch Log Capsule and delegates crash recovery to `invoke_orchestrator()`.

4. `discover_and_sort_batches() -> List[Dict]`
   - **Description**: Scans `DROP_ZONE/<SOURCE>/` via `scan_drop_zone()` and sorts batches in ascending FIFO chronological order via `sort_batches_fifo()`.

5. `process_single_batch(batch_info: Dict, conn_mgr: TeradataConnectionManager, mapper: EntityMapper) -> None`
   - **Description**: Executes stability sentry check (`verify_payload_stability`), maps files via `EntityMapper`, synchronizes schemas via `sync_entity_schema_if_drifted` using pooled `TeradataConnectionManager`, computes MD5 checksums, vaults raw manifests, writes standardized manifest, transfers files to `LANDING_ZONE/<SOURCE>`, writes `POST_TASKS_<SOURCE>_<TS>_<ID>.json`, and invokes `DATA_INGESTION_ORCHESTRATOR` with `FAIL_STOP` semantics.

6. `run(skip_post_tasks: Optional[bool] = None) -> int`
   - **Description**: Executes the complete supervisor workflow inside `with self.acquire_concurrency_guards():` and returns POSIX exit code (`0` or `1`).

---

### 2.2 Module: `lib/scanner.py`
- **Path**: `lib/scanner.py`
- **Role**: Drop Zone discovery, manifest parsing, FIFO queue sorting, and upload stability sentry.

#### Functions in `lib/scanner.py`:
1. `extract_batch_id_from_filenames(declared_files: List[str]) -> Optional[str]`
   - **Description**: Scans payload names using regex `r"-batch([A-Za-z0-9_-]+?)-"` to recover missing batch IDs from files when omitted from the manifest title.

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

### 2.3 Module: `lib/schema_guard.py`
- **Path**: `lib/schema_guard.py`
- **Role**: In-memory header extraction, active keys support, DBC introspection, and automated NOPI DDL synchronization.

#### Functions in `lib/schema_guard.py`:
1. `clean_column_name(col_raw: str) -> str`
   - **Description**: Strips whitespace/quotes, transforms non-alphanumeric chars to `_`, deduplicates underscores, and casts to uppercase.

2. `inspect_file_inner_extension(file_path: str) -> str`
   - **Description**: Inspects archive contents without decompression. Returns `.pecsv` if archive contains active keys or `.csv` for standard payload.

3. `extract_csv_columns(file_path: str) -> List[str]`
   - **Description**: Streams the first line from `.zip` (via `zipfile.ZipFile`), `.gz` (via `gzip.open`), or uncompressed text. Detects delimiter (`;`, `\t`, `,`) and returns ordered list of cleaned column names.

4. `load_entity_mappings(mapping_file: str, filter_source: str = "ORACLE_FUSION") -> List[Dict]`
   - **Description**: Parses `entity_mapping.csv`, resolving `TARGET_TABLE`, `FILE_SIGNATURE`, `EXTENSION`, and `INNER_EXTENSION`.

5. `match_target_table_for_file(filename: str, mapping_rules: List[Dict], file_dir: str = None) -> Optional[str]`
   - **Description**: Matches file against signatures, outer extensions, and inner extensions.

6. `get_teradata_table_columns(connection, target_db: str, target_table: str) -> Optional[List[str]]`
   - **Description**: Queries Teradata system view `DBC.ColumnsV` ordered by `ColumnId`. Excludes `TECHNICAL_AUDIT_COLUMNS` (`INFA_BATCH_ID`, `INFA_LAST_REPLICATED`). Returns `None` if table does not exist.

7. `generate_staging_ddl(target_db: str, target_table: str, columns: List[str], default_type: str = "VARCHAR(4000)", character_set: str = "UNICODE") -> str`
   - **Description**: Validates SQL identifiers and column types, then constructs DDL string for `CREATE MULTISET TABLE ... NO PRIMARY INDEX` with all payload columns typed as `<default_type> CHARACTER SET <character_set> NOT CASESPECIFIC` and audit columns appended.

8. `recreate_staging_table(connection, target_db: str, target_table: str, columns: List[str], default_type: str = "VARCHAR(4000)", character_set: str = "UNICODE")`
   - **Description**: Executes `DROP TABLE` followed by table creation using generated DDL.

9. `sync_entity_schema_if_drifted(connection, target_db: str, target_table: str, source_csv_path: str, default_type: str = "VARCHAR(4000)", character_set: str = "UNICODE", auto_migrate_ddl: bool = True, load_mode: str = "replace") -> Tuple[bool, str, List[str]]`
   - **Description**: Compares source CSV columns against Teradata table columns. Raises `RuntimeError` if `auto_migrate_ddl=False` on missing table or schema drift, or if `load_mode == 'append'` on schema drift (preventing destructive `DROP TABLE`). Otherwise, if table missing $\rightarrow$ creates table (`CREATED`); if columns differ $\rightarrow$ drops and recreates table (`RECREATED`); if columns identical $\rightarrow$ no-op (`IDENTICAL`).

---

### 2.4 Module: `lib/manifest_factory.py`
- **Path**: `lib/manifest_factory.py`
- **Role**: Checksum generation, manifest standardization, and atomic ingress moves.

#### Functions in `lib/manifest_factory.py`:
1. `compute_file_md5(file_path: str) -> str`
   - **Description**: Computes MD5 checksum streaming in 64 KB blocks (`read(65536)`). Memory-safe for multi-gigabyte files.

2. `create_standardized_manifest(output_dir: str, source: str, extract_timestamp: str, batch_id: str, file_checksum_pairs: List[Tuple[str, str]]) -> str`
   - **Description**: Generates `MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF` containing CSV header `filename,checksum`.

3. `vault_raw_manifest(raw_manifest_path: str, archive_base_dir: str, source: str, extract_timestamp: str, batch_id: str, receipt_payload: List[Dict]) -> str`
   - **Description**: Vaults original BICC manifest into Batch Capsule `<archive_base_dir>/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/RAW/` and writes canonical `RECEIPT_<SOURCE>_<EXTRACT_TS>_<BATCH_ID>.json`.

4. `transfer_batch_to_landing_zone(drop_zone_dir: str, landing_zone_dir: str, data_files: List[str], standardized_manifest_path: Optional[str] = None)`
   - **Description**: Moves all manifested data payload files from `DROP_ZONE/<SOURCE>` into `LANDING_ZONE/<SOURCE>`.

---

### 2.5 Module: `lib/orchestrator_bridge.py`
- **Path**: `lib/orchestrator_bridge.py`
- **Role**: Subprocess delegation and unbuffered real-time log streaming.

#### Functions in `lib/orchestrator_bridge.py`:
1. `invoke_orchestrator(orchestrator_dir: str, config_file: str = "config.json", python_bin: Optional[str] = None, env: Optional[Dict] = None, skip_post_tasks: bool = False, source: Optional[str] = None, post_tasks_file: Optional[str] = None) -> int`
   - **Description**: Spawns `<python_bin> orchestrator.py --config <config_file> [--skip-post-tasks] [--source <source>] [--post-tasks-file <file>]` using `subprocess.Popen(bufsize=1, stdout=PIPE, stderr=STDOUT, text=True)`. Iterates over `readline` to relay output in real-time to the controller logger. Returns subprocess exit code.

---

## 3. Teradata System Catalog & Database Interactions

### 3.1 Metadata Query against `DBC.ColumnsV`
To inspect column schema without generating query overhead or table locks, Schema Guard queries the Teradata Data Dictionary:
```sql
SELECT TRIM(ColumnName)
FROM DBC.ColumnsV
WHERE DatabaseName = ?
  AND TableName = ?
ORDER BY ColumnId;
```

### 3.2 Pre-Flight Active Batch Check against `BATCH_RUNS`
To prevent concurrent worker collisions for the same source before touching ingress files:
```sql
SELECT BATCH_ID, SOURCE, START_TIME, PROCESS_PID 
FROM {technical_database}.BATCH_RUNS 
WHERE STATUS = 'RUNNING' AND SOURCE = ?;
```

### 3.3 Automated NOPI Staging Table DDL Template
```sql
CREATE MULTISET TABLE {target_db}.{target_table} ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
         COL_1 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
         COL_2 VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
         ...
         INFA_LAST_REPLICATED TIMESTAMP(0),
         INFA_BATCH_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
     )
NO PRIMARY INDEX;
```

---

## 4. Exit Codes & Process Termination Matrix

The controller adheres to strict POSIX exit semantics:

| Exit Code | Classification | Cause / Trigger | Operational Impact |
|:---:|:---|:---|:---|
| **`0`** | `SUCCESS / IDLE` | All batches in FIFO queue processed and archived successfully, or `DROP_ZONE/<SOURCE>` is empty. | Pipeline ready for next cycle. |
| **`1`** | `FATAL / FAIL_STOP` | - Unhandled configuration or connection error.<br>- Payload stability check failed (file size changing / upload in progress).<br>- Unmapped entity signature in `entity_mapping.csv`.<br>- Teradata Schema Guard DDL failure.<br>- Process lock wait timeout (`lock_timeout_seconds`) exceeded.<br>- Downstream Orchestrator failed with a fatal error. | Pipeline halted immediately under `FAIL_STOP`. Ingress queue frozen to protect data lineage. |
| **`>1`** | `SUBPROCESS_PROPAGATION` | Propagated exit code from `orchestrator.py` during in-flight recovery or batch delegation. | Controller halts with the exact return code of the child process. |
