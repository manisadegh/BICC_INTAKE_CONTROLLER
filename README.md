# BICC Intake Controller

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Platform: Linux / POSIX](https://img.shields.io/badge/platform-Linux%20%7C%20POSIX-lightgrey.svg)](https://www.kernel.org/)
[![Database: Teradata Vantage](https://img.shields.io/badge/database-Teradata%20Vantage%2016.20+-orange.svg)](https://www.teradata.com/)
[![License: Proprietary](https://img.shields.io/badge/license-Proprietary-red.svg)](#)

An enterprise intake supervisor and schema synchronizer engineered to bridge asynchronous **Oracle Fusion Cloud BICC (Business Intelligence Cloud Connector)** batch exports with the high-throughput **Data Ingestion Orchestrator** and **Generic TPT Loader**.

`BICC_INTAKE_CONTROLLER` manages multi-batch intake drop zones, enforces chronological FIFO batch execution, performs automated schema drift detection against Teradata Vantage data dictionaries (`DBC.ColumnsV`), dynamically synthesizes NOPI `VARCHAR(4000)` staging DDLs, calculates cryptographic MD5 checksums, vaults raw Oracle manifests with canonical JSON receipts, prevents concurrency collisions with active polling sentries, performs automated in-flight crash recovery, and delegates single-batch loading with strict fail-stop protection.

---

## 📚 Complete Documentation Suite

Comprehensive operational, technical, and testing manuals are available in the [`docs/`](docs/) directory:

- 📖 **[User How-To & Configuration Guide](docs/01_user_how_to_guide.md)**: Command-line syntax, complete `config.json` reference, log inspection, and troubleshooting runbooks.
- 📐 **[Functional Specification](docs/02_functional_specification.md)**: Architecture, FIFO queue sequencing, concurrency guards, crash recovery, and Auto-DDL contracts.
- ⚙️ **[Technical Manual & Function Inventory](docs/03_technical_manual_and_inventory.md)**: Internal module structure, function signatures, DBC catalog queries, and subprocess delegation.
- 🧪 **[Testing, Validation & QA Guide](docs/04_testing_and_qa_guide.md)**: Test matrices, synthetic dataset synthesis (CSV, ZIP, GZ, PECSV), and automated regression test harnesses.

---

## Table of Contents

- [Key Capabilities](#key-capabilities)
- [Architecture & End-to-End Workflow](#architecture--end-to-end-workflow)
- [Directory Layout & Partitioning](#directory-layout--partitioning)
- [Core Behaviors & Policies](#core-behaviors--policies)
  - [1. Single-Flight Concurrency & Active Polling](#1-single-flight-concurrency--active-polling)
  - [2. Active Orchestrator Guard & In-Flight Crash Recovery](#2-active-orchestrator-guard--in-flight-crash-recovery)
  - [3. FIFO Chronological Batch Ordering](#3-fifo-chronological-batch-ordering)
  - [4. File Stability Sentry (In-Progress Transfers)](#4-file-stability-sentry-in-progress-transfers)
  - [5. Schema Guard & Auto-DDL Synchronization](#5-schema-guard--auto-ddl-synchronization)
  - [6. Manifest Factory & Raw Manifest Vaulting](#6-manifest-factory--raw-manifest-vaulting)
  - [7. Orchestrator Delegation & FAIL_STOP Barrier](#7-orchestrator-delegation--fail_stop-barrier)
- [Configuration Reference (`config.json`)](#configuration-reference-configjson)
- [Entity Mapping Dictionary (`entity_mapping.csv`)](#entity-mapping-dictionary-entity_mappingcsv)
- [Usage & Execution](#usage--execution)
- [Troubleshooting & Exit Codes](#troubleshooting--exit-codes)

---

## Key Capabilities

- **Multi-Batch Drop Zone Feeder**: Ingests asynchronous batches from `DATA/DROP_ZONE/<SOURCE>/` (e.g. `DATA/DROP_ZONE/ORACLE_FUSION/`). Upstream systems deposit files into their designated source sub-directory; unpartitioned files placed at the root of `DROP_ZONE/` are strictly ignored to prevent cross-source contamination.
- **Single-Flight Process Concurrency**: Manages per-source lock files (`.feeder_<SOURCE>.pid`) and enters an active polling wait loop if another feeder instance is active, preventing duplicate processing and table locks.
- **Active Orchestrator Guard**: Inspects Teradata Vantage `${TDENV}SIDFINTCH.BATCH_RUNS` and `LOADING_ZONE/<SOURCE>/.orchestrator.pid` before touching ingress sas, ensuring no active load is running.
- **Automated In-Flight Crash Recovery**: Detects aborted batches (`*.MF`) in `LOADING_ZONE/<SOURCE>` with dead orchestrator PIDs, immediately resuming Orchestrator execution to completion before ingesting new files.
- **Strict FIFO Execution Queue**: Automatically sorts discovered batches in ascending chronological order (oldest extract timestamp first) to maintain state and lineage consistency.
- **Upload Settling Sentry**: Confirms all manifested data files exist, are $>0$ bytes, and have stable file sizes before processing, avoiding partial ingestion during SFTP or cloud transfers.
- **Automated Schema Drift Resolution**: Compares CSV header columns with Teradata `DBC.ColumnsV`. Upon schema evolution or missing tables, automatically creates or updates Teradata staging tables in **`NO PRIMARY INDEX` (`NOPI`)** with all payload columns as **`VARCHAR(4000) UNICODE NOT CASESPECIFIC`**.
- **Audit Columns Auto-Injection**: Appends `INFA_LAST_REPLICATED TIMESTAMP(0)` and `INFA_BATCH_ID VARCHAR(100)` to all target staging definitions.
- **Cryptographic Manifest Factory**: Computes MD5 checksums across all data files and synthesizes standard `MANIFEST_<SOURCE>_<TIMESTAMP>_<BATCH_ID>.MF` manifests for the Orchestrator.
- **Regulatory Manifest Vault**: Preserves pristine raw BICC manifests in `ARCHIVE_ZONE/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/RAW/` alongside a detailed canonical JSON intake audit receipt (`RECEIPT_<SOURCE>_<EXTRACT_TS>_<BATCH_ID>.json` and legacy `intake_receipt.json`).
- **Source-Specific Post-Tasks**: Dumps source-specific post-tasks (`post_ingestion_tasks`) to `LOGS/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/POST_TASKS_<SOURCE>_<EXTRACT_TS>_<BATCH_ID>.json` and forwards them to the Orchestrator via `--post-tasks-file`.
- **Hybrid Decoupling Support**: Supports `--skip-post-tasks` CLI flag to bypass post-load models during maintenance or historical re-inits.
- **FAIL_STOP Barrier**: Halts immediately on the first sign of worker, TPT, or downstream transformation failure, preventing cascade corruption.

---

## Architecture & End-to-End Workflow

```mermaid
flowchart TD
    subgraph SENTRY["1. Concurrency & Pre-Flight Sentry"]
        LOCK{Is Feeder Active?}
        LOCK -- Yes --> WAIT_FEEDER[Active Polling Wait Loop]
        LOCK -- No --> ACQ_LOCK[Acquire .feeder_<SOURCE>.pid]
        ACQ_LOCK --> TD_CHECK{Teradata BATCH_RUNS Active?}
        TD_CHECK -- Yes --> WAIT_ORCH[Active Polling Wait Loop]
        TD_CHECK -- No --> LZ_CHECK{In-Flight Batch in LOADING_ZONE?}
        LZ_CHECK -- Yes --> RESUME[Resume Orchestrator / Crash Recovery]
        LZ_CHECK -- No --> SCAN[Scan DROP_ZONE/<SOURCE>/]
    end

    subgraph INTAKE["2. Ingress & Schema Supervision"]
        SCAN --> FIFO[Sort Queue: Ascending FIFO Order]
        FIFO --> PICK[Select Oldest Batch in Queue]
        PICK --> STABLE{Files Stable & >0 Bytes?}
        STABLE -- No --> FAILSTOP[FAIL_STOP: Abort Immediately]
        STABLE -- Yes --> SCHEMA[Schema Guard: In-Memory Header Stream]
        SCHEMA --> DDL{Matches DBC.ColumnsV?}
        DDL -- No --> AUTODDL[Auto-DDL: DROP & CREATE NOPI VARCHAR 4000]
        DDL -- Yes --> MD5[Compute MD5 Checksums]
        AUTODDL --> MD5
        MD5 --> VAULT[Vault Raw Manifest + Write Canonical RECEIPT]
        VAULT --> HANDOFF[Move Batch to LANDING_ZONE/<SOURCE>/]
    end

    subgraph DELEGATION["3. Orchestrator Delegation"]
        HANDOFF --> POSTTASKS[Generate POST_TASKS_<SOURCE>_...json]
        POSTTASKS --> ORCH[Invoke orchestrator.py --source <SOURCE>]
        ORCH --> EVAL{Exit Code == 0?}
        EVAL -- Yes --> NEXT[Batch Succeeded -> Proceed to Next Batch]
        EVAL -- No --> FAILSTOP
    end
```

---

## Configuration Reference (`config.json`)

The controller configuration uses portable relative paths and environment variable interpolation:

```json
{
  "feeder": {
    "drop_zone": "../../DATA/DROP_ZONE",
    "raw_manifest_archive": "../../DATA/ARCHIVE_ZONE",
    "logs_dir": "../../LOGS",
    "batch_selection_policy": "FIFO_ALL",
    "on_batch_failure": "abort",
    "file_stability_window_seconds": 3,
    "source_system_code": "ORACLE_FUSION",
    "concurrency": {
      "poll_interval_seconds": 5,
      "lock_timeout_seconds": 7200
    },
    "post_ingestion_tasks": [
      {
        "type": "shell",
        "command": "echo 'Post-task specific to BICC completed for batch $CURRENT_BATCH_ID'"
      }
    ]
  },
  "schema_guard": {
    "auto_migrate_ddl": true,
    "default_column_type": "VARCHAR(4000)",
    "character_set": "UNICODE",
    "target_database": "${TDENV}SIDFINSTG",
    "entity_mapping_file": "../../CONFIG/entity_mapping.csv"
  },
  "orchestrator": {
    "orchestrator_dir": "../DATA_INGESTION_ORCHESTRATOR",
    "config_file": "config.json",
    "landing_zone": "../../DATA/LANDING_ZONE",
    "python_executable": "python3"
  },
  "teradata": {
    "host": "${TDPID}",
    "user": "${TDENV}${SID_ADM_USER}",
    "logmech": "TD2"
  }
}
```

---

## Entity Mapping Dictionary (`entity_mapping.csv`)

The mapping file defines how source filenames map to Teradata staging tables, including support for `.csv`, `.zip`, `.gz`, and `.pecsv` (active keys):

```csv
SOURCE,TARGET_TABLE,FILE_SIGNATURE,EXTENSION,INNER_EXTENSION
ORACLE_FUSION,ERP_MSH_INVOICE_HEADER,invoiceheaderextractpvo,".csv,.zip,.gz,.csv.gz",.csv
ORACLE_FUSION,ERP_MSH_INVOICE_HEADER_AK,invoiceheaderextractpvo,".pecsv,.zip,.gz,.pecsv.gz",.pecsv
ORACLE_FUSION,ERP_MSH_USER,userpvo,".csv,.zip,.gz,.csv.gz",.csv
ORACLE_FUSION,ERP_MSH_USER_AK,userpvo,".pecsv,.zip,.gz,.pecsv.gz",.pecsv
```

---

## Usage & Execution

### Standard Batch Ingestion
```bash
python3 feeder.py --config config.json
```

### Ingestion with Post-Tasks Skipped (Maintenance / Inits)
```bash
python3 feeder.py --config config.json --skip-post-tasks
```

### Multi-Source Partition Ingestion
```bash
python3 feeder.py --config config.json --source CRM
```

If `DROP_ZONE` contains no batches, the process exits cleanly with Return Code `0`.

---

## Troubleshooting & Exit Codes

| Exit Code | Meaning | Action |
| :---: | :--- | :--- |
| `0` | All batches processed successfully, or `DROP_ZONE` is empty (`IDLE`). | Normal operation. |
| `1` | Payload stability failure, schema DDL error, lock timeout, or Orchestrator failure (`FAIL_STOP`). | Inspect `LOGS/<SOURCE>/LOG_FEEDER_<SOURCE>_<TIMESTAMP>.log` and Teradata `BATCH_TASKS` table. |

For detailed failure scenarios and step-by-step remediation procedures, consult **[docs/01_user_how_to_guide.md](docs/01_user_how_to_guide.md)**.
