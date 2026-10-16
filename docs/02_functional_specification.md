# BICC_INTAKE_CONTROLLER - Functional Specification

## 1. Business & Functional Objective

The **BICC_INTAKE_CONTROLLER** serves as the intake gateway and schema supervisor for the **DATA_LOADER** flat-file ELT ecosystem feeding Teradata Vantage from **Oracle Fusion Cloud BICC (Business Intelligence Cloud Connector)** extracts.

### Upstream Context
Oracle Fusion Cloud BICC exports data asynchronously in batches consisting of:
- A manifest metadata file indicating the export timestamp and declared payload files.
- One or more data extract files (formatted as `.csv`, `.pecsv`, `.zip`, or `.gz`), which may be chunked into multiple parts.
- Active Keys extracts (`.pecsv`), representing primary keys of active records for change data capture.

### Problem Addressed
Upstream cloud extracts often arrive with variable delays, partial multi-file uploads, occasional schema drift (new or dropped columns from ERP updates), and multiple batches arriving out of order. Ingestion directly into high-speed utility loaders (like Teradata TPT) without strict ingress governance risks:
- Ingesting partial or in-flight uploads.
- Corrupting staging tables due to unnotified ERP schema alterations.
- Inverting batch chronology (processing a newer batch before an older batch), violating temporal consistency.

The BICC Intake Controller establishes deterministic order, transactional stability, and automated schema adaptation before files reach processing stages.

---

## 2. Core Functional Responsibilities

```mermaid
mindmap
  root((BICC_INTAKE_CONTROLLER))
    Ingress Governance
      Scans DROP_ZONE
      Parses Manifests & Payloads
      Enforces FIFO Chronology
      Settling Window Sentry
    Schema Supervision
      In-Memory Header Streaming
      Supports ZIP and GZ Archives
      DBC.ColumnsV Comparison
      Auto-DDL NOPI Recreation
    Vaulting & Standardizing
      Calculates MD5 Checksums
      Vaults Raw Manifest & Receipt
      Creates Standard Manifest
      Atomic Transfer to LANDING_ZONE
    Delegation
      Subprocess Invocation
      Real-Time Log Stream
      FAIL_STOP Pipeline Halt
```

### What the Controller Does:
- **Discovers** incoming manifest files matching standard or legacy BICC naming patterns in `DATA/DROP_ZONE/<SOURCE>/`. Any unpartitioned files at the root of `DROP_ZONE/` are strictly ignored.
- **Extracts** batch IDs and export timestamps from manifest names or declared payload names.
- **Enforces FIFO chronology**: Organizes multi-batch queues in ascending chronological order (`oldest -> newest`).
- **Validates payload stability**: Ensures all manifested files exist, are $>0$ bytes, and have static file sizes over a configurable window.
- **Performs zero-disk schema inspection**: Reads header lines directly from `.zip` or `.gz` in memory.
- **Supervises Teradata staging schemas**: Compares CSV headers against `DBC.ColumnsV`. If columns drifted or table is missing, executes `DROP TABLE` and creates a `NOPI` table with all payload columns as `VARCHAR(4000) UNICODE NOT CASESPECIFIC`.
- **Vaults raw manifests**: Stores the original BICC manifest and audit documents (`RECEIPT_<SOURCE>_<TS>_<BATCH_ID>.json` and legacy `intake_receipt.json`) inside the Batch Capsule in `ARCHIVE_ZONE/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/RAW/`.
- **Synthesizes standardized manifests**: Generates `MANIFEST_<SOURCE>_<TIMESTAMP>_<BATCH_ID>.MF` containing MD5 checksums.
- **Atomically transfers files**: Moves data payloads to `DATA/LANDING_ZONE/<SOURCE>/`.
- **Delegates execution**: Launches `DATA_INGESTION_ORCHESTRATOR` and halts immediately on non-zero exit code (`FAIL_STOP`).

### What the Controller Does NOT Do:
- Does NOT execute TPT or load rows directly into Teradata (delegated to Orchestrator and TPT Loader).
- Does NOT process more than one batch simultaneously into `LANDING_ZONE` (preserves Single-Batch gating).
- Does NOT perform transformation or business ELT logic (handled by post-tasks and downstream dbt).

---

## 3. End-to-End Processing Lifecycle & Sequence

The following sequence diagram illustrates the lifecycle of a batch passing through the controller:

```mermaid
sequenceDiagram
    autonumber
    participant DropZone as DATA/DROP_ZONE/<SOURCE>
    participant Feeder as feeder.py
    participant TD as Teradata Vantage (DBC.ColumnsV)
    participant Archive as Batch Capsule RAW/
    participant LandingZone as DATA/LANDING_ZONE/<SOURCE>
    participant Orch as DATA_INGESTION_ORCHESTRATOR

    Note over Feeder: In-Flight Crash Check (LOADING_ZONE)
    Feeder->>DropZone: Scan for manifest files (*.MF)
    DropZone-->>Feeder: Return list of manifests
    Feeder->>Feeder: Sort discovered batches in FIFO order
    
    loop For each batch in FIFO Queue
        Feeder->>DropZone: Check file existence and stability window
        Feeder->>DropZone: Stream line 1 of payload in memory (ZIP/CSV)
        Feeder->>TD: Query DBC.ColumnsV for target table
        alt Table missing or columns drifted
            Feeder->>TD: DROP TABLE & CREATE NOPI VARCHAR(4000)
        else Identical
            Note over Feeder,TD: Schema matched (no DDL needed)
        end
        Feeder->>DropZone: Compute MD5 checksums of data files
        Feeder->>Archive: Vault original manifest + intake_receipt.json
        Feeder->>LandingZone: Create MANIFEST_<SOURCE>_<TS>_<ID>.MF
        Feeder->>LandingZone: Move data files from DROP_ZONE to LANDING_ZONE
        Feeder->>Orch: Invoke orchestrator.py (subprocess)
        Orch-->>Feeder: Stream stdout/stderr (real-time)
        alt Return Code == 0
            Note over Feeder: Batch completed successfully
        else Return Code != 0
            Note over Feeder: FAIL_STOP triggered -> Abort Feeder
        end
    end
```

---

## 4. Ingestion Contracts & Data Rules

### 4.1 Manifest Format Recognition
The controller supports standard and historical Oracle BICC manifest naming patterns:

| Pattern Type | Filename Pattern | Example |
|---|---|---|
| **Standard Manifest** | `MANIFEST_<SOURCE>_<TIMESTAMP>_<BATCH_ID>.MF` | `MANIFEST_ORACLE_FUSION_20261015_120011_000000011.MF` |
| **BICC Timestamped** | `*MANIFEST-YYYYMMDD_HHMMSS.MF` | `DT2_MANIFEST-20261013_131116.MF` |
| **Legacy Raw BICC** | `MANIFEST-batch<ID>-.MF` | `MANIFEST-batch558766085-.MF` |
| **Single-Token** | `MANIFEST_<TOKEN>.MF` | `MANIFEST_1001.MF` |

### 4.2 Batch ID & Timestamp Extraction
If a manifest filename does not embed the batch ID (e.g. `MANIFEST-20261015_120012.MF`), the scanner inspects the declared payload filenames inside the manifest. For example, from:
```
file_invoiceheaderextractpvo-batch000000012-20261015_120012.zip
```
The regex `-batch([A-Za-z0-9_-]+?)-` extracts `000000012` as the authoritative Batch ID.

### 4.3 FIFO Chronological Queue Sorting
When upstream systems transfer multiple batches at once, `sort_batches_fifo()` computes a deterministic tuple key:
```python
sort_key = (timestamp_float, batch_id)
```
Batches are sorted strictly in ascending order. Batch `N` must complete all post-ingestion steps and archive before Batch `N+1` is unlocked.

---

## 5. Schema Guard & Auto-DDL Mechanics

### 5.1 In-Memory Header Streaming
To avoid wasteful disk writes on massive compressed archives, `extract_csv_columns()` streams the first line directly from the archive in memory:
- **ZIP Archives**: Uses Python's `zipfile.ZipFile` with memory buffer read.
- **GZIP Archives**: Uses `gzip.open` with streaming text reader.
- **PECSV Support**: Inspects `.pecsv` files containing active key records.

### 5.2 Header Sanitation Rules
CSV column headers are sanitized into strict Teradata uppercase identifiers:
- Whitespace and surrounding quotes are stripped.
- Non-alphanumeric characters are converted to single underscores (`_`).
- Consecutive underscores are deduplicated.
- Output is uppercase (e.g. `"Invoice Number (#)"` $\rightarrow$ `INVOICE_NUMBER`).

### 5.3 Automated NOPI DDL Generation
When `DBC.ColumnsV` indicates that the table does not exist or has column drift, the controller executes:
```sql
CREATE MULTISET TABLE DEV0SIDFINSTG.ERP_MSH_INVOICE_HEADER ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
        INVOICE_ID VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
        INVOICE_NUM VARCHAR(4000) CHARACTER SET UNICODE NOT CASESPECIFIC,
        ...
        INFA_LAST_REPLICATED TIMESTAMP(0),
        INFA_BATCH_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
     )
NO PRIMARY INDEX;
```
> [!NOTE]
> Staging tables are intentionally created with `NO PRIMARY INDEX (NOPI)`. This provides maximum bulk-loading throughput via Teradata FastLoad/TPT, eliminating hash distribution bottlenecks during ingestion.

---

## 6. Failure Semantics & Integrity Guarantees

### 6.1 FAIL_STOP Policy
The controller enforces a strict **`FAIL_STOP`** contract:
- If a data file fails MD5 verification, the pipeline halts immediately.
- If a table recreation fails, the pipeline halts immediately.
- If the downstream Orchestrator exits with code $>0$, the controller terminates immediately with exit code `1`.
- Remaining batches in `DROP_ZONE` are left untouched to prevent newer data from being loaded ahead of failed older data.

### 6.2 Raw Manifest Vaulting & Audit Trail
Before files are moved to `LANDING_ZONE/<SOURCE>/`, the raw manifest is preserved permanently in the Batch Capsule:
```
DATA/ARCHIVE_ZONE/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<EXTRACT_TIMESTAMP>/RAW/
```
Along with a canonical intake receipt audit document `RECEIPT_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.json` (plus legacy `intake_receipt.json`) recording:
- Extraction timestamp, Batch ID, and Source system.
- Intake timestamp.
- File sizes in bytes.
- Cryptographic MD5 checksums.
- Target staging table and schema action (`IDENTICAL`, `CREATED`, or `RECREATED`).
