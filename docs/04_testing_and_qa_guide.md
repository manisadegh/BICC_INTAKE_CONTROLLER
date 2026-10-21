# BICC_INTAKE_CONTROLLER - Testing, Validation & QA Guide

## 1. Testing Scope & Objectives

The testing framework for the **BICC_INTAKE_CONTROLLER** validates the ingress gateway's robustness as a **pure, decoupled flat-file intake producer with zero Teradata database dependency**.

### Validation Boundaries
```mermaid
flowchart LR
    subgraph Inputs["Test Vectors"]
        T1["Nominal CSV Batch"]
        T2["ZIP Archive Batch"]
        T3["Multi-Batch Simultaneous Arrival"]
        T4["Active Keys (.pecsv)"]
        T5["Unstable / In-Flight Upload"]
        T6["Zero-Byte / Truncated File"]
        T7["Missing Manifested File"]
        T8["--enqueue-only Producer Mode"]
        T9["Concurrent Feeder Race"]
    end

    subgraph Controller["BICC Intake Controller"]
        Lock["PID Concurrency Guard"]
        Sentry["Stability & Scanner"]
        Header["In-Memory Header Reader"]
        MD5["MD5 Manifest Factory"]
        Capsule["Batch Capsule Packaging"]
        Bridge["Orchestrator Bridge"]
    end

    subgraph ExpectedOutcomes["Verified Assertions"]
        A1["FIFO Chronological Queue Execution"]
        A2["Settling Sentry Abort (RC 1)"]
        A3["0-byte / Missing File Abort (RC 1)"]
        A4["Pristine Raw Manifest Vault + Receipt"]
        A5["batch_descriptor.json Serialization"]
        A6["Atomic Capsule Publishing in LANDING_ZONE"]
        A7["Zero Teradata DB Connection / Query"]
        A8["FAIL_STOP Halt on Downstream Error"]
    end

    Inputs --> Controller --> ExpectedOutcomes
```

---

## 2. Test Environment & Sandbox Setup

To run tests in an isolated sandbox without affecting runtime data directories:

```bash
# Create isolated test sandbox
export SANDBOX_DIR="/sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST"
mkdir -p "${SANDBOX_DIR}/DATA/DROP_ZONE/ORACLE_FUSION"
mkdir -p "${SANDBOX_DIR}/DATA/LANDING_ZONE/ORACLE_FUSION"
mkdir -p "${SANDBOX_DIR}/LOGS/ORACLE_FUSION"
mkdir -p "${SANDBOX_DIR}/CONFIG"

# Copy entity mapping
cp /sg/local/sid/home/Dsidadm/DATA_LOADER/CONFIG/entity_mapping.csv "${SANDBOX_DIR}/CONFIG/"
```

### Test Configuration (`test_config.json`)
```json
{
  "feeder": {
    "drop_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST/DATA/DROP_ZONE",
    "logs_dir": "/sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST/LOGS",
    "file_stability_window_seconds": 2,
    "source_system_code": "ORACLE_FUSION",
    "entity_mapping_file": "/sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST/CONFIG/entity_mapping.csv",
    "concurrency": {
      "poll_interval_seconds": 2,
      "lock_timeout_seconds": 30
    }
  },
  "orchestrator": {
    "orchestrator_dir": "../DATA_INGESTION_ORCHESTRATOR",
    "config_file": "config.json",
    "landing_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST/DATA/LANDING_ZONE",
    "python_executable": "python3",
    "trigger_orchestrator": true
  }
}
```

---

## 3. Test Scenarios & Test Matrix

| Test ID | Test Scenario | Input Dataset | Expected Result | Exit Code |
|---|---|---|---|:---:|
| **TC-FEED-01** | Nominal Single Batch CSV | Single CSV batch in `DROP_ZONE` | Capsule staged in `LANDING_ZONE`, `batch_descriptor.json` created. | 0 |
| **TC-FEED-02** | Nominal Single Batch ZIP | Single ZIP batch in `DROP_ZONE` | In-memory ZIP header extraction, capsule published. | 0 |
| **TC-FEED-03** | Multi-Batch FIFO Queue | 2+ batches simultaneously in `DROP_ZONE` | Batches published in strict ascending chronological FIFO order. | 0 |
| **TC-FEED-04** | Decoupled Mode (`--enqueue-only`) | Batches in `DROP_ZONE` with `--enqueue-only` | Batches published into `LANDING_ZONE` capsules; zero DB connection, Orchestrator not triggered. | 0 |
| **TC-FEED-05** | File Stability Sentry (Incomplete Transfer) | Growing file in `DROP_ZONE` | Detects size alteration during settling window, aborts immediately. | 1 |
| **TC-FEED-06** | Missing Data File | Manifest declares `fileA`, only `fileB` present | Detects missing file, aborts under `FAIL_STOP`. | 1 |
| **TC-FEED-07** | Zero-Byte File Arrival | 0-byte file in `DROP_ZONE` | Stability sentry flags empty file, aborts under `FAIL_STOP`. | 1 |
| **TC-FEED-08** | In-Memory Header Extraction Parity | CSV, ZIP, GZ, and PECSV files | Extracted column headers match file definitions and sanitize to uppercase. | 0 |
| **TC-FEED-09** | Concurrency & Active Polling Wait | 2 concurrent feeder instances | Detects active PID, polls until lock released, avoids collisions. | 0 |
| **TC-FEED-10** | Downstream Fail-Stop Propagation | Orchestrator fails on batch | Feeder halts immediately on non-zero exit code (`FAIL_STOP`). | 1 |
| **TC-FEED-11** | Multi-Source Partition Isolation | Files in `DROP_ZONE/CRM` and `DROP_ZONE/ORACLE_FUSION` | Processes target partition only without touching other sources. | 0 |

> [!TIP]
> The automated test suite in [`DATA_LOADER/TESTS/`](../../TESTS/README.md) validates all these scenarios end-to-end with 100% automated pass verification (`./run_all_tests.sh`).

---

## 4. Detailed Test Procedures & Assertions

### Test Case: TC-FEED-04 (Decoupled Mode `--enqueue-only`)
- **Objective**: Verify that `--enqueue-only` stages complete Batch Capsules into `LANDING_ZONE/<SOURCE>/` without database connections or orchestrator invocation.
- **Execution**:
  ```bash
  python3 feeder.py --config /sg/local/sid/home/Dsidadm/DATA_LOADER/SANDBOX_TEST/test_config.json --enqueue-only
  ```
- **Assertions**:
  - `DROP_ZONE/ORACLE_FUSION/` is empty at completion.
  - `LANDING_ZONE/ORACLE_FUSION/` contains `BATCH_<CLEAN_TS>_<BATCH_ID>/` directories.
  - Inside each capsule, `batch_descriptor.json` exists with pre-extracted `header_columns`, MD5 hashes, and file sizes.
  - Inside each capsule, `RAW/` contains the vaulted original manifest and `RECEIPT_<SOURCE>_<TS>_<BATCH_ID>.json`.
  - Process exits cleanly with Return Code `0`.

---

### Test Case: TC-FEED-05 (File Stability Sentry)
- **Objective**: Verify that an active file upload in `DROP_ZONE` halts processing to prevent partial ingestion.
- **Setup**: In a background process, append data to a file in `DROP_ZONE/ORACLE_FUSION/`.
- **Execution**:
  ```bash
  python3 feeder.py --config test_config.json
  ```
- **Assertions**:
  - Feeder logs: `Payload stability check failed for batch ...: File ... size changed during settling window. Upload in progress.`
  - Feeder halts immediately under `FAIL_STOP` with Return Code `1`.
  - No capsule is published to `LANDING_ZONE`.

---

### Test Case: TC-FEED-09 (Concurrency Guard & Active Polling Wait)
- **Objective**: Verify that two feeder instances launched simultaneously on the same source do not collide.
- **Setup & Execution**:
  ```bash
  # Launch first instance in background
  python3 feeder.py --config test_config.json &
  PID_1=$!
  sleep 1

  # Launch second instance
  python3 feeder.py --config test_config.json
  wait $PID_1
  ```
- **Assertions**:
  - Instance 2 detects `.feeder_ORACLE_FUSION.pid` with PID of Instance 1.
  - Instance 2 logs: `Another BICC Intake Controller is currently active (PID ...). Entering active waiting loop...`
  - Instance 2 polls every 2 seconds until Instance 1 finishes, then resumes without errors.
