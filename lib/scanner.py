"""
lib/scanner.py - Drop Zone Scanner & Payload Stability Sentry

Discovers raw manifests deposited in DROP_ZONE, parses batch metadata,
sorts execution queues in strict ascending chronological order (FIFO),
and validates file existence and upload stability.
"""

import os
import re
import time
import logging
from datetime import datetime

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")

# Precompiled patterns for standard and legacy manifest naming conventions
MANIFEST_PATTERNS = [
    re.compile(r"^MANIFEST[-_]([A-Za-z0-9_]+)[-_](\d{8}_\d{6})[-_]([A-Za-z0-9_-]+)\.MF$", re.IGNORECASE),
    re.compile(r"^MANIFEST[-_]batch([A-Za-z0-9_-]+)[-_]\.MF$", re.IGNORECASE),
    re.compile(r"^MANIFEST[-_]([A-Za-z0-9_-]+)\.MF$", re.IGNORECASE)
]


def parse_manifest_filename(filename: str, default_source: str = "ORACLE_FUSION") -> dict:
    """
    Parses manifest filename to extract source, extract_timestamp, and batch_id.
    Constructs a deterministic sort_key for chronological FIFO ordering.
    """
    # 1. Standard: MANIFEST_<SOURCE>_<TIMESTAMP>_<BATCH_ID>.MF
    m1 = MANIFEST_PATTERNS[0].match(filename)
    if m1:
        source = m1.group(1).upper()
        extract_ts_raw = m1.group(2)
        batch_id = m1.group(3)
        try:
            dt = datetime.strptime(extract_ts_raw, "%Y%m%d_%H%M%S")
            sort_key = (dt.timestamp(), batch_id)
        except ValueError:
            sort_key = (0.0, batch_id)
        return {
            "source": source,
            "extract_timestamp": extract_ts_raw,
            "batch_id": batch_id,
            "sort_key": sort_key
        }

    # 2. Legacy BICC raw: MANIFEST-batch<ID>-.MF
    m2 = MANIFEST_PATTERNS[1].match(filename)
    if m2:
        batch_id = m2.group(1)
        sort_key_val = int(batch_id) if batch_id.isdigit() else batch_id
        return {
            "source": default_source,
            "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "batch_id": batch_id,
            "sort_key": (0.0, sort_key_val)
        }

    # 3. Single-token manifest: MANIFEST_<TOKEN>.MF
    m3 = MANIFEST_PATTERNS[2].match(filename)
    if m3:
        token = m3.group(1)
        sort_key_val = int(token) if token.isdigit() else token
        return {
            "source": default_source,
            "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "batch_id": token,
            "sort_key": (0.0, sort_key_val)
        }

    # 4. Fallback generic naming
    base = os.path.splitext(filename)[0]
    return {
        "source": default_source,
        "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "batch_id": base,
        "sort_key": (0.0, base)
    }


def read_declared_files_from_manifest(manifest_path: str) -> list[str]:
    """
    Reads data file names declared inside the manifest file.
    Supports CSV headers (filename,checksum) or raw lists of filenames.
    Ignores blank lines, comments, and key-value lines.
    """
    declared = []
    with open(manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str or line_str.startswith("#") or "=" in line_str:
                continue

            # Parse delimiter: semicolon, tab, or comma
            if ";" in line_str:
                parts = line_str.split(";")
            elif "\t" in line_str:
                parts = line_str.split("\t")
            elif "," in line_str:
                parts = line_str.split(",")
            else:
                parts = [line_str]

            token = parts[0].strip().strip('"').strip("'")
            if token.lower() in ("filename", "file_name", "file"):
                continue  # Header line

            if token:
                declared.append(token)

    return declared


def scan_drop_zone(drop_zone_dir: str, default_source: str = "ORACLE_FUSION") -> list[dict]:
    """
    Scans the drop zone for all valid manifest files (.MF).
    Returns a list of parsed batch descriptor dictionaries.
    """
    if not os.path.exists(drop_zone_dir):
        logger.warning(f"Drop zone directory does not exist: '{drop_zone_dir}'")
        return []

    manifests = []
    for f in os.listdir(drop_zone_dir):
        if f.upper().endswith(".MF") and not f.startswith("."):
            full_path = os.path.join(drop_zone_dir, f)
            meta = parse_manifest_filename(f, default_source)
            try:
                declared_files = read_declared_files_from_manifest(full_path)
            except Exception as e:
                logger.error(f"Failed to read manifest '{f}': {e}")
                continue

            # If extract timestamp was generated from fallback, refine using file mtime
            mtime = os.path.getmtime(full_path)
            if meta["sort_key"][0] == 0.0:
                dt_mtime = datetime.fromtimestamp(mtime)
                meta["sort_key"] = (mtime, meta["sort_key"][1])
                meta["extract_timestamp"] = dt_mtime.strftime("%Y%m%d_%H%M%S")

            manifests.append({
                "manifest_path": full_path,
                "manifest_filename": f,
                "source": meta["source"],
                "extract_timestamp": meta["extract_timestamp"],
                "batch_id": meta["batch_id"],
                "sort_key": meta["sort_key"],
                "declared_files": declared_files
            })

    return manifests


def sort_batches_fifo(batches: list[dict]) -> list[dict]:
    """
    Sorts batches in strict ascending chronological order (FIFO - oldest first).
    """
    return sorted(batches, key=lambda b: b["sort_key"])


def verify_payload_stability(drop_zone_dir: str, declared_files: list[str], settling_window_sec: int = 5) -> tuple[bool, str]:
    """
    Verifies that all files declared in the manifest exist in DROP_ZONE,
    are non-empty (>0 bytes), and remain stable (unmodified) during the settling window.
    """
    missing_files = []
    zero_byte_files = []

    for fname in declared_files:
        fpath = os.path.join(drop_zone_dir, fname)
        if not os.path.exists(fpath):
            missing_files.append(fname)
        elif os.path.getsize(fpath) == 0:
            zero_byte_files.append(fname)

    if missing_files:
        return False, f"Missing {len(missing_files)} manifested data files in drop zone: {missing_files}"

    if zero_byte_files:
        return False, f"Found {len(zero_byte_files)} empty (0 byte) files in drop zone: {zero_byte_files}"

    # Stability settling check (guarantee SFTP / Cloud transfer has finished)
    if settling_window_sec > 0:
        initial_sizes = {fname: os.path.getsize(os.path.join(drop_zone_dir, fname)) for fname in declared_files}
        time.sleep(settling_window_sec)
        for fname in declared_files:
            fpath = os.path.join(drop_zone_dir, fname)
            current_size = os.path.getsize(fpath)
            if current_size != initial_sizes[fname]:
                return False, f"File '{fname}' size changed during settling window ({initial_sizes[fname]} -> {current_size} bytes). Upload in progress."

    return True, "All declared files exist and are stable."
