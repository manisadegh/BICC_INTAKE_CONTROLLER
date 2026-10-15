"""
lib/scanner.py - Drop Zone Scanner & Payload Stability Sentry

Discovers raw manifests deposited in DROP_ZONE, parses batch metadata,
resolves batch IDs from payload filenames when omitted from manifest names,
sorts execution queues in strict ascending chronological order (FIFO),
and validates file existence and upload stability.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")

# Precompiled patterns for standard and legacy manifest naming conventions
MANIFEST_PATTERNS = [
    # 1. Standard: MANIFEST_<SOURCE>_<TIMESTAMP>_<BATCH_ID>.MF
    re.compile(r"^MANIFEST[-_]([A-Za-z0-9_]+)[-_](\d{8}_\d{6})[-_]([A-Za-z0-9_-]+)\.MF$", re.IGNORECASE),
    # 2. Oracle BICC with Timestamp: *MANIFEST-YYYYMMDD_HHMMSS.MF (e.g. DT2_MANIFEST-20261013_131116.MF)
    re.compile(r"^.*MANIFEST[-_](\d{8}_\d{6})\.MF$", re.IGNORECASE),
    # 3. Legacy BICC raw: MANIFEST-batch<ID>-.MF
    re.compile(r"^MANIFEST[-_]batch([A-Za-z0-9_-]+)[-_]\.MF$", re.IGNORECASE),
    # 4. Single-token manifest: MANIFEST_<TOKEN>.MF
    re.compile(r"^MANIFEST[-_]([A-Za-z0-9_-]+)\.MF$", re.IGNORECASE)
]


def extract_batch_id_from_filenames(declared_files: List[str]) -> Optional[str]:
    """
    Extracts batch_id from declared filenames following the pattern:
      *<signature>-batch<batch_id>-YYYYMMDD_HHMMSS.zip/.csv
      e.g. DT2_file_..._invoiceheaderextractpvo-batch847376139-20261013_131116.zip -> 847376139
    """
    for fname in declared_files:
        m = re.search(r"-batch([A-Za-z0-9_-]+?)-", fname, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def parse_manifest_filename(filename: str, default_source: str = "ORACLE_FUSION") -> Dict:
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
            "sort_key": sort_key,
            "batch_id_from_manifest": True
        }

    # 2. Oracle BICC with Timestamp: *MANIFEST-YYYYMMDD_HHMMSS.MF (e.g. DT2_MANIFEST-20261013_131116.MF)
    m2 = MANIFEST_PATTERNS[1].match(filename)
    if m2:
        extract_ts_raw = m2.group(1)
        try:
            dt = datetime.strptime(extract_ts_raw, "%Y%m%d_%H%M%S")
            ts_float = dt.timestamp()
        except ValueError:
            ts_float = 0.0
        return {
            "source": default_source,
            "extract_timestamp": extract_ts_raw,
            "batch_id": extract_ts_raw,  # Temporary placeholder; will be overridden by payload batch ID
            "sort_key": (ts_float, extract_ts_raw),
            "batch_id_from_manifest": False
        }

    # 3. Legacy BICC raw: MANIFEST-batch<ID>-.MF
    m3 = MANIFEST_PATTERNS[2].match(filename)
    if m3:
        batch_id = m3.group(1)
        sort_key_val = int(batch_id) if batch_id.isdigit() else batch_id
        return {
            "source": default_source,
            "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "batch_id": batch_id,
            "sort_key": (0.0, sort_key_val),
            "batch_id_from_manifest": True
        }

    # 4. Single-token manifest: MANIFEST_<TOKEN>.MF
    m4 = MANIFEST_PATTERNS[3].match(filename)
    if m4:
        token = m4.group(1)
        sort_key_val = int(token) if token.isdigit() else token
        return {
            "source": default_source,
            "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "batch_id": token,
            "sort_key": (0.0, sort_key_val),
            "batch_id_from_manifest": True
        }

    # 5. Fallback generic naming
    base = os.path.splitext(filename)[0]
    return {
        "source": default_source,
        "extract_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "batch_id": base,
        "sort_key": (0.0, base),
        "batch_id_from_manifest": False
    }


def read_declared_files_from_manifest(manifest_path: str) -> List[str]:
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


def scan_drop_zone(drop_zone_dir: str, default_source: str = "ORACLE_FUSION") -> List[Dict]:
    """
    Scans the drop zone for all valid manifest files (.MF).
    Resolves true batch_id from declared payload files if omitted from manifest name.
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

            # Resolve true batch_id from declared payload filenames (e.g. -batch<id>-)
            payload_batch_id = extract_batch_id_from_filenames(declared_files)
            if payload_batch_id:
                meta["batch_id"] = payload_batch_id
                meta["sort_key"] = (meta["sort_key"][0], payload_batch_id)

            # If extract timestamp was generated from fallback (sort_key[0] == 0.0), refine using file mtime
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


def sort_batches_fifo(batches: List[Dict]) -> List[Dict]:
    """
    Sorts batches in strict ascending chronological order (FIFO - oldest first).
    """
    return sorted(batches, key=lambda b: b["sort_key"])


def verify_payload_stability(drop_zone_dir: str, declared_files: List[str], settling_window_sec: int = 5) -> Tuple[bool, str]:
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
