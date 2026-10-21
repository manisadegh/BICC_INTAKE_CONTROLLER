"""
lib/scanner.py - Drop Zone Scanner & Payload Stability Sentry

Discovers raw manifests deposited in DROP_ZONE, parses batch metadata via
`dataloader_core.manifest`, resolves batch IDs from payload filenames when omitted
from manifest names, sorts execution queues in strict ascending chronological
order (FIFO), and validates file existence and upload stability.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.intake import BICCIntakePlugin
from dataloader_core.manifest import (
    MANIFEST_PATTERNS,
    extract_batch_id_from_filenames,
    parse_manifest_filename as _core_parse_manifest_filename,
    read_manifest_entries,
)

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")


def parse_manifest_filename(filename: str, default_source: str = "ORACLE_FUSION") -> Dict:
    """
    Parses manifest filename to extract source, extract_timestamp, and batch_id.
    Constructs a deterministic sort_key for chronological FIFO ordering.
    Delegates to `dataloader_core.manifest.parse_manifest_filename`.
    """
    return _core_parse_manifest_filename(filename, default_source=default_source).to_dict()


def read_declared_files_from_manifest(manifest_path: str) -> List[str]:
    """
    Reads data file names declared inside the manifest file.
    Delegates to `dataloader_core.manifest.read_manifest_entries`.
    """
    entries = read_manifest_entries(manifest_path, require_non_empty=False)
    return [entry.filename for entry in entries]


def scan_drop_zone(drop_zone_dir: str, default_source: str = "ORACLE_FUSION") -> List[Dict]:
    """
    Scans the drop zone for all valid manifest files (.MF).
    Resolves true batch_id from declared payload files if omitted from manifest name.
    Returns a list of parsed batch descriptor dictionaries.
    Delegates to `BICCIntakePlugin`.
    """
    plugin = BICCIntakePlugin(source_code=default_source)
    candidates = plugin.discover_raw_batches(drop_zone_dir)
    return [c.to_dict() for c in candidates]


def sort_batches_fifo(batches: List[Dict]) -> List[Dict]:
    """
    Sorts batches in strict ascending chronological order (FIFO - oldest first).
    """
    return sorted(batches, key=lambda b: b["sort_key"])


def verify_payload_stability(
    drop_zone_dir: str,
    declared_files: List[str],
    settling_window_sec: int = 5,
) -> Tuple[bool, str]:
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
                return (
                    False,
                    f"File '{fname}' size changed during settling window "
                    f"({initial_sizes[fname]} -> {current_size} bytes). Upload in progress.",
                )

    return True, "All declared files exist and are stable."
