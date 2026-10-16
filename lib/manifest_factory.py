"""
lib/manifest_factory.py - Manifest Factory, Vaulting & Ingress Transfer

Computes cryptographic MD5 checksums, synthesizes orchestrator-compliant manifests,
vaults original raw BICC manifests with JSON audit receipts, and transfers
payload batches to the Orchestrator's LANDING_ZONE.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")


def compute_file_md5(file_path: str) -> str:
    """
    Computes cryptographic MD5 hash of a file reading in 64 KB blocks.
    Memory-efficient for multi-gigabyte extracts.
    """
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest().lower()


def create_standardized_manifest(
    output_dir: str,
    source: str,
    extract_timestamp: str,
    batch_id: str,
    file_checksum_pairs: List[Tuple[str, str]]
) -> str:
    """
    Generates an orchestrator-compliant standardized manifest:
      MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF
    Format:
      filename,checksum
      file1.csv,hash1
    Returns the absolute path of the created manifest.
    """
    os.makedirs(output_dir, exist_ok=True)
    manifest_name = f"MANIFEST_{source}_{extract_timestamp}_{batch_id}.MF"
    manifest_path = os.path.join(output_dir, manifest_name)

    lines = ["filename,checksum\n"]
    for fname, cs in file_checksum_pairs:
        lines.append(f"{fname},{cs}\n")

    with open(manifest_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    logger.info(f"Created standardized manifest: '{manifest_path}' with {len(file_checksum_pairs)} file entries.")
    return manifest_path


def vault_raw_manifest(
    raw_manifest_path: str,
    archive_base_dir: str,
    source: str,
    extract_timestamp: str,
    batch_id: str,
    receipt_payload: List[Dict]
) -> str:
    """
    Vaults the pristine original BICC manifest into the Batch Capsule:
      <archive_base_dir>/[<SOURCE>/][<YYYY-MM>/]BATCH_<BATCH_ID>_<EXTRACT_TIMESTAMP>/RAW/
    and writes standardized and legacy intake receipt audit files:
      RECEIPT_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.json & intake_receipt.json
    Returns the path of the RAW vault directory.
    """
    clean_ts = str(extract_timestamp).replace(":", "").replace("-", "").replace(" ", "_")
    year_month = f"{clean_ts[:4]}-{clean_ts[4:6]}" if len(clean_ts) >= 6 else "UNKNOWN_DATE"
    capsule_name = f"BATCH_{batch_id}_{clean_ts}"

    # Build capsule directory path
    if os.path.basename(archive_base_dir).lower() == str(source).lower():
        vault_dir = os.path.join(archive_base_dir, year_month, capsule_name, "RAW")
    else:
        vault_dir = os.path.join(archive_base_dir, str(source), year_month, capsule_name, "RAW")
    os.makedirs(vault_dir, exist_ok=True)

    raw_basename = os.path.basename(raw_manifest_path)
    vault_manifest_path = os.path.join(vault_dir, raw_basename)
    shutil.move(raw_manifest_path, vault_manifest_path)

    clean_src = str(source).replace("/", "_").replace("\\", "_")
    clean_batch = str(batch_id).replace("/", "_").replace("\\", "_")
    std_receipt_name = f"RECEIPT_{clean_src}_{clean_ts}_{clean_batch}.json"
    std_receipt_file = os.path.join(vault_dir, std_receipt_name)
    legacy_receipt_file = os.path.join(vault_dir, "intake_receipt.json")

    receipt_data = {
        "batch_id": batch_id,
        "source": source,
        "extract_timestamp": extract_timestamp,
        "intake_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_manifest_vaulted": raw_basename,
        "receipt_file": std_receipt_name,
        "payload": receipt_payload
    }

    with open(std_receipt_file, "w", encoding="utf-8") as f:
        json.dump(receipt_data, f, indent=2)
    with open(legacy_receipt_file, "w", encoding="utf-8") as f:
        json.dump(receipt_data, f, indent=2)

    logger.info(f"Vaulted original manifest '{raw_basename}' and receipt '{std_receipt_name}' to '{vault_dir}'.")
    return vault_dir


def transfer_batch_to_landing_zone(
    drop_zone_dir: str,
    landing_zone_dir: str,
    data_files: List[str],
    standardized_manifest_path: Optional[str] = None
):
    """
    Moves data files (and standardized manifest if not already generated there)
    from DROP_ZONE to the Orchestrator's LANDING_ZONE.
    """
    os.makedirs(landing_zone_dir, exist_ok=True)

    for fname in data_files:
        src = os.path.join(drop_zone_dir, fname)
        dst = os.path.join(landing_zone_dir, fname)
        if os.path.exists(src):
            shutil.move(src, dst)

    if standardized_manifest_path and os.path.dirname(standardized_manifest_path) != landing_zone_dir:
        mf_name = os.path.basename(standardized_manifest_path)
        shutil.move(standardized_manifest_path, os.path.join(landing_zone_dir, mf_name))

    logger.info(f"Transferred {len(data_files)} data files to Orchestrator landing zone: '{landing_zone_dir}'.")
