"""
lib/manifest_factory.py - Manifest Factory, Capsule Staging & Ingress Publishing

Delegates cryptographic MD5 checksums, standardized manifest synthesis, and
atomic `.staging_BATCH_*` -> `BATCH_*` queue capsule publishing to `dataloader_core`.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.capsule import BatchDescriptor, CapsulePathResolver
from dataloader_core.checksum import compute_file_checksum
from dataloader_core.manifest import write_standardized_manifest

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")


def compute_file_md5(file_path: str) -> str:
    """
    Computes cryptographic MD5 hash of a file reading in 64 KB blocks.
    Delegates to `dataloader_core.checksum.compute_file_checksum`.
    """
    return compute_file_checksum(file_path, algorithm="md5")


def create_standardized_manifest(
    output_dir: str,
    source: str,
    extract_timestamp: str,
    batch_id: str,
    file_checksum_pairs: List[Tuple[str, str]],
) -> str:
    """
    Generates an orchestrator-compliant standardized manifest:
      MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF
    Delegates to `dataloader_core.manifest.write_standardized_manifest`.
    """
    manifest_path = write_standardized_manifest(
        output_dir=output_dir,
        source=source,
        extract_timestamp=extract_timestamp,
        batch_id=batch_id,
        file_checksum_pairs=file_checksum_pairs,
    )
    logger.info(f"Created standardized manifest: '{manifest_path}' with {len(file_checksum_pairs)} file entries.")
    return manifest_path


def stage_batch_capsule_to_landing_zone(
    landing_zone_dir: str,
    drop_zone_dir: str,
    raw_manifest_path: str,
    source: str,
    extract_timestamp: str,
    batch_id: str,
    entities_descriptor: Dict[str, Dict[str, Any]],
    file_checksum_pairs: List[Tuple[str, str]],
    receipt_payload: List[Dict[str, Any]],
    declared_files: List[str],
) -> Tuple[str, BatchDescriptor]:
    """
    Stages a self-contained Batch Capsule in `<landing_zone_dir>/.staging_BATCH_<TS>_<ID>/`
    (containing `RAW/` with vaulted raw manifest + `RECEIPT_*.json`, `MANIFEST_*.MF`,
    `batch_descriptor.json`, and payload files) and atomically renames it to
    `<landing_zone_dir>/BATCH_<TS>_<ID>/`.
    """
    final_dir, descriptor = CapsulePathResolver.stage_and_publish_queue_capsule(
        landing_zone_dir=landing_zone_dir,
        drop_zone_dir=drop_zone_dir,
        raw_manifest_path=raw_manifest_path,
        source=source,
        extract_timestamp=extract_timestamp,
        batch_id=batch_id,
        entities_descriptor=entities_descriptor,
        file_checksum_pairs=file_checksum_pairs,
        receipt_payload=receipt_payload,
        declared_files=declared_files,
    )
    logger.info(
        f"Published queued Batch Capsule '{final_dir}' "
        f"(manifest='{descriptor.standardized_manifest}', receipt='{descriptor.receipt_file}', "
        f"files={len(declared_files)})."
    )
    return final_dir, descriptor


def vault_raw_manifest(
    raw_manifest_path: str,
    archive_base_dir: str,
    source: str,
    extract_timestamp: str,
    batch_id: str,
    receipt_payload: List[Dict],
) -> str:
    """
    Legacy helper: vaults the raw BICC manifest into `<archive_base_dir>/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/RAW/`.
    """
    clean_ts = CapsulePathResolver.normalize_timestamp_token(extract_timestamp)
    clean_src = CapsulePathResolver.sanitize_token(source)
    clean_batch = CapsulePathResolver.sanitize_token(batch_id)

    vault_dir = CapsulePathResolver.get_raw_vault_dir(
        archive_base=archive_base_dir,
        source=clean_src,
        batch_id=clean_batch,
        ts=clean_ts,
        ensure_exists=True,
    )

    raw_basename = os.path.basename(raw_manifest_path)
    vault_manifest_path = os.path.join(vault_dir, raw_basename)
    shutil.move(raw_manifest_path, vault_manifest_path)

    std_receipt_name = f"RECEIPT_{clean_src}_{clean_ts}_{clean_batch}.json"
    std_receipt_file = os.path.join(vault_dir, std_receipt_name)

    receipt_data = {
        "batch_id": batch_id,
        "source": source,
        "extract_timestamp": extract_timestamp,
        "intake_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_manifest_vaulted": raw_basename,
        "receipt_file": std_receipt_name,
        "payload": receipt_payload,
    }

    with open(std_receipt_file, "w", encoding="utf-8") as f:
        json.dump(receipt_data, f, indent=2)

    logger.info(f"Vaulted original manifest '{raw_basename}' and receipt '{std_receipt_name}' to '{vault_dir}'.")
    return vault_dir


def transfer_batch_to_landing_zone(
    drop_zone_dir: str,
    landing_zone_dir: str,
    data_files: List[str],
    standardized_manifest_path: Optional[str] = None,
):
    """
    Moves data files from DROP_ZONE to LANDING_ZONE.
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
