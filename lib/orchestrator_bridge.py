"""
lib/orchestrator_bridge.py - Subprocess Delegation & Real-Time Log Streamer

Synchronously launches DATA_INGESTION_ORCHESTRATOR via subprocess,
relays stdout/stderr streams in real time to the feeder logger,
and returns the process exit code to enforce FAIL_STOP semantics.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from typing import Dict, Optional

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")


def invoke_orchestrator(
    orchestrator_dir: str,
    config_file: str = "config.json",
    python_bin: Optional[str] = None,
    env: Optional[Dict] = None,
    skip_post_tasks: bool = False,
    source: Optional[str] = None,
    post_tasks_file: Optional[str] = None
) -> int:
    """
    Synchronously invokes DATA_INGESTION_ORCHESTRATOR via subprocess:
      <python_bin> orchestrator.py --config <config_file> [--skip-post-tasks] [--source <source>] [--post-tasks-file <file>]
    Streams stdout/stderr in real-time to the logger.
    Returns the process exit code.
    """
    if python_bin is None:
        python_bin = sys.executable

    orch_script = os.path.join(orchestrator_dir, "orchestrator.py")
    if not os.path.exists(orch_script):
        raise FileNotFoundError(f"orchestrator.py not found at: '{orch_script}'")

    cmd = [python_bin, orch_script, "--config", config_file]
    if skip_post_tasks:
        cmd.append("--skip-post-tasks")
    if source:
        cmd.extend(["--source", source])
    if post_tasks_file:
        cmd.extend(["--post-tasks-file", post_tasks_file])
    logger.info(f"Invoking Orchestrator: {' '.join(cmd)} (cwd: '{orchestrator_dir}')")

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    # Launch subprocess with unbuffered line-streaming
    process = subprocess.Popen(
        cmd,
        cwd=orchestrator_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env=run_env
    )

    try:
        for line in iter(process.stdout.readline, ""):
            if line:
                logger.info(f"[ORCHESTRATOR] {line.rstrip()}")
    finally:
        process.stdout.close()

    rc = process.wait()

    if rc == 0:
        logger.info(f"[ORCHESTRATOR] Process completed successfully with RC {rc}.")
    else:
        logger.error(f"[ORCHESTRATOR] Process failed with non-zero exit code {rc}!")

    return rc
