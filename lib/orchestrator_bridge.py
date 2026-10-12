import subprocess
import sys
import os
import logging

logger = logging.getLogger("BICC_INTAKE_CONTROLLER")

def invoke_orchestrator(orchestrator_dir, config_file="config.json", python_bin=None, env=None):
    """
    Synchronously invokes DATA_INGESTION_ORCHESTRATOR via subprocess:
      <python_bin> orchestrator.py --config <config_file>
    Streams stdout/stderr in real-time to the logger.
    Returns the process exit code.
    """
    if python_bin is None:
        python_bin = sys.executable

    orch_script = os.path.join(orchestrator_dir, "orchestrator.py").replace('\\', '/')
    if not os.path.exists(orch_script):
        raise FileNotFoundError(f"orchestrator.py not found at: '{orch_script}'")

    cmd = [python_bin, orch_script, "--config", config_file]
    logger.info(f"Invoking Orchestrator: {' '.join(cmd)} (cwd: '{orchestrator_dir}')")

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    # Launch subprocess with unbuffered output
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

    for line in iter(process.stdout.readline, ''):
        if line:
            # Relay orchestrator logs with indentation
            logger.info(f"[ORCHESTRATOR] {line.rstrip()}")

    process.stdout.close()
    rc = process.wait()

    if rc == 0:
        logger.info(f"[ORCHESTRATOR] Process completed successfully with RC {rc}.")
    else:
        logger.error(f"[ORCHESTRATOR] Process failed with non-zero exit code {rc}!")

    return rc
