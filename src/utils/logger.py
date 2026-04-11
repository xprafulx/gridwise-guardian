# src/utils/logger.py
import sys
import os
from datetime import datetime

def setup_artifact_logger(job_name):
    """Creates a .txt file for the specific run and captures ALL output + errors."""
    log_dir = f"artifacts/{job_name}"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_file = os.path.join(log_dir, f"{timestamp}_{job_name}.txt")
    
    class LoggerTee:
        def __init__(self, filename, terminal):
            self.terminal = terminal
            self.log = open(filename, "a", encoding="utf-8")

        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)

        def flush(self):
            self.terminal.flush()
            self.log.flush()

    # Capture BOTH standard output and standard error
    sys.stdout = LoggerTee(log_file, sys.stdout)
    sys.stderr = LoggerTee(log_file, sys.stderr)
    
    print(f"📝 Artifact Logging started: {log_file}")
    print(f"🕒 Run started at: {datetime.now().isoformat()}")
    print("-" * 50)
    
    return log_file