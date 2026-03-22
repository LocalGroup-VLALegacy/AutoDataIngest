from pathlib import Path

# Unix socket path — set permissions/group after daemon start if needed
SOCKET_PATH = Path("/tmp/vlaxl_autodataingest.sock")

# Directory containing script log files (relative to the working directory)
LOGS_DIR = Path("logs")

# Maximum log lines buffered per script (for newly-connected clients)
LOG_BUFFER_LINES = 500

# Script definitions
#   interval=None  → runs continuously; daemon marks CRASHED if it exits
#   interval=N     → one-shot script; daemon re-runs every N seconds after exit
SCRIPTS = [
    {
        "name": "ingest",
        "label": "main.py",
        "cmd": ["python", "main.py"],
        "log_file": LOGS_DIR / "main.log",
        "interval": None,
    },
    {
        "name": "job_completion",
        "label": "main_job_completion.py",
        "cmd": ["python", "main_job_completion.py"],
        "log_file": LOGS_DIR / "main_job_completion.log",
        "interval": 3600,
    },
    {
        "name": "restarts",
        "label": "main_restarts.py",
        "cmd": ["python", "main_restarts.py"],
        "log_file": LOGS_DIR / "main_restarts.log",
        "interval": 3600,
    },
]
