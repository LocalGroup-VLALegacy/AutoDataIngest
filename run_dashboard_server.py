"""
Entrypoint for the AutoDataIngest dashboard server daemon.

Run from the AutoDataIngest project root:
    python run_dashboard_server.py

The server manages the three pipeline scripts, tails their log files, and
serves real-time status to connected TUI clients.  All three scripts start
in the STOPPED state; use the TUI (python -m dashboard.client) to start them.
"""

import asyncio
import logging
import sys
from pathlib import Path

from dashboard.server import run_server

LOGGER_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=LOGGER_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/dashboard_server.log"),
        ],
    )

    cwd = Path(__file__).parent.resolve()
    logging.getLogger(__name__).info("Working directory: %s", cwd)

    # Ensure logs/ exists before handlers try to write to it
    (cwd / "logs").mkdir(exist_ok=True)

    asyncio.run(run_server(cwd))


if __name__ == "__main__":
    main()
