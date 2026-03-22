"""
Wire protocol for dashboard daemon ↔ TUI client communication.

All messages are newline-delimited JSON (one object per line).

Server → Client:
  StateMsg   — current state of one script (broadcast on change + every 10 s heartbeat)
  LogLine    — one new log line from a script
  Snapshot   — full state + log buffer, sent once on client connect

Client → Server:
  Command    — start / stop / restart a named script
"""

import json
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class StateMsg:
    script: str
    state: str                    # STOPPED | RUNNING | STOPPING | CRASHED | WAITING
    pid: Optional[int] = None
    uptime_s: Optional[float] = None
    exit_code: Optional[int] = None
    next_run_s: Optional[float] = None   # seconds until next scheduled run
    type: str = "state"


@dataclass
class LogLine:
    script: str
    line: str
    type: str = "log"


@dataclass
class Snapshot:
    states: dict                  # name → StateMsg fields as dict
    logs: dict                    # name → list[str]
    type: str = "snapshot"


@dataclass
class Command:
    script: str
    action: str                   # start | stop | restart
    type: str = "cmd"


def encode(msg) -> bytes:
    return (json.dumps(asdict(msg)) + "\n").encode()


def decode_msg(line: str) -> dict:
    return json.loads(line)
