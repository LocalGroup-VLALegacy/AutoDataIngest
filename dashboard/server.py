"""
AutoDataIngest dashboard server daemon.

Manages the three pipeline control scripts as asyncio subprocesses, tails their
log files, and serves live state + log updates to connected TUI clients over a
Unix domain socket.

Start with:
    python run_dashboard_server.py
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

from .config import SOCKET_PATH, LOG_BUFFER_LINES, LOGS_DIR, SCRIPTS
from .protocol import StateMsg, LogLine, Snapshot, Command, encode, decode_msg

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Broadcast bus — fan-out to all connected client queues
# ---------------------------------------------------------------------------

class BroadcastBus:
    def __init__(self) -> None:
        self._queues: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._queues.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._queues.discard(q)

    async def publish(self, msg) -> None:
        data = encode(msg)
        for q in list(self._queues):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass  # slow client — drop rather than block the broadcaster


# ---------------------------------------------------------------------------
# Script runner — manages one pipeline script
# ---------------------------------------------------------------------------

class ScriptRunner:
    """
    Manages one pipeline script process:
    - start / stop / restart
    - captures subprocess stdout; tails the script's own log file
    - broadcasts state changes and log lines to the BroadcastBus

    interval=None  → continuous process; marks CRASHED if it exits unexpectedly
    interval=N     → one-shot process; re-runs every N seconds after each exit
    """

    def __init__(self, cfg: dict, bus: BroadcastBus, cwd: Path) -> None:
        self.name: str = cfg["name"]
        self.label: str = cfg["label"]
        self.cmd: list = cfg["cmd"]
        self.log_file: Path = cfg["log_file"]
        self.interval: Optional[int] = cfg["interval"]
        self._bus = bus
        self._cwd = cwd

        # Observable state (read by DashboardServer for snapshots)
        self.state: str = "STOPPED"
        self.pid: Optional[int] = None
        self.start_time: Optional[datetime] = None
        self.exit_code: Optional[int] = None
        self.next_run_at: Optional[float] = None   # time.monotonic() value

        # Log buffer — last LOG_BUFFER_LINES lines, served to new clients
        self.log_buffer: deque = deque(maxlen=LOG_BUFFER_LINES)

        # Internal
        self._process: Optional[asyncio.subprocess.Process] = None
        self._lifecycle_task: Optional[asyncio.Task] = None
        self._tailer_task: Optional[asyncio.Task] = None
        self._stop_event: asyncio.Event = asyncio.Event()

    # ------------------------------------------------------------------
    # Public control API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self.state in ("RUNNING", "STOPPING"):
            log.warning("%s: start() called while %s — ignoring", self.name, self.state)
            return
        self._stop_event.clear()
        self._lifecycle_task = asyncio.create_task(
            self._lifecycle(), name=f"lifecycle-{self.name}"
        )

    async def stop(self) -> None:
        if self.state == "STOPPED":
            return
        self._stop_event.set()
        if self._process and self._process.returncode is None:
            await self._set_state("STOPPING")
            self._process.terminate()
        if self._lifecycle_task and not self._lifecycle_task.done():
            try:
                await asyncio.wait_for(self._lifecycle_task, timeout=25.0)
            except asyncio.TimeoutError:
                log.warning("%s: lifecycle did not finish in time; force-killing", self.name)
                if self._process and self._process.returncode is None:
                    self._process.kill()
                self._lifecycle_task.cancel()
                try:
                    await self._lifecycle_task
                except asyncio.CancelledError:
                    pass

    async def restart(self) -> None:
        await self.stop()
        await self.start()

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    async def _set_state(self, state: str) -> None:
        self.state = state
        if state not in ("RUNNING", "STOPPING"):
            self.pid = None
        await self._broadcast_state()

    async def _broadcast_state(self) -> None:
        uptime = None
        if self.start_time and self.state == "RUNNING":
            uptime = (datetime.now() - self.start_time).total_seconds()
        next_run = None
        if self.next_run_at is not None and self.state == "WAITING":
            next_run = max(0.0, self.next_run_at - time.monotonic())
        await self._bus.publish(StateMsg(
            script=self.name,
            state=self.state,
            pid=self.pid,
            uptime_s=uptime,
            exit_code=self.exit_code,
            next_run_s=next_run,
        ))

    def snapshot_state(self) -> dict:
        """Return current state as a plain dict for Snapshot messages."""
        uptime = None
        if self.start_time and self.state == "RUNNING":
            uptime = (datetime.now() - self.start_time).total_seconds()
        next_run = None
        if self.next_run_at is not None and self.state == "WAITING":
            next_run = max(0.0, self.next_run_at - time.monotonic())
        return asdict(StateMsg(
            script=self.name,
            state=self.state,
            pid=self.pid,
            uptime_s=uptime,
            exit_code=self.exit_code,
            next_run_s=next_run,
        ))

    # ------------------------------------------------------------------
    # Process lifecycle
    # ------------------------------------------------------------------

    async def _lifecycle(self) -> None:
        """Outer loop: run the script; re-run on interval for scheduled scripts."""
        try:
            while not self._stop_event.is_set():
                await self._run_once()

                if self._stop_event.is_set():
                    break

                if self.interval is None:
                    # Continuous script — only exits on error; mark and halt
                    new_state = "CRASHED" if (self.exit_code or 0) != 0 else "STOPPED"
                    await self._set_state(new_state)
                    break
                else:
                    # Interval script — show exit state briefly, then schedule next run
                    if (self.exit_code or 0) != 0:
                        await self._set_state("CRASHED")
                        # Brief pause so the CRASHED state is visible in the TUI
                        for _ in range(6):   # 3 s total, checking stop every 0.5 s
                            if self._stop_event.is_set():
                                break
                            await asyncio.sleep(0.5)
                        if self._stop_event.is_set():
                            break

                    end_time = time.monotonic() + self.interval
                    self.next_run_at = end_time
                    await self._set_state("WAITING")

                    # Poll every 5 s until interval elapses or stop is requested
                    while time.monotonic() < end_time and not self._stop_event.is_set():
                        remaining = end_time - time.monotonic()
                        await asyncio.sleep(min(5.0, max(0.05, remaining)))

                    self.next_run_at = None
                    if self._stop_event.is_set():
                        break
                    # Loop continues → re-run

        except asyncio.CancelledError:
            pass
        finally:
            if self._tailer_task and not self._tailer_task.done():
                self._tailer_task.cancel()
                try:
                    await self._tailer_task
                except asyncio.CancelledError:
                    pass
            self.next_run_at = None
            await self._set_state("STOPPED")

    async def _run_once(self) -> None:
        """Launch the subprocess, capture logs, wait for exit."""
        LOGS_DIR.mkdir(exist_ok=True)
        stderr_log = LOGS_DIR / f"dashboard_{self.name}_stderr.log"

        log.info("Starting %s: %s", self.name, " ".join(self.cmd))
        stderr_fh = open(stderr_log, "a")
        try:
            self._process = await asyncio.create_subprocess_exec(
                *self.cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_fh,
                cwd=str(self._cwd),
            )
        except Exception as exc:
            log.error("Failed to launch %s: %s", self.name, exc)
            stderr_fh.close()
            self.exit_code = -1
            await self._set_state("CRASHED")
            return

        self.pid = self._process.pid
        self.start_time = datetime.now()
        self.exit_code = None
        await self._set_state("RUNNING")

        # Tail the script's own log file in parallel (scripts log there via FileHandler)
        self._tailer_task = asyncio.create_task(
            self._tail_log(), name=f"tailer-{self.name}"
        )

        # Drain subprocess stdout (normally empty; scripts write to log file not stdout)
        async for raw in self._process.stdout:
            line = raw.decode(errors="replace").rstrip()
            if line:
                self.log_buffer.append(line)
                await self._bus.publish(LogLine(script=self.name, line=line))

        await self._process.wait()
        self.exit_code = self._process.returncode
        stderr_fh.close()
        log.info("%s exited with code %s", self.name, self.exit_code)

        # Give the tailer a moment to flush any remaining lines, then cancel it
        await asyncio.sleep(1.0)
        if self._tailer_task and not self._tailer_task.done():
            self._tailer_task.cancel()
            try:
                await self._tailer_task
            except asyncio.CancelledError:
                pass

    async def _tail_log(self) -> None:
        """Follow the script's log file like `tail -f`, pushing new lines to clients."""
        # Wait up to 10 s for the file to be created (script creates it on first log write)
        deadline = time.monotonic() + 10.0
        while not self.log_file.exists():
            if time.monotonic() > deadline:
                log.debug("%s: log file %s never appeared", self.name, self.log_file)
                return
            await asyncio.sleep(0.5)

        try:
            with open(self.log_file, "r", errors="replace") as fh:
                fh.seek(0, 2)  # jump to end — only show lines written after this start
                while True:
                    line = fh.readline()
                    if line:
                        line = line.rstrip()
                        self.log_buffer.append(line)
                        await self._bus.publish(LogLine(script=self.name, line=line))
                    else:
                        await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("%s tailer error: %s", self.name, exc)


# ---------------------------------------------------------------------------
# Dashboard socket server — accepts TUI clients
# ---------------------------------------------------------------------------

class DashboardServer:
    """
    Listens on a Unix socket. For each connected client:
    - sends a full Snapshot on connect
    - fans out StateMsg and LogLine updates in real time
    - dispatches Command messages back to the appropriate ScriptRunner
    """

    def __init__(self, runners: dict, bus: BroadcastBus) -> None:
        self._runners: dict[str, ScriptRunner] = runners
        self._bus = bus
        self._server = None

    async def start(self) -> None:
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()
        SOCKET_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(SOCKET_PATH)
        )
        # World-readable/writable; restrict further with chgrp + chmod 660 if needed
        os.chmod(SOCKET_PATH, 0o666)
        log.info("Dashboard listening on %s", SOCKET_PATH)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        addr = writer.get_extra_info("peername") or "<unix>"
        log.info("Client connected (%s)", addr)

        q = self._bus.subscribe()

        # Send full snapshot immediately so the TUI can populate on connect
        snapshot = Snapshot(
            states={name: r.snapshot_state() for name, r in self._runners.items()},
            logs={name: list(r.log_buffer) for name, r in self._runners.items()},
        )
        try:
            writer.write(encode(snapshot))
            await writer.drain()
        except Exception:
            self._bus.unsubscribe(q)
            writer.close()
            return

        # Sender task: push queued broadcast data to this client
        async def _sender() -> None:
            while True:
                data = await q.get()
                try:
                    writer.write(data)
                    await writer.drain()
                except Exception:
                    break

        send_task = asyncio.create_task(_sender(), name=f"sender-{addr}")

        # Read commands from this client
        try:
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                try:
                    msg = decode_msg(raw.decode())
                except (json.JSONDecodeError, ValueError):
                    continue
                if msg.get("type") == "cmd":
                    await self._dispatch(msg)
        except Exception:
            pass
        finally:
            send_task.cancel()
            self._bus.unsubscribe(q)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            log.info("Client disconnected (%s)", addr)

    async def _dispatch(self, msg: dict) -> None:
        name = msg.get("script")
        action = msg.get("action")
        runner = self._runners.get(name)
        if not runner:
            log.warning("Command for unknown script %r", name)
            return
        log.info("Command: %s → %s", action, name)
        if action == "start":
            await runner.start()
        elif action == "stop":
            await runner.stop()
        elif action == "restart":
            await runner.restart()
        else:
            log.warning("Unknown action %r", action)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run_server(cwd: Path) -> None:
    """Create all components, start the server, and run until SIGTERM/SIGINT."""
    bus = BroadcastBus()
    runners = {cfg["name"]: ScriptRunner(cfg, bus, cwd) for cfg in SCRIPTS}
    server = DashboardServer(runners, bus)

    # Heartbeat: push state updates every 10 s so uptime / next_run counters refresh
    async def _heartbeat() -> None:
        while True:
            await asyncio.sleep(10)
            for runner in runners.values():
                if runner.state in ("RUNNING", "WAITING"):
                    await runner._broadcast_state()

    hb_task = asyncio.create_task(_heartbeat(), name="heartbeat")

    await server.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal(sig: signal.Signals) -> None:
        log.info("Received %s — shutting down", sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal, sig)

    log.info("Dashboard server ready.  All scripts are STOPPED — use the TUI to start them.")
    await stop_event.wait()

    log.info("Stopping all script runners …")
    hb_task.cancel()
    await asyncio.gather(*(r.stop() for r in runners.values()), return_exceptions=True)
    await server.stop()
    log.info("Shutdown complete.")
