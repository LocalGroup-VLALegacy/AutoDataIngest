"""
AutoDataIngest dashboard TUI client.

Connects to the dashboard server daemon via Unix socket and renders a live
status panel + log viewer using Textual.

Usage:
    python -m dashboard.client
"""

import asyncio
import json
import logging
from typing import Optional

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Footer, Header, RichLog, Static, TabbedContent, TabPane

from .config import SCRIPTS, SOCKET_PATH
from .protocol import Command, encode

log = logging.getLogger(__name__)

SCRIPT_NAMES = [cfg["name"] for cfg in SCRIPTS]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    return f"{s // 3600}h {(s % 3600) // 60:02d}m"


# ---------------------------------------------------------------------------
# ScriptPanel widget
# ---------------------------------------------------------------------------

class ScriptPanel(Static):
    """Status card displayed for one pipeline script."""

    DEFAULT_CSS = """
    ScriptPanel {
        width: 1fr;
        height: 11;
        border: round $text-muted;
        padding: 1 2;
        margin: 0 1;
        content-align: left top;
    }
    ScriptPanel.running  { border: round green; }
    ScriptPanel.crashed  { border: round red; }
    ScriptPanel.waiting  { border: round yellow; }
    ScriptPanel.stopping { border: round yellow; }
    ScriptPanel.selected { border: double white; }
    """

    _STATE_COLOR = {
        "RUNNING":  "green",
        "STOPPED":  "dim white",
        "CRASHED":  "bold red",
        "WAITING":  "yellow",
        "STOPPING": "yellow",
    }
    _STATE_ICON = {
        "RUNNING":  "●",
        "STOPPED":  "○",
        "CRASHED":  "✕",
        "WAITING":  "⏱",
        "STOPPING": "◌",
    }

    def __init__(self, name: str, label: str, **kwargs) -> None:
        super().__init__("", id=f"panel-{name}", **kwargs)
        self.script_name = name
        self.label_text = label
        self._data: dict = {
            "state": "STOPPED",
            "pid": None,
            "uptime_s": None,
            "exit_code": None,
            "next_run_s": None,
        }

    def on_mount(self) -> None:
        self.update(self._render())

    def set_data(self, data: dict) -> None:
        self._data.update(data)
        state_cls = data.get("state", "STOPPED").lower()
        for cls in ("running", "crashed", "waiting", "stopping", "stopped"):
            self.remove_class(cls)
        self.add_class(state_cls)
        self.update(self._render())

    def _render(self) -> str:
        state = self._data.get("state", "STOPPED")
        color = self._STATE_COLOR.get(state, "white")
        icon = self._STATE_ICON.get(state, "?")

        lines = [
            f"[bold]{self.label_text}[/bold]",
            f"[{color}]{icon} {state}[/{color}]",
        ]
        if self._data.get("pid"):
            lines.append(f"PID: {self._data['pid']}")
        if self._data.get("uptime_s") is not None:
            lines.append(f"Up:   {_fmt_duration(self._data['uptime_s'])}")
        if self._data.get("next_run_s") is not None:
            lines.append(f"Next: {_fmt_duration(self._data['next_run_s'])}")
        ec = self._data.get("exit_code")
        if ec is not None and state in ("STOPPED", "CRASHED"):
            ec_color = "red" if ec != 0 else "dim"
            lines.append(f"Exit: [{ec_color}]{ec}[/{ec_color}]")

        return "\n".join(lines)

    def on_click(self) -> None:
        self.app.select_script(self.script_name)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

class DashboardApp(App):
    """VLAXL AutoDataIngest process dashboard."""

    TITLE = "VLAXL AutoDataIngest Dashboard"

    CSS = """
    Screen {
        layout: vertical;
    }
    #panels {
        height: auto;
        padding: 1 0;
    }
    #logs {
        height: 1fr;
    }
    RichLog {
        scrollbar-gutter: stable;
    }
    """

    BINDINGS = [
        Binding("1", "select_script('ingest')",         "ingest",         show=False),
        Binding("2", "select_script('job_completion')",  "job_completion", show=False),
        Binding("3", "select_script('restarts')",        "restarts",       show=False),
        Binding("f1", "control('start')",    "Start"),
        Binding("f2", "control('stop')",     "Stop"),
        Binding("f3", "control('restart')",  "Restart"),
        Binding("q",  "quit",                "Quit"),
    ]

    # Reactive: currently selected script (determines which F1/F2/F3 targets)
    selected_script: reactive[str] = reactive(SCRIPT_NAMES[0], init=False)
    connected: reactive[bool] = reactive(False, init=False)

    # ------------------------------------------------------------------ #

    def __init__(self) -> None:
        super().__init__()
        self._writer: Optional[asyncio.StreamWriter] = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="panels"):
            for cfg in SCRIPTS:
                yield ScriptPanel(cfg["name"], cfg["label"])
        with TabbedContent(id="logs"):
            for cfg in SCRIPTS:
                with TabPane(cfg["label"], id=f"tab-{cfg['name']}"):
                    yield RichLog(
                        id=f"log-{cfg['name']}",
                        highlight=True,
                        markup=True,
                        wrap=False,
                    )
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = "Connecting …"
        # Mark the default selected panel
        try:
            self.query_one(f"#panel-{SCRIPT_NAMES[0]}").add_class("selected")
        except Exception:
            pass
        self._connect()

    # ------------------------------------------------------------------ #
    # Server connection worker
    # ------------------------------------------------------------------ #

    @work(exclusive=True, exit_on_error=False)
    async def _connect(self) -> None:
        """Background worker: connect to daemon, relay messages, retry on disconnect."""
        while True:
            try:
                reader, writer = await asyncio.open_unix_connection(str(SOCKET_PATH))
                self._writer = writer
                self.post_message(self.Connected())
                async for raw in reader:
                    if not raw:
                        break
                    try:
                        data = json.loads(raw.decode())
                    except json.JSONDecodeError:
                        continue
                    self.post_message(self.ServerData(data))
            except (FileNotFoundError, ConnectionRefusedError, OSError):
                pass
            except Exception as exc:
                log.debug("Socket error: %s", exc)
            finally:
                self._writer = None
                self.post_message(self.Disconnected())
            await asyncio.sleep(5)

    # ------------------------------------------------------------------ #
    # Custom messages (posted by the worker, handled on the UI event loop)
    # ------------------------------------------------------------------ #

    class Connected(Message):
        pass

    class Disconnected(Message):
        pass

    class ServerData(Message):
        def __init__(self, data: dict) -> None:
            super().__init__()
            self.data = data

    # ------------------------------------------------------------------ #
    # Message handlers
    # ------------------------------------------------------------------ #

    def on_dashboard_app_connected(self, _: "DashboardApp.Connected") -> None:
        self.connected = True

    def on_dashboard_app_disconnected(self, _: "DashboardApp.Disconnected") -> None:
        self.connected = False

    def on_dashboard_app_server_data(self, msg: "DashboardApp.ServerData") -> None:
        data = msg.data
        t = data.get("type")

        if t == "snapshot":
            for name, state in data.get("states", {}).items():
                self._update_panel(name, state)
            for name, lines in data.get("logs", {}).items():
                log_widget = self.query_one(f"#log-{name}", RichLog)
                for line in lines:
                    log_widget.write(line)

        elif t == "state":
            name = data.get("script")
            if name:
                self._update_panel(name, data)

        elif t == "log":
            name = data.get("script")
            line = data.get("line", "")
            if name:
                try:
                    self.query_one(f"#log-{name}", RichLog).write(line)
                except Exception:
                    pass

    def _update_panel(self, name: str, data: dict) -> None:
        try:
            self.query_one(f"#panel-{name}", ScriptPanel).set_data(data)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Reactive watchers
    # ------------------------------------------------------------------ #

    def watch_selected_script(self, old: str, new: str) -> None:
        try:
            if old:
                self.query_one(f"#panel-{old}").remove_class("selected")
            self.query_one(f"#panel-{new}").add_class("selected")
        except Exception:
            pass

    def watch_connected(self, connected: bool) -> None:
        self.sub_title = "Connected" if connected else "Not connected — retrying in 5 s …"

    # ------------------------------------------------------------------ #
    # Actions (bound to keys via BINDINGS)
    # ------------------------------------------------------------------ #

    def action_select_script(self, name: str) -> None:
        if name in SCRIPT_NAMES:
            self.select_script(name)

    def select_script(self, name: str) -> None:
        """Select a script panel and switch the log tab to match."""
        if name not in SCRIPT_NAMES:
            return
        self.selected_script = name
        try:
            self.query_one(TabbedContent).active = f"tab-{name}"
        except Exception:
            pass

    async def action_control(self, action: str) -> None:
        """Send start/stop/restart for the currently selected script."""
        if self._writer is None:
            self.notify("Not connected to daemon", severity="warning")
            return
        try:
            self._writer.write(encode(Command(script=self.selected_script, action=action)))
            await self._writer.drain()
        except Exception:
            self._writer = None
            self.connected = False
            self.notify("Lost connection to daemon", severity="error")


def main() -> None:
    DashboardApp().run()


if __name__ == "__main__":
    main()
