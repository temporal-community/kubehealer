"""Supervise the MCP server as a child process so the UI can break/restart it.

The dashboard *auto-detects*: if an MCP server is already running (its own
terminal — the killable one), it attaches to that and never spawns a child. Only
when none is reachable does it supervise its own. `mcp_reachable()` is the probe
that decision is built on.
"""

import os
import signal
import subprocess
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent


async def mcp_reachable(url: str, timeout: float = 1.5) -> bool:
    """True if an MCP server answers at `url` (any HTTP status ⇒ a process is up).

    A streamable-HTTP MCP endpoint rejects a bare GET (e.g. 406/400), but that
    still proves a process is listening — which is all we need to decide whether
    to spawn our own. Only a transport-level failure means "nothing there".
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as http:
            await http.get(url)
        return True
    except Exception:
        return False


class McpServerSupervisor:
    def __init__(self, port: int = 8000) -> None:
        self.port = port
        self._proc: subprocess.Popen | None = None

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self) -> None:
        if self.is_alive():
            return
        # Same interpreter (the venv), inherit env (ANTHROPIC_API_KEY, etc.).
        self._proc = subprocess.Popen(
            [sys.executable, "run_mcp_server.py", "--transport", "http",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(PROJECT_ROOT),
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _pid_on_port(self) -> int | None:
        """PID of whatever is LISTENing on our MCP port (a separately-launched
        `make mcp` we don't own), or None."""
        try:
            out = subprocess.run(
                ["lsof", "-ti", f"tcp:{self.port}", "-sTCP:LISTEN"],
                capture_output=True, text=True, timeout=3,
            )
            pids = [int(x) for x in out.stdout.split()]
            return pids[0] if pids else None
        except Exception:
            return None

    def kill(self) -> None:
        """The on-stage crash: hard SIGKILL, no cleanup — exactly like a real crash.

        Kills our child if we spawned one; otherwise kills whatever process is
        serving the MCP port (an externally-launched `make mcp`). So the in-GUI
        Break button works regardless of how the MCP server was started.
        """
        if self.is_alive():
            try:
                self._proc.send_signal(signal.SIGKILL)
            except ProcessLookupError:
                pass
            self._proc = None
            return
        pid = self._pid_on_port()
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    def restart(self) -> None:
        self.kill()
        self.start()
