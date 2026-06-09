"""Tests for the dashboard's MCP auto-detect wiring.

Decision under test: the dashboard attaches to an MCP already running in its own
terminal (external mode) and only spawns its own child when none is reachable. In
external mode the "Break/Restart" buttons must NOT touch a process — you break MCP
by Ctrl-C'ing its terminal.

These import dashboard.gateway, which is only safe because agent.brain imports
fastmcp lazily (otherwise beartype's import hook breaks the workflow sandbox in a
shared test process — see test_agent_brain.py).
"""

from types import SimpleNamespace

import dashboard.gateway as gw
import dashboard.supervisor as supervisor


# ── mcp_reachable probe drives the supervise/attach decision ──────────────────


class _FakeAsyncClient:
    def __init__(self, *_a, ok=True, **_k):
        self._ok = ok

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def get(self, _url):
        if not self._ok:
            raise OSError("connection refused")  # nothing listening
        return SimpleNamespace(status_code=406)  # MCP rejects bare GET, but it's UP


async def test_mcp_reachable_true_when_server_answers(monkeypatch):
    monkeypatch.setattr(supervisor.httpx, "AsyncClient",
                        lambda *a, **k: _FakeAsyncClient(ok=True))
    assert await supervisor.mcp_reachable("http://127.0.0.1:8000/mcp") is True


async def test_mcp_reachable_false_when_nothing_listening(monkeypatch):
    monkeypatch.setattr(supervisor.httpx, "AsyncClient",
                        lambda *a, **k: _FakeAsyncClient(ok=False))
    assert await supervisor.mcp_reachable("http://127.0.0.1:8000/mcp") is False


# ── break/restart respect the mode ────────────────────────────────────────────


class _RecordingHub:
    def __init__(self):
        self.events = []

    def emitter(self):
        async def emit(ev):
            self.events.append(ev)
        return emit

    async def broadcast(self, ev):
        self.events.append(ev)


class _SpySupervisor:
    def __init__(self):
        self.killed = 0
        self.started = 0

    def kill(self):
        self.killed += 1

    def start(self):
        self.started += 1


def _setup(mcp_external: bool):
    gw.ctx.hub = _RecordingHub()
    gw.ctx.supervisor = _SpySupervisor()
    gw.ctx.mcp_external = mcp_external
    return gw.ctx


async def test_break_server_kills_regardless_of_mode():
    # The Break button must work whether MCP is our child (supervised) or a
    # separate `make mcp` process (external) — supervisor.kill() handles both.
    for external in (True, False):
        ctx = _setup(mcp_external=external)
        await gw.handle_command({"action": "break_server"})
        assert ctx.supervisor.killed == 1, f"external={external}"
        alive = [e for e in ctx.hub.events if e.get("type") == "mcp_health"]
        assert alive and alive[-1]["alive"] is False


async def test_restart_server_starts_child_and_takes_ownership():
    ctx = _setup(mcp_external=True)
    await gw.handle_command({"action": "restart_server"})

    assert ctx.supervisor.started == 1       # brings MCP back as our child
    assert ctx.mcp_external is False          # we now own it
    modes = [e for e in ctx.hub.events if e.get("type") == "mcp_mode"]
    assert modes and modes[-1]["external"] is False
