"""Regression tests for agent/brain.py — the MCP-host loop behind Auto Heal / HITL.

Two bugs broke "Auto Heal" in the dashboard:

  Bug A — stale MCP session.
    `HealerSession.list_tools()` (the first thing run_agent calls) had no
    reconnect logic, unlike `.call()`. After the MCP server was ever killed and
    restarted, the dashboard's long-lived agent_session held a dead client, so
    list_tools() failed with "All connection attempts failed" and the heal
    aborted before any Temporal workflow was created.

  Bug B — invalid assistant-message replay.
    run_agent replayed the assistant turn with `b.model_dump()`, which includes
    SDK response-only fields (e.g. parsed_output). The Messages API rejects them:
      400 messages.N.content.0.text.parsed_output: Extra inputs are not permitted
    So even when a workflow was created, the agent crashed a few rounds later.

These tests pin both fixes.
"""

import asyncio
from types import SimpleNamespace

import httpx
import pytest

import agent.brain as brain
from agent.brain import HealerSession, _is_disconnect


# ── disconnect classification (drives reconnect) ──────────────────────────────


def test_is_disconnect_recognizes_session_terminated():
    # FastMCP raises "Session terminated" when the MCP server died/restarted; this
    # MUST be treated as a disconnect so list_tools()/call() reconnect instead of
    # aborting the heal. (Regression: this string was previously unrecognized.)
    assert _is_disconnect(RuntimeError("Session terminated")) is True
    assert _is_disconnect(Exception("session was terminated by peer")) is True


def test_is_disconnect_ignores_genuine_tool_errors():
    assert _is_disconnect(ValueError("invalid namespace 'foo'")) is False


# ── Bug B: assistant content must be re-serialized to API-valid input blocks ──


def test_assistant_content_keeps_only_input_fields():
    # A response text block carries SDK-only fields the API won't accept back.
    text = SimpleNamespace(type="text", text="Healing started.",
                           parsed_output={"foo": 1}, citations=None)
    tool = SimpleNamespace(type="tool_use", id="tu_1", name="heal_cluster",
                           input={"namespace": "default"})

    blocks = brain._assistant_content([text, tool])

    assert blocks == [
        {"type": "text", "text": "Healing started."},
        {"type": "tool_use", "id": "tu_1", "name": "heal_cluster",
         "input": {"namespace": "default"}},
    ]
    # The crux: no response-only keys leak into the replayed message.
    assert "parsed_output" not in blocks[0]
    assert "citations" not in blocks[0]


def test_assistant_content_handles_empty_tool_input():
    tool = SimpleNamespace(type="tool_use", id="tu_2", name="list_pods", input=None)
    blocks = brain._assistant_content([tool])
    assert blocks == [{"type": "tool_use", "id": "tu_2", "name": "list_pods", "input": {}}]


# ── Bug A: list_tools must reconnect when the cached client went stale ─────────


class _FakeClient:
    """Stand-in for fastmcp.Client. `healthy=False` simulates a dead connection."""

    def __init__(self, url=None, *, healthy=True):
        self.url = url
        self.healthy = healthy

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def list_tools(self):
        if not self.healthy:
            raise httpx.ConnectError("All connection attempts failed")
        return [SimpleNamespace(name="list_pods", description="", inputSchema={})]


async def test_list_tools_reconnects_after_stale_client(monkeypatch):
    session = HealerSession("http://127.0.0.1:8000/mcp")
    # Poison the session with a cached-but-dead client (post-restart scenario).
    session._client = _FakeClient(healthy=False)
    # Any reconnect produces a healthy client.
    monkeypatch.setattr(brain, "Client", lambda url: _FakeClient(url, healthy=True))

    tools = await asyncio.wait_for(session.list_tools(), timeout=5)

    assert [t.name for t in tools] == ["list_pods"]


async def test_list_tools_propagates_non_disconnect_errors(monkeypatch):
    """A genuine logic error must NOT be retried forever — it should surface."""
    class _BadClient(_FakeClient):
        async def list_tools(self):
            raise ValueError("schema boom")  # not a disconnect

    session = HealerSession("http://127.0.0.1:8000/mcp")
    monkeypatch.setattr(brain, "Client", lambda url: _BadClient(url))

    with pytest.raises(ValueError, match="schema boom"):
        await asyncio.wait_for(session.list_tools(), timeout=5)
