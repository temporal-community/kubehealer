#!/usr/bin/env python3
"""KubeHealer agent brain — a thin MCP client that heals a cluster, durably.

Connects to the KubeHealer MCP server over HTTP and drives the healing flow:
list pods -> start the durable heal_cluster Task -> watch diagnoses ->
approve/reject each fix -> report the result.

Everything the brain does is emitted as structured events through an async `emit`
callback, so the same core powers both the terminal CLI and the live web dashboard
(`dashboard/`). Event shapes (discriminated by "type"):
  log {level,msg} · agent {delta?|text?,done?} · tool_call {id,name,args} ·
  tool_result {id,name,ok,summary} · task {task_id,status} · mcp_health {alive} ·
  result {results}

Modes:
  * agent     an LLM (Claude) decides what to do via the tools (auto-approves).
  * hitl      the LLM investigates + starts the heal, then STOPS for a human to
              approve/reject (used by the dashboard's approval buttons).
  * scripted  deterministic orchestrator, no LLM/API key (smoke test + safety net).

Crash recovery is built in: every MCP call auto-reconnects if the server dies, and
because heal_cluster is idempotent (deterministic Temporal Workflow ID) the brain
re-attaches to the still-running workflow and finishes — no lost steps.

    python agent/brain.py                 # LLM agent, namespace=default
    python agent/brain.py --mode scripted
"""

import argparse
import asyncio
import itertools
import json
import os
import sys
from typing import Awaitable, Callable

import httpx
from dotenv import load_dotenv

load_dotenv()

# fastmcp is imported lazily (see _client_cls) so that merely importing this module
# — e.g. in unit tests — does not pull in fastmcp's beartype.claw global import hook,
# which conflicts with Temporal's workflow sandbox when both run in one test process.
Client = None  # populated on first real connection; monkeypatched in tests


def _client_cls():
    global Client
    if Client is None:
        from fastmcp import Client as _Client
        Client = _Client
    return Client


DEFAULT_SERVER_URL = os.environ.get("KUBEHEALER_MCP_URL", "http://127.0.0.1:8000/mcp")
MODEL = os.environ.get("KUBEHEALER_MODEL", "claude-sonnet-4-6")
MAX_ROUNDS = 40

Emit = Callable[[dict], Awaitable[None]]
_ids = itertools.count(1)

# Errors that mean "the MCP server is gone" (vs. a tool-level error). We reconnect
# on these; the Temporal workflow is unaffected.
DOWN_ERRORS = (ConnectionError, OSError, httpx.HTTPError, EOFError)
# FastMCP wraps connect-time failures in a generic RuntimeError, so we also sniff
# messages to tell "server is down" apart from a genuine tool/logic error.
_DISCONNECT_HINTS = ("connect", "closed", "disconnect", "peer", "refused", "reset",
                     "eof", "terminated", "session")


class ServerDown(Exception):
    """Raised when the MCP server can't be reached, so callers can reconnect."""


def _is_disconnect(exc: Exception) -> bool:
    if isinstance(exc, (ServerDown, *DOWN_ERRORS)):
        return True
    msg = str(exc).lower()
    return any(hint in msg for hint in _DISCONNECT_HINTS)


AGENT_SYSTEM_PROMPT = """You are KubeHealer, an SRE agent that fixes Kubernetes clusters using MCP tools.

Tools:
- list_pods: see pod health (read-only).
- heal_cluster: LONG-RUNNING, durable. Starts a healing workflow and returns a task. It scans, diagnoses each unhealthy pod with AI, then PAUSES for approval.
- get_healing_status: the live state — phase, diagnoses, and which pods still need a decision. Poll it.
- approve_fix / reject_fix: decide a pod. Approve fixable actions; REJECT any pod whose action is "skip" (e.g. a missing ConfigMap that can't be auto-fixed).

Procedure:
1. list_pods to see the situation.
2. heal_cluster to begin (it runs in the background).
3. Poll get_healing_status. When phase is "awaiting_approval", decide EVERY diagnosed pod (approve fixable, reject "skip").
4. Poll until phase is "done", then give a short summary of what was fixed.

Keep messages short — this is a live demo."""

HITL_SYSTEM_PROMPT = """You are KubeHealer, an SRE agent that fixes Kubernetes clusters using MCP tools.

Tools: list_pods (read-only), heal_cluster (LONG-RUNNING durable task: scans + AI-diagnoses, then pauses for human approval), get_healing_status (live phase + diagnoses).

Procedure:
1. list_pods to see the situation.
2. heal_cluster to begin.
3. Poll get_healing_status until phase is "awaiting_approval".
4. Present the proposed fixes clearly and recommend a decision for each pod, then STOP.
   A HUMAN OPERATOR will click approve/reject — do NOT call approve_fix or reject_fix yourself.

Keep messages short — this is a live demo."""


# ── stdout emitter for the CLI ────────────────────────────────────────────────


def cli_emitter() -> Emit:
    state = {"streaming": False}

    async def emit(ev: dict) -> None:
        t = ev.get("type")
        if t == "agent":
            if ev.get("delta"):
                if not state["streaming"]:
                    print("\n🤖 ", end="", flush=True)
                    state["streaming"] = True
                print(ev["delta"], end="", flush=True)
            if ev.get("done"):
                state["streaming"] = False
                print(flush=True)
        elif t == "log":
            print(ev["msg"], flush=True)
        elif t == "tool_call":
            print(f"  → {ev['name']}({ev.get('args') or {}})", flush=True)
        elif t == "task":
            print(f"  ▶ heal_cluster Task: {ev.get('task_id')}", flush=True)
        elif t == "mcp_health" and not ev.get("alive"):
            print("\n  ⚠  MCP server unreachable — the Temporal workflow keeps running, reconnecting…", flush=True)
        elif t == "result":
            print("\n=== Healing complete ===", flush=True)
            for r in ev.get("results", []):
                icon = "✓" if r.get("success") else "·"
                print(f"  {icon} {r['pod_name']}: {r['action_taken']} — {r['details']}", flush=True)

    return emit


# ── MCP session with transparent reconnect (emits MCP-plane events) ───────────


class HealerSession:
    """Holds an MCP client connection and re-establishes it if the server dies."""

    def __init__(self, url: str, emit: Emit | None = None):
        self.url = url
        self.emit = emit
        self._client: Client | None = None

    async def _ensure(self) -> None:
        if self._client is None:
            client = _client_cls()(self.url)
            try:
                await client.__aenter__()
            except Exception as e:  # connect failures arrive as RuntimeError, etc.
                raise ServerDown(str(e)) from e
            self._client = client

    async def _drop(self) -> None:
        if self._client is not None:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:
                pass
            self._client = None

    async def _send(self, ev: dict) -> None:
        if self.emit:
            await self.emit(ev)

    async def call(self, name: str, args: dict | None = None, *, task: bool = False):
        """Call a tool, reconnecting forever if the server is unreachable."""
        cid = next(_ids)
        await self._send({"type": "tool_call", "id": cid, "name": name, "args": args or {}})
        announced = False
        while True:
            try:
                await self._ensure()
                res = await self._client.call_tool(name, args or {}, task=task)
                if announced:
                    await self._send({"type": "mcp_health", "alive": True})
                if task:
                    await self._send({"type": "task", "task_id": getattr(res, "task_id", None), "status": "working"})
                    summary = f"task {getattr(res, 'task_id', '?')}"
                else:
                    summary = text_of(res)[:400]
                await self._send({"type": "tool_result", "id": cid, "name": name, "ok": True, "summary": summary})
                return res
            except Exception as e:
                if not _is_disconnect(e):
                    await self._send({"type": "tool_result", "id": cid, "name": name, "ok": False, "summary": str(e)[:200]})
                    raise
                await self._drop()
                if not announced:
                    await self._send({"type": "mcp_health", "alive": False})
                    announced = True
                await asyncio.sleep(2)

    async def list_tools(self):
        """List tools, reconnecting if the cached client went stale.

        Mirrors `call()`: after the MCP server is killed/restarted, the cached
        client is dead. Without this, the very first thing run_agent does would
        fail with "All connection attempts failed" and abort the whole heal.
        """
        announced = False
        while True:
            try:
                await self._ensure()
                tools = await self._client.list_tools()
                if announced:
                    await self._send({"type": "mcp_health", "alive": True})
                return tools
            except Exception as e:
                if not _is_disconnect(e):
                    raise
                await self._drop()
                if not announced:
                    await self._send({"type": "mcp_health", "alive": False})
                    announced = True
                await asyncio.sleep(2)

    async def close(self) -> None:
        await self._drop()


def _assistant_content(blocks) -> list[dict]:
    """Re-serialize a response's content blocks into API-valid *input* blocks.

    The SDK's `block.model_dump()` includes response-only fields (e.g.
    `parsed_output`, `citations`) that the Messages API rejects when the assistant
    turn is replayed on the next round ("Extra inputs are not permitted"). We keep
    only the fields the API accepts as input.
    """
    out: list[dict] = []
    for b in blocks:
        if b.type == "text":
            out.append({"type": "text", "text": b.text})
        elif b.type == "tool_use":
            out.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input or {}})
        # other block types (e.g. thinking) are not replayed
    return out


def text_of(result) -> str:
    """Extract a string payload from a CallToolResult (or task result)."""
    if result is None:
        return ""
    content = getattr(result, "content", None)
    if content:
        parts = [b.text for b in content if getattr(b, "type", None) == "text"]
        if parts:
            return "\n".join(parts)
    structured = getattr(result, "structuredContent", None)
    if structured:
        return json.dumps(structured)
    return str(result)


# ── Scripted orchestrator (no LLM) ────────────────────────────────────────────


async def run_scripted(session: HealerSession, namespace: str, emit: Emit) -> None:
    await emit({"type": "log", "msg": f"● Inspecting namespace '{namespace}'…"})
    await session.call("list_pods", {"namespace": namespace})

    # Remember the run we'd see *before* starting, so we don't mistake a stale
    # completed run (from a previous attempt) for this one.
    before = json.loads(text_of(await session.call("get_healing_status", {"namespace": namespace})))
    prev_run = before.get("run_id")

    await emit({"type": "log", "msg": "● Starting durable heal (SEP-1686 Task ► Temporal workflow)…"})
    heal = await session.call("heal_cluster", {"namespace": namespace}, task=True)

    decided: set[str] = set()
    while True:
        status = json.loads(text_of(await session.call("get_healing_status", {"namespace": namespace})))
        phase, run_id = status.get("phase"), status.get("run_id")

        if run_id is None or run_id == prev_run:  # wait for our fresh run (async start)
            await asyncio.sleep(1.0)
            continue
        if phase in (None, "scanning", "diagnosing", "executing"):
            await asyncio.sleep(1.5)
            continue
        if phase == "awaiting_approval":
            for d in status.get("diagnoses", []):
                pod = d["pod_name"]
                if pod in decided:
                    continue
                decision = "reject_fix" if d["action"] == "skip" else "approve_fix"
                await session.call(decision, {"pod_name": pod, "namespace": namespace})
                decided.add(pod)
            await asyncio.sleep(1.0)
            continue
        if phase == "done":
            await emit({"type": "result", "results": status.get("results", [])})
            break

    try:
        await heal.result()  # SEP-1686 fetch-later (best-effort after a crash)
    except Exception:
        pass


# ── LLM agent loop (async streaming) ──────────────────────────────────────────


async def run_agent(session: HealerSession, namespace: str, emit: Emit,
                    system_prompt: str = AGENT_SYSTEM_PROMPT) -> None:
    try:
        import anthropic
    except ImportError:
        await emit({"type": "log", "level": "error", "msg": "anthropic SDK not installed — use scripted mode"})
        return
    if not os.environ.get("ANTHROPIC_API_KEY"):
        await emit({"type": "log", "level": "error", "msg": "ANTHROPIC_API_KEY not set — use scripted mode"})
        return

    ai = anthropic.AsyncAnthropic()
    tools = await session.list_tools()
    schemas = [
        {"name": t.name, "description": t.description or "", "input_schema": t.inputSchema}
        for t in tools
    ]
    messages = [{
        "role": "user",
        "content": (
            f"Investigate the Kubernetes namespace '{namespace}' and heal any unhealthy "
            "pods following your procedure. Start now by using your tools."
        ),
    }]

    heal_started = False
    for _ in range(MAX_ROUNDS):
        # Force a tool until the heal is actually running, so the agent never just
        # chats and stops before doing the work (flaky on stage). Once the heal is
        # going, allow free choice so it can present/summarize and finish.
        tool_choice = {"type": "auto"} if heal_started else {"type": "any"}
        async with ai.messages.stream(
            model=MODEL, max_tokens=2048, system=system_prompt, tools=schemas,
            messages=messages, tool_choice=tool_choice,
        ) as stream:
            async for text in stream.text_stream:
                await emit({"type": "agent", "delta": text})
            final = await stream.get_final_message()
        await emit({"type": "agent", "done": True})

        if final.stop_reason != "tool_use":
            break

        messages.append({"role": "assistant", "content": _assistant_content(final.content)})
        tool_results = []
        for block in final.content:
            if block.type != "tool_use":
                continue
            name, args = block.name, (block.input or {})
            if name == "heal_cluster":
                heal_started = True
                heal = await session.call("heal_cluster", args, task=True)
                out = (
                    f"Healing started as durable Task '{getattr(heal, 'task_id', '?')}', backed by a "
                    "Temporal workflow. Poll get_healing_status, then decide each pod."
                )
            else:
                out = text_of(await session.call(name, args))
                if name == "get_healing_status":
                    try:
                        if json.loads(out).get("phase") in ("scanning", "diagnosing", "executing", "not_started"):
                            await asyncio.sleep(1.5)  # pace polling for a live demo
                    except Exception:
                        pass
            tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": out})

        messages.append({"role": "user", "content": tool_results})


# ── Entry point ───────────────────────────────────────────────────────────────


async def amain() -> None:
    parser = argparse.ArgumentParser(description="KubeHealer agent brain (MCP host)")
    parser.add_argument("--mode", choices=["agent", "scripted"], default="agent")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--server-url", default=DEFAULT_SERVER_URL)
    args = parser.parse_args()

    emit = cli_emitter()
    session = HealerSession(args.server_url, emit=emit)
    print(f"KubeHealer brain → {args.server_url}  (mode={args.mode})")
    try:
        if args.mode == "scripted":
            await run_scripted(session, args.namespace, emit)
        else:
            await run_agent(session, args.namespace, emit)
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(amain())
