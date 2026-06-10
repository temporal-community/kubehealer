"""FastAPI gateway: one WebSocket streams the demo; commands drive it from the browser.

Run via `python run_dashboard.py`. Serves the built UI (ui/dist) on the same origin.
"""

import asyncio
import contextlib
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from temporalio.client import Client

from agent.brain import (
    AGENT_SYSTEM_PROMPT,
    HITL_SYSTEM_PROMPT,
    HealerSession,
    run_agent,
    run_scripted,
)
from dashboard.hub import Hub
from dashboard.sources import mcp_health_pinger, pod_poller, temporal_poller
from dashboard.supervisor import McpServerSupervisor, mcp_reachable
from mcp_server.temporal_client import heal_workflow_id

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = os.environ.get("KUBEHEALER_NS", "default")
MCP_PORT = int(os.environ.get("KUBEHEALER_MCP_PORT", "8000"))
MCP_URL = f"http://127.0.0.1:{MCP_PORT}/mcp"
TEMPORAL_TARGET = os.environ.get("TEMPORAL_TARGET", "localhost:7233")
UI_DIST = PROJECT_ROOT / "ui" / "dist"


class Ctx:
    hub: Hub
    supervisor: McpServerSupervisor
    mcp_external: bool = False      # True = MCP runs in its own terminal (we don't manage it)
    temporal: Client
    agent_session: HealerSession  # used by the heal/agent task
    cmd_session: HealerSession     # used by approve/reject buttons
    heal_task: asyncio.Task | None = None
    bg: list[asyncio.Task] = []


ctx = Ctx()


async def _run_chaos(args: list[str], note: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        *args, cwd=str(PROJECT_ROOT),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    await ctx.hub.broadcast({"type": "log", "level": "info", "msg": note})


async def handle_command(cmd: dict) -> None:
    action = cmd.get("action")
    ns = cmd.get("namespace", NAMESPACE)
    emit = ctx.hub.emitter()

    if action == "start_heal":
        if ctx.heal_task and not ctx.heal_task.done():
            await emit({"type": "log", "level": "warn", "msg": "A heal is already running."})
            return
        mode = cmd.get("mode", "hitl")
        await emit({"type": "log", "level": "info", "msg": f"▶ start_heal (mode={mode})"})

        async def _job():
            try:
                if mode == "scripted":
                    await run_scripted(ctx.agent_session, ns, emit)
                else:
                    prompt = HITL_SYSTEM_PROMPT if mode == "hitl" else AGENT_SYSTEM_PROMPT
                    await run_agent(ctx.agent_session, ns, emit, system_prompt=prompt)
            except Exception as e:  # never let the demo die on a stray error
                await emit({"type": "log", "level": "error", "msg": f"heal task: {e}"})

        ctx.heal_task = asyncio.create_task(_job())

    elif action in ("approve", "reject"):
        tool = "approve_fix" if action == "approve" else "reject_fix"
        await ctx.cmd_session.call(tool, {"pod_name": cmd["pod"], "namespace": ns})

    elif action == "inject_chaos":
        await _run_chaos(["kubectl", "apply", "-f", "chaos/"], "💥 chaos injected")

    elif action == "reset_cluster":
        # Truly reset: stop any in-flight heal so the NEXT heal scans the live pods
        # (otherwise USE_EXISTING re-attaches to a stale run with old pod names).
        try:
            await ctx.temporal.get_workflow_handle(heal_workflow_id(ns)).terminate("dashboard reset")
        except Exception:
            pass  # nothing running / already closed
        await _run_chaos(["kubectl", "delete", "-f", "chaos/", "--ignore-not-found"], "cluster reset")
        await _run_chaos(["kubectl", "apply", "-f", "chaos/"], "chaos redeployed")
        # Clear stale diagnoses/history from the UI so the drawer starts empty.
        await emit({"type": "workflow", "phase": "not_started", "run_id": None})
        await emit({"type": "history", "reset": True, "events": []})

    elif action == "break_server":
        # Works whether MCP is our child or a separate `make mcp` process.
        ctx.supervisor.kill()
        await emit({"type": "mcp_health", "alive": False})
        await emit({"type": "log", "level": "alert", "msg": "💥 MCP server KILLED — Temporal keeps running"})

    elif action == "restart_server":
        ctx.supervisor.start()  # bring it back as our child
        ctx.mcp_external = False  # we own it now
        await emit({"type": "mcp_mode", "external": False})
        await emit({"type": "log", "level": "info", "msg": "MCP server restarting…"})


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    ctx.hub = Hub()
    ctx.supervisor = McpServerSupervisor(port=MCP_PORT)
    # Auto-detect: attach to an MCP already running in its own terminal (the
    # killable one). Only spawn our own child if nothing is reachable.
    ctx.mcp_external = await mcp_reachable(MCP_URL)
    if ctx.mcp_external:
        print(f"  MCP detected at {MCP_URL} — attaching (break it from its own terminal)")
    else:
        print(f"  No MCP at {MCP_URL} — supervising our own child (Break/Restart buttons active)")
        ctx.supervisor.start()
    for attempt in range(30):  # tolerate Temporal still starting (compose)
        try:
            ctx.temporal = await Client.connect(TEMPORAL_TARGET)
            break
        except Exception as e:
            print(f"  waiting for Temporal at {TEMPORAL_TARGET} ({attempt + 1}/30): {e}")
            await asyncio.sleep(2)
    emit = ctx.hub.emitter()
    ctx.agent_session = HealerSession(MCP_URL, emit=emit)
    ctx.cmd_session = HealerSession(MCP_URL, emit=emit)
    ctx.bg = [
        asyncio.create_task(pod_poller(ctx.hub, NAMESPACE)),
        asyncio.create_task(temporal_poller(ctx.hub, ctx.temporal, NAMESPACE)),
        asyncio.create_task(mcp_health_pinger(ctx.hub, MCP_URL)),
    ]
    # Tell the UI which mode we're in so it shows Break/Restart buttons (supervised)
    # or a "Ctrl-C its terminal" hint (external).
    await ctx.hub.broadcast({"type": "mcp_mode", "external": ctx.mcp_external})
    try:
        yield
    finally:
        for t in ctx.bg:
            t.cancel()
        if ctx.heal_task:
            ctx.heal_task.cancel()
        await ctx.agent_session.close()
        await ctx.cmd_session.close()
        if not ctx.mcp_external:  # never kill an MCP we don't own
            ctx.supervisor.kill()


app = FastAPI(title="KubeHealer Mission Control", lifespan=lifespan)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await websocket.accept()
    q = ctx.hub.subscribe()
    for ev in ctx.hub.snapshot():  # bring a fresh client up to current state
        await websocket.send_json(ev)

    async def pump() -> None:
        while True:
            await websocket.send_json(await q.get())

    async def recv() -> None:
        while True:
            cmd = await websocket.receive_json()
            await handle_command(cmd)

    pump_t = asyncio.create_task(pump())
    recv_t = asyncio.create_task(recv())
    try:
        await asyncio.wait({pump_t, recv_t}, return_when=asyncio.FIRST_COMPLETED)
    except WebSocketDisconnect:
        pass
    finally:
        pump_t.cancel()
        recv_t.cancel()
        ctx.hub.unsubscribe(q)


if UI_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(UI_DIST), html=True), name="ui")
