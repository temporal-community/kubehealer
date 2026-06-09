"""Background producers that stream the live demo state into the Hub.

Durable plane (independent of the MCP server):
  * pod_poller       — Kubernetes pod health every ~1s
  * temporal_poller  — HealerWorkflow phase + diagnoses + event-history deltas
Fragile plane:
  * mcp_health_pinger — is the MCP server process answering?
"""

import asyncio

import httpx
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError

from activities.k8s_activities import v1
from mcp_server.temporal_client import heal_workflow_id

POLL_SECONDS = 1.0


# ── Kubernetes ────────────────────────────────────────────────────────────────


def _pod_snapshot(namespace: str) -> list[dict]:
    pods = v1.list_namespaced_pod(namespace=namespace)
    out = []
    for pod in pods.items:
        phase = pod.status.phase or "Unknown"
        ready_count = total = restarts = 0
        if pod.status.container_statuses:
            total = len(pod.status.container_statuses)
            ready_count = sum(1 for cs in pod.status.container_statuses if cs.ready)
            restarts = sum(cs.restart_count for cs in pod.status.container_statuses)
            for cs in pod.status.container_statuses:
                if cs.state and cs.state.waiting and cs.state.waiting.reason:
                    phase = cs.state.waiting.reason
                    break
                if cs.state and cs.state.terminated and cs.state.terminated.reason:
                    phase = cs.state.terminated.reason
                    break
        healthy = phase == "Running" and total > 0 and ready_count == total
        out.append({
            "name": pod.metadata.name,
            "app": (pod.metadata.labels or {}).get("app", pod.metadata.name.rsplit("-", 2)[0]),
            "status": phase,
            "ready": f"{ready_count}/{total}",
            "restarts": restarts,
            "healthy": healthy,
        })
    out.sort(key=lambda p: p["name"])
    return out


async def pod_poller(hub, namespace: str = "default") -> None:
    while True:
        try:
            pods = await asyncio.to_thread(_pod_snapshot, namespace)
            await hub.broadcast({"type": "pods", "pods": pods})
        except Exception as e:
            await hub.broadcast({"type": "log", "level": "warn", "msg": f"pod poll: {e}"})
        await asyncio.sleep(POLL_SECONDS)


# ── Temporal (durable plane) ──────────────────────────────────────────────────


async def _resolve_run(client: Client, namespace: str):
    """Resolve the *currently running* heal for this namespace, or None.

    Only a RUNNING run is actionable. A closed run (completed/terminated/failed)
    must NOT be surfaced: a heal terminated while awaiting approval keeps its frozen
    `awaiting_approval` state, which would otherwise re-populate the drawer with
    phantom Approve/Reject buttons for pods that no longer exist.

    RPCError is intentionally allowed to propagate so an unreachable Temporal
    surfaces as DOWN (handled by the caller) rather than as a false not_started.
    """
    wid = heal_workflow_id(namespace)
    async for wf in client.list_workflows(f"WorkflowId = '{wid}'"):
        if wf.status == WorkflowExecutionStatus.RUNNING:
            return client.get_workflow_handle(wid, run_id=wf.run_id)
    return None


def _event_label(ev) -> str:
    name = EventType.Name(ev.event_type).replace("EVENT_TYPE_", "")
    return name.replace("_", " ").title()


async def temporal_poller(hub, client: Client, namespace: str = "default") -> None:
    seen_run: str | None = None
    last_event_id = 0
    while True:
        try:
            handle = await _resolve_run(client, namespace)
            if handle is None:
                # Reaching here means the visibility query succeeded → Temporal is up,
                # there's just no heal workflow yet. Report LIVE so the badge is honest
                # before the first heal (otherwise it sticks on DOWN).
                await hub.broadcast({"type": "temporal_health", "alive": True})
                await hub.broadcast({"type": "workflow", "phase": "not_started", "run_id": None})
                await asyncio.sleep(POLL_SECONDS)
                continue

            if handle.run_id != seen_run:  # new run → reset the history feed
                seen_run, last_event_id = handle.run_id, 0
                await hub.broadcast({"type": "history", "reset": True, "events": []})

            try:
                state = await handle.query("get_state")
                decided = set(state.get("decisions", {}))
                state["run_id"] = handle.run_id
                state["needs_approval"] = state.get("phase") == "awaiting_approval"
                state["undecided_pods"] = [
                    d["pod_name"] for d in state.get("diagnoses", []) if d["pod_name"] not in decided
                ]
                state["type"] = "workflow"
                await hub.broadcast(state)
            except RPCError:
                pass  # completed runs can't always be queried; history still flows

            new_events = []
            async for ev in handle.fetch_history_events():
                if ev.event_id > last_event_id:
                    last_event_id = ev.event_id
                    new_events.append({
                        "id": ev.event_id,
                        "label": _event_label(ev),
                        "ts": ev.event_time.ToDatetime().isoformat() if ev.event_time else None,
                    })
            if new_events:
                await hub.broadcast({"type": "history", "events": new_events})
            await hub.broadcast({"type": "temporal_health", "alive": True})
        except Exception as e:
            await hub.broadcast({"type": "temporal_health", "alive": False})
            await hub.broadcast({"type": "log", "level": "warn", "msg": f"temporal poll: {e}"})
        await asyncio.sleep(POLL_SECONDS)


# ── MCP health (fragile plane) ────────────────────────────────────────────────


async def mcp_health_pinger(hub, url: str) -> None:
    prev: bool | None = None
    async with httpx.AsyncClient(timeout=1.5) as http:
        while True:
            alive = False
            try:
                resp = await http.get(url)  # any HTTP status (e.g. 406) ⇒ process is up
                alive = resp.status_code < 500 or resp.status_code == 406
            except Exception:
                alive = False
            await hub.broadcast({"type": "mcp_health", "alive": alive})
            # Announce health TRANSITIONS so the log reflects recovery — otherwise the
            # dramatic "💥 KILLED" line lingers and the plane looks dead after it's back.
            if prev is not None and alive != prev:
                if alive:
                    await hub.broadcast({"type": "log", "level": "info",
                                         "msg": "✓ MCP server back online — client reconnected"})
                else:
                    await hub.broadcast({"type": "log", "level": "alert",
                                         "msg": "⚠ MCP server down — Temporal keeps running"})
            prev = alive
            await asyncio.sleep(POLL_SECONDS)
