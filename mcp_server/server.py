#!/usr/bin/env python3
"""KubeHealer MCP server — durable Kubernetes auto-healing tools.

Every long-running tool is a *thin wrapper* over a Temporal Workflow:

    heal_cluster  ──►  start HealerWorkflow (Workflow ID = kubehealer-heal-<ns>)
                       returned to the client as a SEP-1686 Task
    get_healing_status ──►  HealerWorkflow.get_state  (Temporal query)
    approve_fix / reject_fix ──►  HealerWorkflow.approve_pod / reject_pod (signals)

Because the work lives in Temporal, this MCP server process is disposable: kill it
mid-heal and the Workflow keeps running on the Worker; restart it and the client
re-attaches by the deterministic Workflow ID without losing a single step.

Read-only tools (list_pods) talk to Kubernetes directly — fast tools don't need a
workflow, and pretending they do is the anti-pattern this demo argues against.
"""

import json
from typing import Annotated

from fastmcp import Context, FastMCP
from pydantic import Field
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError

# Reuse the existing project code (run from the kubehealer project root).
from activities.k8s_activities import v1
from models import HealerInput
from workflows.healer_workflow import HealerWorkflow

from mcp_server.temporal_client import (
    get_temporal_client,
    heal_workflow_id,
    workflow_ui_url,
)

mcp = FastMCP(
    "kubehealer_mcp",
    instructions=(
        "Tools to inspect and auto-heal a Kubernetes cluster, backed by durable "
        "Temporal workflows. Typical flow: call heal_cluster (a long-running Task), "
        "poll get_healing_status until phase is 'awaiting_approval', review the "
        "diagnoses, then approve_fix / reject_fix EACH pod (reject 'skip' actions). "
        "The Task completes once every pod is decided and fixes are applied."
    ),
)

NS = Annotated[
    str,
    Field(description="Kubernetes namespace to operate on", default="default"),
]

# namespace -> run_id of the heal workflow this server started. Shared in-process
# with the task body (the memory:// Docket worker runs here too). Lost on restart;
# _resolve_run() then falls back to Temporal to find the still-running workflow.
_ACTIVE: dict[str, str] = {}


async def _resolve_run(client: Client, namespace: str) -> WorkflowHandle | None:
    """Return a handle pinned to the *current* heal run for this namespace.

    Prefers the run this server started (`_ACTIVE`); after a server restart, falls
    back to Temporal — preferring a RUNNING run (a heal we're recovering), else the
    most recently started run. This is what stops a stale, completed run from a
    previous attempt being mistaken for the current one.
    """
    wid = heal_workflow_id(namespace)
    rid = _ACTIVE.get(namespace)
    if rid:
        return client.get_workflow_handle(wid, run_id=rid)

    running, newest = None, None
    try:
        async for wf in client.list_workflows(f"WorkflowId = '{wid}'"):
            if wf.status == WorkflowExecutionStatus.RUNNING and running is None:
                running = wf
            if newest is None or wf.start_time > newest.start_time:
                newest = wf
    except RPCError:
        return None

    chosen = running or newest
    if chosen is None:
        return None
    _ACTIVE[namespace] = chosen.run_id
    return client.get_workflow_handle(wid, run_id=chosen.run_id)


# ── Read-only tool (no workflow needed — and that's the point) ─────────────────


@mcp.tool(
    annotations={
        "title": "List Pods",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def list_pods(namespace: str = "default") -> str:
    """List pods in a namespace with status, readiness, and restart count.

    Read-only and fast, so it calls the Kubernetes API directly — no Temporal
    workflow involved. Use this to spot unhealthy pods before healing.

    Args:
        namespace (str): Kubernetes namespace (e.g. "default").

    Returns:
        str: A plain-text table: NAME, STATUS, READY, RESTARTS.
    """
    pods = v1.list_namespaced_pod(namespace=namespace)
    lines = [f"{'NAME':<40} {'STATUS':<25} {'READY':<8} RESTARTS", "-" * 85]
    for pod in pods.items:
        name = pod.metadata.name
        phase = pod.status.phase or "Unknown"
        ready, restarts = "0/0", 0
        if pod.status.container_statuses:
            total = len(pod.status.container_statuses)
            ready_count = sum(1 for cs in pod.status.container_statuses if cs.ready)
            ready = f"{ready_count}/{total}"
            restarts = sum(cs.restart_count for cs in pod.status.container_statuses)
            for cs in pod.status.container_statuses:
                if cs.state and cs.state.waiting and cs.state.waiting.reason:
                    phase = cs.state.waiting.reason
                    break
                if cs.state and cs.state.terminated and cs.state.terminated.reason:
                    phase = cs.state.terminated.reason
                    break
        lines.append(f"{name:<40} {phase:<25} {ready:<8} {restarts}")
    return "\n".join(lines)


# ── Long-running durable tool (SEP-1686 Task backed by a Temporal Workflow) ────


@mcp.tool(
    task=True,  # SEP-1686: call-now / fetch-later. Survives this server crashing.
    annotations={
        "title": "Heal Cluster (durable)",
        "readOnlyHint": False,
        "destructiveHint": True,  # may patch/restart workloads (gated by approval)
        "idempotentHint": True,  # deterministic workflow id ⇒ re-attach, never duplicate
        "openWorldHint": True,
    },
)
async def heal_cluster(namespace: str = "default", ctx: Context | None = None) -> str:
    """Scan a namespace, diagnose every unhealthy pod with AI, and apply approved fixes.

    This is a LONG-RUNNING tool. It starts a Temporal `HealerWorkflow` and waits for
    it to finish. The workflow: (1) scans the cluster, (2) diagnoses each unhealthy
    pod, (3) PAUSES for human approval, (4) applies approved fixes, (5) returns a
    summary. While it is paused, call `get_healing_status` to see the diagnoses and
    `approve_fix` / `reject_fix` to decide each pod.

    Durability: the Workflow ID is deterministic (`kubehealer-heal-<namespace>`). If
    this MCP server crashes mid-heal, the workflow keeps running on the Temporal
    Worker; calling this tool again simply re-attaches and returns the same result —
    fixes are never applied twice.

    Args:
        namespace (str): Kubernetes namespace to heal (e.g. "default").

    Returns:
        str: A human-readable summary of every pod and the action taken, e.g.
            "Healed 3/3 pods: [+] storefront: fix_image -- Patched image to nginx:latest".
    """
    client = await get_temporal_client()
    wid = heal_workflow_id(namespace)

    # USE_EXISTING: if a heal is already running for this namespace (e.g. the agent
    # retried after a crash), attach to it instead of starting a duplicate. If the
    # previous run is closed, a fresh run starts.
    handle = await client.start_workflow(
        HealerWorkflow.run,
        HealerInput(namespace=namespace, auto_approve=False),
        id=wid,
        task_queue="kubehealer",
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    _ACTIVE[namespace] = handle.result_run_id or handle.first_execution_run_id
    if ctx is not None:
        await ctx.info(f"Healing workflow started: {wid}\nTrace: {workflow_ui_url(wid)}")

    try:
        # Blocks (durably) through scan → diagnose → human approval → fix.
        return await handle.result()
    except WorkflowFailureError as e:
        raise RuntimeError(f"Healing workflow failed: {e.cause or e}") from e


# ── Status + approval tools (Temporal query + signals) ─────────────────────────


@mcp.tool(
    annotations={
        "title": "Get Healing Status",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def get_healing_status(namespace: str = "default") -> str:
    """Get the live state of the healing workflow for a namespace.

    Queries the running Temporal workflow (does not start one). Use it to learn the
    current phase, see AI diagnoses, and find which pods still need a decision.

    Args:
        namespace (str): Kubernetes namespace whose healing run to inspect.

    Returns:
        str: JSON with this schema:
            {
              "phase": str,            # scanning|diagnosing|awaiting_approval|executing|done
              "run_id": str,           # the Temporal run currently being tracked
              "needs_approval": bool,  # True while waiting on approve_fix/reject_fix
              "undecided_pods": [str], # pods still awaiting a decision
              "diagnoses": [ {pod_name, root_cause, severity, action, explanation, fix_details} ],
              "decisions": { pod_name: "approved"|"rejected" },
              "results": [ {pod_name, success, action_taken, details} ]
            }
        Or {"phase": "not_started", ...} if no healing run exists yet.
    """
    client = await get_temporal_client()
    handle = await _resolve_run(client, namespace)
    if handle is None:
        return json.dumps({"phase": "not_started", "run_id": None, "needs_approval": False, "undecided_pods": []})
    try:
        state = await handle.query(HealerWorkflow.get_state)
    except RPCError:
        return json.dumps({"phase": "not_started", "run_id": None, "needs_approval": False, "undecided_pods": []})

    decided = set(state.get("decisions", {}))
    undecided = [d["pod_name"] for d in state.get("diagnoses", []) if d["pod_name"] not in decided]
    state["run_id"] = handle.run_id
    state["needs_approval"] = state.get("phase") == "awaiting_approval"
    state["undecided_pods"] = undecided
    return json.dumps(state, indent=2)


async def _signal_decision(namespace: str, pod_name: str, *, approve: bool) -> str:
    client = await get_temporal_client()
    handle = await _resolve_run(client, namespace)
    no_run = f"No active healing workflow for namespace '{namespace}'. Call heal_cluster first."
    if handle is None:
        return no_run
    signal = HealerWorkflow.approve_pod if approve else HealerWorkflow.reject_pod
    verb = "approved" if approve else "rejected"
    try:
        await handle.signal(signal, pod_name)
    except RPCError:
        return no_run
    return f"{verb.capitalize()} fix for pod '{pod_name}'."


@mcp.tool(
    annotations={
        "title": "Approve Fix",
        "readOnlyHint": False,
        "destructiveHint": True,  # green-lights a real cluster mutation
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def approve_fix(pod_name: str, namespace: str = "default") -> str:
    """Approve the AI-proposed fix for a pod (signals the healing workflow).

    Once every diagnosed pod has been approved or rejected, the workflow applies the
    approved fixes and the heal_cluster Task completes.

    Args:
        pod_name (str): Pod whose fix to approve (from get_healing_status diagnoses).
        namespace (str): Kubernetes namespace of the healing run.

    Returns:
        str: Confirmation message, or guidance if no healing run is active.
    """
    return await _signal_decision(namespace, pod_name, approve=True)


@mcp.tool(
    annotations={
        "title": "Reject Fix",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def reject_fix(pod_name: str, namespace: str = "default") -> str:
    """Reject the AI-proposed fix for a pod (signals the healing workflow).

    Use this for pods whose action is "skip" (e.g. a missing ConfigMap that can't be
    auto-fixed) or any fix you don't want applied. The pod is left untouched.

    Args:
        pod_name (str): Pod whose fix to reject.
        namespace (str): Kubernetes namespace of the healing run.

    Returns:
        str: Confirmation message, or guidance if no healing run is active.
    """
    return await _signal_decision(namespace, pod_name, approve=False)


if __name__ == "__main__":
    mcp.run()
