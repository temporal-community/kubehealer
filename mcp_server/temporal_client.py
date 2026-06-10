"""Shared Temporal connection helpers for the MCP server.

The MCP tools never run business logic themselves — they connect to Temporal and
start / signal / query the existing KubeHealer workflows. The key idea that makes
the server "invincible": the Workflow ID is *deterministic per namespace*, so when
the MCP server crashes and restarts, re-calling a tool re-attaches to the SAME
running (or already-completed) workflow instead of starting fresh.
"""

import os

from temporalio.client import Client

TEMPORAL_TARGET = os.environ.get("TEMPORAL_TARGET", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = "kubehealer"  # must match worker.py

# Base URL for the Temporal Web UI (handy to surface to the agent/operator).
TEMPORAL_UI = os.environ.get("TEMPORAL_UI", "http://localhost:8233")

_client: Client | None = None


async def get_temporal_client() -> Client:
    """Return a cached Temporal client (connect once per process)."""
    global _client
    if _client is None:
        _client = await Client.connect(TEMPORAL_TARGET, namespace=TEMPORAL_NAMESPACE)
    return _client


def heal_workflow_id(namespace: str) -> str:
    """Deterministic Workflow ID for a namespace's healing run.

    Deterministic on purpose: a crashed-then-restarted MCP server (or a retrying
    agent) re-attaches to the same workflow rather than spawning a duplicate.
    """
    return f"kubehealer-heal-{namespace}"


def workflow_ui_url(workflow_id: str) -> str:
    """Link to the workflow's full event history in the Temporal Web UI."""
    return f"{TEMPORAL_UI}/namespaces/{TEMPORAL_NAMESPACE}/workflows/{workflow_id}"
