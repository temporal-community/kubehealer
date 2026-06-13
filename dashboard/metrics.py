"""Prometheus metrics for the agentic / AIOps plane.

The dashboard's pollers (dashboard/sources.py) already compute everything Grafana
needs to tell the durability + self-healing story — MCP up/down, Temporal up, pod
health, and the heal phase. Rather than add a second source of truth, we just mirror
those polled values into Prometheus gauges here and serve them from the dashboard's
`/metrics` endpoint (see dashboard/gateway.py).

The Temporal *worker* exposes its own SDK metrics separately (see worker.py); this
module is only the agentic/business + fragile-plane (MCP) signals.
"""

from prometheus_client import Gauge

# ── Fragile plane (MCP) ────────────────────────────────────────────────────────
MCP_UP = Gauge("kubehealer_mcp_up", "MCP server reachable (1) or down (0)")
TEMPORAL_UP = Gauge("kubehealer_temporal_up", "Temporal reachable (1) or down (0)")

# ── Cluster / self-healing outcome ─────────────────────────────────────────────
PODS_TOTAL = Gauge("kubehealer_pods_total", "Pods observed in the namespace")
PODS_HEALTHY = Gauge("kubehealer_pods_healthy", "Pods currently healthy")
PODS_UNHEALTHY = Gauge("kubehealer_pods_unhealthy", "Pods currently unhealthy")
PODS_HEALED = Gauge("kubehealer_pods_healed", "Pods successfully healed in the current run")

# ── Heal phase (one series per phase; active phase = 1, others = 0) ─────────────
HEAL_PHASE = Gauge("kubehealer_heal_phase", "Current heal phase (active=1)", ["phase"])
_PHASES = ("scanning", "diagnosing", "awaiting_approval", "executing", "done", "not_started")


def set_mcp_up(alive: bool) -> None:
    MCP_UP.set(1 if alive else 0)


def set_temporal_up(alive: bool) -> None:
    TEMPORAL_UP.set(1 if alive else 0)


def set_pods(total: int, healthy: int) -> None:
    PODS_TOTAL.set(total)
    PODS_HEALTHY.set(healthy)
    PODS_UNHEALTHY.set(max(0, total - healthy))


def set_heal_phase(phase: str) -> None:
    """Light up the active phase, clear the rest (so Grafana shows a phase timeline)."""
    for p in _PHASES:
        HEAL_PHASE.labels(phase=p).set(1 if p == phase else 0)


def set_pods_healed(n: int) -> None:
    PODS_HEALED.set(n)
