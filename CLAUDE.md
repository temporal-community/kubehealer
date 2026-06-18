# CLAUDE.md — KubeHealer

AI agent that heals broken Kubernetes pods, made durable by Temporal and fronted by a
crash-proof MCP server. Built for a "Durable & Observable AIOps" talk. Python 3.11+.

## Run it (one piece per terminal)

```bash
make temporal    # Temporal dev server + Web UI (:8233, gRPC :7234)
make worker      # runs the durable workflows + activities
make mcp         # MCP server (:8000) — the killable/fragile plane
make dashboard   # Mission Control GUI (:8090)   — or `make agent` for the CLI
make chaos       # break the demo pods   (make reset = fresh broken set)
make auto        # headless one-shot auto-heal
make test        # pytest suite
```

Ports: Temporal gRPC `7234`, UI `8233`, MCP `8000`, dashboard `8090`, worker metrics `9469`.
Target cluster: local kind cluster `kind-kubehealer`. Needs `ANTHROPIC_API_KEY` in `.env`.

## Architecture

- **MCP server** (`mcp_server/`, FastMCP, `:8000`) — thin & disposable. Its tools just
  start / query / signal a Temporal **`HealerWorkflow`** (deterministic id
  `kubehealer-heal-<ns>`). Kill it mid-heal; the workflow keeps running on the worker.
- **Worker** (`worker.py`) — runs `HealerWorkflow` → one **`HealPodWorkflow`** child per
  approved pod (`fix → settle timer → verify_healed`, a heartbeating+retrying activity).
- **Dashboard** (`dashboard/` + `ui/`, FastAPI+React, `:8090`) — reads Temporal + k8s
  **directly**. The **"Heal" button runs the agent** (`agent/brain.py`, a Claude agent
  that drives the MCP tools); **approvals route through MCP on purpose** (that's the
  fragile-plane contrast).
- Two AI call paths, both use Claude: the **agent** (`agent/brain.py`) and the
  **workflow activities** (`diagnose_pod`, `call_claude`).

## ⚠️ Operational gotchas (check these first when "the demo is broken")

1. **Restart the worker and dashboard after any code change.** They're long-running and
   hold the old code in memory — the #1 cause of "it stopped working." A stale worker
   silently stops processing (workflows sit `RUNNING` with no progress).
2. **The Claude model id is hardcoded in 3 places — keep them in sync:**
   `agent/brain.py` (`KUBEHEALER_MODEL` default), `activities/llm_activities.py`,
   `activities/chat_activities.py`. Current: **`claude-sonnet-4-6`**. Model *snapshots get
   retired* (→ Anthropic `404 not_found`); when a heal fails with that, update to a
   current model id (see the `claude-api` skill / model catalog). Override at runtime with
   `KUBEHEALER_MODEL`.
3. **kind cluster `ImagePullBackOff` on all pods after the machine sleeps** = transient
   Docker Desktop DNS, not a code bug. Test: `docker exec kubehealer-control-plane crictl
   pull redis`. If it fails: `docker restart kubehealer-control-plane` (or restart Docker
   Desktop), then re-trigger the pods.
4. **Search attributes self-register on worker startup** (`search_attributes.ensure_registered`)
   — no manual `--search-attribute` flags on the server needed.

## Tests

`make test` — pytest with Temporal time-skipping. Workflows are started with
`HealerInput(track_phase=False)` so the suite needs no server with search attributes
registered.

## Conventions

- Commit/push only when asked. Branch off `main` for new work; keep hotfixes (e.g. a
  retired model id) on their own branch so they merge independently.
- Don't commit secrets — `.env` holds `ANTHROPIC_API_KEY`.

> Observability (Grafana + Prometheus, `make observability`) lands via the
> `feat/grafana-observability` branch / PR — once merged, the worker exposes Temporal SDK
> metrics on `:9469` and the dashboard exposes `kubehealer_*` on `:8090/metrics`.
