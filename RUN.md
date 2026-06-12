# Running KubeHealer

Every process runs in its own terminal. The **MCP server is the one you break** —
just `Ctrl-C` its terminal. The durable plane (Temporal + worker) keeps the heal
going; restart the MCP terminal and the client reconnects.

> Ports are overridable Makefile vars. Defaults: Temporal gRPC `7234`, Temporal UI
> `8233`, MCP `8000`, dashboard `8090`. Override like `make temporal TEMPORAL_PORT=7233`.
> (The defaults avoid `7233`, which another local project may be using.)

Prereqs: a Kubernetes cluster (the `kind-kubehealer` cluster), `temporal` CLI,
deps installed (`pip install -r requirements.txt`), and `ANTHROPIC_API_KEY` in `.env`.

---

## Mode 1 — CLI only (no GUI)

Four terminals:

```bash
make temporal     # 1. durable backend + Web UI (http://localhost:8233)
make worker       # 2. runs the workflows + activities
make mcp          # 3. the MCP server  ← the killable plane
make agent        # 4. the CLI agent (Claude drives the heal via MCP)
```

Then, to seed broken pods (any terminal): `make chaos` (or `make reset` for a fresh set).

**The break demo:** while `make agent` is mid-heal, go to terminal 3 and press
`Ctrl-C`. The agent prints "MCP server unreachable — the Temporal workflow keeps
running, reconnecting…". Re-run `make mcp` in terminal 3 → the agent reconnects and
finishes. Nothing was lost, because the work lives in the Temporal workflow.

---

## Mode 2 — GUI (mission control)

Same four terminals as above, plus a fifth:

```bash
make dashboard    # 5. http://localhost:8090
```

The dashboard **auto-detects** the MCP server:

- **MCP already running** (you started `make mcp`): the dashboard attaches to it.
  The control bar shows a hint — *"MCP runs in its own terminal — Ctrl-C it to
  break"* — because it doesn't own that process. Break it the same way as Mode 1;
  the MCP plane flatlines while the Temporal plane and pod cards keep updating.
- **No MCP running** (you skipped `make mcp`): the dashboard spawns its own MCP
  child, and the **💥 Break MCP Server / Restart** buttons work in-GUI.

So you can run the GUI with a separate, killable MCP **or** as a single
self-contained dashboard — your choice, no flags.

---

## Mode 3 — Headless one-shot (no CLI, no GUI)

```bash
make temporal
make worker
make auto         # starter.py: scan → diagnose → auto-fix, then exits
```

---

## Observability — Grafana + Prometheus (Durable & Observable AIOps)

A self-contained Grafana stack visualizes the whole story. Two metric sources are
already wired into the app — no extra flags:

- the **worker** exposes Temporal SDK metrics on `:9469` (workflows, activities,
  retries, worker task-slots/pollers) — see `worker.py`;
- the **dashboard** exposes agentic + fragile-plane metrics on `:8090/metrics`
  (`kubehealer_mcp_up`, pod health, heal phase, pods healed) — see `dashboard/`.

```bash
make observability      # docker compose: Prometheus (:9090) + Grafana (:3000), pre-provisioned
make observability-down # stop it
```

Open Grafana at **http://localhost:3000** (anonymous viewer; admin/admin) → the
**"KubeHealer — Durable & Observable AIOps"** dashboard loads automatically. Four rows:
**MCP fragile plane** (up/down timeline, uptime %, outages) · **Workflows & activities**
(completion rate, latency p95, retries) · **Worker health** (task slots, pollers) ·
**AIOps self-healing** (healthy vs unhealthy pods, heal phase, pods healed).

The worker and dashboard must be running for their metrics to appear. Prometheus
scrapes the host via `host.docker.internal`. Ports are overridable
(`GRAFANA_PORT`, `PROM_PORT`); the worker port via `WORKER_METRICS_ADDR` (default
`0.0.0.0:9469` — `9464` is avoided because it's a common Temporal default that may
already be taken).

**Demo beats on the dashboard:** break the MCP server → the MCP row flips to DOWN and
uptime % dips; run a heal → the AIOps row climbs (unhealthy→healthy, pods healed) and
the workflow/activity panels light up; `Ctrl-C` the worker → its scrape target drops
and slots flatline, restart → it recovers.

---

## Other targets

```bash
make mcp-naive    # the non-durable "before" MCP server (talk contrast) on :8001
make cli          # plain conversational CLI (talks to Temporal directly, not MCP)
make test         # pytest suite
make help         # list everything
```

## Notes
- **Search Attributes:** the **worker self-registers** the heal's custom Search
  Attributes (`HealPhase`, `HealNamespace`, `PodName`, `PodsTotal`, `PodsHealed`) on
  startup, so the Web UI shows each heal's live phase + progress with no manual setup —
  nothing to add to the Temporal server command. Registration is idempotent and
  best-effort: if the server can't register them the worker still runs (it just logs a
  warning). Tests pass `HealerInput(track_phase=False)` to skip emitting them entirely.
- `make cli` is a *different* CLI from `make agent`: `cli.py` chats with Temporal
  directly (no MCP), so breaking the MCP server doesn't affect it. `agent/brain.py`
  is the MCP client — that's the one the break demo is about.
- The MCP server, dashboard, worker, and CLIs all read `TEMPORAL_TARGET` (the
  Makefile sets it from `TEMPORAL_PORT`), so everything points at the same backend.
- Before a fresh live run, `make reset` so the agent scans new broken pods rather
  than re-attaching to a previous heal workflow.
