# KubeHealer

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue?logo=python&logoColor=white)](https://python.org)
[![Temporal](https://img.shields.io/badge/Temporal-Durable_Execution-8B5CF6?logo=temporal&logoColor=white)](https://temporal.io)
[![Anthropic Claude](https://img.shields.io/badge/Claude-Sonnet_4-D97757?logo=anthropic&logoColor=white)](https://anthropic.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Cluster_Ops-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> AI-powered Kubernetes debugging and auto-remediation, orchestrated by Temporal —
> exposed as a **crash-proof MCP server**.

AI agent that finds broken Kubernetes pods, diagnoses them with Claude, and fixes
them automatically. The healing logic runs inside **Temporal workflows**, and the
whole thing is fronted by an **MCP server** that is deliberately disposable: kill it
mid-heal and the workflow keeps running — restart it and the client re-attaches and
finishes. That contrast is the point of the demo.

## How It Works

1. **Scan** — finds unhealthy pods (CrashLoopBackOff, OOMKilled, ImagePullBackOff…)
2. **Diagnose** — sends pod details to Claude, gets root cause + fix plan
3. **Approve** — a human (or the agent) approves each fix; the workflow waits, durably
4. **Fix** — executes remediation (restart, patch image, adjust resources)

Everything runs inside Temporal workflows = crash-proof, retryable, fully observable.

## The crux: a crash-proof MCP server

A basic MCP server keeps its work **inside the process** — kill it mid-task and the
work, the state, and any in-flight progress die with it. KubeHealer's MCP server keeps
the work **inside Temporal** and is just a thin pointer to it:

```
heal_cluster (MCP tool)  ──►  start HealerWorkflow   (id = kubehealer-heal-<ns>)
get_healing_status       ──►  HealerWorkflow.get_state     (Temporal query)
approve_fix / reject_fix ──►  HealerWorkflow.approve/reject (Temporal signals)
```

Because the Workflow ID is **deterministic per namespace**, a crashed-then-restarted
MCP server re-attaches to the same running workflow by recomputing the ID — no lost
steps, no double-applied fixes. See [`CRASHPROOF_MCP.md`](CRASHPROOF_MCP.md) for the
full naive-vs-durable contrast with code.

## Three ways to drive it

All three talk to the same MCP server + Temporal backend.

- **GUI — Mission Control dashboard** (`make dashboard`): a live web UI showing the
  agent's reasoning, the MCP plane, the cluster, and human approve/reject buttons.
- **CLI — agent brain** (`make agent`): an LLM agent (MCP host) that drives the heal
  from the terminal and reconnects automatically if the MCP server dies.
- **Headless — one-shot** (`make auto`): scans, diagnoses, and auto-fixes, then exits.

There's also a plain conversational CLI (`make cli`) that chats with Temporal directly
(no MCP), and a non-durable "before" MCP server (`make mcp-naive`) for the talk's contrast.

## Quick Start

Prerequisites: Python 3.11+, Docker, Kind, Temporal CLI, an Anthropic API key.

```bash
./setup.sh                                   # kind cluster + broken demo pods
pip install -r requirements.txt
cp .env.example .env                         # paste your ANTHROPIC_API_KEY
```

Then run one piece per terminal (the **MCP server is the one you break**):

```bash
make temporal     # durable backend + Web UI
make worker       # runs the workflows + activities
make mcp          # the MCP server  ← Ctrl-C this to break the demo
make agent        # CLI agent   (or: make dashboard for the GUI)
```

`make help` lists every target. **[`RUN.md`](RUN.md)** has the full CLI-only / GUI /
headless recipes, port overrides, and the break-and-recover demo walkthrough.

## What Gets Fixed

| Broken App | Problem | AI Diagnosis | Auto-Fix |
|---|---|---|---|
| storefront | Image `nginx:latestt` (typo) | Detects typo | Patches to `nginx:latest` |
| checkout-api | 10Mi limit + stress 100M | OOMKilled | Raises memory limit |
| catalog-cache | Image `redis:latst` (typo) | Detects typo | Patches to `redis:latest` |

## Architecture

```
  CLI agent  ┐                      ┌──────────────────────┐
  Dashboard  ┼── MCP (HTTP) ──►     │  MCP server          │  thin & disposable
  (clients)  ┘   reconnects         │  (FastMCP, SEP-1686) │  — kill it anytime
                                    └──────────┬───────────┘
                                  start / signal / query
                                               ▼
                          ┌──────────────────────────────────────┐
                          │  TEMPORAL  ── HealerWorkflow           │  durable plane
                          │  scan → diagnose → AWAIT APPROVAL → fix │  state persisted
                          │  activities: scan_cluster, diagnose_pod,│  every step
                          │              get_pod_details, execute_fix
                          └──────────────────┬─────────────────────┘
                                    runs on the Worker
                                             ▼
                                       ┌────────────┐
                                       │ Kubernetes │  (kind cluster)
                                       └────────────┘

  💥 kill the MCP server  ──►  workflow keeps running, loses nothing.
     restart it           ──►  same deterministic ID re-attaches to the live run.
```

The dashboard also keeps a **direct** Temporal + Kubernetes connection (independent of
MCP), so when you break the MCP plane the audience sees it flatline while the Temporal
plane and pod cards keep advancing.

### Key Design Decisions

- **MCP server = thin wrapper over Temporal** — tools start/signal/query workflows; no
  business logic lives in the disposable process.
- **Deterministic Workflow ID** (`kubehealer-heal-<ns>`) — any client/process re-attaches
  to the same heal by recomputing the ID; `USE_EXISTING` makes re-calls idempotent.
- **Indefinite, durable approval wait** — the workflow parks at `awaiting_approval` with
  no timeout, surviving worker/MCP crashes for as long as it takes a human to decide.
- **Each Claude call and tool call = separate activity** — individually retryable, visible
  in the Temporal UI, with per-tool timeouts.
- **Conversational CLI uses Temporal Updates** + continue-as-new at 50 turns + a fixed
  workflow ID, so it reconnects to the same conversation after a crash.

## Tech Stack

| Component | Role |
|-----------|------|
| ![Temporal](https://img.shields.io/badge/-Temporal-8B5CF6?style=flat-square&logo=temporal&logoColor=white) | Durable workflow orchestration |
| ![Claude](https://img.shields.io/badge/-Claude_Sonnet_4-D97757?style=flat-square&logo=anthropic&logoColor=white) | LLM diagnosis + conversational agent |
| FastMCP | MCP server + SEP-1686 durable Tasks |
| FastAPI + React | Mission Control live dashboard (`dashboard/` + `ui/`) |
| ![Kubernetes](https://img.shields.io/badge/-Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white) ![Kind](https://img.shields.io/badge/-Kind-326CE5?style=flat-square&logo=kubernetes&logoColor=white) | Target cluster (local Kind) |
| ![Python](https://img.shields.io/badge/-Python_3.11+-3776AB?style=flat-square&logo=python&logoColor=white) | Everything glued together |

## Built For

AIOps India meetup — demonstrating AIOps, DevOps, AI Agents, and Durable Execution in
practice. See [`CRASHPROOF_MCP.md`](CRASHPROOF_MCP.md) for the durability deep-dive.

---

<p align="center">
  <sub>Built with Temporal durable execution and Claude AI</sub>
</p>
