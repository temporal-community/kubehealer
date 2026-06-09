# KubeHealer — one command per terminal.
#
# Ports are overridable, e.g.:  make temporal TEMPORAL_PORT=7233
#
# Typical CLI-only demo (4 terminals):  make temporal | make worker | make mcp | make agent
# Typical GUI demo (5 terminals):       + make dashboard   (auto-attaches to the running mcp)
# Break the MCP plane in either case:   Ctrl-C the `make mcp` terminal.

TEMPORAL_PORT    ?= 7234
TEMPORAL_UI_PORT ?= 8233
MCP_PORT         ?= 8000
DASH_PORT        ?= 8090
NS               ?= default

TEMPORAL_TARGET := localhost:$(TEMPORAL_PORT)
RUN := TEMPORAL_TARGET=$(TEMPORAL_TARGET) KUBEHEALER_MCP_PORT=$(MCP_PORT) KUBEHEALER_NS=$(NS)

.DEFAULT_GOAL := help
.PHONY: help temporal worker mcp mcp-naive agent cli dashboard auto chaos reset test

help: ## Show this help
	@echo "KubeHealer make targets (each runs one piece in its own terminal):"
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Ports: TEMPORAL_PORT=$(TEMPORAL_PORT) UI=$(TEMPORAL_UI_PORT) MCP_PORT=$(MCP_PORT) DASH_PORT=$(DASH_PORT)"

temporal: ## Temporal dev server (gRPC + Web UI)
	temporal server start-dev --port $(TEMPORAL_PORT) --ui-port $(TEMPORAL_UI_PORT)

worker: ## Temporal worker (runs the durable workflows + activities)
	$(RUN) python worker.py

mcp: ## MCP server — the killable plane (Ctrl-C this to break the demo)
	$(RUN) python run_mcp_server.py --port $(MCP_PORT)

mcp-naive: ## Non-durable MCP server (the "before" contrast)
	$(RUN) python -m mcp_server.naive_server --port 8001

agent: ## CLI agent (MCP host) — drives a heal, reconnects if MCP dies
	$(RUN) KUBEHEALER_MCP_URL=http://127.0.0.1:$(MCP_PORT)/mcp python agent/brain.py

cli: ## Plain conversational CLI (talks to Temporal directly, no MCP)
	$(RUN) python cli.py

dashboard: ## GUI mission control (auto-attaches to a running mcp, else spawns one)
	$(RUN) python run_dashboard.py --port $(DASH_PORT)

auto: ## Headless one-shot auto-heal (no CLI/GUI)
	$(RUN) python starter.py --namespace $(NS)

chaos: ## Deploy the broken pods into the cluster
	kubectl apply -f chaos/

reset: ## Recreate the broken pods (fresh demo state)
	kubectl delete -f chaos/ --ignore-not-found
	kubectl apply -f chaos/

test: ## Run the pytest suite
	python -m pytest -q
