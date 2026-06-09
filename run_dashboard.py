#!/usr/bin/env python3
"""Run the KubeHealer live dashboard.

Starts the FastAPI gateway, which supervises the MCP server child process, connects
to Temporal + Kubernetes, and serves the UI (if built) on one port.

Prereqs in other terminals: `temporal server start-dev` and `python worker.py`.
Build the UI once with `cd ui && npm install && npm run build`.

    python run_dashboard.py            # http://localhost:8080
    python run_dashboard.py --port 9090
"""

import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the KubeHealer dashboard gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    print(f"  KubeHealer Mission Control: http://{args.host}:{args.port}")
    uvicorn.run("dashboard.gateway:app", host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
