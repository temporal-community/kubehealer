#!/usr/bin/env python3
"""Run the KubeHealer MCP server.

Defaults to HTTP transport so the server is a standalone, *killable* process for
the live crash demo (its own terminal, its own PID). Use --transport stdio to run
it as a subprocess of an MCP host (e.g. Claude Desktop) instead.

    python run_mcp_server.py                 # http://127.0.0.1:8000/mcp
    python run_mcp_server.py --port 9000
    python run_mcp_server.py --transport stdio
"""

import argparse

from dotenv import load_dotenv

load_dotenv()

from mcp_server.server import mcp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the KubeHealer MCP server")
    parser.add_argument("--transport", choices=["http", "stdio"], default="http")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run()
    else:
        print(f"  KubeHealer MCP server: http://{args.host}:{args.port}/mcp")
        # Disable uvicorn's per-request access log: the dashboard health-pings this
        # endpoint every second (a bare GET → 406), which would otherwise flood the
        # terminal. Startup + real errors still print.
        mcp.run(transport="http", host=args.host, port=args.port,
                uvicorn_config={"access_log": False})


if __name__ == "__main__":
    main()
