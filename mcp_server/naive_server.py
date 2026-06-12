#!/usr/bin/env python3
"""NAIVE KubeHealer MCP server — the "before" for the talk's contrast.

Same heal_cluster tool, but it runs ALL the logic *in this process*: scan,
diagnose with the LLM, then apply fixes. There is no durable backend. All state
(the diagnoses, which pods were fixed) lives in local variables.

Kill this server mid-heal and:
  * the SEP-1686 Task is gone from the in-memory task store,
  * the (expensive) LLM diagnoses are lost,
  * and if the crash lands *between* fixes, the cluster is left half-healed with
    no record of what was already done — re-running risks double-applying.

Run it the same way as the durable server (different port):

    python -m mcp_server.naive_server --port 8001
"""

import argparse
import asyncio

from dotenv import load_dotenv

load_dotenv()

from fastmcp import Context, FastMCP

from activities.k8s_activities import scan_cluster, get_pod_details, execute_fix
from activities.llm_activities import diagnose_pod

mcp = FastMCP(
    "kubehealer_naive_mcp",
    instructions="Non-durable Kubernetes healing tools (demo contrast — do not use in prod).",
)

# Seconds to "work" before/between applying fixes — the window to kill the server
# on stage and demonstrate lost progress.
FIX_PAUSE_SECONDS = 6


@mcp.tool(task=True, annotations={"title": "Heal Cluster (NAIVE / non-durable)"})
async def heal_cluster(namespace: str = "default", ctx: Context | None = None) -> str:
    """Scan, diagnose, and fix unhealthy pods — entirely in-process (NOT durable).

    Args:
        namespace (str): Kubernetes namespace to heal.

    Returns:
        str: Summary of fixes applied. (Only if the process survives long enough.)
    """
    issues = await scan_cluster(namespace)
    if not issues:
        return "All pods healthy! Nothing to fix."

    # All of this state is ephemeral — it dies with the process.
    diagnoses = []
    for issue in issues:
        details = await get_pod_details(issue.name, issue.namespace)
        diagnosis = await diagnose_pod(details)
        diagnosis.namespace = issue.namespace
        diagnoses.append(diagnosis)
        if ctx is not None:
            await ctx.info(f"Diagnosed {diagnosis.pod_name}: {diagnosis.action}")

    results = []
    for diagnosis in diagnoses:
        if diagnosis.action == "skip":
            results.append(f"[-] {diagnosis.pod_name}: skipped ({diagnosis.explanation})")
            continue
        # Simulated long-running apply: the danger window. Kill the server here.
        if ctx is not None:
            await ctx.info(f"Applying fix to {diagnosis.pod_name} (working {FIX_PAUSE_SECONDS}s)...")
        await asyncio.sleep(FIX_PAUSE_SECONDS)
        res = await execute_fix(diagnosis)
        icon = "+" if res.success else "-"
        results.append(f"[{icon}] {res.pod_name}: {res.action_taken} -- {res.details}")

    return f"Healed {sum('[+]' in r for r in results)}/{len(results)} pods:\n" + "\n".join(results)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NAIVE (non-durable) MCP server")
    parser.add_argument("--transport", choices=["http", "stdio"], default="http")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    if args.transport == "stdio":
        mcp.run()
    else:
        print(f"  NAIVE MCP server (non-durable): http://{args.host}:{args.port}/mcp")
        mcp.run(transport="http", host=args.host, port=args.port,
                uvicorn_config={"access_log": False})


if __name__ == "__main__":
    main()
