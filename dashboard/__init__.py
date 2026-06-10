"""Live dashboard gateway for KubeHealer — a FastAPI app that streams the demo.

Holds two independent data planes and fans both to the browser over one WebSocket:
  * fragile plane — a FastMCP client + MCP health pinger (dies when the server is killed)
  * durable plane — a DIRECT Temporal client + Kubernetes watch (keeps streaming anyway)

That contrast — MCP panel flatlining while the Temporal/cluster panels keep advancing —
is the whole point of the "Invincible MCP Server" demo, made visible in one screen.
"""
