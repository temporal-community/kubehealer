"""The on-stage 'brain': a thin MCP host that drives the KubeHealer MCP server.

It is deliberately separate from the MCP server process so we can kill the server
mid-heal and watch the brain reconnect and recover — the whole point of the talk.
"""
