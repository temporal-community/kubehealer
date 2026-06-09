"""Durable MCP layer for KubeHealer.

The MCP server is a thin, *crash-proof* wrapper: its long-running tools start
Temporal Workflows and return SEP-1686 Tasks. The MCP server process can die at
any moment — the Workflow keeps running on the Temporal Worker and is re-attached
by its (deterministic) Workflow ID when the server comes back.
"""
