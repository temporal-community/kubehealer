"""Tiny async pub/sub so background producers can fan events to every WebSocket."""

import asyncio


class Hub:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        # Last value per event "type" so a freshly-connected client gets current state.
        self._latest: dict[str, dict] = {}

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def snapshot(self) -> list[dict]:
        """Current sticky state (pods, workflow, health…) for a new client."""
        return list(self._latest.values())

    async def broadcast(self, event: dict) -> None:
        # Keep the latest snapshot for sticky event types (not transient log/agent/tool).
        if event.get("type") in {"pods", "workflow", "mcp_health", "temporal_health", "task", "mcp_mode"}:
            self._latest[event["type"]] = event
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow client: drop it rather than block the whole demo.
                self.unsubscribe(q)

    def emitter(self):
        """An async `emit(event)` callable for the agent brain / sources."""
        async def emit(event: dict) -> None:
            await self.broadcast(event)
        return emit
