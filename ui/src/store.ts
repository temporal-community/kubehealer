import { create } from "zustand";
import type { HistoryEvent, LogLine, Pod, ToolCall, Workflow } from "./types";

type State = {
  connected: boolean;
  mcpAlive: boolean;
  mcpExternal: boolean;
  temporalAlive: boolean;
  pods: Pod[];
  workflow: Workflow | null;
  history: HistoryEvent[];
  agentBlocks: string[];
  agentCurrent: string;
  tools: ToolCall[];
  taskId: string | null;
  logs: LogLine[];
  send: (cmd: Record<string, unknown>) => void;
};

let socket: WebSocket | null = null;

export const useStore = create<State>((set, get) => ({
  connected: false,
  mcpAlive: false,
  mcpExternal: false,
  temporalAlive: false,
  pods: [],
  workflow: null,
  history: [],
  agentBlocks: [],
  agentCurrent: "",
  tools: [],
  taskId: null,
  logs: [],
  send: (cmd) => socket?.readyState === WebSocket.OPEN && socket.send(JSON.stringify(cmd)),
}));

function apply(ev: any) {
  const s = useStore.getState();
  switch (ev.type) {
    case "pods":
      useStore.setState({ pods: ev.pods });
      break;
    case "workflow":
      useStore.setState({ workflow: ev as Workflow });
      break;
    case "mcp_health":
      useStore.setState({ mcpAlive: ev.alive });
      break;
    case "temporal_health":
      useStore.setState({ temporalAlive: ev.alive });
      break;
    case "mcp_mode":
      useStore.setState({ mcpExternal: ev.external });
      break;
    case "task":
      useStore.setState({ taskId: ev.task_id ?? s.taskId });
      break;
    case "history":
      if (ev.reset) useStore.setState({ history: [] });
      if (ev.events?.length) {
        const seen = new Set(s.history.map((h) => h.id));
        const merged = [...s.history, ...ev.events.filter((e: HistoryEvent) => !seen.has(e.id))];
        useStore.setState({ history: merged.slice(-300) });
      }
      break;
    case "agent":
      if (ev.delta) useStore.setState({ agentCurrent: s.agentCurrent + ev.delta });
      if (ev.done && s.agentCurrent.trim())
        useStore.setState({ agentBlocks: [...s.agentBlocks, s.agentCurrent].slice(-12), agentCurrent: "" });
      break;
    case "tool_call":
      useStore.setState({
        tools: [...s.tools, { id: ev.id, name: ev.name, args: ev.args }].slice(-60),
      });
      break;
    case "tool_result":
      useStore.setState({
        tools: s.tools.map((t) => (t.id === ev.id ? { ...t, ok: ev.ok, summary: ev.summary } : t)),
      });
      break;
    case "log":
      useStore.setState({
        logs: [...s.logs, { level: ev.level || "info", msg: ev.msg, t: Date.now() }].slice(-100),
      });
      break;
  }
}

export function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const url = `${proto}://${location.host}/ws`;
  const ws = new WebSocket(url);
  socket = ws;
  ws.onopen = () => useStore.setState({ connected: true });
  ws.onclose = () => {
    useStore.setState({ connected: false });
    setTimeout(connectWS, 1500); // gateway restart / reconnect
  };
  ws.onmessage = (e) => {
    try {
      apply(JSON.parse(e.data));
    } catch {
      /* ignore */
    }
  };
}
