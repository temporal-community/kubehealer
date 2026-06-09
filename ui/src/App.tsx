import { motion } from "framer-motion";
import type { ReactNode } from "react";
import {
  AgentPanel, ApprovalDrawer, ClusterPanel, ControlBar, Header, LogBar, McpPanel,
} from "./panels";

const reveal = {
  hidden: { opacity: 0, y: 10 },
  show: (i: number) => ({ opacity: 1, y: 0, transition: { delay: i * 0.06 } }),
};

function Cell({ i, area, children }: { i: number; area: string; children: ReactNode }) {
  return (
    <motion.div
      custom={i}
      variants={reveal}
      initial="hidden"
      animate="show"
      style={{ gridArea: area, minHeight: 0, display: "flex" }}
    >
      {children}
    </motion.div>
  );
}

export default function App() {
  return (
    <div className="app">
      <Header />
      <ControlBar />
      {/* Story flow: the agent reasons + talks to MCP (top), the cluster reacts
          (middle), a human approves (bottom). Temporal's detail lives in its own UI. */}
      <div className="grid">
        <Cell i={0} area="agent"><AgentPanel /></Cell>
        <Cell i={1} area="mcp"><McpPanel /></Cell>
        <Cell i={2} area="cluster"><ClusterPanel /></Cell>
        <Cell i={3} area="hitl"><ApprovalDrawer /></Cell>
      </div>
      <LogBar />
    </div>
  );
}
