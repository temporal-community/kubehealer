import { AnimatePresence, motion } from "framer-motion";
import { useStore } from "./store";
import { PHASES } from "./types";

/* ── Header + status badges ─────────────────────────────── */
export function Header() {
  const mcp = useStore((s) => s.mcpAlive);
  const tmp = useStore((s) => s.temporalAlive);
  return (
    <div className="header">
      <div className="brand-row">
        <img className="tlogo" src="/temporal-logo-anim.gif" alt="Temporal" />
        <span className="brand-div" />
        <div className="brand">
          THE <b>INVINCIBLE</b> MCP SERVER
          <span className="sub">KUBEHEALER · DURABLE EXECUTION · MISSION CONTROL</span>
        </div>
      </div>
      <div className="badges">
        <Badge label="MCP Server" sub="fragile" alive={mcp} />
        <Badge label="Temporal" sub="durable" alive={tmp} />
      </div>
    </div>
  );
}

function Badge({ label, sub, alive }: { label: string; sub?: string; alive: boolean }) {
  return (
    <div className={`badge ${alive ? "alive" : "dead"}`}>
      <span className="dot" />
      {label}{sub ? <span className="badge-sub"> · {sub}</span> : null} · {alive ? "LIVE" : "DOWN"}
    </div>
  );
}

/* ── Control bar ────────────────────────────────────────── */
export function ControlBar() {
  const send = useStore((s) => s.send);
  return (
    <div className="controls">
      <button className="btn amber" onClick={() => send({ action: "inject_chaos" })}>Break Pods (Inject Chaos)</button>
      <button className="btn" onClick={() => send({ action: "start_heal", mode: "hitl" })}>▶ Heal (you approve)</button>
      <button className="btn" onClick={() => send({ action: "reset_cluster" })}>Reset</button>
      <span className="spacer" />
      <span className="hint">kill the AI's link — Temporal keeps healing ↓</span>
      <button className="btn danger" onClick={() => send({ action: "break_server" })}>💥 Break MCP Server</button>
      <button className="btn" onClick={() => send({ action: "restart_server" })}>Restart</button>
    </div>
  );
}

/* ── Agent reasoning (streaming) ────────────────────────── */
export function AgentPanel() {
  const blocks = useStore((s) => s.agentBlocks);
  const current = useStore((s) => s.agentCurrent);
  return (
    <div className="panel">
      <div className="panel-hd">AI Agent <span className="tag">Claude — reasons &amp; decides</span></div>
      <div className="panel-bd agent-stream">
        {blocks.length === 0 && !current && (
          <div className="agent-empty">Idle. Hit “Start Heal” to dispatch the agent.</div>
        )}
        {blocks.map((b, i) => (
          <div className="agent-block" key={i}>{b}</div>
        ))}
        {current && (
          <div className="agent-block live">{current}<span className="caret" /></div>
        )}
      </div>
    </div>
  );
}

/* ── MCP plane (tools + LIVE/DEAD + EKG) ────────────────── */
export function McpPanel() {
  const alive = useStore((s) => s.mcpAlive);
  const tools = useStore((s) => s.tools);
  const taskId = useStore((s) => s.taskId);
  return (
    <div className="panel fragile">
      <div className="panel-hd">
        MCP Server <span className="tag tag-warn">⚠ the AI's link — you'll break this</span>
      </div>
      <div className="panel-bd">
        <div className={`mcp-signal ${alive ? "alive" : "dead"}`}>
          <span className="state">{alive ? "LIVE" : "DEAD"}</span>
          <Ekg alive={alive} />
        </div>
        {taskId && <div className="task-chip">◇ durable task · {taskId.slice(0, 18)}…</div>}
        <div className="tools">
          {tools.length === 0 && <div className="empty">No tool calls yet.</div>}
          {tools.slice().reverse().map((t) => (
            <div className="tool" key={t.id}>
              <span className="nm">{t.name}</span>
              <span className="ar">{JSON.stringify(t.args)}</span>
              <span className={`st ${t.ok === undefined ? "run" : t.ok ? "ok" : "err"}`}>
                {t.ok === undefined ? "···" : t.ok ? "✓" : "✗"}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Ekg({ alive }: { alive: boolean }) {
  if (!alive) {
    return (
      <svg className="ekg" viewBox="0 0 240 34" preserveAspectRatio="none">
        <line x1="0" y1="17" x2="240" y2="17" stroke="var(--red)" strokeWidth="1.5" opacity="0.8" />
      </svg>
    );
  }
  const beat = "M0 17 H30 l5 -11 l5 22 l5 -11 H70 l5 -11 l5 22 l5 -11 H120";
  return (
    <svg className="ekg" viewBox="0 0 120 34" preserveAspectRatio="none">
      <motion.g
        animate={{ x: [0, -120] }}
        transition={{ repeat: Infinity, duration: 1.8, ease: "linear" }}
      >
        <path d={beat} fill="none" stroke="var(--green)" strokeWidth="1.5" />
        <path d={beat} fill="none" stroke="var(--green)" strokeWidth="1.5" transform="translate(120,0)" />
      </motion.g>
    </svg>
  );
}

/* ── Temporal plane (stepper + event history) ───────────── */
const STEP_LABELS: Record<string, string> = {
  scanning: "Scan", diagnosing: "Diagnose", awaiting_approval: "Approve",
  executing: "Execute", done: "Done",
};
export function TemporalPanel() {
  const wf = useStore((s) => s.workflow);
  const history = useStore((s) => s.history);
  const idx = wf ? PHASES.indexOf(wf.phase as any) : -1;
  return (
    <div className="panel">
      <div className="panel-hd">Temporal Plane <span className="tag">durable · survives crashes</span></div>
      <div className="panel-bd">
        <div className="stepper">
          {PHASES.map((p, i) => (
            <span key={p} style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span className={`step ${p === "awaiting_approval" ? "gate" : ""} ${idx > i ? "done" : ""} ${idx === i ? "active" : ""}`}>
                <span className="pip" />{STEP_LABELS[p]}
              </span>
              {i < PHASES.length - 1 && <span className="arr">›</span>}
            </span>
          ))}
        </div>
        <div className="run-id">run · {wf?.run_id ? wf.run_id.slice(0, 24) + "…" : "—"}</div>
        <div className="panel-hd" style={{ border: 0, padding: "6px 0", fontSize: 11 }}>
          Event History <span className="history-tag">the audit log · {history.length} events</span>
        </div>
        <div className="history">
          <AnimatePresence initial={false}>
            {history.slice().reverse().map((h) => (
              <motion.div className="h-row" key={h.id}
                initial={{ opacity: 0, x: -8 }} animate={{ opacity: 1, x: 0 }}>
                <span className="h-id">{h.id}</span>
                <span className="h-lbl">{h.label}</span>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}

/* ── Cluster (pod cards) ────────────────────────────────── */
export function ClusterPanel() {
  const pods = useStore((s) => s.pods);
  return (
    <div className="panel cluster-strip">
      <div className="panel-hd">Kubernetes Cluster <span className="tag">your apps</span></div>
      <div className="panel-bd">
        <div className="pods">
          {pods.length === 0 && <div className="empty">No pods. Hit “Break Pods (Inject Chaos)” to begin.</div>}
          {pods.map((p) => (
            <motion.div layout className={`pod ${p.healthy ? "good" : "bad"}`} key={p.name}
              initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}>
              <div className="app">{p.app}</div>
              <div className="status">{p.healthy ? "● HEALTHY" : "✗ " + p.status}</div>
              <div className="meta">ready {p.ready} · restarts {p.restarts}</div>
              <div className="bar" />
            </motion.div>
          ))}
        </div>
      </div>
    </div>
  );
}

/* ── Approval drawer (HITL) ─────────────────────────────── */
export function ApprovalDrawer() {
  const wf = useStore((s) => s.workflow);
  const send = useStore((s) => s.send);
  if (!wf || !wf.diagnoses?.length) {
    return (
      <div className="panel">
        <div className="panel-hd">Your Approvals <span className="tag">you decide each fix</span></div>
        <div className="panel-bd"><div className="empty">No pending decisions.</div></div>
      </div>
    );
  }
  return (
    <div className="panel">
      <div className="panel-hd">Your Approvals <span className="tag">{wf.needs_approval ? "awaiting you" : "decided"}</span></div>
      <div className="panel-bd">
        <div className="approvals">
          {wf.diagnoses.map((d) => {
            const decision = wf.decisions?.[d.pod_name];
            const skip = d.action === "skip";
            return (
              <div className="appr" key={d.pod_name}>
                <div className="info">
                  <div className="pn">{d.pod_name}</div>
                  <div className="rc">{d.root_cause}</div>
                  <div className="action">→ {d.action}{skip ? " (recommend reject)" : ""}</div>
                </div>
                {decision ? (
                  <span className={`decided ${decision}`}>{decision}</span>
                ) : wf.needs_approval ? (
                  <>
                    <button className="btn mini ok" onClick={() => send({ action: "approve", pod: d.pod_name })}>Approve</button>
                    <button className="btn mini no" onClick={() => send({ action: "reject", pod: d.pod_name })}>Reject</button>
                  </>
                ) : (
                  // Still diagnosing other pods — don't let a human decide a partial
                  // set (the rest are still arriving). Buttons appear once ALL are in.
                  <span className="pending">diagnosing…</span>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
