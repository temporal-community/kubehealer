export type Pod = {
  name: string;
  app: string;
  status: string;
  ready: string;
  restarts: number;
  healthy: boolean;
};

export type Diagnosis = {
  pod_name: string;
  root_cause: string;
  severity: string;
  action: string;
  explanation: string;
  fix_details?: Record<string, unknown>;
};

export type Result = {
  pod_name: string;
  success: boolean;
  action_taken: string;
  details: string;
};

export type Workflow = {
  phase: string;
  run_id: string | null;
  diagnoses: Diagnosis[];
  decisions: Record<string, string>;
  results: Result[];
  needs_approval: boolean;
  undecided_pods: string[];
};

export type ToolCall = {
  id: number;
  name: string;
  args: Record<string, unknown>;
  ok?: boolean;
  summary?: string;
};

export type HistoryEvent = { id: number; label: string; ts: string | null };
export type LogLine = { level: string; msg: string; t: number };

export const PHASES = ["scanning", "diagnosing", "awaiting_approval", "executing", "done"] as const;
