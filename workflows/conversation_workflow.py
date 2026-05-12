from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.chat_activities import (
        call_claude,
        list_pods_activity,
        get_pod_details_activity,
        get_pod_logs_activity,
        get_pod_events_activity,
    )
    from activities.k8s_activities import scan_cluster, get_pod_details, execute_fix
    from activities.llm_activities import diagnose_pod
    from models import ClaudeRequest, ClaudeResponse, ConversationInput, Diagnosis


# ── Tool definitions for Claude ────────────────────────────────

TOOLS = [
    {
        "name": "list_pods",
        "description": "List all pods in a namespace with status, readiness, and restart count.",
        "input_schema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "description": "Kubernetes namespace", "default": "default"},
            },
        },
    },
    {
        "name": "get_pod_details",
        "description": "Get detailed info about a specific pod: status, container states, conditions, resource limits.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string", "description": "Name of the pod"},
                "namespace": {"type": "string", "description": "Kubernetes namespace", "default": "default"},
            },
            "required": ["pod_name"],
        },
    },
    {
        "name": "get_pod_logs",
        "description": "Get recent log output from a pod.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string", "description": "Name of the pod"},
                "namespace": {"type": "string", "description": "Kubernetes namespace", "default": "default"},
                "tail_lines": {"type": "integer", "description": "Number of recent lines", "default": 50},
            },
            "required": ["pod_name"],
        },
    },
    {
        "name": "get_pod_events",
        "description": "Get Kubernetes events for a pod (warnings, errors, scheduling info).",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string", "description": "Name of the pod"},
                "namespace": {"type": "string", "description": "Kubernetes namespace", "default": "default"},
            },
            "required": ["pod_name"],
        },
    },
    {
        "name": "start_healing",
        "description": "Start healing: scan for unhealthy pods, diagnose each with AI, return proposed fixes. Then use approve_fix or reject_fix for each pod.",
        "input_schema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "description": "Kubernetes namespace to heal", "default": "default"},
            },
        },
    },
    {
        "name": "approve_fix",
        "description": "Approve a proposed fix for a pod. If all pods are decided, executes fixes and returns results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string", "description": "Pod name to approve"},
            },
            "required": ["pod_name"],
        },
    },
    {
        "name": "reject_fix",
        "description": "Reject a proposed fix for a pod. The pod won't be modified. If all pods are decided, returns results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pod_name": {"type": "string", "description": "Pod name to reject"},
            },
            "required": ["pod_name"],
        },
    },
]

SYSTEM_PROMPT = """You are KubeHealer, an AI Kubernetes debugging assistant running in a terminal.

You help users understand and fix issues in their Kubernetes cluster. You have tools to inspect pods, read logs, check events, and run healing workflows.

How to behave:
- Use tools to get real data. Never guess pod names, statuses, or counts.
- Keep responses short — this is a terminal, not a document.
- When the user asks to heal/fix pods, use start_healing to scan and diagnose.
- After start_healing, present the diagnoses clearly and ask which fixes to approve.
- For "skip" actions (like missing ConfigMap), automatically reject them and explain why.
- When the user says "approve all" or "fix everything", approve all fixable pods and reject skip-only ones.
- Always tell the user what you're about to do before taking action.

Healing workflow:
1. start_healing -> scans cluster, diagnoses with AI, returns proposed fixes
2. approve_fix / reject_fix -> for each pod, then fixes are executed automatically
"""

MAX_TURNS = 50
MAX_TOOL_ROUNDS = 20  # Safety cap: max tool-call iterations per user message


@workflow.defn
class ConversationWorkflow:

    def __init__(self):
        self._namespace: str = "default"
        self._session_id: str = ""
        self._messages: list[dict] = []
        self._latest_response: str = ""
        self._waiting_for_input: bool = True
        self._processing: bool = False
        self._turn_count: int = 0
        self._done: bool = False
        self._needs_continue_as_new: bool = False

        # Healing state
        self._healing_diagnoses: list[dict] = []
        self._healing_decisions: dict[str, str] = {}
        self._healing_pending: list[str] = []

    # ── Update: send message and get response back ────────────

    @workflow.update
    async def send_message(self, text: str) -> str:
        """Receive a user message and return Claude's response.

        This is a Temporal Update — the caller blocks until the full
        agentic loop (Claude + tool calls) completes and gets the
        response back directly. No polling needed.
        """
        if text.strip().lower() in ("exit", "quit", "bye"):
            self._done = True
            return "Goodbye!"

        self._waiting_for_input = False
        self._processing = True

        self._messages.append({"role": "user", "content": text})
        self._turn_count += 1

        workflow.logger.info(f"Turn {self._turn_count}: {text[:80]}")

        # Run agentic loop — Claude + tool calls
        await self._run_agentic_loop(self._namespace)

        self._processing = False
        self._waiting_for_input = True

        # Trigger continue-as-new if we've hit the turn limit
        if self._turn_count >= MAX_TURNS:
            self._needs_continue_as_new = True

        return self._latest_response

    @send_message.validator
    def validate_send_message(self, text: str) -> None:
        if not text or not text.strip():
            raise ValueError("Message cannot be empty")
        if self._processing:
            raise ValueError("Already processing a message, please wait")
        if self._needs_continue_as_new:
            raise ValueError("Session is resetting, please retry in a moment")

    # ── Queries (for reconnection and observability) ──────────

    @workflow.query
    def get_state(self) -> dict:
        return {
            "latest_response": self._latest_response,
            "waiting_for_input": self._waiting_for_input,
            "processing": self._processing,
            "turn_count": self._turn_count,
            "messages_count": len(self._messages),
            "healing_pending": list(self._healing_pending),
        }

    @workflow.query
    def get_messages(self) -> list[dict]:
        return list(self._messages)

    # ── Main loop ─────────────────────────────────────────────

    @workflow.run
    async def run(self, input: ConversationInput) -> str:
        self._namespace = input.namespace
        self._session_id = input.session_id

        # Restore state from continue-as-new. Each restore is guarded so we
        # never clobber state already mutated by an update handler that fired
        # before run() reached its first await.
        if input.messages:
            self._messages = list(input.messages)
        if input.healing_diagnoses:
            self._healing_diagnoses = list(input.healing_diagnoses)
        if input.healing_decisions:
            self._healing_decisions = dict(input.healing_decisions)
            decided = set(input.healing_decisions.keys())
            all_pods = {d["pod_name"] for d in input.healing_diagnoses}
            self._healing_pending = list(all_pods - decided)
        if input.turn_count:
            self._turn_count = input.turn_count

        # Wait until exit or continue-as-new is needed
        await workflow.wait_condition(
            lambda: self._done or self._needs_continue_as_new
        )

        if self._needs_continue_as_new:
            trimmed = self._messages[-40:] if len(self._messages) > 40 else list(self._messages)
            workflow.continue_as_new(
                ConversationInput(
                    namespace=self._namespace,
                    session_id=self._session_id,
                    messages=trimmed,
                    healing_diagnoses=self._healing_diagnoses,
                    healing_decisions=self._healing_decisions,
                    turn_count=0,
                )
            )

        return "Conversation ended."

    # ── Agentic tool loop ─────────────────────────────────────

    async def _run_agentic_loop(self, namespace: str) -> None:
        for round_num in range(MAX_TOOL_ROUNDS):
            request = ClaudeRequest(
                messages=self._messages,
                tools=TOOLS,
                system_prompt=SYSTEM_PROMPT,
            )

            response: ClaudeResponse = await workflow.execute_activity(
                call_claude,
                request,
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=RetryPolicy(maximum_attempts=3, backoff_coefficient=2.0),
            )

            # Store assistant response
            self._messages.append({"role": "assistant", "content": response.content})

            # Extract text for display
            text_parts = [b["text"] for b in response.content if b.get("type") == "text"]
            if text_parts:
                self._latest_response = "\n".join(text_parts)

            if response.stop_reason == "end_turn":
                return

            # Process tool calls
            tool_results = []
            for block in response.content:
                if block.get("type") == "tool_use":
                    workflow.logger.info(f"Tool call: {block['name']} (round {round_num + 1}/{MAX_TOOL_ROUNDS})")
                    result_text = await self._execute_tool(block["name"], block["input"], namespace)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": result_text,
                    })

            if tool_results:
                self._messages.append({"role": "user", "content": tool_results})

        # Safety: hit max rounds without end_turn
        workflow.logger.warning(f"Agentic loop hit {MAX_TOOL_ROUNDS} rounds cap, forcing stop")
        self._latest_response = self._latest_response or "(Agent reached maximum tool call limit)"

    # ── Tool dispatch ─────────────────────────────────────────

    async def _execute_tool(self, name: str, tool_input: dict, namespace: str) -> str:
        try:
            if name == "list_pods":
                return await workflow.execute_activity(
                    list_pods_activity,
                    tool_input.get("namespace", namespace),
                    start_to_close_timeout=timedelta(seconds=30),
                )

            elif name == "get_pod_details":
                return await workflow.execute_activity(
                    get_pod_details_activity,
                    args=[tool_input["pod_name"], tool_input.get("namespace", namespace)],
                    start_to_close_timeout=timedelta(seconds=30),
                )

            elif name == "get_pod_logs":
                return await workflow.execute_activity(
                    get_pod_logs_activity,
                    args=[tool_input["pod_name"], tool_input.get("namespace", namespace), tool_input.get("tail_lines", 50)],
                    start_to_close_timeout=timedelta(seconds=30),
                )

            elif name == "get_pod_events":
                return await workflow.execute_activity(
                    get_pod_events_activity,
                    args=[tool_input["pod_name"], tool_input.get("namespace", namespace)],
                    start_to_close_timeout=timedelta(seconds=30),
                )

            elif name == "start_healing":
                return await self._handle_start_healing(tool_input.get("namespace", namespace))

            elif name == "approve_fix":
                return await self._handle_approve_fix(tool_input["pod_name"])

            elif name == "reject_fix":
                return await self._handle_reject_fix(tool_input["pod_name"])

            else:
                return f"Unknown tool: {name}"

        except Exception as e:
            return f"Error executing {name}: {e}"

    # ── Healing logic ─────────────────────────────────────────

    async def _handle_start_healing(self, namespace: str) -> str:
        if self._healing_pending:
            pending = ", ".join(sorted(self._healing_pending))
            return f"Healing already active with pending decisions for: {pending}\nUse approve_fix/reject_fix first."

        self._healing_diagnoses = []
        self._healing_decisions = {}
        self._healing_pending = []

        issues = await workflow.execute_activity(
            scan_cluster, namespace,
            start_to_close_timeout=timedelta(seconds=30),
        )

        if not issues:
            return "All pods are healthy! Nothing to fix."

        for issue in issues:
            details = await workflow.execute_activity(
                get_pod_details,
                args=[issue.name, issue.namespace],
                start_to_close_timeout=timedelta(seconds=30),
            )

            diagnosis: Diagnosis = await workflow.execute_activity(
                diagnose_pod, details,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=3, backoff_coefficient=2.0),
            )

            diag_dict = {
                "pod_name": diagnosis.pod_name,
                "namespace": namespace,
                "root_cause": diagnosis.root_cause,
                "severity": diagnosis.severity,
                "action": diagnosis.action,
                "explanation": diagnosis.explanation,
                "fix_details": diagnosis.fix_details,
            }
            self._healing_diagnoses.append(diag_dict)
            self._healing_pending.append(diagnosis.pod_name)

        lines = [f"Found {len(self._healing_diagnoses)} issue(s):\n"]
        for i, d in enumerate(self._healing_diagnoses, 1):
            lines.append(f"  {i}. {d['pod_name']}")
            lines.append(f"     Severity: {d['severity'].upper()}")
            lines.append(f"     Root Cause: {d['root_cause']}")
            lines.append(f"     Action: {d['action']}")
            lines.append(f"     Explanation: {d['explanation']}")
            if d["fix_details"]:
                lines.append(f"     Fix Details: {d['fix_details']}")
            lines.append("")

        lines.append(f"Pending approval: {', '.join(sorted(self._healing_pending))}")
        return "\n".join(lines)

    async def _handle_approve_fix(self, pod_name: str) -> str:
        if not self._healing_pending:
            return "No active healing session. Use start_healing first."
        if pod_name not in self._healing_pending:
            return f"'{pod_name}' is not pending. Pending: {', '.join(sorted(self._healing_pending)) or 'none'}"

        self._healing_decisions[pod_name] = "approved"
        self._healing_pending.remove(pod_name)

        if not self._healing_pending:
            return await self._execute_all_fixes()
        return f"Approved '{pod_name}'. Still pending: {', '.join(sorted(self._healing_pending))}"

    async def _handle_reject_fix(self, pod_name: str) -> str:
        if not self._healing_pending:
            return "No active healing session. Use start_healing first."
        if pod_name not in self._healing_pending:
            return f"'{pod_name}' is not pending. Pending: {', '.join(sorted(self._healing_pending)) or 'none'}"

        self._healing_decisions[pod_name] = "rejected"
        self._healing_pending.remove(pod_name)

        if not self._healing_pending:
            return await self._execute_all_fixes()
        return f"Rejected '{pod_name}'. Still pending: {', '.join(sorted(self._healing_pending))}"

    async def _execute_all_fixes(self) -> str:
        lines = ["All decisions made. Executing fixes...\n"]

        for diag_dict in self._healing_diagnoses:
            decision = self._healing_decisions.get(diag_dict["pod_name"], "rejected")

            if decision == "approved" and diag_dict["action"] != "skip":
                diagnosis = Diagnosis(
                    pod_name=diag_dict["pod_name"],
                    root_cause=diag_dict["root_cause"],
                    severity=diag_dict["severity"],
                    action=diag_dict["action"],
                    explanation=diag_dict["explanation"],
                    fix_details=diag_dict["fix_details"],
                    namespace=diag_dict.get("namespace", "default"),
                )
                result = await workflow.execute_activity(
                    execute_fix, diagnosis,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                icon = "OK" if result.success else "--"
                lines.append(f"  [{icon}] {result.pod_name}: {result.action_taken} — {result.details}")
            else:
                reason = diag_dict["explanation"] if diag_dict["action"] == "skip" else "Rejected by user"
                lines.append(f"  [--] {diag_dict['pod_name']}: skipped — {reason}")

        # Reset healing state
        self._healing_diagnoses = []
        self._healing_decisions = {}
        self._healing_pending = []

        return "\n".join(lines)
