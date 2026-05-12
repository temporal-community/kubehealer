"""Integration tests for ConversationWorkflow.

We register the real workflow against a time-skipping WorkflowEnvironment,
but every activity is replaced by a stub registered under the same name.
This isolates the workflow's orchestration logic (agentic loop, healing
state machine, validator) from external dependencies (Claude API, k8s).
"""

import uuid
from typing import Any
from unittest.mock import patch

import pytest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.worker import Worker

from models import (
    ClaudeRequest,
    ClaudeResponse,
    ConversationInput,
    Diagnosis,
    HealResult,
    PodIssue,
)
from workflows.conversation_workflow import ConversationWorkflow


TASK_QUEUE = "kubehealer-test"


def _wf_id(name: str) -> str:
    """Unique id per test so the session-scoped env can run them in parallel."""
    return f"test-{name}-{uuid.uuid4().hex[:8]}"


# ── Stub activity factories ──────────────────────────────────────
# Each test composes the set of stubs it needs. Stubs use the same
# `name=` as the production activities so workflow-side `execute_activity`
# calls resolve to them.


def make_call_claude_stub(responses: list[ClaudeResponse]):
    """Returns a stubbed call_claude that yields `responses` in order.

    The list captures the agentic loop: e.g. [tool_use_response, end_turn_response].
    """
    state = {"i": 0, "requests": []}

    @activity.defn(name="call_claude")
    async def stub(request: ClaudeRequest) -> ClaudeResponse:
        state["requests"].append(request)
        i = state["i"]
        if i >= len(responses):
            # Default: end the turn so the loop doesn't run away
            return ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "(default end)"}],
            )
        state["i"] += 1
        return responses[i]

    stub.calls = state  # tests can introspect
    return stub


@activity.defn(name="list_pods_activity")
async def list_pods_stub(namespace: str) -> str:
    return f"(stubbed pod list for {namespace})"


@activity.defn(name="get_pod_details_activity")
async def get_pod_details_stub(pod_name: str, namespace: str) -> str:
    return f"(stubbed details for {pod_name})"


@activity.defn(name="get_pod_logs_activity")
async def get_pod_logs_stub(pod_name: str, namespace: str, tail_lines: int) -> str:
    return f"(stubbed logs for {pod_name}, tail={tail_lines})"


@activity.defn(name="get_pod_events_activity")
async def get_pod_events_stub(pod_name: str, namespace: str) -> str:
    return f"(stubbed events for {pod_name})"


@activity.defn(name="scan_cluster")
async def scan_cluster_stub(namespace: str) -> list[PodIssue]:
    return [
        PodIssue(
            name="bad-pod",
            namespace=namespace,
            status="CrashLoopBackOff",
            reason="CrashLoopBackOff",
            message="container failed",
        )
    ]


@activity.defn(name="get_pod_details")
async def get_pod_details_k8s_stub(pod_name: str, namespace: str) -> str:
    return f"(details for {pod_name})"


@activity.defn(name="diagnose_pod")
async def diagnose_pod_stub(pod_details: str) -> Diagnosis:
    return Diagnosis(
        pod_name="bad-pod",
        root_cause="image bad",
        severity="high",
        action="fix_image",
        explanation="latestt is a typo",
        fix_details={"image": "nginx:latest"},
        namespace="default",
    )


@activity.defn(name="execute_fix")
async def execute_fix_stub(diagnosis: Diagnosis) -> HealResult:
    return HealResult(
        pod_name=diagnosis.pod_name,
        success=True,
        action_taken=diagnosis.action,
        details=f"applied {diagnosis.fix_details}",
    )


def base_activities(call_claude_stub) -> list[Any]:
    """Activity registrations needed by every workflow run."""
    return [
        call_claude_stub,
        list_pods_stub,
        get_pod_details_stub,
        get_pod_logs_stub,
        get_pod_events_stub,
        scan_cluster_stub,
        get_pod_details_k8s_stub,
        diagnose_pod_stub,
        execute_fix_stub,
    ]


# ── Tests ────────────────────────────────────────────────────────


class TestSendMessageBasics:
    async def test_simple_end_turn_returns_text(self, workflow_env):
        """Single Claude call with stop_reason=end_turn → response returned to caller."""
        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "Hello, world!"}],
            ),
        ])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("simple-end-turn")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            response = await handle.execute_update(
                ConversationWorkflow.send_message, "say hi"
            )
            assert response == "Hello, world!"

            # Cleanly end the workflow
            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            assert await handle.result() == "Conversation ended."

    async def test_validator_rejects_empty_message(self, workflow_env):
        """The send_message validator should refuse blank input."""
        call_claude = make_call_claude_stub([])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("empty-msg")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update(ConversationWorkflow.send_message, "   ")

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()


class TestAgenticLoop:
    async def test_tool_use_round_then_end_turn(self, workflow_env):
        """Workflow handles tool_use → tool_result → end_turn correctly."""
        # Round 1: Claude asks to call list_pods
        # Round 2: Claude returns final text
        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "tool_abc",
                        "name": "list_pods",
                        "input": {"namespace": "default"},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "There is 1 pod."}],
            ),
        ])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("tool-use")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            response = await handle.execute_update(
                ConversationWorkflow.send_message, "list pods"
            )

            assert response == "There is 1 pod."
            assert call_claude.calls["i"] == 2  # both rounds happened

            # Second call's request should include the tool_result
            second_request = call_claude.calls["requests"][1]
            tool_result_msg = second_request.messages[-1]
            assert tool_result_msg["role"] == "user"
            assert tool_result_msg["content"][0]["type"] == "tool_result"
            assert tool_result_msg["content"][0]["tool_use_id"] == "tool_abc"
            assert "stubbed pod list" in tool_result_msg["content"][0]["content"]

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()

    async def test_unknown_tool_returns_error_string_not_crash(self, workflow_env):
        """An unrecognized tool name should surface as a tool_result, not crash the loop."""
        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "tool_x",
                        "name": "definitely_not_a_real_tool",
                        "input": {},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "ok"}],
            ),
        ])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("unknown-tool")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            response = await handle.execute_update(
                ConversationWorkflow.send_message, "do something weird"
            )
            assert response == "ok"

            second_request = call_claude.calls["requests"][1]
            tool_result = second_request.messages[-1]["content"][0]
            assert "Unknown tool" in tool_result["content"]

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()


class TestHealingFlow:
    async def test_start_healing_then_approve_fix_executes(self, workflow_env):
        """Full healing path: scan → diagnose → approve → execute_fix → success."""
        # Round 1: start_healing
        # Round 2: text after diagnoses are presented (LLM summarizes)
        # Round 3: approve_fix for bad-pod
        # Round 4: final text after fix executed
        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "t1",
                        "name": "start_healing",
                        "input": {"namespace": "default"},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "Found 1 issue. Approve?"}],
            ),
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "t2",
                        "name": "approve_fix",
                        "input": {"pod_name": "bad-pod"},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "Fixed the pod."}],
            ),
        ])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("heal-approve")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            # First user turn triggers start_healing
            r1 = await handle.execute_update(
                ConversationWorkflow.send_message, "heal my cluster"
            )
            assert "Found 1 issue" in r1

            # Second user turn approves the fix
            r2 = await handle.execute_update(
                ConversationWorkflow.send_message, "yes approve"
            )
            assert "Fixed the pod" in r2

            # The approve_fix tool_result should mention the OK marker from execute_fix
            approve_response_request = call_claude.calls["requests"][3]
            approve_tool_result = approve_response_request.messages[-1]["content"][0]
            assert "[OK]" in approve_tool_result["content"]
            assert "applied" in approve_tool_result["content"]

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()

    async def test_reject_fix_skips_execution(self, workflow_env):
        """Rejected fixes must NOT call execute_fix."""
        execute_fix_calls = {"n": 0}

        @activity.defn(name="execute_fix")
        async def counting_execute_fix(diagnosis: Diagnosis) -> HealResult:
            execute_fix_calls["n"] += 1
            return HealResult(
                pod_name=diagnosis.pod_name,
                success=True,
                action_taken=diagnosis.action,
                details="should not happen",
            )

        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "t1",
                        "name": "start_healing",
                        "input": {"namespace": "default"},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "Diagnosed."}],
            ),
            ClaudeResponse(
                stop_reason="tool_use",
                content=[
                    {
                        "type": "tool_use",
                        "id": "t2",
                        "name": "reject_fix",
                        "input": {"pod_name": "bad-pod"},
                    }
                ],
            ),
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "Skipped."}],
            ),
        ])

        activities = [
            call_claude,
            list_pods_stub,
            get_pod_details_stub,
            get_pod_logs_stub,
            get_pod_events_stub,
            scan_cluster_stub,
            get_pod_details_k8s_stub,
            diagnose_pod_stub,
            counting_execute_fix,  # replaces the default execute_fix_stub
        ]

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=activities,
        ):
            wf_id = _wf_id("heal-reject")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            await handle.execute_update(
                ConversationWorkflow.send_message, "heal my cluster"
            )
            r2 = await handle.execute_update(
                ConversationWorkflow.send_message, "no, reject"
            )
            assert "Skipped" in r2
            assert execute_fix_calls["n"] == 0

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()


class TestExitAndQueries:
    async def test_exit_completes_workflow(self, workflow_env):
        call_claude = make_call_claude_stub([])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("exit")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            response = await handle.execute_update(
                ConversationWorkflow.send_message, "exit"
            )
            assert response == "Goodbye!"
            assert await handle.result() == "Conversation ended."
            # call_claude must NOT have been invoked — exit short-circuits
            assert call_claude.calls["i"] == 0

    async def test_get_state_query_reflects_progress(self, workflow_env):
        call_claude = make_call_claude_stub([
            ClaudeResponse(
                stop_reason="end_turn",
                content=[{"type": "text", "text": "first reply"}],
            ),
        ])

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=base_activities(call_claude),
        ):
            wf_id = _wf_id("query-state")
            handle = await workflow_env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationInput(namespace="default", session_id=wf_id),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )

            await handle.execute_update(
                ConversationWorkflow.send_message, "hello"
            )

            state = await handle.query(ConversationWorkflow.get_state)
            assert state["latest_response"] == "first reply"
            assert state["turn_count"] == 1
            assert state["waiting_for_input"] is True

            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()
