"""Regression tests for cli.get_or_start_workflow.

The bug: previously the function used `handle.query(...)` to detect a live
workflow. But Temporal lets you query *completed* workflows too, so a dead
workflow would masquerade as live and every subsequent message would loop
on "Session expired. Starting fresh...".

The fix: use `handle.describe()` and only reuse the handle when status is
RUNNING. These tests pin that behavior.
"""

import pytest
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.worker import Worker

import cli
from models import ClaudeResponse, ConversationInput
from workflows.conversation_workflow import ConversationWorkflow


TASK_QUEUE = "kubehealer"  # cli.py hardcodes this — we must match


# Minimal stub set so ConversationWorkflow can be hosted by a worker.
# These tests don't drive the agentic loop — they just need the workflow
# to start, accept "exit", and complete.

@activity.defn(name="call_claude")
async def call_claude_noop(request) -> ClaudeResponse:
    return ClaudeResponse(
        stop_reason="end_turn",
        content=[{"type": "text", "text": "ok"}],
    )


@activity.defn(name="list_pods_activity")
async def _list_pods(namespace: str) -> str: return ""
@activity.defn(name="get_pod_details_activity")
async def _get_pod_details(pod_name: str, namespace: str) -> str: return ""
@activity.defn(name="get_pod_logs_activity")
async def _get_pod_logs(pod_name: str, namespace: str, tail_lines: int) -> str: return ""
@activity.defn(name="get_pod_events_activity")
async def _get_pod_events(pod_name: str, namespace: str) -> str: return ""
@activity.defn(name="scan_cluster")
async def _scan_cluster(namespace: str): return []
@activity.defn(name="get_pod_details")
async def _get_details(pod_name: str, namespace: str) -> str: return ""
@activity.defn(name="diagnose_pod")
async def _diagnose(pod_details: str): raise NotImplementedError
@activity.defn(name="execute_fix")
async def _execute(diagnosis): raise NotImplementedError


STUB_ACTIVITIES = [
    call_claude_noop,
    _list_pods, _get_pod_details, _get_pod_logs, _get_pod_events,
    _scan_cluster, _get_details, _diagnose, _execute,
]


class TestGetOrStartWorkflow:
    async def test_starts_fresh_when_no_workflow_exists(self, workflow_env, monkeypatch):
        """First call ever for this id → new workflow is started."""
        monkeypatch.setattr(cli, "WORKFLOW_ID", "test-cli-fresh")

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=STUB_ACTIVITIES,
        ):
            handle, is_new = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            assert is_new is True

            desc = await handle.describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING

            # Tear down
            await handle.execute_update(ConversationWorkflow.send_message, "exit")
            await handle.result()

    async def test_reuses_handle_when_workflow_is_running(self, workflow_env, monkeypatch):
        """Second call while the workflow is alive → same run, is_new=False."""
        monkeypatch.setattr(cli, "WORKFLOW_ID", "test-cli-reuse")

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=STUB_ACTIVITIES,
        ):
            handle1, is_new1 = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            assert is_new1 is True
            run_id_1 = handle1.first_execution_run_id

            handle2, is_new2 = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            assert is_new2 is False
            # Same workflow id, and the live one wasn't replaced
            assert handle2.id == handle1.id

            desc = await handle2.describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING
            assert desc.run_id == run_id_1

            await handle2.execute_update(ConversationWorkflow.send_message, "exit")
            await handle2.result()

    async def test_starts_new_run_after_previous_completed(
        self, workflow_env, monkeypatch
    ):
        """The bug fix: completed workflow must NOT be reused — start fresh.

        Before the fix, this test would have failed: handle.query() succeeds
        on completed workflows, so get_or_start_workflow would have returned
        the dead handle and is_new=False. Now we use describe()+status, so
        a completed workflow is correctly recognized as not-running.
        """
        monkeypatch.setattr(cli, "WORKFLOW_ID", "test-cli-completed")

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=STUB_ACTIVITIES,
        ):
            # Run 1: start and let it complete via "exit"
            handle1, _ = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            await handle1.execute_update(ConversationWorkflow.send_message, "exit")
            await handle1.result()

            desc1 = await handle1.describe()
            assert desc1.status == WorkflowExecutionStatus.COMPLETED
            run_id_1 = desc1.run_id

            # Run 2: now ask again — fix should detect non-RUNNING and start fresh
            handle2, is_new2 = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            assert is_new2 is True, (
                "Bug regression: completed workflow was reused instead of "
                "starting a fresh one"
            )

            desc2 = await handle2.describe()
            assert desc2.status == WorkflowExecutionStatus.RUNNING
            assert desc2.run_id != run_id_1

            await handle2.execute_update(ConversationWorkflow.send_message, "exit")
            await handle2.result()

    async def test_starts_new_run_after_previous_terminated(
        self, workflow_env, monkeypatch
    ):
        """Same fix path for terminated workflows (e.g., the stale-session branch)."""
        monkeypatch.setattr(cli, "WORKFLOW_ID", "test-cli-terminated")

        async with Worker(
            workflow_env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow],
            activities=STUB_ACTIVITIES,
        ):
            handle1, _ = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            await handle1.terminate(reason="test")

            handle2, is_new2 = await cli.get_or_start_workflow(
                workflow_env.client, "default"
            )
            assert is_new2 is True

            desc = await handle2.describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING

            await handle2.execute_update(ConversationWorkflow.send_message, "exit")
            await handle2.result()
