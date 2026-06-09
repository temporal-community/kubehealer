"""Regression test for HealerWorkflow's human-approval wait.

The bug: the approval gate used `wait_condition(self._all_decided,
timeout=timedelta(days=2))`. wait_condition RAISES asyncio.TimeoutError on
timeout (it does not return False), so an un-approved heal would CRASH after two
days instead of waiting — defeating the durability guarantee the whole demo is
about.

The fix: an indefinite `wait_condition(self._all_decided)` (no timeout). These
tests pin that a heal parked at awaiting_approval stays RUNNING across a long
time-skip and still completes when finally approved.
"""

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from datetime import timedelta

from models import Diagnosis, HealResult, HealerInput, PodIssue
from workflows.healer_workflow import HealerWorkflow

TASK_QUEUE = "kubehealer-test"


def _wf_id(name: str) -> str:
    return f"test-{name}-{uuid.uuid4().hex[:8]}"


# ── Stub activities (resolve by production name) ──────────────────────────────


@activity.defn(name="scan_cluster")
async def scan_one(namespace: str) -> list[PodIssue]:
    return [PodIssue(name="bad-pod", namespace=namespace, status="ImagePullBackOff",
                     reason="ImagePullBackOff", message="bad image")]


@activity.defn(name="get_pod_details")
async def details(pod_name: str, namespace: str) -> str:
    return f"(details for {pod_name})"


@activity.defn(name="diagnose_pod")
async def diagnose(pod_details: str) -> Diagnosis:
    return Diagnosis(pod_name="bad-pod", root_cause="image typo", severity="high",
                     action="fix_image", explanation="latestt is a typo",
                     fix_details={"image": "nginx:latest"}, namespace="default")


@activity.defn(name="execute_fix")
async def fix(diagnosis: Diagnosis) -> HealResult:
    return HealResult(pod_name=diagnosis.pod_name, success=True,
                      action_taken=diagnosis.action, details="patched")


ACTIVITIES = [scan_one, details, diagnose, fix]


async def _await_phase(handle, phase: str, tries: int = 60):
    for _ in range(tries):
        state = await handle.query(HealerWorkflow.get_state)
        if state["phase"] == phase:
            return state
        await asyncio.sleep(0.1)
    raise AssertionError(f"workflow never reached phase {phase!r}")


async def test_heal_survives_multiday_wait_then_completes():
    """A heal parked at awaiting_approval must NOT time out — it waits, then heals.

    Uses its OWN time-skipping environment: advancing the clock 3 days would
    otherwise pollute the session-scoped `workflow_env` shared by other tests.
    """
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        client = env.client
        async with Worker(client, task_queue=TASK_QUEUE,
                          workflows=[HealerWorkflow], activities=ACTIVITIES):
            handle = await client.start_workflow(
                HealerWorkflow.run,
                HealerInput(namespace="default", auto_approve=False),
                id=_wf_id("heal-wait"), task_queue=TASK_QUEUE,
            )
            await _await_phase(handle, "awaiting_approval")

            # Skip far past the old 2-day timeout. With the bug this fires the timer
            # and the workflow FAILS; with the fix there is no timer, so it waits.
            await env.sleep(timedelta(days=3))

            desc = await handle.describe()
            assert desc.status == WorkflowExecutionStatus.RUNNING

            # Now approve — the durable wait releases and the heal finishes.
            await handle.signal(HealerWorkflow.approve_pod, "bad-pod")
            result = await handle.result()
            assert "Healed 1/1" in result
    finally:
        await env.shutdown()


async def test_reject_skips_execution(workflow_env):
    """Sanity: rejecting the only pod completes the heal with nothing fixed."""
    client = workflow_env.client
    async with Worker(client, task_queue=TASK_QUEUE,
                      workflows=[HealerWorkflow], activities=ACTIVITIES):
        handle = await client.start_workflow(
            HealerWorkflow.run,
            HealerInput(namespace="default", auto_approve=False),
            id=_wf_id("heal-reject"), task_queue=TASK_QUEUE,
        )
        await _await_phase(handle, "awaiting_approval")
        await handle.signal(HealerWorkflow.reject_pod, "bad-pod")
        result = await handle.result()
        assert "Healed 0/1" in result
