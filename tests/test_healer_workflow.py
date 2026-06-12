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
from workflows.heal_pod_workflow import HealPodWorkflow

TASK_QUEUE = "kubehealer-test"
WORKFLOWS = [HealerWorkflow, HealPodWorkflow]  # parent + per-pod child


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
                      action_taken=diagnosis.action, details="patched", deployment="dep")


@activity.defn(name="verify_healed")
async def verify_ok(deployment_name: str, namespace: str) -> bool:
    return True


ACTIVITIES = [scan_one, details, diagnose, fix, verify_ok]


# Two-pod stubs (for the incremental-approval test): details echoes the pod name so
# diagnose can produce a distinct Diagnosis per pod.
@activity.defn(name="scan_cluster")
async def scan_two(namespace: str) -> list[PodIssue]:
    return [
        PodIssue(name="pod-a", namespace=namespace, status="ImagePullBackOff",
                 reason="ImagePullBackOff", message="a"),
        PodIssue(name="pod-b", namespace=namespace, status="OOMKilled",
                 reason="OOMKilled", message="b"),
    ]


@activity.defn(name="get_pod_details")
async def details_echo(pod_name: str, namespace: str) -> str:
    return pod_name  # echo so diagnose can key off it


@activity.defn(name="diagnose_pod")
async def diagnose_echo(pod_details: str) -> Diagnosis:
    return Diagnosis(pod_name=pod_details, root_cause="x", severity="high",
                     action="fix_image", explanation="e",
                     fix_details={"image": "nginx:latest"}, namespace="default")


ACTIVITIES_TWO = [scan_two, details_echo, diagnose_echo, fix, verify_ok]


# Flaky verify: fails the first two attempts, then reports healthy — exercises the
# child workflow's verify_healed RetryPolicy (the backoff is what the Web UI shows).
_verify_calls = {"n": 0}


@activity.defn(name="verify_healed")
async def verify_flaky(deployment_name: str, namespace: str) -> bool:
    _verify_calls["n"] += 1
    if _verify_calls["n"] < 3:
        raise RuntimeError("rollout not healthy yet")
    return True


ACTIVITIES_FLAKY = [scan_one, details, diagnose, fix, verify_flaky]


async def _await_phase(handle, phase: str, tries: int = 60):
    for _ in range(tries):
        state = await handle.query(HealerWorkflow.get_state)
        if state["phase"] == phase:
            return state
        await asyncio.sleep(0.1)
    raise AssertionError(f"workflow never reached phase {phase!r}")


async def _await_result(handle, pod: str, tries: int = 60):
    for _ in range(tries):
        state = await handle.query(HealerWorkflow.get_state)
        if any(r["pod_name"] == pod for r in state["results"]):
            return state
        await asyncio.sleep(0.1)
    raise AssertionError(f"pod {pod} never executed")


async def test_heal_survives_multiday_wait_then_completes():
    """A heal parked at awaiting_approval must NOT time out — it waits, then heals.

    Uses its OWN time-skipping environment: advancing the clock 3 days would
    otherwise pollute the session-scoped `workflow_env` shared by other tests.
    """
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        client = env.client
        async with Worker(client, task_queue=TASK_QUEUE,
                          workflows=WORKFLOWS, activities=ACTIVITIES):
            handle = await client.start_workflow(
                HealerWorkflow.run,
                HealerInput(namespace="default", auto_approve=False, track_phase=False),
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
                      workflows=WORKFLOWS, activities=ACTIVITIES):
        handle = await client.start_workflow(
            HealerWorkflow.run,
            HealerInput(namespace="default", auto_approve=False, track_phase=False),
            id=_wf_id("heal-reject"), task_queue=TASK_QUEUE,
        )
        await _await_phase(handle, "awaiting_approval")
        await handle.signal(HealerWorkflow.reject_pod, "bad-pod")
        result = await handle.result()
        assert "Healed 0/1" in result


async def test_each_pod_heals_as_it_is_approved(workflow_env):
    """Incremental HITL: approving one pod applies its fix IMMEDIATELY while the
    other stays pending — pods heal click-by-click, not all-at-once at the end."""
    client = workflow_env.client
    async with Worker(client, task_queue=TASK_QUEUE,
                      workflows=WORKFLOWS, activities=ACTIVITIES_TWO):
        handle = await client.start_workflow(
            HealerWorkflow.run,
            HealerInput(namespace="default", auto_approve=False, track_phase=False),
            id=_wf_id("heal-incr"), task_queue=TASK_QUEUE,
        )
        await _await_phase(handle, "awaiting_approval")

        # Approve only pod-a → its fix runs now, while pod-b is still undecided.
        await handle.signal(HealerWorkflow.approve_pod, "pod-a")
        state = await _await_result(handle, "pod-a")
        results = {r["pod_name"]: r for r in state["results"]}
        assert results["pod-a"]["success"] is True
        assert "pod-b" not in results                      # pod-b not touched yet
        assert state["phase"] == "awaiting_approval"        # still waiting on pod-b

        # Approve pod-b → the heal completes.
        await handle.signal(HealerWorkflow.approve_pod, "pod-b")
        result = await handle.result()
        assert "Healed 2/2" in result


async def test_child_retries_verify_until_healthy(workflow_env):
    """The per-pod child workflow keeps retrying verify_healed until the rollout is
    healthy — a fix isn't 'done' until recovery is confirmed. Time-skipping fast-
    forwards the retry backoff; the heal still completes and counts the pod healed."""
    _verify_calls["n"] = 0
    client = workflow_env.client
    async with Worker(client, task_queue=TASK_QUEUE,
                      workflows=WORKFLOWS, activities=ACTIVITIES_FLAKY):
        handle = await client.start_workflow(
            HealerWorkflow.run,
            HealerInput(namespace="default", auto_approve=False, track_phase=False),
            id=_wf_id("heal-verify"), task_queue=TASK_QUEUE,
        )
        await _await_phase(handle, "awaiting_approval")
        await handle.signal(HealerWorkflow.approve_pod, "bad-pod")
        result = await handle.result()
        assert "Healed 1/1" in result
        assert _verify_calls["n"] >= 3              # verify was retried, not one-shot
        state = await handle.query(HealerWorkflow.get_state)
        details = state["results"][0]["details"]
        assert "verified healthy" in details
