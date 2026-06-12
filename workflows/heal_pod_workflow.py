"""HealPodWorkflow — one durable child workflow per pod being fixed.

The parent HealerWorkflow spawns one of these per approved pod, which gives the
Temporal Web UI a parent → children tree to look at. Each child also runs the
parts of a heal that take *time* and can *fail*: apply the fix, let the rollout
settle (a visible Timer), then verify the pod actually recovered (a heartbeating,
retrying activity). Those are exactly the steps that keep the Web UI moving while
the MCP server is being killed and restarted.

A pod that never goes healthy must NOT crash the whole heal: a failed verify is
caught and reported as "applied; not yet confirmed healthy" rather than raising.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from activities.k8s_activities import execute_fix, verify_healed
    from models import Diagnosis
    from search_attributes import HEAL_PHASE, POD_NAME

# Actions that mutate nothing (or never ran) — there's no rollout to verify.
_NO_VERIFY = {"skip", "validation_failed"}


@workflow.defn
class HealPodWorkflow:

    @workflow.run
    async def run(self, diagnosis: Diagnosis, track_phase: bool = True) -> dict:
        def _phase(value: str) -> None:
            if track_phase:
                workflow.upsert_search_attributes(
                    [POD_NAME.value_set(diagnosis.pod_name), HEAL_PHASE.value_set(value)]
                )

        _phase("fixing")
        result = await workflow.execute_activity(
            execute_fix,
            diagnosis,
            start_to_close_timeout=timedelta(seconds=30),
        )

        result_dict = {
            "pod_name": result.pod_name,
            "success": result.success,
            "action_taken": result.action_taken,
            "details": result.details,
        }

        # Nothing applied (skip / validation failure / no deployment) → nothing to verify.
        if not result.success or result.action_taken in _NO_VERIFY or not result.deployment:
            _phase("done")
            return result_dict

        # Give the rollout a moment before checking — a deliberate, visible Timer in
        # the event history (TimerStarted → TimerFired) rather than a busy-wait.
        _phase("settling")
        await workflow.sleep(timedelta(seconds=5))

        # Verify the pod really recovered. Heartbeats + retry backoff are what the
        # Web UI's Pending Activities panel renders live during the MCP-down window.
        _phase("verifying")
        try:
            await workflow.execute_activity(
                verify_healed,
                args=[result.deployment, diagnosis.namespace],
                start_to_close_timeout=timedelta(seconds=30),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=8),
                    maximum_attempts=15,
                ),
            )
            _phase("healed")
            result_dict["details"] += " (verified healthy)"
        except ActivityError:
            # Fix was applied but the rollout never settled in time. Don't fail the
            # heal over it — report honestly and let the parent move on.
            _phase("unconfirmed")
            result_dict["details"] += " (applied; not yet confirmed healthy)"

        return result_dict
