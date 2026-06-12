import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.k8s_activities import scan_cluster, get_pod_details
    from activities.llm_activities import diagnose_pod
    from workflows.heal_pod_workflow import HealPodWorkflow
    from models import Diagnosis, HealerInput
    from search_attributes import (
        HEAL_NAMESPACE,
        HEAL_PHASE,
        PODS_HEALED,
        PODS_TOTAL,
    )

# diagnose_pod talks to the Anthropic API — retry transient rate-limit / 5xx errors.
from temporalio.common import RetryPolicy

_DIAGNOSE_RETRY = RetryPolicy(maximum_attempts=3, backoff_coefficient=2.0)


@workflow.defn
class HealerWorkflow:

    def __init__(self):
        self._phase: str = "starting"
        self._diagnoses: list[Diagnosis] = []
        self._decisions: dict[str, str] = {}
        self._results: list[dict] = []
        self._healed: int = 0
        self._track_phase: bool = True

    # ── Query ──────────────────────────────────────────────────

    @workflow.query
    def get_state(self) -> dict:
        return {
            "phase": self._phase,
            "diagnoses": [
                {
                    "pod_name": d.pod_name,
                    "root_cause": d.root_cause,
                    "severity": d.severity,
                    "action": d.action,
                    "explanation": d.explanation,
                    "fix_details": d.fix_details,
                }
                for d in self._diagnoses
            ],
            "decisions": dict(self._decisions),
            "results": list(self._results),
        }

    # ── Signals ────────────────────────────────────────────────

    @workflow.signal
    async def approve_pod(self, pod_name: str) -> None:
        self._decisions[pod_name] = "approved"

    @workflow.signal
    async def reject_pod(self, pod_name: str) -> None:
        self._decisions[pod_name] = "rejected"

    # ── Search-attribute helper ────────────────────────────────

    def _set_phase(self, phase: str, *extra) -> None:
        """Advance the phase and mirror it into the Web UI's Search Attributes.

        `self._phase` (the dashboard's query contract) is always updated; the custom
        Search Attributes are only emitted when tracking is on (see HealerInput).
        """
        self._phase = phase
        if self._track_phase:
            workflow.upsert_search_attributes([HEAL_PHASE.value_set(phase), *extra])

    # ── Main workflow ──────────────────────────────────────────

    @workflow.run
    async def run(self, input: HealerInput) -> str:
        namespace = input.namespace
        auto_approve = input.auto_approve
        self._track_phase = input.track_phase

        if self._track_phase:
            workflow.upsert_search_attributes([HEAL_NAMESPACE.value_set(namespace)])

        # Phase 1: Scan
        self._set_phase("scanning")
        workflow.logger.info("Scanning cluster for unhealthy pods...")

        issues = await workflow.execute_activity(
            scan_cluster,
            namespace,
            start_to_close_timeout=timedelta(seconds=30),
        )

        if not issues:
            self._set_phase("done", PODS_TOTAL.value_set(0))
            workflow.logger.info("All pods healthy!")
            return "All pods healthy! Nothing to fix."

        workflow.logger.info(f"Found {len(issues)} unhealthy pod(s). Diagnosing...")

        # Phase 2: Diagnose ALL pods
        self._set_phase("diagnosing", PODS_TOTAL.value_set(len(issues)))

        for issue in issues:
            workflow.logger.info(f"Diagnosing: {issue.name} ({issue.reason})")

            details = await workflow.execute_activity(
                get_pod_details,
                args=[issue.name, issue.namespace],
                start_to_close_timeout=timedelta(seconds=30),
            )

            diagnosis = await workflow.execute_activity(
                diagnose_pod,
                details,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=_DIAGNOSE_RETRY,
            )

            diagnosis.namespace = issue.namespace
            workflow.logger.info(
                f"[{diagnosis.severity}] {diagnosis.root_cause} -> {diagnosis.action}"
            )
            self._diagnoses.append(diagnosis)

        # Phase 3 + 4: decide, then heal each approved pod in its own child workflow
        # THE MOMENT its decision lands — so the cluster heals click-by-click and the
        # Web UI shows a parent → children tree. (Auto mode pre-decides everything.)
        if auto_approve:
            for d in self._diagnoses:
                self._decisions[d.pod_name] = "rejected" if d.action == "skip" else "approved"
            self._set_phase("executing")
        else:
            self._set_phase("awaiting_approval")
            workflow.logger.info("Waiting for human approval (each fix applies as it's approved)...")

        heal_tasks: list[asyncio.Task] = []
        processed: set[str] = set()
        while len(processed) < len(self._diagnoses):
            # Durable, indefinite wait for the NEXT decision: survives worker/MCP-server
            # crashes; a human can take minutes or days. No timeout (it would RAISE and
            # crash the heal). Phase stays 'awaiting_approval' while any pod is still
            # undecided, so the remaining approve/reject buttons keep showing.
            await workflow.wait_condition(
                lambda: any(
                    d.pod_name in self._decisions and d.pod_name not in processed
                    for d in self._diagnoses
                )
            )
            for diagnosis in self._diagnoses:
                pod = diagnosis.pod_name
                if pod in processed or pod not in self._decisions:
                    continue
                processed.add(pod)
                if self._decisions[pod] == "approved" and diagnosis.action != "skip":
                    workflow.logger.info(f"Fixing {pod}: {diagnosis.action}")
                    # Spawn the per-pod child concurrently; it appends its result to
                    # _results when it finishes (see _heal_one).
                    heal_tasks.append(
                        asyncio.create_task(self._heal_one(diagnosis))
                    )
                else:
                    reason = diagnosis.explanation if diagnosis.action == "skip" else "Rejected by user"
                    workflow.logger.info(f"Skipping {pod}: {reason}")
                    self._results.append({
                        "pod_name": pod,
                        "success": False,
                        "action_taken": "skipped",
                        "details": reason,
                    })

        # Every pod is decided; wait for the in-flight child heals to finish.
        if heal_tasks:
            await asyncio.gather(*heal_tasks)

        # Phase 5: Done
        self._set_phase("done")

        healed = sum(1 for r in self._results if r["success"])
        total = len(self._results)

        lines = [f"Healed {healed}/{total} pods:\n"]
        for r in self._results:
            icon = "+" if r["success"] else "-"
            lines.append(f"  [{icon}] {r['pod_name']}: {r['action_taken']} -- {r['details']}")

        summary = "\n".join(lines)
        workflow.logger.info(summary)
        return summary

    async def _heal_one(self, diagnosis: Diagnosis) -> None:
        """Run one pod's fix-and-verify in a child workflow, then record its result."""
        res = await workflow.execute_child_workflow(
            HealPodWorkflow.run,
            args=[diagnosis, self._track_phase],
            id=f"{workflow.info().workflow_id}-{diagnosis.pod_name}",
        )
        self._results.append(res)
        if res["success"]:
            self._healed += 1
            if self._track_phase:
                workflow.upsert_search_attributes([PODS_HEALED.value_set(self._healed)])
