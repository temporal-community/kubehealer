from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.k8s_activities import scan_cluster, get_pod_details, execute_fix
    from activities.llm_activities import diagnose_pod
    from models import Diagnosis, HealerInput


@workflow.defn
class HealerWorkflow:

    def __init__(self):
        self._phase: str = "starting"
        self._diagnoses: list[Diagnosis] = []
        self._decisions: dict[str, str] = {}
        self._results: list[dict] = []

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

    # ── Main workflow ──────────────────────────────────────────

    def _all_decided(self) -> bool:
        return all(d.pod_name in self._decisions for d in self._diagnoses)

    @workflow.run
    async def run(self, input: HealerInput) -> str:
        namespace = input.namespace
        auto_approve = input.auto_approve

        # Phase 1: Scan
        self._phase = "scanning"
        workflow.logger.info("Scanning cluster for unhealthy pods...")

        issues = await workflow.execute_activity(
            scan_cluster,
            namespace,
            start_to_close_timeout=timedelta(seconds=30),
        )

        if not issues:
            self._phase = "done"
            workflow.logger.info("All pods healthy!")
            return "All pods healthy! Nothing to fix."

        workflow.logger.info(f"Found {len(issues)} unhealthy pod(s). Diagnosing...")

        # Phase 2: Diagnose ALL pods
        self._phase = "diagnosing"

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
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    backoff_coefficient=2.0,
                ),
            )

            diagnosis.namespace = issue.namespace
            workflow.logger.info(
                f"[{diagnosis.severity}] {diagnosis.root_cause} -> {diagnosis.action}"
            )
            self._diagnoses.append(diagnosis)

        # Phase 3: Approval gate
        if auto_approve:
            for d in self._diagnoses:
                self._decisions[d.pod_name] = "rejected" if d.action == "skip" else "approved"
        else:
            self._phase = "awaiting_approval"
            workflow.logger.info("Waiting for human approval...")
            # Durable, indefinite wait: survives worker/MCP-server crashes. A human
            # (or the agent) can take minutes or days to approve — the workflow simply
            # waits, forever if need be. No timeout: a timeout here would RAISE and
            # crash the heal, defeating the durability guarantee this demo is about.
            await workflow.wait_condition(self._all_decided)
            workflow.logger.info("All decisions received.")

        # Phase 4: Execute approved fixes
        self._phase = "executing"

        for diagnosis in self._diagnoses:
            decision = self._decisions.get(diagnosis.pod_name, "rejected")

            if decision == "approved" and diagnosis.action != "skip":
                workflow.logger.info(f"Fixing {diagnosis.pod_name}: {diagnosis.action}")
                result = await workflow.execute_activity(
                    execute_fix,
                    diagnosis,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self._results.append({
                    "pod_name": result.pod_name,
                    "success": result.success,
                    "action_taken": result.action_taken,
                    "details": result.details,
                })
            else:
                reason = diagnosis.explanation if diagnosis.action == "skip" else "Rejected by user"
                workflow.logger.info(f"Skipping {diagnosis.pod_name}: {reason}")
                self._results.append({
                    "pod_name": diagnosis.pod_name,
                    "success": False,
                    "action_taken": "skipped",
                    "details": reason,
                })

        # Phase 5: Done
        self._phase = "done"

        healed = sum(1 for r in self._results if r["success"])
        total = len(self._results)

        lines = [f"Healed {healed}/{total} pods:\n"]
        for r in self._results:
            icon = "+" if r["success"] else "-"
            lines.append(f"  [{icon}] {r['pod_name']}: {r['action_taken']} -- {r['details']}")

        summary = "\n".join(lines)
        workflow.logger.info(summary)
        return summary
