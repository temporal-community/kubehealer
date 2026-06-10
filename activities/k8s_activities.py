import re
import time

from kubernetes import client, config
from temporalio import activity

from models import PodIssue, Diagnosis, HealResult


def _init_k8s():
    """Initialize Kubernetes client. Tries in-cluster first, falls back to kubeconfig."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except config.ConfigException:
            raise RuntimeError(
                "No Kubernetes cluster found. "
                "Run ./setup.sh first, or set KUBECONFIG."
            )
    return client.CoreV1Api(), client.AppsV1Api()


v1, apps_v1 = _init_k8s()

UNHEALTHY_REASONS = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "OOMKilled",
    "CreateContainerConfigError",
    "RunContainerError",
    "InvalidImageName",
}

VALID_ACTIONS = {"restart_pod", "fix_image", "patch_resources", "skip"}

# Safety: memory must match K8s resource format (digits + unit suffix)
MEMORY_PATTERN = re.compile(r"^\d+[EPTGMK]i?$")
# Safety: image must look like a container image (no spaces, no shell chars)
IMAGE_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_./:@-]+$")


@activity.defn
async def scan_cluster(namespace: str) -> list[PodIssue]:
    activity.logger.info(f"Scanning namespace '{namespace}' for unhealthy pods")
    pods = v1.list_namespaced_pod(namespace=namespace)
    issues = []

    for pod in pods.items:
        pod_name = pod.metadata.name
        phase = pod.status.phase
        found = False  # one issue per pod — ImagePullBackOff/ConfigError are also phase=Pending

        # Check container statuses
        if pod.status.container_statuses:
            for cs in pod.status.container_statuses:
                waiting = cs.state.waiting if cs.state else None
                terminated = cs.state.terminated if cs.state else None

                if waiting and waiting.reason in UNHEALTHY_REASONS:
                    issues.append(PodIssue(
                        name=pod_name,
                        namespace=namespace,
                        status=phase,
                        reason=waiting.reason,
                        message=waiting.message or "",
                    ))
                    found = True
                    break

                if terminated and terminated.reason == "OOMKilled":
                    issues.append(PodIssue(
                        name=pod_name,
                        namespace=namespace,
                        status=phase,
                        reason="OOMKilled",
                        message="Container was killed due to out-of-memory",
                    ))
                    found = True
                    break

        # Check for pods stuck in Pending (only if not already flagged above)
        if not found and phase == "Pending" and pod.status.start_time:
            pending_seconds = time.time() - pod.status.start_time.timestamp()
            if pending_seconds > 60:
                reason = "StuckPending"
                message = f"Pod has been Pending for {int(pending_seconds)}s"

                if pod.status.conditions:
                    for cond in pod.status.conditions:
                        if cond.status == "False" and cond.message:
                            message = cond.message
                            break

                issues.append(PodIssue(
                    name=pod_name,
                    namespace=namespace,
                    status=phase,
                    reason=reason,
                    message=message,
                ))

    activity.logger.info(f"Found {len(issues)} unhealthy pod(s)")
    for issue in issues:
        activity.logger.info(f"  {issue.name}: {issue.reason} — {issue.message}")
    return issues


@activity.defn
async def get_pod_details(pod_name: str, namespace: str) -> str:
    activity.logger.info(f"Getting details for pod '{pod_name}'")

    pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    lines = [f"Pod: {pod_name}", f"Namespace: {namespace}", f"Phase: {pod.status.phase}"]

    # Container statuses
    if pod.status.container_statuses:
        for cs in pod.status.container_statuses:
            lines.append(f"\nContainer: {cs.name}")
            lines.append(f"  Image: {cs.image}")
            lines.append(f"  Ready: {cs.ready}")
            lines.append(f"  Restart Count: {cs.restart_count}")
            if cs.state:
                if cs.state.waiting:
                    lines.append(f"  State: Waiting — {cs.state.waiting.reason}: {cs.state.waiting.message}")
                elif cs.state.terminated:
                    lines.append(f"  State: Terminated — {cs.state.terminated.reason}")
                elif cs.state.running:
                    lines.append("  State: Running")

    # Conditions
    if pod.status.conditions:
        lines.append("\nConditions:")
        for cond in pod.status.conditions:
            lines.append(f"  {cond.type}: {cond.status} — {cond.message or ''}")

    # Pod spec (resource limits)
    for container in pod.spec.containers:
        if container.resources and container.resources.limits:
            lines.append(f"\nResource Limits ({container.name}):")
            for k, val in container.resources.limits.items():
                lines.append(f"  {k}: {val}")

    # Logs
    try:
        logs = v1.read_namespaced_pod_log(
            name=pod_name, namespace=namespace, tail_lines=50
        )
        lines.append(f"\nLast 50 log lines:\n{logs}")
    except Exception:
        lines.append("\nLogs: unavailable (container may not be running)")

    # Events
    events = v1.list_namespaced_event(
        namespace=namespace,
        field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod",
    )
    if events.items:
        lines.append("\nRecent Events:")
        for event in events.items[-10:]:
            lines.append(f"  [{event.type}] {event.reason}: {event.message}")

    details = "\n".join(lines)
    activity.logger.info(f"Collected {len(lines)} lines of diagnostic info")
    return details


def _get_deployment_name(pod_name: str, namespace: str) -> str:
    """Walk ownerReferences: Pod -> ReplicaSet -> Deployment.

    This is the correct way to find the owning deployment.
    Falls back to string-splitting heuristic if ownerReferences are missing.
    """
    try:
        pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)

        # Find the owning ReplicaSet
        rs_name = None
        if pod.metadata.owner_references:
            for ref in pod.metadata.owner_references:
                if ref.kind == "ReplicaSet":
                    rs_name = ref.name
                    break

        if not rs_name:
            activity.logger.warning(f"Pod '{pod_name}' has no ReplicaSet owner, using string heuristic")
            return _deployment_name_heuristic(pod_name)

        # Find the owning Deployment from the ReplicaSet
        rs = apps_v1.read_namespaced_replica_set(name=rs_name, namespace=namespace)
        if rs.metadata.owner_references:
            for ref in rs.metadata.owner_references:
                if ref.kind == "Deployment":
                    return ref.name

        activity.logger.warning(f"ReplicaSet '{rs_name}' has no Deployment owner, using string heuristic")
        return _deployment_name_heuristic(pod_name)

    except Exception as e:
        activity.logger.warning(f"ownerReferences lookup failed for '{pod_name}': {e}, using string heuristic")
        return _deployment_name_heuristic(pod_name)


def _deployment_name_heuristic(pod_name: str) -> str:
    """Fallback: strip last two dash-segments (replicaset hash + pod hash)."""
    parts = pod_name.split("-")
    if len(parts) > 2:
        return "-".join(parts[:-2])
    return pod_name


def _validate_fix(diagnosis: Diagnosis) -> str | None:
    """Validate LLM-generated fix before executing. Returns error string or None."""
    if diagnosis.action not in VALID_ACTIONS:
        return f"Invalid action '{diagnosis.action}'. Must be one of: {VALID_ACTIONS}"

    if diagnosis.action == "fix_image":
        image = diagnosis.fix_details.get("image", "")
        if not image:
            return "fix_image action requires 'image' in fix_details"
        if not IMAGE_PATTERN.match(image):
            return f"Image '{image}' contains invalid characters"

    if diagnosis.action == "patch_resources":
        memory = diagnosis.fix_details.get("memory", "")
        if not memory:
            return "patch_resources action requires 'memory' in fix_details"
        if not MEMORY_PATTERN.match(memory):
            return f"Memory value '{memory}' is not a valid K8s resource quantity"

    return None


@activity.defn
async def execute_fix(diagnosis: Diagnosis) -> HealResult:
    pod_name = diagnosis.pod_name
    action = diagnosis.action
    namespace = diagnosis.namespace

    activity.logger.info(f"Executing fix for '{pod_name}': action={action}")

    # Validate LLM output before touching the cluster
    validation_error = _validate_fix(diagnosis)
    if validation_error:
        activity.logger.error(f"Validation failed for '{pod_name}': {validation_error}")
        return HealResult(
            pod_name=pod_name,
            success=False,
            action_taken="validation_failed",
            details=validation_error,
        )

    if action == "skip":
        activity.logger.info(f"Skipping '{pod_name}': {diagnosis.explanation}")
        return HealResult(
            pod_name=pod_name,
            success=False,
            action_taken="skip",
            details=diagnosis.explanation,
        )

    if action == "restart_pod":
        v1.delete_namespaced_pod(name=pod_name, namespace=namespace)
        activity.logger.info(f"Deleted pod '{pod_name}' (will be recreated by deployment)")
        return HealResult(
            pod_name=pod_name,
            success=True,
            action_taken="restart_pod",
            details="Deleted pod to trigger restart",
        )

    # For fix_image and patch_resources, we need the deployment
    deployment_name = _get_deployment_name(pod_name, namespace)

    deployment = apps_v1.read_namespaced_deployment(
        name=deployment_name, namespace=namespace
    )
    container_name = deployment.spec.template.spec.containers[0].name

    if action == "fix_image":
        correct_image = diagnosis.fix_details["image"]
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"name": container_name, "image": correct_image}]
                    }
                }
            }
        }
        apps_v1.patch_namespaced_deployment(
            name=deployment_name, namespace=namespace, body=patch
        )
        activity.logger.info(f"Patched deployment '{deployment_name}' image to '{correct_image}'")
        return HealResult(
            pod_name=pod_name,
            success=True,
            action_taken="fix_image",
            details=f"Patched image to {correct_image}",
        )

    if action == "patch_resources":
        memory = diagnosis.fix_details["memory"]
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": container_name,
                            "resources": {"limits": {"memory": memory}},
                        }]
                    }
                }
            }
        }
        apps_v1.patch_namespaced_deployment(
            name=deployment_name, namespace=namespace, body=patch
        )
        activity.logger.info(f"Patched deployment '{deployment_name}' memory limit to '{memory}'")
        return HealResult(
            pod_name=pod_name,
            success=True,
            action_taken="patch_resources",
            details=f"Patched memory limit to {memory}",
        )

    return HealResult(
        pod_name=pod_name,
        success=False,
        action_taken=action,
        details=f"Unknown action: {action}",
    )
