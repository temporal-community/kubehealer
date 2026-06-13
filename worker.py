import asyncio
import concurrent.futures
import os
import sys

from dotenv import load_dotenv

load_dotenv()


def preflight_checks():
    """Validate environment before starting the worker."""
    errors = []

    # Check Anthropic API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        errors.append(
            "ANTHROPIC_API_KEY not set. "
            "Copy .env.example to .env and paste your key."
        )

    # Check Kubernetes connectivity (import triggers config load)
    try:
        from activities.k8s_activities import v1
        v1.list_namespace(limit=1)
    except RuntimeError as e:
        errors.append(str(e))
    except Exception as e:
        errors.append(f"Kubernetes cluster unreachable: {e}")

    if errors:
        print("\n  Preflight checks failed:\n")
        for err in errors:
            print(f"    [FAIL] {err}")
        print()
        sys.exit(1)

    print("  [OK] Anthropic API key")
    print("  [OK] Kubernetes cluster")


preflight_checks()

from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig, PrometheusConfig
from temporalio.worker import Worker

from activities.k8s_activities import scan_cluster, get_pod_details, execute_fix, verify_healed
from activities.llm_activities import diagnose_pod
from activities.chat_activities import (
    call_claude,
    list_pods_activity,
    get_pod_details_activity,
    get_pod_logs_activity,
    get_pod_events_activity,
)
from workflows.healer_workflow import HealerWorkflow
from workflows.heal_pod_workflow import HealPodWorkflow
from workflows.conversation_workflow import ConversationWorkflow


async def main():
    target = os.environ.get("TEMPORAL_TARGET", "localhost:7233")

    # Expose the SDK's Prometheus metrics (workflow/activity/worker health) so Grafana
    # can scrape the durable plane. durations_as_seconds keeps latency histograms in
    # seconds (Prometheus convention). Default port 9469 (9464 is a common Temporal
    # default and may already be taken on a dev box); override with WORKER_METRICS_ADDR.
    metrics_addr = os.environ.get("WORKER_METRICS_ADDR", "0.0.0.0:9469")
    runtime = Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(
        bind_address=metrics_addr,
        durations_as_seconds=True,
    )))
    print(f"  [OK] Worker metrics on http://{metrics_addr}/metrics")

    client = None
    for attempt in range(30):  # tolerate Temporal still starting (compose)
        try:
            client = await Client.connect(target, runtime=runtime)
            break
        except Exception as e:
            print(f"  waiting for Temporal at {target} ({attempt + 1}/30): {e}")
            await asyncio.sleep(2)
    if client is None:
        sys.exit(f"Could not connect to Temporal at {target}")

    # Register the heal's custom Search Attributes up front so the workflow's live
    # phase/progress show in the Web UI. Best-effort: a server that doesn't support
    # the operator API shouldn't stop the worker from running.
    from search_attributes import ensure_registered
    try:
        await ensure_registered(client)
        print("  [OK] Search Attributes registered")
    except Exception as e:
        print(f"  [warn] could NOT register Search Attributes ({e}).")
        print("         Heals run normally, but ones started with track_phase=True will")
        print("         fail on upsert. Register them on the server, or start with track_phase=False.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        worker = Worker(
            client,
            task_queue="kubehealer",
            workflows=[HealerWorkflow, HealPodWorkflow, ConversationWorkflow],
            activities=[
                # Healing activities
                scan_cluster,
                get_pod_details,
                execute_fix,
                verify_healed,
                diagnose_pod,
                # Conversation activities
                call_claude,
                list_pods_activity,
                get_pod_details_activity,
                get_pod_logs_activity,
                get_pod_events_activity,
            ],
            activity_executor=executor,
        )

        print("\n  KubeHealer worker started. Waiting for tasks...\n")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
