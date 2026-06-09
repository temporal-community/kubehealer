import argparse
import asyncio
import os
import time

from dotenv import load_dotenv
from temporalio.client import Client

load_dotenv()

from models import HealerInput
from workflows.healer_workflow import HealerWorkflow


async def main():
    parser = argparse.ArgumentParser(description="Start the KubeHealer workflow (auto mode)")
    parser.add_argument("--namespace", default="default", help="Kubernetes namespace to scan")
    args = parser.parse_args()

    client = await Client.connect(os.environ.get("TEMPORAL_TARGET", "localhost:7233"))

    workflow_id = f"kubehealer-{int(time.time())}"
    print(f"🚀 Starting KubeHealer workflow (id={workflow_id})...")
    print(f"   Namespace: {args.namespace}")
    print()

    result = await client.execute_workflow(
        HealerWorkflow.run,
        HealerInput(namespace=args.namespace, auto_approve=True),
        id=workflow_id,
        task_queue="kubehealer",
    )

    print()
    print(result)
    print()
    print(f"📊 View workflow trace: http://localhost:8233/namespaces/default/workflows/{workflow_id}")


if __name__ == "__main__":
    asyncio.run(main())
