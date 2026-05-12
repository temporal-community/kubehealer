import asyncio
import sys

from dotenv import load_dotenv
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError

load_dotenv()

from models import ConversationInput
from workflows.conversation_workflow import ConversationWorkflow

WORKFLOW_ID = "kubehealer-conversation"

BANNER = """
\033[1;96m  ╔═══════════════════════════════════════╗
  ║         KubeHealer AI Assistant        ║
  ╚═══════════════════════════════════════╝\033[0m
  \033[2mAI-powered Kubernetes debugging, orchestrated by Temporal.\033[0m
"""


async def terminate_stale_workflow(client: Client):
    """Terminate a stale workflow from a previous code version."""
    try:
        handle = client.get_workflow_handle(WORKFLOW_ID)
        await handle.terminate(reason="Restarting with updated workflow code")
        print("  \033[2m[terminated stale session]\033[0m")
        await asyncio.sleep(1)
    except RPCError:
        pass


async def get_or_start_workflow(client: Client, namespace: str):
    """Reconnect to a running conversation or start a new one.

    A handle + successful query is not enough: Temporal lets you query
    completed workflows, so we must check the execution status explicitly.
    """
    try:
        handle = client.get_workflow_handle(WORKFLOW_ID)
        desc = await handle.describe()
        if desc.status == WorkflowExecutionStatus.RUNNING:
            return handle, False
    except RPCError:
        pass

    handle = await client.start_workflow(
        ConversationWorkflow.run,
        ConversationInput(namespace=namespace, session_id=WORKFLOW_ID),
        id=WORKFLOW_ID,
        task_queue="kubehealer",
    )
    return handle, True


async def main():
    client = await Client.connect("localhost:7233")
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"

    handle, is_new = await get_or_start_workflow(client, namespace)

    print(BANNER)
    if is_new:
        print(f"  \033[2mSession: {WORKFLOW_ID} | Namespace: {namespace}\033[0m")
        print("  Talk to me about your cluster. Type 'exit' to quit.\n")
    else:
        print(f"  \033[2mSession: {WORKFLOW_ID} | Namespace: {namespace} (reconnected)\033[0m")
        state = await handle.query(ConversationWorkflow.get_state)
        if state["latest_response"]:
            print(f"\n  \033[2m[Last response]:\033[0m\n{state['latest_response']}")
        print()

    while True:
        try:
            user_input = input("\033[96myou>\033[0m ")
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if not user_input.strip():
            continue

        if user_input.strip().lower() in ("exit", "quit", "bye"):
            try:
                await handle.execute_update(
                    ConversationWorkflow.send_message, "exit"
                )
            except Exception:
                pass
            print("  Goodbye!")
            break

        # Send message via Temporal Update — blocks until response is ready
        print(f"  \033[2m[thinking...]\033[0m", end="\r", flush=True)

        try:
            response = await handle.execute_update(
                ConversationWorkflow.send_message, user_input
            )
            print(f"\033[2K\n{response}\n")
        except RPCError as e:
            error_msg = str(e)
            # Workflow terminated or gone — start fresh and retry the message
            if "workflow execution already completed" in error_msg.lower():
                print(f"\033[2K  \033[93mSession expired. Starting fresh...\033[0m")
                handle, _ = await get_or_start_workflow(client, namespace)
                try:
                    response = await handle.execute_update(
                        ConversationWorkflow.send_message, user_input
                    )
                    print(f"\033[2K\n{response}\n")
                except Exception as retry_err:
                    print(f"\033[2K  \033[91mError after restart: {retry_err}\033[0m\n")
                continue
            print(f"\033[2K  \033[91mError: {e}\033[0m\n")
        except Exception as e:
            error_msg = str(e)
            # Detect stale workflow from old code version (non-determinism error)
            if "nondeterminism" in error_msg.lower():
                print(f"\033[2K  \033[93mStale session detected. Restarting...\033[0m")
                await terminate_stale_workflow(client)
                handle, _ = await get_or_start_workflow(client, namespace)
                # Retry the message
                try:
                    response = await handle.execute_update(
                        ConversationWorkflow.send_message, user_input
                    )
                    print(f"\033[2K\n{response}\n")
                except Exception as retry_err:
                    print(f"\033[2K  \033[91mError: {retry_err}\033[0m\n")
            else:
                print(f"\033[2K  \033[91mError: {e}\033[0m\n")


if __name__ == "__main__":
    asyncio.run(main())
