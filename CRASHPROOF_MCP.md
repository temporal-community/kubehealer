# The Crash-Proof MCP Server

> The crux of the talk in one line: **a basic MCP server keeps its work *inside the
> process*. Our crash-proof one keeps the work *inside Temporal* and the MCP server
> just points at it.** Kill the process — the work doesn't even notice.

---

## The setup

An MCP tool like `heal_cluster` is **long-running**: it scans a cluster, asks an LLM
to diagnose each broken pod, waits for a human to approve, then applies fixes. That's
seconds-to-minutes of expensive, stateful work.

The question the whole talk turns on:

> **What happens if the MCP server process dies in the middle of that work?**

Two servers, same tool, same result on a good day. They only differ when something
goes wrong — which, in production, it always eventually does.

---

## 1. The naive MCP server (the "before")

All the logic runs **in this process**. The diagnoses, the list of what's been fixed —
all of it lives in local variables.

```python
@mcp.tool(task=True)
async def heal_cluster(namespace: str = "default") -> str:
    issues = await scan_cluster(namespace)

    # State lives in local variables — it dies with the process.
    diagnoses = []
    for issue in issues:
        details = await get_pod_details(issue.name, namespace)
        diagnosis = await diagnose_pod(details)   # <- expensive LLM call
        diagnoses.append(diagnosis)

    results = []
    for d in diagnoses:
        await asyncio.sleep(6)        # the apply step — the danger window
        res = await execute_fix(d)    # actually mutates the cluster
        results.append(res)

    return summarize(results)
```

**Kill the process anywhere in here and:**

- the **SEP-1686 Task disappears** from the in-memory task store — the client's handle
  now points at nothing;
- the **LLM diagnoses are gone** — you pay for them again on retry;
- worst of all, if the crash lands *between* `execute_fix` calls, the cluster is left
  **half-healed with no record** of what was already done. Re-running risks
  **double-applying** fixes.

There is no source of truth except a Python list that no longer exists.

---

## 2. The crash-proof MCP server (the "after")

The tool does **no business logic**. It starts a **Temporal Workflow** and hands the
client a Task that simply *tracks* that workflow.

```python
@mcp.tool(task=True)
async def heal_cluster(namespace: str = "default") -> str:
    client = await get_temporal_client()
    wid = f"kubehealer-heal-{namespace}"        # deterministic ID — the whole trick

    handle = await client.start_workflow(
        HealerWorkflow.run,
        HealerInput(namespace=namespace),
        id=wid,
        task_queue="kubehealer",
        # If a heal is already running for this namespace, ATTACH to it
        # instead of starting a duplicate.
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )

    # Block (durably) until the workflow finishes. The work runs on the
    # Temporal *Worker*, not here.
    return await handle.result()
```

The actual scan → diagnose → approve → fix logic lives in the **Workflow**, whose
state Temporal persists to its database after **every single step**:

```python
@workflow.defn
class HealerWorkflow:
    @workflow.run
    async def run(self, input):
        issues = await workflow.execute_activity(scan_cluster, ...)

        for issue in issues:                       # each step is checkpointed
            d = await workflow.execute_activity(diagnose_pod, ...)
            self._diagnoses.append(d)              # durable state, not a local list

        # Pauses here — for minutes or days — and survives crashes.
        await workflow.wait_condition(self._all_decided)

        for d in self._diagnoses:
            if self._decisions[d.pod_name] == "approved":
                await workflow.execute_activity(execute_fix, d)
```

**Now kill the MCP server mid-heal:**

- the Workflow **keeps running on the Worker** — it never knew the MCP server existed;
- restart the MCP server and call `heal_cluster` again → because the Workflow ID is
  **deterministic**, `USE_EXISTING` **re-attaches** to the same in-flight run;
- already-applied fixes are **never re-applied** — Temporal replays completed steps from
  history instead of re-executing them.

The MCP server became **disposable**. The durable state moved to Temporal.

---

## The one idea that makes it work: a deterministic Workflow ID

```python
def heal_workflow_id(namespace: str) -> str:
    return f"kubehealer-heal-{namespace}"      # same input -> same ID, always
```

Because the ID is derived from the input (not random), *any* process — the original
MCP server, a restarted one, a retrying agent — can recompute it and reconnect to the
exact same workflow. No shared memory, no database lookups, no coordination. The ID
*is* the handle.

Approvals and status work the same disposable way — they're just Temporal
**signals** and **queries** against that ID, so they survive a server restart too:

```python
# approve_fix tool  ->  a signal
await handle.signal(HealerWorkflow.approve_pod, pod_name)

# get_healing_status tool  ->  a query
state = await handle.query(HealerWorkflow.get_state)
```

---

## The two architectures

**Naive — work lives inside the process. Kill it and the work dies with it.**

```
        ┌──────────┐        ┌──────────────────────────────────────┐
        │  Agent   │  MCP   │         NAIVE MCP SERVER (process)    │
        │ (Claude) │ ─────► │                                      │
        └──────────┘        │   heal_cluster():                    │
                            │     diagnoses = []   ◄── state in    │
                            │     scan → diagnose → fix    RAM     │
                            │            │                         │
                            │            ▼                         │
                            │      ┌────────────┐                  │
                            │      │ Kubernetes │                  │
                            │      └────────────┘                  │
                            └──────────────────────────────────────┘
                                            │
                                       💥 kill -9
                                            │
                                            ▼
                            ┌──────────────────────────────────────┐
                            │  diagnoses = ❌   task = ❌           │
                            │  cluster = half-healed, no record ❌  │
                            └──────────────────────────────────────┘
```

**Crash-proof — work lives in Temporal. The MCP server is just a pointer to it.**

```
        ┌──────────┐        ┌─────────────────────────┐
        │  Agent   │  MCP   │  CRASH-PROOF MCP SERVER │   thin wrapper:
        │ (Claude) │ ─────► │  heal_cluster():        │   start + watch only
        └──────────┘        │    start_workflow(      │
              ▲             │      id="heal-default") │
              │             └───────────┬─────────────┘
              │ re-attach by            │ start / signal / query
              │ deterministic ID        ▼
              │             ┌─────────────────────────────────────────┐
              │             │            TEMPORAL                      │
              └─────────────│   HealerWorkflow  (id = heal-default)    │
                            │   state persisted after EVERY step  ✔    │
                            │   scan ✔  diagnose ✔  approve… fix ✔     │
                            └───────────────────┬─────────────────────┘
                                                │ runs on the Worker
                                                ▼
                                          ┌────────────┐
                                          │ Kubernetes │
                                          └────────────┘

      💥 kill -9 the MCP server  ──►  workflow keeps running, loses nothing.
         restart it, call again   ──►  same ID re-attaches to the live run.
```

The difference in one glance: in the naive diagram the work is *trapped inside the box
that dies*. In the crash-proof one, the box that dies (the MCP server) holds **no
state** — the work sits safely in Temporal, and the deterministic ID is the rope the
restarted server uses to climb back to it.

---

## Side by side

| | Naive MCP server | Crash-proof MCP server |
|---|---|---|
| Where the work runs | In the MCP process | In the Temporal Worker |
| Where state lives | Local variables (RAM) | Temporal's persisted history |
| MCP server crashes mid-heal | Work + diagnoses lost | Work continues untouched |
| Reconnect after restart | Impossible — handle is gone | `USE_EXISTING` on a deterministic ID |
| Risk of double-applying fixes | Yes (no record of progress) | No (completed steps replay) |
| Human approval pause | Lost on crash | Durable — waits days if needed |
| The MCP server is… | The system of record | A thin, replaceable pointer |

---

## A bonus point worth making

Not every tool needs a workflow. A **read-only** tool like `list_pods` talks to
Kubernetes directly — it's fast, stateless, and safe to just re-run:

```python
@mcp.tool(annotations={"readOnlyHint": True})
async def list_pods(namespace: str = "default") -> str:
    return v1.list_namespaced_pod(namespace).items   # no workflow — and that's correct
```

The durability lesson isn't "wrap everything in Temporal." It's: **the work that is
long-running, stateful, and costly to lose is exactly the work that must not live
inside a disposable process.** Match the tool to the risk.

---

## The line to land on

> "An MCP server is just a process — and processes die. The fix isn't to make the
> process more reliable. It's to make the process *not matter*: move the durable work
> into Temporal, and let the MCP server be as crashable as it really is."
