"""Custom Search Attributes — the heal's live, queryable metadata in the Web UI.

These are what make a run *legible* at a glance in the Temporal UI: the workflow
list and the run header show the current phase, namespace, and heal progress, and
they update live as the workflow advances — so a run is never a blank box, even
while it's parked waiting for approval or while the MCP server is being restarted.

The key NAMES here must match the `--search-attribute` flags that `make temporal`
registers with the dev server (see the Makefile). An unregistered attribute makes
`upsert_search_attributes` fail the workflow task, so emission is gated behind the
`track_phase` flag (off in tests / when the server hasn't registered them).
"""

from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.client import Client
from temporalio.common import SearchAttributeKey
from temporalio.service import RPCError, RPCStatusCode

# Keyword = exact-match string; Int = numeric (sortable in the UI).
HEAL_PHASE = SearchAttributeKey.for_keyword("HealPhase")           # scanning|diagnosing|...
HEAL_NAMESPACE = SearchAttributeKey.for_keyword("HealNamespace")   # cluster namespace
POD_NAME = SearchAttributeKey.for_keyword("PodName")               # child workflow only
PODS_TOTAL = SearchAttributeKey.for_int("PodsTotal")               # unhealthy pods found
PODS_HEALED = SearchAttributeKey.for_int("PodsHealed")             # fixes applied so far

# name -> Temporal indexed type, for programmatic registration.
_REGISTRY = {
    HEAL_PHASE.name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
    HEAL_NAMESPACE.name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
    POD_NAME.name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
    PODS_TOTAL.name: IndexedValueType.INDEXED_VALUE_TYPE_INT,
    PODS_HEALED.name: IndexedValueType.INDEXED_VALUE_TYPE_INT,
}


async def ensure_registered(client: Client, namespace: str = "default") -> None:
    """Register our custom Search Attributes if they aren't already.

    Self-registering on startup is what makes the SAs *safe*: an unregistered attribute
    makes the workflow's `upsert_search_attributes` fail the workflow task (which would
    kill a heal mid-demo). Calling this before any heal runs guarantees they exist, so
    nobody has to remember a `--search-attribute` flag on the server.

    Idempotent: registers each attribute on its own, so an already-present one can't
    block the rest (AddSearchAttributes is all-or-nothing per request and errors if
    *any* key in the batch exists). "Already exists" is the success case.
    """
    for name, value_type in _REGISTRY.items():
        try:
            await client.operator_service.add_search_attributes(
                AddSearchAttributesRequest(search_attributes={name: value_type}, namespace=namespace)
            )
        except RPCError as e:
            if e.status != RPCStatusCode.ALREADY_EXISTS:
                raise  # genuine failure (unsupported server, etc.) — caller decides
