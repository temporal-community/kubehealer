"""Shared pytest fixtures for the kubehealer test suite.

`workflow_env` is session-scoped because spinning up the time-skipping
Temporal dev server is the slowest thing in the suite — sharing it across
tests keeps the run fast while each test still gets a unique workflow id.
"""

import sys
from pathlib import Path

import pytest_asyncio
from temporalio.testing import WorkflowEnvironment

# Make the project root importable so tests can `from workflows... import ...`
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest_asyncio.fixture(scope="session")
async def workflow_env():
    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()
