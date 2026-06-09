"""End-to-end browser test of the Mission Control dashboard (Playwright).

Drives a real headless Chromium against a running dashboard and asserts the UI
renders and its live data planes connect. Requires the full stack up:

    make temporal | make worker | make mcp | make dashboard

It SKIPS cleanly when Playwright isn't installed or the dashboard isn't on :8090,
so the normal `make test` run stays green without the stack.

Setup once:  pip install playwright && playwright install chromium
"""

import socket

import pytest

pytest.importorskip("playwright.async_api")
from playwright.async_api import async_playwright

DASH_URL = "http://localhost:8090"


def _dashboard_up() -> bool:
    try:
        with socket.create_connection(("localhost", 8090), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _dashboard_up(),
    reason="dashboard not running on :8090 (start it with `make dashboard`)",
)


async def test_dashboard_renders_and_connects():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(DASH_URL, wait_until="load")

        # Brand + the Temporal logo (it's a Temporal talk).
        await page.wait_for_selector("text=INVINCIBLE", timeout=10_000)
        assert await page.locator('img[alt="Temporal"]').count() == 1

        # All three status badges are present.
        for label in ("MCP Server", "Temporal", "Gateway"):
            assert await page.locator(".badge", has_text=label).count() >= 1

        # The four panels render (Temporal Plane was intentionally removed).
        for panel in ("Agent", "MCP Plane", "Kubernetes Cluster", "Human-in-the-loop"):
            assert await page.get_by_text(panel, exact=False).count() >= 1

        # Core control buttons exist.
        assert await page.get_by_role("button", name="Auto Heal").count() == 1
        assert await page.get_by_role("button", name="Inject Chaos").count() == 1

        # The durable plane connects: the Temporal badge must go LIVE (this is the
        # bug we fixed — it used to stick on DOWN before any heal ran).
        await page.locator(".badge.alive", has_text="Temporal").wait_for(timeout=15_000)
        await page.locator(".badge.alive", has_text="Gateway").wait_for(timeout=15_000)

        await page.screenshot(path="/tmp/kubehealer_e2e.png", full_page=True)
        await browser.close()


async def test_external_mode_shows_ctrlc_hint_not_break_button():
    """When an MCP server is running in its own terminal, the dashboard auto-detects
    'external' mode: it shows a Ctrl-C hint and hides the in-GUI Break button."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(DASH_URL, wait_until="load")

        # Give the WS a moment to deliver the sticky mcp_mode event.
        await page.wait_for_timeout(1500)

        external = await page.get_by_text("Ctrl-C", exact=False).count() >= 1
        has_break = await page.get_by_role("button", name="Break MCP Server").count() >= 1
        # Exactly one of the two modes must hold, and they must be consistent.
        assert external != has_break, "external hint and Break button must be mutually exclusive"

        await browser.close()
