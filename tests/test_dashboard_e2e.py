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

        # The two status badges that tell the story (the insider "Gateway" badge
        # was removed; MCP/Temporal now read "fragile"/"durable").
        for label in ("MCP Server", "Temporal"):
            assert await page.locator(".badge", has_text=label).count() >= 1

        # The four panels render (plain-English titles; Temporal Plane removed).
        for panel in ("AI Agent", "MCP Server", "Kubernetes Cluster", "Your Approvals"):
            assert await page.get_by_text(panel, exact=False).count() >= 1

        # Core control buttons exist (plain-English labels; single Heal button).
        assert await page.get_by_role("button", name="Heal (you approve)").count() == 1
        assert await page.get_by_role("button", name="Break MCP Server").count() == 1

        # The durable plane connects: the Temporal badge must go LIVE (this is the
        # bug we fixed — it used to stick on DOWN before any heal ran).
        await page.locator(".badge.alive", has_text="Temporal").wait_for(timeout=15_000)

        await page.screenshot(path="/tmp/kubehealer_e2e.png", full_page=True)
        await browser.close()


async def test_break_and_restart_buttons_always_present():
    """The Break/Restart buttons are always shown — they kill whatever serves the MCP
    port (a `make mcp` process or the dashboard's own child), regardless of mode."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(DASH_URL, wait_until="load")
        await page.wait_for_timeout(1000)

        assert await page.get_by_role("button", name="Break MCP Server").count() == 1
        assert await page.get_by_role("button", name="Restart").count() == 1

        await browser.close()
