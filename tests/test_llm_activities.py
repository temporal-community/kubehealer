"""Unit tests for activities/llm_activities.py.

These are pure-logic tests — we mock anthropic.Anthropic so no API calls
happen. We test through the activity using ActivityEnvironment so the
@activity.defn decorator's logger contract holds.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from activities.llm_activities import (
    VALID_ACTIONS,
    _parse_json_response,
    diagnose_pod,
)


# ── _parse_json_response: pure helper ────────────────────────────


class TestParseJsonResponse:
    def test_plain_json(self):
        assert _parse_json_response('{"a": 1}') == {"a": 1}

    def test_strips_lowercase_json_fence(self):
        text = '```json\n{"action": "restart_pod"}\n```'
        assert _parse_json_response(text) == {"action": "restart_pod"}

    def test_strips_uppercase_json_fence(self):
        text = '```JSON\n{"x": true}\n```'
        assert _parse_json_response(text) == {"x": True}

    def test_strips_bare_fence(self):
        text = '```\n{"x": 1}\n```'
        assert _parse_json_response(text) == {"x": 1}

    def test_handles_surrounding_whitespace(self):
        assert _parse_json_response('   {"x": 1}   \n') == {"x": 1}

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_json_response("not json at all")


# ── diagnose_pod activity ────────────────────────────────────────


def _fake_anthropic_response(text: str):
    """Build a stand-in for anthropic.Anthropic().messages.create()."""
    block = MagicMock()
    block.text = text
    response = MagicMock()
    response.content = [block]
    return response


def _patch_anthropic(response_text: str | None = None, side_effect=None):
    """Patch anthropic.Anthropic so diagnose_pod doesn't hit the network."""
    fake_client = MagicMock()
    if side_effect is not None:
        fake_client.messages.create.side_effect = side_effect
    else:
        fake_client.messages.create.return_value = _fake_anthropic_response(
            response_text or "{}"
        )
    return patch("activities.llm_activities.anthropic.Anthropic", return_value=fake_client)


class TestDiagnosePod:
    async def test_happy_path_returns_diagnosis(self):
        payload = json.dumps({
            "pod_name": "nginx-abc",
            "root_cause": "image not found",
            "severity": "high",
            "action": "fix_image",
            "explanation": "image tag is wrong",
            "fix_details": {"image": "nginx:latest"},
        })

        with _patch_anthropic(payload):
            result = await ActivityEnvironment().run(diagnose_pod, "pod info")

        assert result.pod_name == "nginx-abc"
        assert result.action == "fix_image"
        assert result.severity == "high"
        assert result.fix_details == {"image": "nginx:latest"}

    async def test_strips_markdown_fences(self):
        payload = (
            "```json\n"
            + json.dumps({
                "pod_name": "p1",
                "root_cause": "x",
                "severity": "low",
                "action": "restart_pod",
                "explanation": "y",
                "fix_details": {},
            })
            + "\n```"
        )

        with _patch_anthropic(payload):
            result = await ActivityEnvironment().run(diagnose_pod, "pod info")

        assert result.action == "restart_pod"

    async def test_unknown_action_falls_back_to_skip(self):
        """Defends the activity against an LLM hallucinating an action verb."""
        payload = json.dumps({
            "pod_name": "p1",
            "root_cause": "x",
            "severity": "low",
            "action": "delete_cluster",  # not in VALID_ACTIONS
            "explanation": "y",
            "fix_details": {},
        })

        with _patch_anthropic(payload):
            result = await ActivityEnvironment().run(diagnose_pod, "pod info")

        assert result.action == "skip"
        assert "delete_cluster" not in VALID_ACTIONS

    async def test_invalid_json_raises_application_error(self):
        with _patch_anthropic("this is not json"):
            with pytest.raises(ApplicationError, match="parse"):
                await ActivityEnvironment().run(diagnose_pod, "pod info")

    async def test_empty_response_raises_application_error(self):
        fake_client = MagicMock()
        empty = MagicMock()
        empty.content = []
        fake_client.messages.create.return_value = empty

        with patch("activities.llm_activities.anthropic.Anthropic", return_value=fake_client):
            with pytest.raises(ApplicationError, match="empty"):
                await ActivityEnvironment().run(diagnose_pod, "pod info")

    async def test_auth_error_is_non_retryable(self):
        import anthropic as anthropic_pkg

        # Build a real AuthenticationError. Its __init__ requires (message, response, body).
        fake_response = MagicMock()
        fake_response.status_code = 401
        fake_response.headers = {}
        auth_err = anthropic_pkg.AuthenticationError(
            "auth failed", response=fake_response, body=None
        )

        with _patch_anthropic(side_effect=auth_err):
            with pytest.raises(ApplicationError) as exc_info:
                await ActivityEnvironment().run(diagnose_pod, "pod info")

        assert exc_info.value.non_retryable is True

    async def test_rate_limit_error_is_retryable(self):
        import anthropic as anthropic_pkg

        fake_response = MagicMock()
        fake_response.status_code = 429
        fake_response.headers = {}
        rate_err = anthropic_pkg.RateLimitError(
            "rate limited", response=fake_response, body=None
        )

        with _patch_anthropic(side_effect=rate_err):
            with pytest.raises(ApplicationError) as exc_info:
                await ActivityEnvironment().run(diagnose_pod, "pod info")

        # Rate-limit must be retryable so Temporal's retry policy can recover
        assert exc_info.value.non_retryable is False
