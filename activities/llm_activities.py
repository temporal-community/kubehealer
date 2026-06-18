import json
import re

import anthropic
from temporalio import activity
from temporalio.exceptions import ApplicationError

from models import Diagnosis

SYSTEM_PROMPT = """You are a Kubernetes SRE expert. You receive pod diagnostic info and must identify the root cause and suggest a fix.

Respond ONLY with valid JSON, no markdown, no explanation outside the JSON:
{
  "pod_name": "the pod name from the input",
  "root_cause": "brief root cause",
  "severity": "low or medium or high",
  "action": "one of: restart_pod, fix_image, patch_resources, skip",
  "explanation": "one sentence a human would understand",
  "fix_details": {}
}

Rules for fix_details:
- If action is "fix_image": include {"image": "corrected-image:tag"}
- If action is "patch_resources": include {"memory": "128Mi"} or appropriate limit
- If action is "restart_pod" or "skip": empty {}

Common patterns:
- "latestt" is a typo for "latest"
- OOMKilled means memory limit is too low, suggest 128Mi or 256Mi
- Missing ConfigMap cannot be auto-fixed, use action "skip"
"""

VALID_ACTIONS = {"restart_pod", "fix_image", "patch_resources", "skip"}


def _parse_json_response(text: str) -> dict:
    """Parse JSON from Claude's response, stripping markdown fences if present."""
    # Strip opening fences (```json, ```JSON, ```) and closing fences (```)
    cleaned = re.sub(r"```(?:json|JSON)?\s*", "", text)
    cleaned = re.sub(r"```\s*$", "", cleaned, flags=re.MULTILINE)
    cleaned = cleaned.strip()
    return json.loads(cleaned)


@activity.defn
async def diagnose_pod(pod_details: str) -> Diagnosis:
    activity.logger.info("Asking Claude to diagnose pod")

    ai = anthropic.Anthropic()

    try:
        response = ai.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": pod_details}],
        )
    except anthropic.AuthenticationError as e:
        raise ApplicationError(
            f"Anthropic auth failed: {e}",
            non_retryable=True,
        )
    except anthropic.RateLimitError as e:
        raise ApplicationError(f"Anthropic rate limit: {e}")
    except anthropic.APIStatusError as e:
        if e.status_code >= 500:
            raise ApplicationError(f"Anthropic server error: {e}")
        raise ApplicationError(
            f"Anthropic API error: {e}",
            non_retryable=True,
        )

    if not response.content:
        raise ApplicationError("Claude returned empty response for diagnosis")

    raw_text = response.content[0].text

    try:
        data = _parse_json_response(raw_text)
    except (json.JSONDecodeError, ValueError) as e:
        raise ApplicationError(f"Failed to parse Claude diagnosis JSON: {e}")

    # Validate action is in known set
    action = data.get("action", "skip")
    if action not in VALID_ACTIONS:
        activity.logger.warning(f"LLM returned unknown action '{action}', defaulting to 'skip'")
        action = "skip"

    diagnosis = Diagnosis(
        pod_name=data["pod_name"],
        root_cause=data["root_cause"],
        severity=data["severity"],
        action=action,
        explanation=data["explanation"],
        fix_details=data.get("fix_details", {}),
    )

    activity.logger.info(f"Diagnosis: [{diagnosis.severity.upper()}] {diagnosis.root_cause}")
    activity.logger.info(f"Action: {diagnosis.action} — {diagnosis.explanation}")
    return diagnosis
