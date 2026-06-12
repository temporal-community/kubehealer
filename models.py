from dataclasses import dataclass, field


@dataclass
class PodIssue:
    name: str
    namespace: str
    status: str
    reason: str
    message: str


@dataclass
class Diagnosis:
    pod_name: str
    root_cause: str
    severity: str
    action: str
    explanation: str
    fix_details: dict = field(default_factory=dict)
    namespace: str = "default"


@dataclass
class HealResult:
    pod_name: str
    success: bool
    action_taken: str
    details: str
    deployment: str = ""  # workload the fix touched — what verify_healed watches


@dataclass
class HealerInput:
    namespace: str = "default"
    auto_approve: bool = True
    # Emit custom Search Attributes (HealPhase, PodsHealed, …) for the Web UI. On by
    # default; tests pass False so they don't need a server with the SAs registered.
    track_phase: bool = True


@dataclass
class ConversationInput:
    namespace: str = "default"
    session_id: str = ""
    messages: list = field(default_factory=list)
    healing_diagnoses: list = field(default_factory=list)
    healing_decisions: dict = field(default_factory=dict)
    turn_count: int = 0


@dataclass
class ClaudeRequest:
    messages: list
    tools: list
    system_prompt: str


@dataclass
class ClaudeResponse:
    stop_reason: str
    content: list
