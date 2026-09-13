"""Channel-neutral output contract. No Pi objects or channel API payloads."""
from typing import Any, Callable, Protocol


class AgentAdapter(Protocol):
    """Initial runtime port used by the HTTP gateway; no Pi RPC types escape.

    Session-worker lifecycle/abort ports will be added when their semantics are
    implemented. The current adapter still serializes requests in one worker.
    """
    def is_ready(self) -> bool: ...
    def available_models(self) -> list[dict]: ...
    def control(self, request: dict, timeout_s: float = 30) -> dict: ...
    def chat(self, message: str, on_delta: Callable[[str], None] | None,
             timeout_s: float, provider: str | None = None, model: str | None = None,
             session_id: str = "", context_start_seq: int = 0, retry: bool = False, on_progress: Callable[[str], None] | None = None) -> tuple[str, dict]: ...

EVENT_TYPES = frozenset({
    "user_message", "assistant_start", "assistant_delta", "thinking_delta",
    "tool_start", "tool_update", "tool_end", "attachment", "assistant_final",
    "abort", "error", "assistant_progress",
})


def agent_event(kind: str, data: dict[str, Any]) -> dict[str, Any]:
    if kind not in EVENT_TYPES:
        raise ValueError("unknown agent event")
    return {"schema": "sparkpush.agent_event.v1", "type": kind, "data": data}


def public_model(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    provider, model_id = value.get("provider"), value.get("id")
    if not isinstance(provider, str) or not isinstance(model_id, str):
        return None
    target = provider + ":" + model_id
    if not provider or not model_id or ":" in provider or len(target.encode()) > 128:
        return None
    if any(ord(c) < 33 or ord(c) == 127 for c in target):
        return None
    name = value.get("name")
    if not isinstance(name, str) or not name.strip():
        name = model_id
    return {"provider": provider, "id": model_id,
            "name": "".join(c for c in name if ord(c) >= 32)[:160],
            "reasoning": value.get("reasoning") is True}


THINKING_LEVELS = ("off", "minimal", "low", "medium", "high", "xhigh", "max")
COMMAND_LABELS = {"model": "选择模型", "reasoning": "思考等级", "retry": "重试上一条",
                  "restart": "重启服务", "restart_confirm": "确认重启 technical",
                  "new": "新建上下文", "status": "会话状态", "help": "全部命令",
                  "new_confirm": "确认新建", "retry_confirm": "确认重试", "cancel": "取消"}


def command_card(title: str, actions: list[str]) -> dict:
    return {"kind": "command_card", "title": title,
            "actions": [{"id": key, "label": COMMAND_LABELS[key]} for key in actions]}
