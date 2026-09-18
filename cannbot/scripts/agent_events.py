"""Channel-neutral Agent contract and Spark message envelope helpers.

The envelope identifies one logical route/turn independently from its current
transport. Channel adapters may project it to legacy payloads, but they must
not invent a second session identity or reorder event sequence numbers.
"""
import hashlib
import json
import re
import time
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
             session_id: str = "", context_start_seq: int = 0, retry: bool = False,
             on_progress: Callable[[str], None] | None = None,
             request_id: str = "") -> tuple[str, dict]: ...

EVENT_TYPES = frozenset({
    "user_message", "assistant_start", "assistant_delta", "thinking_delta",
    "tool_start", "tool_update", "tool_end", "attachment", "assistant_final",
    "abort", "error", "assistant_progress",
})

ENVELOPE_SCHEMA = "sparkpush.agent_envelope.v1"
_SLUG = re.compile(r"[a-z][a-z0-9_-]{0,47}")


def canonical_route(tenant_id: str, channel_id: str, conversation_id: str,
                    user_id: int, agent_id: str, thread_id: str = "_") -> dict[str, Any]:
    """Build the stable route/session identity from trusted adapter fields."""
    for value, label in ((tenant_id, "tenant"), (channel_id, "channel")):
        if not isinstance(value, str) or not _SLUG.fullmatch(value):
            raise ValueError(f"invalid {label} id")
    if not isinstance(agent_id, str) or not _SLUG.fullmatch(agent_id):
        raise ValueError("invalid Agent id")
    if (not isinstance(conversation_id, str) or not conversation_id or
            len(conversation_id.encode("utf-8")) > 160 or
            any(ord(c) < 33 or ord(c) == 127 for c in conversation_id)):
        raise ValueError("invalid conversation id")
    if (not isinstance(thread_id, str) or not thread_id or
            len(thread_id.encode("utf-8")) > 96 or
            any(ord(c) < 33 or ord(c) == 127 for c in thread_id)):
        raise ValueError("invalid thread id")
    if type(user_id) is not int or user_id <= 0:
        raise ValueError("invalid route user")
    material = json.dumps({"tenant": tenant_id, "channel": channel_id,
                           "conversation": conversation_id, "thread": thread_id},
                          sort_keys=True, separators=(",", ":")).encode("utf-8")
    route_key = "rt_" + hashlib.sha256(material).hexdigest()[:32]
    return {
        "route_key": route_key,
        "session_key": f"agent:{agent_id}:{channel_id}:{route_key[3:]}",
        "tenant_id": tenant_id,
        "channel_id": channel_id,
        "conversation_id": conversation_id,
        "thread_id": thread_id,
        "user_id": user_id,
        "agent_id": agent_id,
    }


def event_envelope(request_id: str, route: dict[str, Any], sequence: int,
                   *, terminal: bool = False, replayed: bool = False) -> dict[str, Any]:
    if (not isinstance(request_id, str) or not request_id or
            len(request_id.encode("utf-8")) > 121 or
            any(ord(c) < 33 or ord(c) == 127 for c in request_id)):
        raise ValueError("invalid request id")
    if type(sequence) is not int or sequence < 0 or sequence >= 2**31:
        raise ValueError("invalid Agent event sequence")
    required = {"route_key", "session_key", "tenant_id", "channel_id",
                "conversation_id", "thread_id", "agent_id"}
    if not isinstance(route, dict) or not required.issubset(route):
        raise ValueError("invalid Agent route")
    return {
        "schema": ENVELOPE_SCHEMA,
        "event_id": "evt_" + hashlib.sha256(
            f"{request_id}\0{sequence}".encode("utf-8")).hexdigest()[:32],
        "request_id": request_id,
        "route_key": route["route_key"],
        "session_key": route["session_key"],
        "tenant_id": route["tenant_id"],
        "channel_id": route["channel_id"],
        "conversation_id": route["conversation_id"],
        "thread_id": route["thread_id"],
        "agent_id": route["agent_id"],
        "sequence": sequence,
        "created_at_ms": int(time.time() * 1000),
        "replayable": True,
        "terminal": terminal,
        "replayed": replayed,
    }


def agent_event(kind: str, data: dict[str, Any],
                envelope: dict[str, Any] | None = None) -> dict[str, Any]:
    if kind not in EVENT_TYPES:
        raise ValueError("unknown agent event")
    event = {"schema": "sparkpush.agent_event.v1", "type": kind, "data": data}
    if envelope is not None:
        if envelope.get("schema") != ENVELOPE_SCHEMA:
            raise ValueError("invalid Agent event envelope")
        event["envelope"] = envelope
    return event


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
