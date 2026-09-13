#!/usr/bin/env python3
"""Deterministic Pi 0.84.4-shaped RPC peer; never calls a model."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def emit(event):
    print(json.dumps(event, ensure_ascii=False), flush=True)


def assistant(text, reason="stop"):
    message = {"role": "assistant", "content": [{"type": "text", "text": text}], "stopReason": reason}
    emit({"type": "message_start", "message": {"role": "assistant", "content": []}})
    emit({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": text}})
    emit({"type": "message_end", "message": message})
    emit({"type": "agent_end", "messages": [message], "willRetry": reason == "error"})


session = None
models = [{"provider": "fixture", "id": "default", "name": "Default"},
          {"provider": "fixture", "id": "other", "name": "Other", "headers": {"Secret": "never-expose"}},
          {"provider": "fixture", "id": "unconfirmed", "name": "Unconfirmed"}]
model = models[0]
thinking = "off"
messages = []
for line in sys.stdin.buffer:
    command = json.loads(line)
    kind = command["type"]
    response = {"type": "response", "id": command.get("id"), "command": kind, "success": True}
    if kind == "extension_ui_response":
        assistant("confirmed=" + str(command.get("confirmed")))
        emit({"type": "agent_settled"})
        continue
    if kind == "hang":
        time.sleep(60)
    if kind == "wrong-command":
        response["command"] = "unrelated"
    if kind == "get_state":
        response["data"] = {"sessionFile": session, "model": model, "thinkingLevel": thinking, "messageCount": len(messages)}
    elif kind == "get_available_thinking_levels":
        response["data"] = {"levels": ["off"] if model["id"] == "other" else ["off", "minimal", "low", "medium", "high", "xhigh", "max"]}
    elif kind == "set_thinking_level":
        thinking = command["level"]
    elif kind == "get_messages":
        response["data"] = {"messages": messages}
    elif kind == "get_session_stats":
        response["data"] = {"totalMessages": len(messages), "toolCalls": 0, "tokens": {"total": 42}, "sessionFile": session}
    elif kind == "get_available_models":
        response["data"] = {"models": models}
    elif kind == "switch_session":
        target = command["sessionPath"]
        if "cancel-switch" in target:
            response["data"] = {"cancelled": True}
        elif "fail-switch" in target:
            response.update(success=False, error="injected switch failure")
        else:
            session = target if "wrong-switch" not in target else "/tmp/wrong.jsonl"
            transcript = Path(session).with_suffix(".fixture.json")
            messages = json.loads(transcript.read_text()) if transcript.exists() else []
            response["data"] = {"cancelled": False}
    elif kind == "set_model" and command.get("modelId") == "bad-model":
        response.update(success=False, error="injected unknown model")
    elif kind == "set_model":
        if command["modelId"] != "unconfirmed":
            model = {"provider": command["provider"], "id": command["modelId"]}
        response["data"] = model
    emit(response)
    if kind != "prompt":
        continue
    text = command["message"]
    messages.append({"role": "user", "content": [{"type": "text", "text": text}]})
    Path(session).with_suffix(".fixture.json").write_text(json.dumps(messages))
    emit({"type": "agent_start"})
    if text == "tools-progress":
        emit({"type": "tool_execution_start", "toolName": "read", "toolCallId": "t1"})
        emit({"type": "tool_execution_end", "toolName": "read", "toolCallId": "t1", "result": {}})
        emit({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "delta": "private-thinking"}})
    if text == "eof":
        sys.stdout.close()
        os._exit(0)
    if text == "malformed":
        print("not json", flush=True)
        continue
    if text == "settled-only":
        emit({"type": "agent_settled"})
        continue
    if text == "confirm":
        emit({"type": "extension_ui_request", "id": "approval", "method": "confirm", "title": "Execute?"})
        continue
    if text.startswith("child:"):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        Path(text[6:]).write_text(str(child.pid))
        time.sleep(60)
    if text == "retry-success":
        assistant("failed preview", "error")
        emit({"type": "auto_retry_start", "attempt": 1})
        emit({"type": "agent_start"})
        assistant("recovered")
        emit({"type": "auto_retry_end", "success": True})
    elif text in {"error", "aborted", "length", "toolUse"}:
        assistant("partial answer", text)
    elif text == "retries-exhausted":
        assistant("partial answer", "error")
        emit({"type": "auto_retry_end", "success": False})
    else:
        assistant(session if text == "session" else model["id"] if text == "which-model" else thinking if text == "which-thinking" else text)
    emit({"type": "agent_settled"})
