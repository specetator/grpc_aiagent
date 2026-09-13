#!/usr/bin/env python3
"""OpenAI-compatible HTTP front for a local Pi coding-agent RPC process.

Spark Push's hermes_bridge keeps posting to /v1/chat/completions. This process
is the drop-in replacement for Hermes' API server: it drives Pi, streams text
deltas, then emits a hermes.final/pi.final event with verified CANN citations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import queue
import re
import select
import signal
import subprocess
import sys
import threading
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from pi_citations import TurnCitationState
from agent_events import THINKING_LEVELS, command_card, AgentAdapter, agent_event, public_model

DIALOG_METHODS = {"select", "confirm", "input", "editor"}


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def rewrite_user_text(text: str) -> str:
    stripped = text.strip()
    lowered = stripped.lower()
    if lowered == "/cann" or lowered.startswith("/cann "):
        query = stripped[5:].strip()
        if not query:
            return "[CANN_FORCE_SEARCH] 请先给出要检索的 CANN 开发问题。"
        return f"[CANN_FORCE_SEARCH] {query}"
    if lowered == "/kb" or lowered.startswith("/kb "):
        query = stripped[3:].strip()
        if not query:
            return "[CANN_FORCE_SEARCH] 请先给出要检索的问题；/kb status 可查看知识库状态。"
        if query.lower() == "status":
            return (
                "[CANN_STATUS] 请调用 cann_knowledge_status，并只根据工具结果说明"
                "来源、文档数和当前 generation。"
            )
        return f"[CANN_FORCE_SEARCH] {query}"
    return text


def latest_user_text(messages: list[dict]) -> str:
    for item in reversed(messages):
        if not isinstance(item, dict):
            raise ValueError("messages must contain objects")
        if str(item.get("role") or "") != "user":
            continue
        content = item.get("content")
        if not isinstance(content, str):
            raise ValueError("only text input is supported by this gateway version")
        text = rewrite_user_text(content)
        if text.strip():
            return text
        raise ValueError("the latest user message must not be empty")
    raise ValueError("a non-empty user message is required")


def session_filename(session_id: str, context_start_seq: int) -> str:
    if not isinstance(session_id, str) or not session_id.strip() or len(session_id) > 512:
        raise ValueError("session_id must contain 1-512 characters")
    if type(context_start_seq) is not int or not 0 <= context_start_seq <= 2**63 - 1:
        raise ValueError("context_start_seq must be a non-negative int64")
    # Preserve existing Spark single-chat filenames; reserve a separate namespace
    # for arbitrary IDs instead of lossy character replacement/truncation.
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,119}", session_id) and not session_id.startswith("sha256-"):
        safe = session_id
    else:
        safe = "sha256-" + hashlib.sha256(session_id.encode("utf-8")).hexdigest()
    return f"{safe}__{context_start_seq}.jsonl"


def extract_text(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for part in content:
        if isinstance(part, str):
            parts.append(part)
        elif isinstance(part, dict) and part.get("type") in {"text", "output_text"}:
            parts.append(str(part.get("text") or ""))
    return "".join(parts)


def auto_ui_response(request: dict) -> dict | None:
    method = request.get("method")
    request_id = request.get("id")
    if method not in DIALOG_METHODS or not request_id:
        return None
    if method == "confirm":
        return {"type": "extension_ui_response", "id": request_id, "confirmed": False}
    return {"type": "extension_ui_response", "id": request_id, "cancelled": True}


class PiRpcClient:
    MAX_RECORD_BYTES = 8 * 1024 * 1024

    def __init__(self, argv: list[str], cwd: Path, env: dict[str, str], session_dir: Path):
        self.argv = argv
        self.cwd = cwd
        self.env = env
        self.session_dir = session_dir
        self.lock = threading.Lock()
        self.io_lock = threading.RLock()
        self.write_lock = threading.Lock()
        self.proc: subprocess.Popen[bytes] | None = None
        self.pending: dict[str, queue.Queue] = {}
        self.events: queue.Queue = queue.Queue(maxsize=4096)
        self.cmd_id = 0
        self.reader: threading.Thread | None = None
        self.current_session_path: Path | None = None
        self.ready = False
        self.usable = False
        self.deadline: float | None = None
        self.default_model: dict | None = None
        self.default_thinking = "off"
        self.active_context_start_seq = 0

    def start(self) -> None:
        self._spawn()
        try:
            initial = self.rpc({"type": "get_state"}, timeout=20).get("data") or {}
            self.default_model = public_model(initial.get("model"))
            self.default_thinking = initial.get("thinkingLevel", "off")
            with self.io_lock:
                if not self.usable or not self.proc or self.proc.poll() is not None:
                    raise RuntimeError("Pi exited during startup")
                self.ready = True
        except Exception:
            self.stop()
            raise

    def is_ready(self) -> bool:
        with self.io_lock:
            return bool(self.ready and self.usable and self.proc and self.proc.poll() is None)

    def _spawn(self) -> None:
        with self.io_lock:
            if self.proc is not None:
                raise RuntimeError("previous Pi process must be stopped before replacement")
            log("starting Pi RPC")
            self.current_session_path = None
            self.ready = False
            self.events = queue.Queue(maxsize=4096)
            self.proc = subprocess.Popen(
                self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=str(self.cwd), env=self.env,
                bufsize=0, start_new_session=True,
            )
            self.usable = True
            self.reader = threading.Thread(target=self._read_stdout, args=(self.proc,), daemon=True)
            self.reader.start()
            threading.Thread(target=self._read_stderr, args=(self.proc,), daemon=True).start()

    def _read_stdout(self, proc: subprocess.Popen) -> None:
        assert proc.stdout
        reason = "Pi RPC stdout closed"
        try:
            while True:
                line = proc.stdout.readline(self.MAX_RECORD_BYTES + 1)
                if not line:
                    break
                if len(line) > self.MAX_RECORD_BYTES or not line.endswith(b"\n"):
                    raise ValueError("invalid RPC record framing or size")
                text = line.decode("utf-8").rstrip("\r\n")
                if not text:
                    continue
                event = json.loads(text)
                if not isinstance(event, dict):
                    raise ValueError("RPC event must be an object")
                self._dispatch(event, proc)
        except (OSError, ValueError, RuntimeError, queue.Full):
            reason = "Pi RPC stream failed or exceeded its buffer limit"
        finally:
            self._fail_process(proc, reason)

    def _read_stderr(self, proc: subprocess.Popen) -> None:
        assert proc.stderr
        # Drain stderr without copying tool output or credentials into IM logs.
        try:
            while proc.stderr.read(4096):
                pass
        except (OSError, ValueError):
            pass

    def _fail_process(self, proc: subprocess.Popen, reason: str) -> None:
        with self.io_lock:
            if proc is not self.proc:
                return
            self.usable = self.ready = False
            self.current_session_path = None
            for waiter in self.pending.values():
                try:
                    waiter.put_nowait(RuntimeError(reason))
                except queue.Full:
                    pass
            log(reason)

    def _dispatch(self, event: dict, proc: subprocess.Popen) -> None:
        with self.io_lock:
            # A reader belongs to exactly one process. Never forward its late
            # replies/events into a replacement process's queue.
            if proc is not self.proc or not self.usable:
                return
            if event.get("type") == "response":
                rid = event.get("id")
                waiter = self.pending.get(rid) if isinstance(rid, str) else None
                if waiter is not None:
                    try:
                        waiter.put_nowait(event)
                    except queue.Full:
                        log("duplicate Pi RPC response ignored")
                else:
                    log("unmatched Pi RPC response ignored")
                return
            if event.get("type") == "extension_ui_request":
                reply = auto_ui_response(event)
            else:
                self.events.put_nowait(event)
                return
        if reply is not None:
            self._write(reply, proc)

    def _write(self, payload: dict, expected_proc: subprocess.Popen | None = None,
               timeout: float = 30.0) -> None:
        data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        if len(data) > self.MAX_RECORD_BYTES:
            raise ValueError("Pi RPC command is too large")
        deadline = min(time.monotonic() + timeout, self.deadline or math.inf)
        if not self.write_lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise TimeoutError("Pi RPC writer is busy")
        fd = None
        try:
            with self.io_lock:
                proc = self.proc
                if (not self.usable or not proc or not proc.stdin or proc.poll() is not None
                        or (expected_proc is not None and proc is not expected_proc)):
                    raise RuntimeError("Pi RPC process is not running")
                # Pin this pipe across concurrent shutdown, avoiding fd reuse.
                fd = os.dup(proc.stdin.fileno())
            os.set_blocking(fd, False)
            # Do not hold the state/reader lock while waiting for stdin capacity.
            remaining = memoryview(data)
            while remaining:
                wait = deadline - time.monotonic()
                if wait <= 0 or not select.select([], [fd], [], wait)[1]:
                    raise TimeoutError("Pi RPC write timed out")
                try:
                    written = os.write(fd, remaining)
                except BlockingIOError:
                    continue
                if not written:
                    raise BrokenPipeError("Pi RPC write failed")
                remaining = remaining[written:]
        finally:
            if fd is not None:
                os.close(fd)
            self.write_lock.release()

    def _ensure(self) -> None:
        if not self.is_ready():
            self.stop()
            self.start()

    def rpc(self, payload: dict, timeout: float = 30.0) -> dict:
        # Restart only at a chat boundary, never midway through session binding
        # or an accepted prompt. Command responses only confirm acceptance.
        if self.deadline is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
        if timeout <= 0:
            raise TimeoutError("Pi request deadline expired before command dispatch")
        with self.io_lock:
            proc = self.proc
            if proc is None or not self.usable:
                raise RuntimeError("Pi RPC process is not running")
            self.cmd_id += 1
            request_id = f"cmd-{self.cmd_id}"
            waiter: queue.Queue = queue.Queue(maxsize=1)
            self.pending[request_id] = waiter
        try:
            command_deadline = time.monotonic() + timeout
            self._write({**payload, "id": request_id}, proc, timeout)
            response = waiter.get(timeout=max(0, command_deadline - time.monotonic()))
        except queue.Empty as exc:
            self.stop()
            raise TimeoutError(f"Pi RPC {payload.get('type')} timed out") from exc
        except OSError:
            self.stop()
            raise
        finally:
            with self.io_lock:
                self.pending.pop(request_id, None)
        if isinstance(response, Exception):
            raise response
        if response.get("command") != payload.get("type"):
            self.stop()
            raise RuntimeError("Pi RPC response command mismatch")
        if response.get("success") is not True:
            raise RuntimeError(f"Pi RPC {payload.get('type')} failed")
        return response

    def drain_events(self) -> None:
        while True:
            try:
                self.events.get_nowait()
            except queue.Empty:
                return

    def _state_path(self, session_id: str, suffix: str) -> Path:
        self.session_dir.mkdir(parents=True, exist_ok=True)
        path = self.session_dir.resolve() / session_filename(session_id, 0)
        path = path.with_suffix(suffix)
        if path.is_symlink() or path.resolve().parent != self.session_dir.resolve():
            raise ValueError("invalid Agent state path")
        return path

    @staticmethod
    def _read_state(path: Path) -> dict | None:
        if not path.exists():
            return None
        if path.stat().st_size > 4096:
            raise ValueError("invalid Agent state size")
        saved = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(saved, dict) or saved.get("schema") != 1:
            raise ValueError("invalid Agent state")
        return saved

    def _route(self, session_id: str) -> int:
        saved = self._read_state(self._state_path(session_id, ".route.json"))
        revision = saved.get("context_start_seq") if saved else 0
        session_filename(session_id, revision)
        return revision

    def bind_session(self, session_id: str, context_start_seq: int) -> None:
        # Runtime confirmation wins over delayed IM history after /new.
        context_start_seq = max(context_start_seq, self._route(session_id))
        self._bind_exact_session(session_id, context_start_seq)

    def _bind_exact_session(self, session_id: str, context_start_seq: int) -> None:
        self.active_context_start_seq = context_start_seq
        self.session_dir.mkdir(parents=True, exist_ok=True)
        root = self.session_dir.resolve()
        path = root / session_filename(session_id, context_start_seq)
        if path.is_symlink() or path.resolve().parent != root:
            raise ValueError("invalid session file path")
        if self.current_session_path == path:
            return
        self.current_session_path = None
        response = self.rpc({"type": "switch_session", "sessionPath": str(path)}, timeout=20)
        if (response.get("data") or {}).get("cancelled") is not False:
            raise RuntimeError("Pi session switch was cancelled or not confirmed")
        state = self.rpc({"type": "get_state"}, timeout=10).get("data") or {}
        actual = state.get("sessionFile")
        if not isinstance(actual, str) or Path(actual).resolve() != path:
            raise RuntimeError("Pi did not bind the requested session")
        self.current_session_path = path
        log(f"Pi session {path.name}")

    def _model_path(self) -> Path:
        if self.current_session_path is None:
            raise RuntimeError("session is not bound")
        path = self.current_session_path.with_suffix(".model.json")
        if path.is_symlink() or path.resolve().parent != self.session_dir.resolve():
            raise ValueError("invalid model state path")
        return path

    def _load_model_state(self) -> dict | None:
        path = self._model_path()
        if not path.exists():
            return None
        if path.stat().st_size > 4096:
            raise ValueError("invalid model state size")
        saved = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(saved, dict) or saved.get("schema") != 1 or
                type(saved.get("override")) is not bool or
                type(saved.get("revision")) is not int or saved["revision"] < 0 or
                (saved["override"] and public_model(saved.get("model")) is None)):
            raise ValueError("invalid saved model state")
        return saved

    def _save_model_state(self, saved: dict) -> None:
        self._save_state(self._model_path(), saved)

    @staticmethod
    def _save_state(path: Path, saved: dict) -> None:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                             prefix=".model-", delete=False) as output:
                temporary = Path(output.name)
                json.dump(saved, output, ensure_ascii=False)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _set_verified_model(self, target: dict) -> dict:
        actual = public_model((self.rpc({"type": "get_state"}).get("data") or {}).get("model"))
        if actual is None or (actual["provider"], actual["id"]) != (target["provider"], target["id"]):
            self.rpc({"type": "set_model", "provider": target["provider"], "modelId": target["id"]}, timeout=15)
            actual = public_model((self.rpc({"type": "get_state"}).get("data") or {}).get("model"))
        if actual is None or (actual["provider"], actual["id"]) != (target["provider"], target["id"]):
            raise RuntimeError("Pi did not confirm the selected model")
        return actual

    def _activate_model(self, provider: str | None = None, model: str | None = None) -> dict:
        saved = self._load_model_state()
        override = bool(saved and saved["override"])
        target = saved["model"] if override else self.default_model
        # Compatibility for confirmed pre-migration IM history. A durable
        # gateway setting wins over a request built from stale message history.
        if saved is None and model and model not in {"hermes-agent", "pi-agent", "pi"}:
            if not provider:
                raise ValueError("model override requires an explicit provider")
            target = public_model({"provider": provider, "id": model})
            override = True
        if target is None:
            raise RuntimeError("Pi has no configured default model")
        actual = self._set_verified_model(target)
        return {"override": override, "model": actual,
                "revision": saved["revision"] if saved else 0}

    def model_control(self, request: dict, timeout_s: float = 30) -> dict:
        """Compatibility alias for the original model-only adapter port."""
        return self.control(request, timeout_s)

    def control(self, request: dict, timeout_s: float = 30) -> dict:
        session_id = request.get("session_id")
        context_start_seq = request.get("context_start_seq", 0)
        session_filename(session_id, context_start_seq)
        operation = request.get("operation")
        if operation not in {"list_models", "set_model", "reset_model"}:
            return self.command_control(request, timeout_s)
        revision = request.get("command_seq", 0)
        if operation != "list_models" and (type(revision) is not int or not 0 < revision < 2**63):
            raise ValueError("model changes require a positive command_seq")
        if not self.lock.acquire(timeout=timeout_s):
            raise TimeoutError("Agent is busy; retry the model command later")
        self.deadline = time.monotonic() + timeout_s
        try:
            self._ensure()
            self.bind_session(session_id, context_start_seq)
            if operation != "list_models" and revision <= self.active_context_start_seq:
                raise ValueError("模型命令属于旧上下文，请重新发送 /model")
            models = self._catalog()
            catalog = {(m["provider"], m["id"]): m for m in models}
            state = None
            if operation == "list_models":
                state = self._activate_model(request.get("provider"), request.get("model")) if context_start_seq == self.active_context_start_seq else self._activate_model()
            if operation != "list_models":
                target = self.default_model if operation == "reset_model" else public_model({
                    "provider": request.get("target_provider"), "id": request.get("target_model")})
                if target is None or (target["provider"], target["id"]) not in catalog:
                    raise ValueError("模型当前不可用，请重新发送 /model 刷新列表")
                saved = self._load_model_state()
                override = operation == "set_model"
                if saved and revision <= saved["revision"]:
                    if (revision != saved["revision"] or override != saved["override"] or
                            (override and (target["provider"], target["id"]) !=
                             (saved["model"]["provider"], saved["model"]["id"]))):
                        raise ValueError("模型选择已过期，请重新发送 /model")
                actual = self._set_verified_model(target)
                self._save_model_state({"schema": 1, "revision": revision,
                                        "override": override, "model": actual})
                state = {"override": override, "model": actual, "revision": revision}
            current = state["model"]
            label = current["provider"] + ":" + current["id"]
            text = ("当前模型：" if operation == "list_models" else "已切换模型：") + label
            # The current IM history uses MySQL TEXT (64 KiB). Keep the card
            # within a byte budget; manual selection still searches all models.
            shown, size = [], 0
            for model in models[:512]:
                size += len(json.dumps(model, ensure_ascii=False).encode("utf-8"))
                if size > 24000:
                    break
                shown.append(model)
            presentation = {"kind": "model_picker", "models": shown,
                            "current": current, "truncated": len(models) > len(shown)}
            if operation != "list_models":
                presentation = command_card("模型已更新", ["model", "reasoning", "status"])
            return agent_event("assistant_final", {
                "text": text, "model_state": state,
                "context_start_seq": self.active_context_start_seq,
                "thinking_state": self._activate_thinking(),
                "presentation": presentation})
        except Exception:
            self.stop()
            raise
        finally:
            self.deadline = None
            self.lock.release()

    def _thinking_path(self) -> Path:
        path = self._model_path().with_suffix(".thinking.json")
        if path.is_symlink() or path.resolve().parent != self.session_dir.resolve():
            raise ValueError("invalid thinking state path")
        return path

    def _thinking_levels(self) -> list[str]:
        levels = (self.rpc({"type": "get_available_thinking_levels"}).get("data") or {}).get("levels")
        if (not isinstance(levels, list) or not levels or
                any(level not in THINKING_LEVELS for level in levels)):
            raise RuntimeError("Pi returned invalid thinking levels")
        return list(dict.fromkeys(levels))

    def _thinking_saved(self) -> dict | None:
        saved = self._read_state(self._thinking_path())
        if saved and (saved.get("level") not in THINKING_LEVELS or
                      type(saved.get("revision")) is not int or not 0 < saved["revision"] < 2**63):
            raise ValueError("invalid thinking preference")
        return saved

    def _set_thinking(self, level: str) -> str:
        self.rpc({"type": "set_thinking_level", "level": level})
        actual = (self.rpc({"type": "get_state"}).get("data") or {}).get("thinkingLevel")
        if actual != level:
            raise RuntimeError("Pi did not confirm the thinking level")
        return actual

    def _activate_thinking(self) -> dict:
        levels = self._thinking_levels()
        saved = self._thinking_saved()
        preferred = saved["level"] if saved else self.default_thinking
        actual = self._set_thinking(preferred if preferred in levels else levels[0])
        return {"level": actual, "preferred": preferred, "levels": levels}

    def _retry_message(self) -> str:
        messages = (self.rpc({"type": "get_messages"}).get("data") or {}).get("messages")
        if not isinstance(messages, list):
            raise RuntimeError("Pi returned invalid messages")
        for message in reversed(messages):
            if isinstance(message, dict) and message.get("role") == "user":
                content = message.get("content")
                if isinstance(content, list) and any(part.get("type") != "text" for part in content if isinstance(part, dict)):
                    raise ValueError("上一条包含附件，请重新发送完整消息后重试")
                text = extract_text(message)
                if text.strip():
                    return text
                break
        raise ValueError("当前 Agent 上下文没有可重试的用户消息")

    def command_control(self, request: dict, timeout_s: float = 30) -> dict:
        operation = request.get("operation")
        if operation not in {"help", "status", "new", "new_session", "retry", "list_reasoning", "set_reasoning"}:
            raise ValueError("unsupported Agent control operation")
        session_id, context = request.get("session_id"), request.get("context_start_seq", 0)
        session_filename(session_id, context)
        revision = request.get("command_seq", 0)
        if operation in {"set_reasoning", "new_session"} and (type(revision) is not int or not 0 < revision < 2**63):
            raise ValueError("Agent changes require a positive command_seq")
        if not self.lock.acquire(timeout=timeout_s):
            raise TimeoutError("Agent is busy; retry the command later")
        self.deadline = time.monotonic() + timeout_s
        try:
            self._ensure()
            self.bind_session(session_id, context)
            if operation == "new_session":
                previous = self.active_context_start_seq
                if revision < previous:
                    raise ValueError("新建命令已过期，请重新发送 /new")
                self._bind_exact_session(session_id, revision)
                # switch_session creates an empty Pi session for a fresh path;
                # validate before publishing the durable routing pointer.
                state = (self.rpc({"type": "get_state"}).get("data") or {})
                if revision > previous and state.get("messageCount") != 0:
                    raise RuntimeError("new Agent context is not empty")
            model_state = (self._activate_model(request.get("provider"), request.get("model"))
                           if context == self.active_context_start_seq and operation != "new_session"
                           else self._activate_model())
            thinking = self._activate_thinking()
            if operation == "set_reasoning":
                if revision <= self.active_context_start_seq:
                    raise ValueError("思考等级命令属于旧上下文，请刷新 /reasoning")
                level = request.get("level")
                if level not in thinking["levels"]:
                    raise ValueError("此模型不支持该思考等级，请刷新 /reasoning")
                saved = self._thinking_saved()
                if saved and (revision < saved["revision"] or
                              (revision == saved["revision"] and level != saved["level"])):
                    raise ValueError("思考等级选择已过期，请刷新 /reasoning")
                self._set_thinking(level)
                self._save_state(self._thinking_path(), {"schema": 1, "level": level, "revision": revision})
                thinking = {**thinking, "level": level, "preferred": level}
            card = command_card("会话操作", ["model", "reasoning", "retry", "new", "status", "help"])
            if operation in {"list_reasoning", "set_reasoning"}:
                text = ("已设置思考深度：" if operation == "set_reasoning" else "当前思考深度：") + thinking["level"]
                if thinking["preferred"] != thinking["level"]:
                    text += "（偏好 " + thinking["preferred"] + " 不受当前模型支持）"
                card = {"kind": "command_card", "title": "选择思考等级", "actions": [
                    {"id": "reasoning_set", "label": level, "value": level,
                     "selected": level == thinking["level"]} for level in thinking["levels"]]}
                card["actions"] += command_card("", ["model"])["actions"]
                if operation == "set_reasoning":
                    text += "。后续消息将使用此等级。"
                    card = command_card("思考深度已更新", ["reasoning", "model", "status"])
                elif thinking["levels"] == ["off"]:
                    text += "。当前模型未声明可调思考深度，请选择支持推理的模型。"
            elif operation == "new":
                text = "新建将切换到空白 Agent 上下文，并恢复默认模型与思考等级。IM 聊天记录保留。"
                card = command_card("新建上下文？", ["new_confirm", "cancel"])
            elif operation == "new_session":
                text = "已开启新的 Agent 上下文，IM 聊天记录保留。"
            elif operation == "retry":
                self._retry_message()
                text = "重试会将当前 Agent 上下文的最后一条用户消息再次发给 Agent，工具可能再次执行；已有回答和文件操作不会撤销。"
                card = command_card("重试上一条消息？", ["retry_confirm", "cancel"])
            elif operation == "status":
                state = self.rpc({"type": "get_state"}).get("data") or {}
                stats = self.rpc({"type": "get_session_stats"}).get("data") or {}
                count = lambda value: value if type(value) is int and value >= 0 else 0
                model = model_state["model"]
                text = ("Agent 会话状态\n模型：" + model["provider"] + ":" + model["id"] +
                        "\n思考等级：" + thinking["level"] +
                        "\n上下文消息：" + str(count(state.get("messageCount"))) +
                        "\n工具调用：" + str(count(stats.get("toolCalls"))) +
                        "\n累计 tokens：" + str(count((stats.get("tokens") or {}).get("total"))) +
                        "\n状态：空闲（命令按队列执行后查询）")
            else:
                text = ("Agent 常用命令\n/model：模型选择\n/reasoning（/thinking）：思考等级\n"
                        "/retry：重试确认卡片\n/new（/reset、/clear）：新建确认卡片\n"
                        "/status：实际会话状态\n/help（/commands）：命令菜单\n"
                        "/cann <问题>、/kb <问题>：知识库检索；/kb status：知识库状态\n"
                        "可直接发送 /new now 或 /retry now 确认操作。")
            if operation == "new_session":
                self._save_state(self._state_path(session_id, ".route.json"),
                                 {"schema": 1, "context_start_seq": revision})
            return agent_event("assistant_final", {"text": text, "presentation": card,
                "model_state": model_state, "thinking_state": thinking,
                "context_start_seq": self.active_context_start_seq})
        except Exception:
            self.stop()
            raise
        finally:
            self.deadline = None
            self.lock.release()

    def _catalog(self) -> list[dict]:
        raw = (self.rpc({"type": "get_available_models"}, timeout=15).get("data") or {}).get("models")
        if not isinstance(raw, list):
            raise RuntimeError("Pi returned an invalid model catalog")
        catalog = {}
        for value in raw:
            model = public_model(value)
            if model:
                catalog[(model["provider"], model["id"])] = model
        return sorted(catalog.values(), key=lambda item: (item["provider"], item["id"]))

    def available_models(self) -> list[dict]:
        if not self.lock.acquire(timeout=10):
            raise TimeoutError("Agent is busy")
        self.deadline = time.monotonic() + 20
        try:
            self._ensure()
            return self._catalog()
        except Exception:
            self.stop()
            raise
        finally:
            self.deadline = None
            self.lock.release()

    def chat(
        self,
        message: str,
        on_delta: Callable[[str], None] | None,
        timeout_s: float,
        provider: str | None = None,
        model: str | None = None,
        session_id: str = "",
        context_start_seq: int = 0,
        retry: bool = False,
        on_progress: Callable[[str], None] | None = None,
    ) -> tuple[str, dict]:
        session_filename(session_id, context_start_seq)
        if not retry and (not isinstance(message, str) or not message.strip()):
            raise ValueError("a non-empty user message is required")
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("timeout must be positive and finite")
        deadline = time.monotonic() + timeout_s
        if not self.lock.acquire(timeout=timeout_s):
            raise TimeoutError("Pi session wait exceeded request deadline")
        self.deadline = deadline
        try:
            self._ensure()
            self.bind_session(session_id, context_start_seq)
            model_state = self._activate_model(provider, model) if context_start_seq == self.active_context_start_seq else self._activate_model()
            thinking_state = self._activate_thinking()
            if retry:
                message = self._retry_message()
            if on_progress:
                on_progress("等待模型回答 · 思考深度 " + thinking_state["level"])
            started_at = time.monotonic()
            last_progress = started_at
            active_tools = set()
            completed_tools = 0
            phase = "等待模型回答"
            log(f"Pi turn started session={self.current_session_path.name} thinking={thinking_state['level']}")
            self.drain_events()
            self.rpc({"type": "prompt", "message": message}, timeout=15)
            citations = TurnCitationState()
            final_message: dict | None = None
            retry_error: str | None = None
            settled = False
            while time.monotonic() < deadline:
                if not self.is_ready():
                    raise RuntimeError("Pi RPC stream failed during prompt")
                now = time.monotonic()
                if on_progress and now - last_progress >= 15:
                    on_progress(f"{phase} · 已用时 {int(now - started_at)} 秒 · 思考深度 {thinking_state['level']}")
                    last_progress = now
                remaining = deadline - now
                try:
                    event = self.events.get(timeout=max(0.001, min(0.25, remaining)))
                except queue.Empty:
                    continue
                kind = event.get("type")
                if kind == "agent_start":
                    # A new low-level run can follow retry/compaction. Never
                    # finalize from the previous run's failed assistant.
                    final_message = None
                    retry_error = None
                elif kind == "message_start":
                    if (event.get("message") or {}).get("role") == "assistant":
                        final_message = None
                elif kind == "message_end":
                    message_value = event.get("message") or {}
                    if message_value.get("role") == "assistant":
                        final_message = message_value
                elif kind == "message_update":
                    delta_event = event.get("assistantMessageEvent") or {}
                    if delta_event.get("type") == "text_delta":
                        delta = delta_event.get("delta")
                        if isinstance(delta, str) and delta and on_delta:
                            on_delta(delta)
                        phase = "正在生成回答"
                    elif delta_event.get("type") in {"thinking_start", "thinking_delta"}:
                        phase = "模型正在思考"
                elif kind == "tool_execution_start":
                    name = str(event.get("toolName") or "tool")
                    log(f"tool start {name}")
                    active_tools.add(event.get("toolCallId") or name)
                    phase = "正在调用工具：" + name
                    if on_progress:
                        on_progress(phase)
                        last_progress = time.monotonic()
                    elif on_delta:
                        on_delta(f"\n⚙ {name}\n")
                elif kind == "tool_execution_end":
                    name = str(event.get("toolName") or "")
                    log(f"tool end {name} error={event.get('isError', False)}")
                    citations.observe_tool(name, event.get("result"))
                    active_tools.discard(event.get("toolCallId") or name)
                    completed_tools += 1
                    phase = "等待其他工具完成" if active_tools else "工具调用已完成，等待模型回答"
                    if on_progress:
                        on_progress(f"{phase} · 已完成 {completed_tools} 次")
                        last_progress = time.monotonic()
                elif kind == "auto_retry_start":
                    phase = "模型请求重试中"
                    if on_progress:
                        on_progress(phase)
                        last_progress = time.monotonic()
                elif kind == "auto_retry_end" and event.get("success") is False:
                    retry_error = "Pi exhausted automatic retries"
                elif kind == "extension_error":
                    log("Pi extension reported an error")
                elif kind == "agent_settled":
                    settled = True
                    break
            if not settled:
                raise TimeoutError(f"Pi prompt exceeded {timeout_s:.0f}s")
            if retry_error:
                raise RuntimeError(retry_error)
            if final_message is None:
                raise RuntimeError("Pi settled without an assistant result for this prompt")
            reason = final_message.get("stopReason")
            if reason != "stop":
                # Partial text, toolUse, length, error and aborted are not a
                # successful answer. Only this prompt's message_end is trusted.
                raise RuntimeError(f"Pi did not complete the answer (stopReason={reason})")
            final_text = extract_text(final_message)
            if not final_text.strip():
                raise RuntimeError("Pi returned empty assistant content")
            transformed = citations.transform(final_text)
            transformed["metadata"]["model_state"] = model_state
            transformed["metadata"]["thinking_state"] = thinking_state
            transformed["metadata"]["context_start_seq"] = self.active_context_start_seq
            log(f"Pi turn completed session={self.current_session_path.name} elapsed_s={time.monotonic() - started_at:.1f} tools={completed_tools}")
            return transformed["text"], transformed["metadata"]
        except Exception:
            # Includes switch failure, RPC ambiguity, deadline and a downstream
            # disconnect. Destroy the worker before another request can bind it.
            # A later phase adds cooperative per-turn abort and durable execution.
            self.stop()
            raise
        finally:
            self.deadline = None
            self.lock.release()

    def stop(self) -> None:
        with self.io_lock:
            proc = self.proc
            self.proc = None
            self.ready = self.usable = False
            self.current_session_path = None
            for waiter in self.pending.values():
                try:
                    waiter.put_nowait(RuntimeError("Pi RPC process stopped"))
                except queue.Full:
                    pass
            self.pending.clear()
        if proc is None:
            return
        # start_new_session gives each worker its own process group, including
        # tools. Reap the main process and do not leave child commands running.
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream:
                    stream.close()


class GatewayState:
    def __init__(self, rpc: AgentAdapter, api_key: str, default_model: str):
        self.rpc = rpc
        self.api_key = api_key
        self.default_model = default_model


def make_handler(state: GatewayState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            log("%s - " % self.address_string() + format % args)

        def _unauthorized(self) -> bool:
            header = self.headers.get("Authorization", "")
            expected = "Bearer " + state.api_key
            if header != expected:
                self._json(401, {"error": {"message": "unauthorized"}})
                return True
            return False

        def _read_json(self) -> dict:
            if self.headers.get("Transfer-Encoding"):
                raise ValueError("chunked requests are not supported")
            length = int(self.headers.get("Content-Length") or "0")
            if length < 0 or length > 1024 * 1024:
                raise ValueError("request body must not exceed 1 MiB")
            self.connection.settimeout(30)
            raw = self.rfile.read(length) if length else b"{}"
            if length and len(raw) != length:
                raise ValueError("incomplete request body")
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict):
                raise ValueError("request body must be an object")
            return value

        def _json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def _sse_headers(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()

        def _sse(self, data: str, event: str | None = None) -> None:
            if event:
                self.wfile.write(f"event: {event}\n".encode("utf-8"))
            self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
            self.wfile.flush()

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in {"/health", "/health/"}:
                ready = state.rpc.is_ready()
                self._json(200 if ready else 503, {"ok": ready, "agent": "multi" if hasattr(state.rpc, "entries") else "pi"})
                return
            if self._unauthorized():
                return
            if path in {"/v1/models", "/models"}:
                try:
                    models = state.rpc.available_models()
                    self._json(200, {"object": "list", "data": [
                        {"id": m["provider"] + ":" + m["id"], "object": "model",
                         "owned_by": m["provider"], "name": m["name"]} for m in models]})
                except Exception:
                    self._json(503, {"error": {"message": "Agent model catalog unavailable"}})
                return
            self._json(404, {"error": {"message": "not found"}})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if self._unauthorized():
                return
            if path not in {"/v1/chat/completions", "/chat/completions", "/v1/agent/control"}:
                self._json(404, {"error": {"message": "not found"}})
                return
            try:
                request = self._read_json()
            except Exception as exc:
                self._json(400, {"error": {"message": f"invalid json: {exc}"}})
                return
            if path == "/v1/agent/control":
                try:
                    self._json(200, state.rpc.control(request, timeout_s=120 if request.get("operation")=="restart_agent" else 30))
                except ValueError as exc:
                    self._json(422, {"error": {"message": str(exc)}})
                except TimeoutError:
                    self._json(503, {"error": {"message": "Agent busy; retry later"}})
                except Exception:
                    self._json(502, {"error": {"message": "Agent control failed"}})
                return
            retry = request.get("turn_operation") == "retry"
            messages = request.get("messages") or []
            if not isinstance(messages, list) or (not messages and not retry):
                self._json(400, {"error": {"message": "messages are empty"}})
                return
            try:
                prompt = "" if retry else latest_user_text(messages)
                session_id = request.get("session_id")
                context_start_seq = request.get("context_start_seq", 0)
                session_filename(session_id, context_start_seq)
            except ValueError as exc:
                self._json(400, {"error": {"message": str(exc)}})
                return
            stream = bool(request.get("stream"))
            generic_events = request.get("agent_events") is True
            provider = str(request.get("provider") or "") or None
            model = str(request.get("model") or "") or None
            timeout_s = float(os.environ.get("SPARK_PUSH_PI_TIMEOUT_S", "600"))

            if not stream:
                try:
                    text, metadata = state.rpc.chat(
                        prompt, None, timeout_s, provider, model,
                        session_id, context_start_seq, retry=retry,
                    )
                except Exception as exc:
                    self._json(502, {"error": {"message": str(exc)}})
                    return
                self._json(
                    200,
                    {
                        "id": "pi-chat",
                        "object": "chat.completion",
                        "model": model or state.default_model,
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": text},
                                "finish_reason": "stop",
                            }
                        ],
                        "hermes": {"response_metadata": metadata},
                    },
                )
                return

            self._sse_headers()
            try:
                def on_delta(delta: str) -> None:
                    if generic_events:
                        self._sse(json.dumps(agent_event("assistant_delta", {"text": delta}),
                                             ensure_ascii=False), event="agent.event")
                    else:
                        self._sse(json.dumps({"choices": [{"index": 0, "delta": {"content": delta}}]},
                                             ensure_ascii=False))

                def on_progress(status: str) -> None:
                    self._sse(json.dumps(agent_event("assistant_progress", {"text": status}),
                                         ensure_ascii=False), event="agent.event")

                text, metadata = state.rpc.chat(
                    prompt, on_delta, timeout_s, provider, model,
                    session_id, context_start_seq, retry=retry,
                    on_progress=on_progress if generic_events else None,
                )
                if generic_events:
                    self._sse(
                        json.dumps(agent_event("assistant_final", {"text": text, "metadata": metadata}),
                                   ensure_ascii=False), event="agent.event")
                self._sse(
                    json.dumps(
                        {
                            "final_response": text,
                            "response_metadata": metadata,
                        },
                        ensure_ascii=False,
                    ),
                    event="pi.final",
                )
                self._sse(
                    json.dumps(
                        {
                            "final_response": text,
                            "response_metadata": metadata,
                        },
                        ensure_ascii=False,
                    ),
                    event="hermes.final",
                )
                self._sse("[DONE]")
            except Exception as exc:
                try:
                    self._sse(json.dumps({"error": {"message": str(exc)}}, ensure_ascii=False))
                except (OSError, TimeoutError):
                    pass
                self.close_connection = True

    return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("SPARK_PUSH_PI_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SPARK_PUSH_PI_PORT", "8643")))
    parser.add_argument("--api-key", default=os.environ.get("SPARK_PUSH_HERMES_API_KEY") or os.environ.get("SPARK_PUSH_PI_API_KEY", ""))
    parser.add_argument("--pi-bin", default=os.environ.get("SPARK_PUSH_PI_BIN", "pi"))
    parser.add_argument("--pi-home", default=os.environ.get("PI_CODING_AGENT_DIR", str(Path.home() / ".pi-spark-agent")))
    parser.add_argument("--cwd", default=os.environ.get("SPARK_PUSH_PI_CWD", os.getcwd()))
    parser.add_argument("--provider", default=os.environ.get("SPARK_PUSH_PI_PROVIDER", "cli-relay"))
    parser.add_argument("--model", default=os.environ.get("SPARK_PUSH_PI_MODEL", "gpt-5.6-luna"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        log("SPARK_PUSH_HERMES_API_KEY or SPARK_PUSH_PI_API_KEY is required")
        return 2
    pi_home = Path(args.pi_home).expanduser().resolve()
    cwd = Path(args.cwd).expanduser().resolve()
    env = os.environ.copy()
    env["PI_CODING_AGENT_DIR"] = str(pi_home)
    env["CANN_KNOWLEDGE_ROOT"] = env.get(
        "CANN_KNOWLEDGE_ROOT",
        str(cwd.parent / "cann-agent-knowledge"),
    )
    node_bin = str(Path.home() / ".local" / "node" / "bin")
    env["PATH"] = node_bin + os.pathsep + env.get("PATH", "")
    env_file = pi_home / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            env.setdefault(key.strip(), value.strip().strip('"'))

    session_dir = pi_home / "sessions" / "spark"
    session_dir.mkdir(parents=True, exist_ok=True)
    prompt_file = Path(__file__).resolve().parents[1] / "pi" / "spark-chat-system-prompt.md"
    command = [
        args.pi_bin,
        "--mode",
        "rpc",
        "--approve",
        "--session-dir",
        str(session_dir),
        "--provider",
        args.provider,
        "--model",
        args.model,
    ]
    if prompt_file.is_file():
        command.extend(["--append-system-prompt", str(prompt_file)])
    rpc = PiRpcClient(command, cwd, env, session_dir)
    try:
        rpc.start()
        log("Pi RPC ready")
    except Exception as exc:
        log(f"Pi RPC get_state failed: {type(exc).__name__}: {exc}")
    registry = Path(os.environ.get("SPARK_PUSH_AGENT_REGISTRY", str(pi_home / "agents.json")))
    adapter = rpc
    if registry.is_file():
        from agent_router import load_router
        try:
            adapter = load_router(registry, rpc, pi_home / "im-router")
        except Exception:
            rpc.stop()
            raise
        log("Agent registry loaded")
    server = ThreadingHTTPServer((args.host, args.port), make_handler(GatewayState(adapter, args.api_key, args.model)))
    log(f"Pi gateway listening on http://{args.host}:{args.port}/v1")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        adapter.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
