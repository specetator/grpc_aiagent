#!/usr/bin/env python3
"""RPC lifecycle and input/HTTP contracts without a live Pi or network."""
import io
import json
import os
from pathlib import Path
import queue
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock

from pi_gateway import PiRpcClient, GatewayState, auto_ui_response, latest_user_text, make_handler, session_filename

FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/fake_pi_rpc.py"


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.client = PiRpcClient([sys.executable, str(FIXTURE)], self.root,
                                  os.environ.copy(), self.root / "sessions")

    def tearDown(self):
        self.client.stop()
        self.temp.cleanup()

    def chat(self, text, session="s_1_900000000001", **kwargs):
        return self.client.chat(text, kwargs.pop("on_delta", None), kwargs.pop("timeout_s", 3),
                                session_id=session, **kwargs)[0]

    def test_roundtrip_uses_current_message_and_preserves_unicode(self):
        self.assertEqual(self.chat("old answer"), "old answer")
        self.assertEqual(self.chat("中文\u2028emoji🙂\u2029tail"), "中文\u2028emoji🙂\u2029tail")

    def test_session_switch_and_restart_rebind(self):
        self.assertEqual(self.chat("session", session="a"), str(self.root / "sessions/a__0.jsonl"))
        self.assertEqual(self.chat("session", session="b"), str(self.root / "sessions/b__0.jsonl"))
        self.client.proc.kill()
        self.client.proc.wait()
        self.assertEqual(self.chat("session", session="b"), str(self.root / "sessions/b__0.jsonl"))

    def test_failed_cancelled_or_wrong_switch_never_prompts_old_session(self):
        for target in ("fail-switch", "cancel-switch", "wrong-switch"):
            with self.subTest(target=target):
                self.chat("old session")
                with self.assertRaises(RuntimeError):
                    self.chat("must not run", session=target)
                self.assertIsNone(self.client.current_session_path)
                self.assertIsNone(self.client.proc)
                self.assertEqual(self.chat("next"), "next")

    def test_no_final_from_previous_turn_or_partial(self):
        for text in ("settled-only", "error", "aborted", "length", "toolUse", "retries-exhausted"):
            with self.subTest(text=text):
                self.chat("old successful answer")
                with self.assertRaises(RuntimeError):
                    self.chat(text)
                self.assertIsNone(self.client.proc)

    def test_retry_waits_until_settled_and_uses_recovered_message(self):
        deltas = []
        self.assertEqual(self.chat("retry-success", on_delta=deltas.append), "recovered")
        self.assertEqual(deltas, ["failed preview", "recovered"])

    def test_eof_or_bad_json_fails_and_next_request_recovers(self):
        for text in ("eof", "malformed"):
            with self.subTest(text=text):
                with self.assertRaises(RuntimeError):
                    self.chat(text)
                self.assertEqual(self.chat("recovered"), "recovered")

    def test_unknown_response_and_old_reader_are_not_delivered(self):
        self.client.start()
        waiter = queue.Queue(maxsize=1)
        self.client.pending["expected"] = waiter
        self.client._dispatch({"type": "response", "id": "unknown"}, self.client.proc)
        self.client._dispatch({"type": "response", "id": "expected"}, object())
        self.client._dispatch({"type": "agent_settled"}, object())
        self.assertTrue(waiter.empty())
        self.assertTrue(self.client.events.empty())
        self.client.pending.pop("expected")

    def test_rpc_timeout_invalidates_process(self):
        self.client.start()
        with self.assertRaises(TimeoutError):
            self.client.rpc({"type": "hang"}, timeout=0.05)
        self.assertFalse(self.client.is_ready())
        self.assertFalse(self.client.pending)

    def test_wrong_command_response_invalidates_process(self):
        self.client.start()
        with self.assertRaisesRegex(RuntimeError, "command mismatch"):
            self.client.rpc({"type": "wrong-command"})
        self.assertFalse(self.client.is_ready())

    def test_blocked_stdin_obeys_command_timeout(self):
        self.client.start()
        self.client._write({"type": "hang"})
        with self.assertRaises(TimeoutError):
            self.client.rpc({"type": "prompt", "message": "x" * 1024 * 1024}, timeout=0.1)
        self.assertIsNone(self.client.proc)

    def test_downstream_failure_destroys_worker(self):
        def disconnected(_):
            raise BrokenPipeError("injected disconnect")
        with self.assertRaises(BrokenPipeError):
            self.chat("partial", on_delta=disconnected)
        self.assertIsNone(self.client.proc)
        self.assertEqual(self.chat("next"), "next")

    def test_deadline_stops_process_group(self):
        pid_file = self.root / "child.pid"
        with self.assertRaises(TimeoutError):
            self.chat("child:" + str(pid_file), timeout_s=0.5)
        self.assertIsNone(self.client.proc)
        child = int(pid_file.read_text())
        # A killed orphan may briefly remain as a zombie until reaped by init.
        for _ in range(50):
            status = Path(f"/proc/{child}/stat")
            if not status.exists() or status.read_text().split()[2] == "Z":
                break
            time.sleep(0.02)
        else:
            self.fail("tool child survived worker cancellation")

    def test_wait_timeout_does_not_stop_another_request(self):
        self.client.start()
        proc = self.client.proc
        self.client.lock.acquire()
        try:
            with self.assertRaises(TimeoutError):
                self.chat("queued", timeout_s=0.01)
            self.assertIs(self.client.proc, proc)
            self.assertTrue(self.client.is_ready())
        finally:
            self.client.lock.release()

    def test_model_failure_is_not_silently_ignored(self):
        with self.assertRaises(ValueError):
            self.chat("do not run", model="unknown")
        with self.assertRaises(RuntimeError):
            self.chat("do not run", provider="fixture", model="bad-model")

    def control(self, operation="list_models", session="a", **kwargs):
        return self.client.model_control({"operation": operation, "session_id": session, **kwargs}, 3)

    def test_model_menu_uses_public_catalog_without_prompt(self):
        self.client.rpc = Mock(wraps=self.client.rpc)
        result = self.control()
        self.assertEqual(result["type"], "assistant_final")
        self.assertEqual(result["data"]["presentation"]["kind"], "model_picker")
        self.assertEqual(len(result["data"]["presentation"]["models"]), 3)
        self.assertNotIn("never-expose", json.dumps(result))
        self.assertFalse(any(c.args[0]["type"] == "prompt" for c in self.client.rpc.call_args_list))

    def test_model_selection_is_durable_and_session_scoped(self):
        result = self.control("set_model", command_seq=2, target_provider="fixture", target_model="other")
        self.assertEqual(result["data"]["model_state"]["model"]["id"], "other")
        self.assertEqual(self.chat("which-model", session="a"), "other")
        self.assertEqual(self.chat("which-model", session="b"), "default")
        self.client.stop()
        # Saved selection takes precedence over stale IM request history.
        self.assertEqual(self.chat("which-model", session="a", provider="fixture", model="default"), "other")
        self.control("reset_model", command_seq=3)
        self.assertEqual(self.chat("which-model", session="a"), "default")
        self.client.stop()
        self.assertEqual(self.chat("which-model", session="a", provider="fixture", model="other"), "default")

    def test_model_revision_prevents_old_replay_and_duplicate_conflict(self):
        args = dict(command_seq=4, target_provider="fixture", target_model="other")
        self.control("set_model", **args)
        self.control("set_model", **args)  # idempotent retry
        for seq in (3, 4):
            with self.assertRaises(ValueError):
                self.control("reset_model", command_seq=seq)
            self.assertEqual(self.chat("which-model", session="a"), "other")

    def test_model_unavailable_or_unconfirmed_does_not_replace_saved_selection(self):
        self.control("set_model", command_seq=1, target_provider="fixture", target_model="other")
        for target in ("missing", "unconfirmed"):
            with self.assertRaises((ValueError, RuntimeError)):
                self.control("set_model", command_seq=2, target_provider="fixture", target_model=target)
            self.assertEqual(self.chat("which-model", session="a"), "other")
        self.assertEqual(self.chat("which-model", session="a", context_start_seq=10), "default")

    def test_model_state_symlink_rejected(self):
        self.client.session_dir.mkdir()
        (self.client.session_dir / "a__0.model.json").symlink_to(self.root / "outside")
        with self.assertRaises(ValueError):
            self.control()
        self.assertFalse((self.root / "outside").exists())

    def test_selection_returns_receipt_instead_of_repeating_picker(self):
        result = self.control("set_reasoning", level="high", command_seq=1)["data"]
        self.assertIn("已设置思考深度：high", result["text"])
        self.assertFalse(any(a["id"] == "reasoning_set" for a in result["presentation"]["actions"]))
        result = self.control("set_model", target_provider="fixture", target_model="other", command_seq=2)["data"]
        self.assertEqual(result["presentation"]["kind"], "command_card")
        self.assertNotIn("models", result["presentation"])
        result = self.control("list_reasoning")["data"]
        self.assertIn("未声明可调思考深度", result["text"])

    def test_reasoning_uses_supported_levels_and_survives_switch_restart(self):
        self.control("set_reasoning", level="max", command_seq=3)
        self.assertEqual(self.chat("which-thinking", session="a"), "max")
        self.assertEqual(self.chat("which-thinking", session="b"), "off")
        self.client.stop()
        self.assertEqual(self.chat("which-thinking", session="a"), "max")
        state = self.control("set_model", target_provider="fixture", target_model="other", command_seq=4)["data"]
        self.assertEqual(state["thinking_state"]["level"], "off")
        self.assertEqual(state["thinking_state"]["preferred"], "max")
        with self.assertRaises(ValueError):
            self.control("set_reasoning", level="max", command_seq=5)
        state = self.control("reset_model", command_seq=6)["data"]
        self.assertEqual(state["thinking_state"]["level"], "max")
        with self.assertRaises(ValueError):
            self.control("set_reasoning", level="low", command_seq=2)

    def test_new_preview_does_not_mutate_confirmed_context(self):
        self.chat("old", session="a")
        preview = self.control("new")["data"]
        self.assertEqual(preview["presentation"]["actions"][0]["id"], "new_confirm")
        self.assertEqual(preview["context_start_seq"], 0)
        self.assertEqual(self.chat("", session="a", retry=True), "old")

    def test_new_confirmation_is_durable_and_stale_history_cannot_rebind(self):
        self.control("set_model", target_provider="fixture", target_model="other", command_seq=2)
        self.control("set_reasoning", level="off", command_seq=3)
        self.chat("old", session="a")
        result = self.control("new_session", command_seq=7)["data"]
        self.assertEqual(result["context_start_seq"], 7)
        self.assertFalse(result["model_state"]["override"])
        with self.assertRaises(ValueError):
            self.control("set_model", target_provider="fixture", target_model="other", command_seq=6)
        with self.assertRaises(ValueError):
            self.control("set_reasoning", level="high", command_seq=6)
        with self.assertRaises(ValueError):
            self.chat("", session="a", retry=True)
        self.assertEqual(self.chat("which-model", session="a", provider="fixture", model="other"), "default")
        self.control("new_session", command_seq=7)  # same command does not clear again
        self.assertEqual(self.chat("", session="a", retry=True), "default")
        self.client.stop()
        self.assertTrue(self.chat("session", session="a").endswith("a__7.jsonl"))
        with self.assertRaises(ValueError):
            self.control("new_session", command_seq=6)
        self.assertTrue(self.chat("session", session="a").endswith("a__7.jsonl"))
        self.assertTrue(self.chat("session", session="b").endswith("b__0.jsonl"))

    def test_restart_and_creative_commands_stay_local_on_pi(self):
        restart = self.control("restart")["data"]
        self.assertIn("/new", restart["text"])
        self.assertEqual(restart["presentation"]["kind"], "command_card")
        scene = self.control("scene")["data"]
        self.assertIn("Hermes", scene["text"])
        self.assertNotIn(str(self.root), json.dumps(restart) + json.dumps(scene))

    def test_help_status_reasoning_and_retry_preview_do_not_prompt(self):
        self.chat("last", session="a")
        before = self.client.rpc({"type": "get_messages"})["data"]
        for operation in ("help", "status", "list_reasoning", "retry", "new"):
            result = self.control(operation)["data"]
            self.assertEqual(result["presentation"]["kind"], "command_card")
            self.assertNotIn(str(self.root), json.dumps(result))
        self.assertEqual(self.client.rpc({"type": "get_messages"})["data"], before)
        self.chat("other session", session="b")
        self.assertEqual(self.chat("ignore IM prompt", session="a", retry=True), "last")

    def test_route_and_reasoning_symlinks_are_rejected(self):
        self.chat("ok", session="a")
        outside = self.root / "outside.json"
        outside.write_text('{"schema":1,"context_start_seq":999}')
        path = self.root / "sessions/a__0.route.json"
        path.symlink_to(outside)
        with self.assertRaises(ValueError):
            self.chat("unsafe", session="a")
        path.unlink()
        path = self.root / "sessions/a__0.model.thinking.json"
        path.symlink_to(outside)
        with self.assertRaises(ValueError):
            self.control("list_reasoning")
        self.assertEqual(json.loads(outside.read_text())["context_start_seq"], 999)

    def test_model_control_wait_does_not_abort_busy_agent(self):
        self.client.start()
        proc = self.client.proc
        self.client.lock.acquire()
        try:
            with self.assertRaises(TimeoutError):
                self.client.model_control({"session_id": "a", "operation": "list_models"}, .01)
            self.assertIs(proc, self.client.proc)
        finally:
            self.client.lock.release()

    def test_http_control_rejects_missing_session_and_invalid_sequence(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.path = "/v1/agent/control"
        request._unauthorized = lambda: False
        request._json = Mock()
        for payload in ({"operation": "list_models"},
                        {"session_id": "a", "operation": "set_model", "command_seq": True}):
            request._read_json = lambda: payload
            request.do_POST()
            self.assertEqual(request._json.call_args.args[0], 422)

    def test_http_stream_uses_channel_neutral_events(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.path = "/v1/chat/completions"
        request._unauthorized = lambda: False
        request._read_json = lambda: {"session_id": "a", "stream": True,
            "agent_events": True,
            "messages": [{"role": "user", "content": "hello"}]}
        request._sse_headers = Mock()
        request._sse = Mock()
        request.do_POST()
        events = [json.loads(c.args[0]) for c in request._sse.call_args_list if c.kwargs.get("event") == "agent.event"]
        self.assertEqual([e["type"] for e in events], ["assistant_progress", "assistant_delta", "assistant_final"])
        self.assertEqual(events[1]["data"]["text"], "hello")

    def test_tool_progress_does_not_pollute_answer_or_expose_thinking(self):
        progress, deltas = [], []
        answer, _ = self.client.chat("tools-progress", deltas.append, 3, session_id="a", on_progress=progress.append)
        self.assertEqual(answer, "tools-progress")
        self.assertEqual(deltas, ["tools-progress"])
        self.assertTrue(any("已完成 1 次" in status for status in progress))
        self.assertTrue(any("等待模型回答" in status for status in progress))
        self.assertNotIn("private-thinking", str(progress) + str(deltas))

    def test_confirmation_denied_without_approval_ui(self):
        self.assertEqual(self.chat("confirm"), "confirmed=False")
        for method in ("select", "input", "editor"):
            self.assertTrue(auto_ui_response({"id": "x", "method": method, "options": ["yes"]})["cancelled"])

    def test_filename_compatibility_and_no_lossy_collision(self):
        self.assertEqual(session_filename("s_1_900000000001", 12), "s_1_900000000001__12.jsonl")
        names = {session_filename(s, 0) for s in ("a/b", "a?b", " a", "a", "a" * 121, "a" * 122)}
        self.assertEqual(len(names), 6)
        hashed = session_filename("a/b", 0).removesuffix("__0.jsonl")
        self.assertNotEqual(session_filename(hashed, 0), session_filename("a/b", 0))
        for value in ("", "  ", None):
            with self.assertRaises(ValueError):
                session_filename(value, 0)
        for seq in (-1, True, "0", 2**63):
            with self.assertRaises(ValueError):
                session_filename("a", seq)

    def test_session_symlink_rejected(self):
        self.client.session_dir.mkdir()
        (self.client.session_dir / "escape__0.jsonl").symlink_to(self.root / "outside")
        with self.assertRaises(ValueError):
            self.chat("must not run", session="escape")

    def test_input_rejects_unsupported_blocks_and_missing_user(self):
        for messages in ([{"role": "user", "content": [{"type": "image"}]}],
                         [{"role": "assistant", "content": "not an input"}], [None],
                         [{"role": "user", "content": "old"}, {"role": "user", "content": " "}]):
            with self.assertRaises(ValueError):
                latest_user_text(messages)

    def test_http_sse_error_has_no_final_or_done(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.path = "/v1/chat/completions"
        request._unauthorized = lambda: False
        request._read_json = lambda: {"session_id": "a", "stream": True,
                                      "messages": [{"role": "user", "content": "error"}]}
        request._sse_headers = Mock()
        request._sse = Mock()
        request.do_POST()
        calls = request._sse.call_args_list
        self.assertTrue(any('"error"' in c.args[0] for c in calls))
        self.assertFalse(any(c.kwargs.get("event") in {"pi.final", "hermes.final"} for c in calls))
        self.assertFalse(any(c.args[0] == "[DONE]" for c in calls))

    def test_http_requires_explicit_session(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.path = "/v1/chat/completions"
        request._unauthorized = lambda: False
        request._read_json = lambda: {"messages": [{"role": "user", "content": "hi"}]}
        request._json = Mock()
        request.do_POST()
        self.assertEqual(request._json.call_args.args[0], 400)
        self.assertIsNone(self.client.proc)

    def test_health_reflects_rpc_readiness(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.path = "/health"
        request._json = Mock()
        request.do_GET()
        self.assertEqual(request._json.call_args.args[0], 503)
        self.client.start()
        request.do_GET()
        self.assertEqual(request._json.call_args.args[0], 200)

    def test_bounded_json_body(self):
        handler = make_handler(GatewayState(self.client, "test-only", "pi-agent"))
        request = object.__new__(handler)
        request.connection = Mock()
        for payload, length in ((b"[]", "2"), (b"{}", "3"), (b"{}", "1048577"), (b"{}", "-1")):
            request.headers = {"Content-Length": length}
            request.rfile = io.BytesIO(payload)
            with self.assertRaises(ValueError):
                request._read_json()


if __name__ == "__main__":
    unittest.main()
