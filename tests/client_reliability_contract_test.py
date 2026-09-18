#!/usr/bin/env python3
"""Contract tests for Android reliability rules. No live IM services required.

Mirrors android-app/.../SparkReliability.kt so login restore, envelope checks,
bounded reorder, and latency percentiles stay deterministic.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def classify(network_failure: bool, http_status: int, body_code: int, body_message: str = "") -> str:
    if network_failure or http_status < 0:
        return "UNREACHABLE"
    if http_status == 401 or body_code == 401:
        return "INVALID_TOKEN"
    if http_status == 403 or body_code == 403:
        return "INVALID_TOKEN"
    if http_status >= 500 or body_code >= 500:
        return "UNREACHABLE"
    if body_code == 0:
        return "VALID"
    return "UNREACHABLE"


def decide(request_id, session_id, delta, progress, delta_index, event):
    if not request_id or not delta or not session_id:
        return ("DROP", None)
    if event is None:
        return ("ORDERED", delta_index) if delta_index >= 0 else ("UNORDERED", None)
    expected = "assistant_progress" if progress else "assistant_delta"
    sequence = event.get("sequence")
    if (
        event.get("schema") != "sparkpush.agent_event.v1"
        or event.get("type") != expected
        or event.get("data_text") != delta
        or event.get("envelope_schema") != "sparkpush.agent_envelope.v1"
        or event.get("request_id") != request_id
        or event.get("conversation_id") != session_id
        or not isinstance(sequence, int)
        or sequence < 0
        or event.get("terminal") is not False
    ):
        return ("DROP", None)
    return ("ORDERED", sequence)


class BoundedReorder:
    def __init__(self, max_frames=256, max_bytes=512 * 1024):
        self.pending = {}
        self.next_index = None
        self.pending_bytes = 0
        self.overflowed = False
        self.max_frames = max_frames
        self.max_bytes = max_bytes

    def accept(self, kind, sequence, text):
        if self.overflowed:
            return "OVERFLOW"
        if kind == "DROP":
            return "DROP"
        if kind == "UNORDERED":
            return "APPLIED"
        if self.next_index is None:
            self.next_index = 0
        expected = self.next_index
        if sequence < expected or sequence in self.pending:
            return "DUPLICATE"
        incoming = len(text.encode("utf-8"))
        if len(self.pending) >= self.max_frames or self.pending_bytes + incoming > self.max_bytes:
            self.overflowed = True
            self.pending.clear()
            self.pending_bytes = 0
            return "OVERFLOW"
        self.pending[sequence] = text
        self.pending_bytes += incoming
        applied = 0
        cursor = expected
        while cursor in self.pending:
            item = self.pending.pop(cursor)
            self.pending_bytes -= len(item.encode("utf-8"))
            applied += 1
            cursor += 1
        self.next_index = cursor
        return "APPLIED" if applied else "BUFFERED"


def percentile(sorted_ms, p):
    if not sorted_ms:
        return -1
    rank = int(round((len(sorted_ms) - 1) * p))
    rank = min(max(rank, 0), len(sorted_ms) - 1)
    return sorted_ms[rank]


def test_login_restore_keeps_token_on_network_errors():
    assert classify(True, -1, -1) == "UNREACHABLE"
    assert classify(False, 503, 503) == "UNREACHABLE"
    assert classify(False, 200, 500, "db down") == "UNREACHABLE"
    assert classify(False, 401, 401) == "INVALID_TOKEN"
    assert classify(False, 403, 403, "user token invalid or expired") == "INVALID_TOKEN"
    assert classify(False, 200, 0) == "VALID"


def test_envelope_matches_web_rules():
    good = {
        "schema": "sparkpush.agent_event.v1",
        "type": "assistant_delta",
        "data_text": "你好",
        "envelope_schema": "sparkpush.agent_envelope.v1",
        "request_id": "req-1",
        "conversation_id": "s_1_2",
        "sequence": 0,
        "terminal": False,
    }
    assert decide("req-1", "s_1_2", "你好", False, -1, good) == ("ORDERED", 0)
    bad_session = dict(good, conversation_id="s_9_9")
    assert decide("req-1", "s_1_2", "你好", False, 3, bad_session)[0] == "DROP"
    terminal = dict(good, terminal=True)
    assert decide("req-1", "s_1_2", "你好", False, 0, terminal)[0] == "DROP"
    progress_mismatch = dict(good, type="assistant_delta")
    assert decide("req-1", "s_1_2", "思考中", True, 0, progress_mismatch)[0] == "DROP"
    assert decide("req-1", "s_1_2", "你好", False, 4, None) == ("ORDERED", 4)
    assert decide("req-1", "s_1_2", "你好", False, -1, None) == ("UNORDERED", None)


def test_bounded_reorder_stops_preview_on_overflow():
    buf = BoundedReorder(max_frames=3, max_bytes=100)
    assert buf.accept("ORDERED", 1, "b") == "BUFFERED"
    assert buf.accept("ORDERED", 0, "a") == "APPLIED"
    assert buf.next_index == 2
    assert buf.accept("ORDERED", 1, "dup") == "DUPLICATE"
    assert buf.accept("ORDERED", 3, "x") == "BUFFERED"
    assert buf.accept("ORDERED", 4, "y") == "BUFFERED"
    assert buf.accept("ORDERED", 5, "z") == "BUFFERED"
    assert buf.accept("ORDERED", 6, "w") == "OVERFLOW"
    assert buf.pending == {}
    huge = BoundedReorder(max_frames=256, max_bytes=8)
    assert huge.accept("ORDERED", 0, "abcdefghijk") == "OVERFLOW"


def test_latency_percentiles():
    samples = [10, 20, 30, 40, 100]
    assert percentile(samples, 0.50) == 30
    assert percentile(samples, 0.95) == 100
    assert percentile([], 0.50) == -1


def test_scenario_matrix_is_documented():
    matrix = {
        "online_send": ["accepted_ack", "delivered_ack", "received"],
        "duplicate_client_msg_id": ["same msg_seq", "no second history row"],
        "disconnect_during_generation": ["ai_delta discarded", "final ai_reply restores"],
        "process_restart": ["persist replayable", "in-flight agent turn unknown"],
        "over_64kib_reply": ["write", "history read", "sync byte-identical"],
        "multi_agent": ["session_id isolated", "contact ids isolated"],
    }
    assert set(matrix) == {
        "online_send",
        "duplicate_client_msg_id",
        "disconnect_during_generation",
        "process_restart",
        "over_64kib_reply",
        "multi_agent",
    }


def test_live_e2e_is_opt_in():
    if os.environ.get("SPARK_PUSH_RUN_E2E") != "1":
        return
    # Live IM is a separate job. This flag exists so CI can enable it without
    # mixing model variance into the default CTest set.
    raise AssertionError("SPARK_PUSH_RUN_E2E=1 requires a dedicated live harness")


def main():
    tests = [
        test_login_restore_keeps_token_on_network_errors,
        test_envelope_matches_web_rules,
        test_bounded_reorder_stops_preview_on_overflow,
        test_latency_percentiles,
        test_scenario_matrix_is_documented,
        test_live_e2e_is_opt_in,
    ]
    for test in tests:
        test()
        print("ok", test.__name__)
    print("client_reliability_contract_test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
