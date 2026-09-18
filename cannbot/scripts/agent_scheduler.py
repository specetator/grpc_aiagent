"""Session-serial, cross-session-parallel Agent execution.

One Gateway process still owns SQLite. Parallelism comes from a bounded
adapter pool (multiple Pi RPC processes), not from extra Gateway replicas.
"""
from __future__ import annotations

import os
import threading
import time
from collections import deque


def env_int(name, default, minimum, maximum):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value < minimum or value > maximum:
        raise ValueError(f"{name} must be in [{minimum}, {maximum}]")
    return value


class SchedulerMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.queue_wait_ms = []
        self.inflight = 0
        self.queued = 0
        self.rejected = 0
        self.unknown_turns = 0
        self.per_agent_inflight = {}

    def snapshot(self):
        with self.lock:
            samples = list(self.queue_wait_ms[-64:])
            waits = sorted(samples)
            def pct(p):
                if not waits:
                    return -1
                rank = int(round((len(waits) - 1) * p))
                return waits[min(max(rank, 0), len(waits) - 1)]
            return {
                "inflight": self.inflight,
                "queued": self.queued,
                "rejected": self.rejected,
                "unknown_turns": self.unknown_turns,
                "queue_wait_p50_ms": pct(0.50),
                "queue_wait_p95_ms": pct(0.95),
                "per_agent_inflight": dict(self.per_agent_inflight),
            }


class SessionScheduler:
    """At most one running job per session; a global cap across sessions."""

    def __init__(self, max_inflight=2, max_queue=16, queue_timeout_s=30,
                 max_per_agent=2):
        if max_inflight < 1 or max_queue < 1 or queue_timeout_s <= 0 or max_per_agent < 1:
            raise ValueError("invalid Agent scheduler limits")
        self.max_inflight = max_inflight
        self.max_queue = max_queue
        self.queue_timeout_s = queue_timeout_s
        self.max_per_agent = max_per_agent
        self.cv = threading.Condition()
        self.session_running = set()
        self.session_waiters = {}
        self.agent_inflight = {}
        self.inflight = 0
        self.metrics = SchedulerMetrics()

    @classmethod
    def from_env(cls):
        return cls(
            max_inflight=env_int("SPARK_PUSH_AGENT_MAX_INFLIGHT", 2, 1, 16),
            max_queue=env_int("SPARK_PUSH_AGENT_MAX_QUEUE", 16, 1, 256),
            queue_timeout_s=env_int("SPARK_PUSH_AGENT_QUEUE_TIMEOUT_S", 30, 1, 600),
            max_per_agent=env_int("SPARK_PUSH_AGENT_MAX_PER_AGENT", 2, 1, 8),
        )

    def run(self, session_id, agent_id, timeout_s, on_progress, fn):
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("invalid scheduler session")
        agent_id = agent_id or "_"
        deadline = time.monotonic() + min(max(0.001, timeout_s), self.queue_timeout_s)
        queued_at = time.monotonic()
        with self.cv:
            queued = self.metrics.queued
            waiting = self.session_waiters.get(session_id, 0)
            if self.inflight + queued + 1 > self.max_queue:
                self.metrics.rejected += 1
                raise TimeoutError("Agent 队列已满，请稍后重试")
            self.session_waiters[session_id] = waiting + 1
            self.metrics.queued += 1
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Agent 排队超时，请稍后重试")
                if on_progress:
                    ahead = max(0, self.metrics.queued - 1)
                    on_progress(f"排队中 · 前面有 {ahead} 个任务")
                with self.cv:
                    can_run = (
                        session_id not in self.session_running
                        and self.inflight < self.max_inflight
                        and self.agent_inflight.get(agent_id, 0) < self.max_per_agent
                    )
                    if can_run:
                        self.session_running.add(session_id)
                        self.inflight += 1
                        self.agent_inflight[agent_id] = self.agent_inflight.get(agent_id, 0) + 1
                        self.metrics.inflight = self.inflight
                        self.metrics.per_agent_inflight[agent_id] = self.agent_inflight[agent_id]
                        wait_ms = int((time.monotonic() - queued_at) * 1000)
                        self.metrics.queue_wait_ms.append(wait_ms)
                        if len(self.metrics.queue_wait_ms) > 256:
                            self.metrics.queue_wait_ms = self.metrics.queue_wait_ms[-64:]
                        break
                    self.cv.wait(timeout=min(0.25, remaining))
            if on_progress:
                on_progress("正在执行")
            return fn()
        finally:
            with self.cv:
                waiting = self.session_waiters.get(session_id, 0)
                if waiting <= 1:
                    self.session_waiters.pop(session_id, None)
                else:
                    self.session_waiters[session_id] = waiting - 1
                if self.metrics.queued > 0:
                    self.metrics.queued -= 1
                if session_id in self.session_running:
                    self.session_running.discard(session_id)
                    self.inflight = max(0, self.inflight - 1)
                    self.agent_inflight[agent_id] = max(0, self.agent_inflight.get(agent_id, 0) - 1)
                    self.metrics.inflight = self.inflight
                    self.metrics.per_agent_inflight[agent_id] = self.agent_inflight[agent_id]
                    if self.agent_inflight[agent_id] == 0:
                        self.agent_inflight.pop(agent_id, None)
                        self.metrics.per_agent_inflight.pop(agent_id, None)
                self.cv.notify_all()


class AdapterPool:
    """Session-affine pool. Same session reuses one adapter; others may run in parallel."""

    def __init__(self, workers):
        if not workers:
            raise ValueError("adapter pool requires workers")
        self.workers = list(workers)
        self.last = {}
        self.busy = set()
        self.cv = threading.Condition()

    def is_ready(self):
        return any(worker.is_ready() for worker in self.workers)

    def available_models(self):
        return self.workers[0].available_models()

    def control(self, request, timeout_s=30):
        session = request.get("session_id", "") if isinstance(request, dict) else ""
        worker = self._acquire(session, timeout_s)
        try:
            return worker.control(request, timeout_s)
        finally:
            self._release(worker)

    def chat(self, message, on_delta, timeout_s, provider=None, model=None,
             session_id="", context_start_seq=0, retry=False, on_progress=None,
             request_id=""):
        worker = self._acquire(session_id, timeout_s)
        try:
            return worker.chat(message, on_delta, timeout_s, provider, model,
                               session_id, context_start_seq, retry, on_progress,
                               request_id)
        finally:
            self._release(worker)

    def stop(self):
        for worker in self.workers:
            stop = getattr(worker, "stop", None)
            if stop:
                stop()

    def _acquire(self, session_id, timeout_s):
        deadline = time.monotonic() + max(0.001, timeout_s)
        with self.cv:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Agent worker 排队超时")
                preferred = self.last.get(session_id)
                if preferred is not None:
                    if preferred not in self.busy:
                        self.busy.add(preferred)
                        return preferred
                else:
                    idle = next((worker for worker in self.workers if worker not in self.busy), None)
                    if idle is not None:
                        self.busy.add(idle)
                        if session_id:
                            self.last[session_id] = idle
                        return idle
                self.cv.wait(timeout=min(0.25, remaining))

    def _release(self, worker):
        with self.cv:
            self.busy.discard(worker)
            self.cv.notify_all()
