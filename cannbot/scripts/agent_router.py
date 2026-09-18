"""Trusted Agent registry, durable conversation selection and runtime isolation.

Only the authenticated Spark bridge calls this port. Registry entries are
operator provisioned; chat input cannot select commands, URLs or filesystem paths.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time

from agent_events import agent_event, canonical_route, command_card
from agent_scheduler import SessionScheduler

AGENT_ID = re.compile(r"[a-z][a-z0-9_-]{0,47}")


class AgentStore:
    def __init__(self, root: Path):
        if root.is_symlink():
            raise ValueError("Agent state directory cannot be a symlink")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = root / "routing.sqlite3"
        if self.path.is_symlink():
            raise ValueError("Agent state database cannot be a symlink")
        with self.connect() as db:
            # Event previews are written while text is streaming. WAL keeps
            # replay readers from blocking those short writes, while NORMAL
            # synchronous mode still makes committed terminal events durable.
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS states (namespace TEXT, key TEXT, value TEXT NOT NULL, PRIMARY KEY(namespace,key))")
            db.execute("""CREATE TABLE IF NOT EXISTS agent_routes (
                route_key TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                channel_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                thread_id TEXT NOT NULL, user_id INTEGER NOT NULL,
                agent_id TEXT NOT NULL, mode TEXT NOT NULL,
                backend_session_ref TEXT NOT NULL, revision INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL,
                UNIQUE(tenant_id,channel_id,conversation_id,thread_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS agent_turns (
                request_id TEXT PRIMARY KEY, route_key TEXT NOT NULL,
                input_hash TEXT NOT NULL, status TEXT NOT NULL,
                result_text TEXT, metadata_json TEXT, error_code TEXT,
                created_at_ms INTEGER NOT NULL, completed_at_ms INTEGER)""")
            db.execute("""CREATE TABLE IF NOT EXISTS agent_events (
                request_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                event_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                terminal INTEGER NOT NULL DEFAULT 0,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY(request_id,sequence))""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_agent_events_request ON agent_events(request_id,sequence)")
        os.chmod(self.path, 0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=NORMAL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, namespace, key):
        with self.connect() as db:
            row = db.execute("SELECT value FROM states WHERE namespace=? AND key=?", (namespace,key)).fetchone()
        return json.loads(row[0]) if row else {}

    def put(self, namespace, key, value):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO states VALUES (?,?,?)", (namespace,key,json.dumps(value,ensure_ascii=False)))

    def update(self, namespace, key, mutate):
        """Atomically read/modify/write one routing record under SQLite's write lock."""
        if not callable(mutate):
            raise ValueError("mutate must be callable")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM states WHERE namespace=? AND key=?", (namespace, key)).fetchone()
            current = json.loads(row[0]) if row else {}
            value = mutate(current)
            if not isinstance(value, dict):
                raise ValueError("state mutation must return an object")
            db.execute("INSERT OR REPLACE INTO states VALUES (?,?,?)", (namespace, key, json.dumps(value, ensure_ascii=False)))
            return value

    def append_context(self, key, role, text, max_messages=24):
        """Persist a bounded context projection without replacing IM history."""
        if role not in {"user", "assistant"} or not isinstance(text, str):
            raise ValueError("invalid context entry")
        def mutate(state):
            messages = state.get("recent_messages", [])
            if not isinstance(messages, list):
                messages = []
            messages.append({"role": role, "text": text[:12000]})
            state["recent_messages"] = messages[-max_messages:]
            state["context_revision"] = int(state.get("context_revision", 0)) + 1
            state["last_message_role"] = role
            state["updated_at_ms"] = int(time.time() * 1000)
            return state
        return self.update("context", key, mutate)

    def context(self, key):
        state = self.get("context", key)
        return state if isinstance(state, dict) else {}

    def compact_context(self, key, summary, keep_last=4):
        """Store an explicit context summary while retaining a small recent tail."""
        if not isinstance(summary, str) or not summary.strip() or len(summary) > 24000:
            raise ValueError("invalid context summary")
        if type(keep_last) is not int or not 0 <= keep_last <= 24:
            raise ValueError("invalid context tail size")
        def mutate(state):
            messages = state.get("recent_messages", [])
            if not isinstance(messages, list): messages = []
            state["summary"] = summary[:24000]
            state["recent_messages"] = messages[-keep_last:] if keep_last else []
            state["context_revision"] = int(state.get("context_revision", 0)) + 1
            state["compacted_at_ms"] = int(time.time() * 1000)
            return state
        return self.update("context", key, mutate)

    def bind_route(self, route, mode, backend_session_ref, revision=0):
        if mode not in {"sticky", "contact"}:
            raise ValueError("invalid route mode")
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT agent_id,mode,backend_session_ref,revision FROM agent_routes WHERE route_key=?",
                (route["route_key"],)).fetchone()
            if row:
                if row[1] == "contact" and row[0] != route["agent_id"]:
                    raise ValueError("Agent contact route conflicts with persisted identity")
                if revision < row[3]:
                    raise ValueError("Agent route selection is stale")
                db.execute("""UPDATE agent_routes SET agent_id=?,mode=?,backend_session_ref=?,
                    revision=?,updated_at_ms=? WHERE route_key=?""",
                           (route["agent_id"], mode, backend_session_ref,
                            max(revision, row[3]), now, route["route_key"]))
            else:
                db.execute("""INSERT INTO agent_routes
                    (route_key,tenant_id,channel_id,conversation_id,thread_id,user_id,
                     agent_id,mode,backend_session_ref,revision,created_at_ms,updated_at_ms)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (route["route_key"], route["tenant_id"], route["channel_id"],
                            route["conversation_id"], route["thread_id"], route["user_id"],
                            route["agent_id"], mode, backend_session_ref, revision, now, now))
        return self.route(route["route_key"])

    def route(self, route_key):
        with self.connect() as db:
            row = db.execute("""SELECT tenant_id,channel_id,conversation_id,thread_id,user_id,
                agent_id,mode,backend_session_ref,revision,created_at_ms,updated_at_ms
                FROM agent_routes WHERE route_key=?""", (route_key,)).fetchone()
        if not row:
            return {}
        keys = ("tenant_id", "channel_id", "conversation_id", "thread_id", "user_id",
                "agent_id", "mode", "backend_session_ref", "revision", "created_at_ms", "updated_at_ms")
        return {"route_key": route_key, **dict(zip(keys, row))}

    def begin_turn(self, request_id, route_key, input_hash):
        if not isinstance(request_id, str) or not request_id or len(request_id.encode("utf-8")) > 121:
            raise ValueError("invalid Agent request id")
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT input_hash,status,result_text,metadata_json,error_code
                FROM agent_turns WHERE request_id=?""", (request_id,)).fetchone()
            if row:
                if row[0] != input_hash:
                    raise ValueError("request_id was reused with different Agent input")
                result = {"status": row[1]}
                if row[2] is not None:
                    result["text"] = row[2]
                if row[3]:
                    result["metadata"] = json.loads(row[3])
                if row[4]:
                    result["error_code"] = row[4]
                return result
            db.execute("""INSERT INTO agent_turns
                (request_id,route_key,input_hash,status,created_at_ms) VALUES(?,?,?,?,?)""",
                       (request_id, route_key, input_hash, "running", now))
        return {"status": "new"}

    def get_turn(self, request_id):
        if not isinstance(request_id, str) or not request_id:
            return {}
        with self.connect() as db:
            row = db.execute("""SELECT input_hash,status,result_text,metadata_json,error_code
                FROM agent_turns WHERE request_id=?""", (request_id,)).fetchone()
        if not row:
            return {}
        result = {"input_hash": row[0], "status": row[1]}
        if row[2] is not None:
            result["text"] = row[2]
        if row[3]:
            result["metadata"] = json.loads(row[3])
        if row[4]:
            result["error_code"] = row[4]
        return result

    def reclaim_running_turns(self, max_age_ms=0):
        """Mark leftover running turns unknown. Never auto-retry tool side effects."""
        if type(max_age_ms) is not int or max_age_ms < 0:
            raise ValueError("invalid running-turn age")
        now = int(time.time() * 1000)
        cutoff = now if max_age_ms == 0 else now - max_age_ms
        with self.connect() as db:
            changed = db.execute("""UPDATE agent_turns SET status='unknown',
                error_code='process_restart', completed_at_ms=?
                WHERE status='running' AND created_at_ms<=?""",
                                 (now, cutoff)).rowcount
        return changed

    def complete_turn(self, request_id, text, metadata):
        now = int(time.time() * 1000)
        encoded = json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))
        with self.connect() as db:
            changed = db.execute("""UPDATE agent_turns SET status='completed',result_text=?,
                metadata_json=?,error_code=NULL,completed_at_ms=?
                WHERE request_id=? AND status='running'""",
                                 (text, encoded, now, request_id)).rowcount
        if changed != 1:
            raise RuntimeError("Agent turn completion lost its running owner")

    def fail_turn(self, request_id, error_code):
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute("""UPDATE agent_turns SET status='failed',error_code=?,completed_at_ms=?
                WHERE request_id=? AND status='running'""",
                       (str(error_code)[:80], now, request_id))

    def append_event(self, event):
        envelope = event.get("envelope") if isinstance(event, dict) else None
        if not isinstance(envelope, dict):
            raise ValueError("Agent event has no envelope")
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        request_id, sequence = envelope.get("request_id"), envelope.get("sequence")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT payload FROM agent_events WHERE request_id=? AND sequence=?",
                             (request_id, sequence)).fetchone()
            if row:
                if row[0] != payload:
                    raise ValueError("conflicting Agent event sequence")
                return False
            db.execute("""INSERT INTO agent_events
                (request_id,sequence,event_id,payload,terminal,created_at_ms)
                VALUES(?,?,?,?,?,?)""",
                       (request_id, sequence, envelope["event_id"], payload,
                        1 if envelope.get("terminal") else 0, envelope["created_at_ms"]))
            # The final event contains the complete answer. Bound preview
            # history without running a pruning scan for every 40 ms batch.
            if envelope.get("terminal") or (type(sequence) is int and sequence > 0 and sequence % 256 == 0):
                db.execute("""DELETE FROM agent_events WHERE request_id=? AND terminal=0
                    AND sequence IN (SELECT sequence FROM agent_events
                    WHERE request_id=? AND terminal=0 ORDER BY sequence DESC
                    LIMIT -1 OFFSET 4096)""", (request_id, request_id))
        return True

    def replay_events(self, request_id, after_sequence=-1):
        if (not isinstance(request_id, str) or not request_id or
                type(after_sequence) is not int or after_sequence < -1):
            raise ValueError("invalid Agent event replay cursor")
        with self.connect() as db:
            rows = db.execute("""SELECT payload FROM agent_events
                WHERE request_id=? AND sequence>? ORDER BY sequence LIMIT 4097""",
                              (request_id, after_sequence)).fetchall()
        return [json.loads(row[0]) for row in rows[:4096]], len(rows) > 4096


class ArtifactStore:
    """Small durable artifact boundary; callers receive opaque IDs, never paths."""
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.root.is_symlink():
            raise ValueError("artifact directory cannot be a symlink")
        self.lock = threading.Lock()

    def put_text(self, owner: str, name: str, text: str, mime="text/plain"):
        if not isinstance(owner, str) or not owner or not isinstance(name, str) or not isinstance(text, str):
            raise ValueError("invalid artifact")
        if len(text.encode('utf-8')) > 256 * 1024 or len(name) > 120 or any(c in name for c in '/\\\x00'):
            raise ValueError("artifact exceeds limits")
        artifact_id = "art_" + hashlib.sha256((owner + '\0' + name + '\0' + text).encode()).hexdigest()[:24]
        path = self.root / artifact_id
        with self.lock:
            if not path.exists():
                path.write_text(text, encoding='utf-8')
                os.chmod(path, 0o600)
        # Metadata is kept in a sidecar so reads can enforce ownership without
        # exposing filesystem paths to callers.
        meta = path.with_suffix('.json')
        if not meta.exists():
            meta.write_text(json.dumps({"owner": owner, "name": name, "mime": mime, "created_at": time.time()}, ensure_ascii=False), encoding='utf-8')
            os.chmod(meta, 0o600)
        return {"id": artifact_id, "name": name, "mime": mime, "bytes": len(text.encode('utf-8'))}

    def read_text(self, owner: str, artifact_id: str):
        if not isinstance(owner, str) or not re.fullmatch(r"art_[0-9a-f]{24}", str(artifact_id)):
            raise ValueError("invalid artifact id")
        path = self.root / artifact_id
        meta = path.with_suffix('.json')
        with self.lock:
            if not path.is_file() or not meta.is_file():
                raise FileNotFoundError("artifact not found")
            info = json.loads(meta.read_text(encoding='utf-8'))
            if info.get('owner') != owner:
                raise PermissionError("artifact owner mismatch")
            return {"id": artifact_id, "name": info.get('name','artifact'), "mime": info.get('mime','application/octet-stream'),
                    "text": path.read_text(encoding='utf-8')}

    def purge(self, max_age_s=86400):
        """Remove expired artifacts without following symlinks."""
        if type(max_age_s) not in (int, float) or max_age_s < 0:
            raise ValueError("invalid artifact age")
        cutoff = time.time() - max_age_s; removed = 0
        with self.lock:
            for meta in self.root.glob('art_*.json'):
                try:
                    info = json.loads(meta.read_text(encoding='utf-8'))
                    if float(info.get('created_at', 0)) >= cutoff: continue
                    artifact = self.root / meta.stem
                    if artifact.is_file() and not artifact.is_symlink(): artifact.unlink()
                    meta.unlink(); removed += 1
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    continue
        return removed


class SessionLocks:
    """Bounded lock registry; unrelated sessions never share a runtime lock."""
    def __init__(self):
        self.guard = threading.Lock()
        self.entries = {}

    @contextmanager
    def hold(self, key, timeout):
        with self.guard:
            if key not in self.entries:
                if len(self.entries) >= 1024:
                    raise TimeoutError("Too many active Agent sessions")
                self.entries[key] = [threading.Lock(), 0]
            entry = self.entries[key]
            entry[1] += 1
        acquired = False
        try:
            acquired = entry[0].acquire(timeout=max(0, timeout))
            if not acquired:
                raise TimeoutError("Agent session busy; retry later")
            yield
        finally:
            if acquired:
                entry[0].release()
            with self.guard:
                entry[1] -= 1
                if entry[1] == 0:
                    del self.entries[key]


class AgentRouter:
    def __init__(self, entries: list[dict], adapters: dict, store: AgentStore,
                 default="pi", bot_user_id=900000000001,
                 tenant_id="local", channel_id="spark_pc"):
        self.entries = {}
        for entry in entries:
            key, name, owners = entry.get("id"), entry.get("name"), entry.get("owner_user_ids")
            routing_keywords = entry.get("routing_keywords", [])
            if (not isinstance(key,str) or not AGENT_ID.fullmatch(key) or key in self.entries or
                not isinstance(name,str) or not name.strip() or len(name.encode()) > 120 or
                any(ord(c)<32 for c in name) or
                not isinstance(owners,list) or any(type(uid) is not int or uid<=0 for uid in owners) or
                not isinstance(routing_keywords,list) or len(routing_keywords)>32 or
                any(not isinstance(word,str) or not word.strip() or len(word.encode("utf-8"))>64 or
                    any(ord(c)<32 for c in word) for word in routing_keywords)):
                raise ValueError("invalid Agent registry entry")
            if key not in adapters:
                raise ValueError("missing Agent adapter")
            normalized = list(dict.fromkeys(word.strip().casefold() for word in routing_keywords))
            self.entries[key] = {**entry, "routing_keywords": normalized}
        if default not in self.entries or len(entries)>12:
            raise ValueError("invalid default Agent or registry size")
        self.adapters, self.store, self.default = adapters, store, default
        self.tenant_id, self.channel_id = tenant_id, channel_id
        # Validate deployment-owned identity labels once at startup.
        canonical_route(tenant_id, channel_id, "bootstrap", 1, default)
        self.artifacts = ArtifactStore(store.path.parent / "artifacts")
        # Best-effort startup hygiene; serving traffic never depends on cleanup.
        try:
            self.artifacts.purge()
        except OSError:
            pass
        for adapter in self.adapters.values():
            # Optional capability keeps legacy adapters independent of storage.
            if hasattr(adapter, "artifact_store"):
                adapter.artifact_store = self.artifacts
        self.bot_user_id = bot_user_id
        self.bot_agents = {}
        for key,entry in self.entries.items():
            contact=entry.get("bot_user_id")
            if contact is not None:
                if type(contact) is not int or not 0<contact<=2**53-1 or contact in self.bot_agents:
                    raise ValueError("invalid or duplicate Agent contact")
                self.bot_agents[contact]=key
        if self.bot_agents and len(self.bot_agents)!=len(self.entries):
            raise ValueError("all Agents must have independent contact IDs")
        self.locks = SessionLocks()
        self.scheduler = SessionScheduler.from_env()
        try:
            reclaimed = self.store.reclaim_running_turns(0)
            self.scheduler.metrics.unknown_turns += reclaimed
        except Exception:
            pass

    def _identity(self, session):
        # Spark currently has one local tenant and direct bot conversations.
        # Never infer identity from the user-controlled message body.
        match = re.fullmatch(r"s_([1-9][0-9]*)_([1-9][0-9]*)", session) if isinstance(session,str) else None
        participants = [int(match[1]),int(match[2])] if match else []
        known=set(self.bot_agents) or {self.bot_user_id}
        if (len(participants)!=2 or participants[0]>=participants[1] or
                participants[1]>=2**63 or len(set(participants)&known)!=1):
            raise ValueError("invalid authenticated Spark Agent conversation")
        uid = next(value for value in participants if value not in known)
        bot = next(value for value in participants if value in known)
        return uid,bot

    def _visible(self, session):
        uid,_ = self._identity(session)
        return {key:entry for key,entry in self.entries.items()
                if not entry["owner_user_ids"] or uid in entry["owner_user_ids"]}

    def _selected(self, session):
        visible = self._visible(session)
        if not visible:
            raise ValueError("当前用户没有可访问的 Agent")
        saved = self.store.get("selection", session)
        key = self.bot_agents[self._identity(session)[1]] if self.bot_agents else saved.get("agent_id",self.default)
        if key not in visible:
            raise ValueError("当前 Agent 已不可访问，请发送 /agent 重新选择")
        return key, saved

    def _select_for_first_message(self, session, message):
        """Resolve an unbound hub conversation once, then keep it sticky."""
        if self.bot_agents:
            return self._selected(session)
        visible = self._visible(session)
        if not visible:
            raise ValueError("当前用户没有可访问的 Agent")
        saved = self.store.get("selection", session)
        if saved.get("agent_id"):
            return self._selected(session)
        folded = message.casefold() if isinstance(message, str) else ""
        scored = []
        for order, (key, entry) in enumerate(visible.items()):
            score = sum(len(word) for word in entry["routing_keywords"] if word in folded)
            if score:
                scored.append((score, key == self.default, -order, key))
        if scored:
            key = max(scored)[3]
            source = "first_message_policy"
        else:
            key = self.default if self.default in visible else next(iter(visible))
            source = "default"
        saved = {"agent_id": key, "revision": 1, "source": source}
        self.store.put("selection", session, saved)
        route = canonical_route(self.tenant_id, self.channel_id, session,
                                self._identity(session)[0], key)
        self.store.bind_route(route, "sticky", session, saved["revision"])
        return key, saved

    def _describe_route(self, session, message=None):
        key, saved = (self._select_for_first_message(session, message)
                      if message is not None else self._selected(session))
        user_id, _ = self._identity(session)
        route = canonical_route(self.tenant_id, self.channel_id, session, user_id, key)
        revision = saved.get("revision", 0) if isinstance(saved, dict) else 0
        mode = "contact" if self.bot_agents else "sticky"
        persisted = self.store.bind_route(route, mode, session, revision)
        return {**route, "mode": mode,
                "selection_source": "contact" if self.bot_agents else saved.get("source", "persisted"),
                "revision": persisted.get("revision", revision),
                "backend_session_ref": persisted.get("backend_session_ref", session)}

    def describe_route(self, session, message=None, timeout_s=30):
        # Gateway calls this before sending SSE headers. Serializing the
        # first-message decision here prevents two concurrent hub requests
        # from selecting different Agents before the chat lock is acquired.
        with self.locks.hold(session, timeout_s):
            return self._describe_route(session, message)

    def record_event(self, event):
        return self.store.append_event(event)

    def replay_events(self, request_id, after_sequence=-1):
        return self.store.replay_events(request_id, after_sequence)

    def _decorate(self, key, data):
        data = dict(data)
        data["agent_state"] = {"id":key,"name":self.entries[key]["name"]}
        data["text"] = "【" + self.entries[key]["name"] + "】\n\n" + data.get("text", "")
        card = data.get("presentation")
        if isinstance(card,dict) and card.get("kind")=="command_card":
            card = {**card,"actions":list(card["actions"])}
            if not any(a["id"]=="agent" for a in card["actions"]):
                card["actions"].append({"id":"agent","label":"其他 Agent 对话"})
            data["presentation"] = card
        return data

    def is_ready(self):
        return self.adapters[self.default].is_ready()

    def available_models(self):
        return self.adapters[self.default].available_models()

    def control(self, request, timeout_s=30):
        session = request.get("session_id")
        visible = self._visible(session)
        operation = request.get("operation")
        started = time.monotonic()
        with self.locks.hold(session,timeout_s):
            if operation in {"list_agents","set_agent"}:
                if self.bot_agents:
                    key,_=self._selected(session)
                    target=request.get("target_agent") if operation=="set_agent" else None
                    if operation=="set_agent" and (not isinstance(target,str) or target not in visible):
                        raise ValueError("Agent 不存在或当前用户无权使用")
                    return agent_event("assistant_final",{"text":"当前对话："+self.entries[key]["name"]+
                        "。每个 Agent 使用独立对话框；点击下方按钮打开对应会话。",
                        "presentation":{"kind":"command_card","title":"打开 Agent 对话","actions":[
                            {"id":"agent_open","label":entry["name"],"value":str(entry["bot_user_id"])}
                            for k,entry in visible.items() if target is None or k==target]}})
                saved = self.store.get("selection",session)
                key = saved.get("agent_id",self.default)
                if operation=="set_agent":
                    target, revision = request.get("target_agent"), request.get("command_seq")
                    if not isinstance(target,str) or target not in visible:
                        raise ValueError("Agent 不存在或当前用户无权使用")
                    if type(revision) is not int or not 0<revision<2**63:
                        raise ValueError("Agent selection requires positive command_seq")
                    previous = saved.get("revision",0)
                    if revision<previous or (revision==previous and target!=key):
                        raise ValueError("Agent 选择已过期，请重新发送 /agent")
                    if not self.adapters[target].is_ready():
                        raise ValueError("目标 Agent 暂不可用，当前选择未改变")
                    key=target
                    self.store.put("selection",session,{"agent_id":key,"revision":revision,
                                                        "source":"manual"})
                    route = canonical_route(self.tenant_id, self.channel_id, session,
                                            self._identity(session)[0], key)
                    self.store.bind_route(route, "sticky", session, revision)
                text = ("已切换 Agent：" if operation=="set_agent" else "当前 Agent：") + self.entries.get(key,{}).get("name","不可访问")
                text += "。各 Agent 分别保留上下文；/new 只重置当前 Agent。"
                return agent_event("assistant_final",{"text":text,"agent_state":{"id":key},
                    "presentation":{"kind":"command_card","title":"选择 AI Agent","actions":[
                        {"id":"agent_set","label":entry["name"],"value":k,"selected":k==key}
                        for k,entry in visible.items()]}})
            key,_ = self._selected(session)
            selected = {} if self.bot_agents else self.store.get("selection",session)
            revision = request.get("command_seq",0)
            if type(revision) is not int or revision < selected.get("revision",0):
                raise ValueError("命令属于之前的 Agent 选择，请重新发送")
            # A different Agent's history projection must never reset this
            # runtime or inject its model choice. Adapter stores are authoritative.
            routed = {k:v for k,v in request.items() if k not in {"provider","model"}}
            routed["context_start_seq"] = 0
            event = self.adapters[key].control(routed,max(0,timeout_s-(time.monotonic()-started)))
            return {**event,"data":self._decorate(key,event["data"])}

    def chat(self,message,on_delta,timeout_s,provider=None,model=None,session_id="",
             context_start_seq=0,retry=False,on_progress=None,request_id=""):
        self._visible(session_id)
        started=time.monotonic()
        try:
            agent_hint,_ = self._selected(session_id)
        except Exception:
            agent_hint = self.default
        if request_id:
            existing = self.store.get_turn(request_id)
            if existing.get("status") == "completed":
                route = self._describe_route(session_id)
                input_hash = hashlib.sha256(json.dumps({
                    "route_key": route["route_key"], "agent_id": agent_hint,
                    "message": message, "retry": retry,
                    "context_start_seq": context_start_seq}, ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                if existing.get("input_hash") != input_hash:
                    raise ValueError("request_id was reused with different Agent input")
                metadata = dict(existing.get("metadata") or {})
                metadata.update({"turn_replayed": True, "request_id": request_id,
                                 "agent_route": route})
                return existing.get("text", ""), metadata
            if existing.get("status") == "unknown":
                raise RuntimeError("上次执行结果未知，未自动重跑带副作用的请求")
            if existing.get("status") == "failed":
                raise RuntimeError("Agent 请求先前已失败，未使用相同 request_id 重复执行")

        def run_turn():
            with self.locks.hold(session_id, max(0.001, timeout_s-(time.monotonic()-started))):
                key,_ = self._select_for_first_message(session_id, message)
                route = self._describe_route(session_id)
                input_hash = hashlib.sha256(json.dumps({
                    "route_key": route["route_key"], "agent_id": key,
                    "message": message, "retry": retry,
                    "context_start_seq": context_start_seq}, ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
                turn = (self.store.begin_turn(request_id, route["route_key"], input_hash)
                        if request_id else {"status": "new"})
                if turn["status"] == "completed":
                    metadata = dict(turn.get("metadata") or {})
                    metadata.update({"turn_replayed": True, "request_id": request_id,
                                     "agent_route": route})
                    return turn.get("text", ""), metadata
                if turn["status"] == "running":
                    raise RuntimeError("Agent 请求仍在执行或上次结果未确认，未重复调用工具")
                if turn["status"] == "unknown":
                    raise RuntimeError("上次执行结果未知，未自动重跑带副作用的请求")
                if turn["status"] == "failed":
                    raise RuntimeError("Agent 请求先前已失败，未使用相同 request_id 重复执行")
                self.store.append_context(session_id, "user", message)
                try:
                    if on_progress:
                        on_progress(self.entries[key]["name"] + " · 正在生成")
                    text, metadata = self.adapters[key].chat(
                        message, on_delta, max(0, timeout_s-(time.monotonic()-started)),
                        None, None, route["backend_session_ref"], 0, retry=retry,
                        on_progress=on_progress, request_id=request_id)
                    context = self.store.append_context(session_id, "assistant", text)
                    data = self._decorate(key,{"text":text})
                    metadata = {**metadata, "agent_state":data["agent_state"],
                        "agent_route":route, "request_id":request_id,
                        "context_revision":context["context_revision"],
                        "context_message_count":len(context["recent_messages"]),
                        "queue_wait_ms": int((time.monotonic()-started)*1000)}
                    if request_id:
                        self.store.complete_turn(request_id, data["text"], metadata)
                    return data["text"], metadata
                except Exception as exc:
                    if request_id:
                        self.store.fail_turn(request_id, type(exc).__name__)
                    raise

        return self.scheduler.run(session_id, agent_hint, timeout_s, on_progress, run_turn)

    def stop(self):
        for adapter in self.adapters.values():
            stop=getattr(adapter,"stop",None)
            if stop:
                stop()


def load_router(path: Path, pi, state_root: Path):
    from hermes_adapter import HermesHttpAdapter
    if path.is_symlink() or path.stat().st_size>65536:
        raise ValueError("invalid Agent registry file")
    config=json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema")!=1:
        raise ValueError("invalid Agent registry schema")
    store=AgentStore(state_root)
    from agent_scheduler import AdapterPool, env_int
    worker_count = env_int("SPARK_PUSH_PI_WORKERS", 2, 1, 8)
    pi_adapter = pi
    if worker_count > 1 and hasattr(pi, "argv"):
        extras = []
        for _ in range(worker_count - 1):
            clone = type(pi)(pi.argv, pi.cwd, pi.env, pi.session_dir)
            try:
                clone.start()
            except Exception:
                clone.stop()
                raise
            extras.append(clone)
        pi_adapter = AdapterPool([pi, *extras])
    adapters={}
    for entry in config["agents"]:
        runtime=entry.get("runtime")
        # Multiple independent Spark contacts may use the same Pi runtime.
        # The adapter already binds each request to its validated session
        # file; allowing more than the default ``pi`` entry lets Android have
        # a separate contact and model/context state without sharing the PC
        # contact's history.
        if runtime=="pi_rpc":
            adapters[entry["id"]]=pi_adapter
        elif runtime=="hermes_http":
            adapters[entry["id"]]=HermesHttpAdapter(entry,store)
        else:
            raise ValueError("unsupported Agent runtime")
    return AgentRouter(config["agents"], adapters, store,
        config.get("default_agent", "pi"), config.get("bot_user_id", 900000000001),
        config.get("tenant_id", "local"), config.get("channel_id", "spark_pc"))
