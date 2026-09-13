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

from agent_events import agent_event, command_card

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
            db.execute("CREATE TABLE IF NOT EXISTS states (namespace TEXT, key TEXT, value TEXT NOT NULL, PRIMARY KEY(namespace,key))")
        os.chmod(self.path, 0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
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

    def append_context(self, key, role, text, max_messages=24):
        """Persist a bounded context projection without replacing IM history."""
        if role not in {"user", "assistant"} or not isinstance(text, str):
            raise ValueError("invalid context entry")
        state = self.get("context", key)
        messages = state.get("recent_messages", [])
        if not isinstance(messages, list):
            messages = []
        messages.append({"role": role, "text": text[:12000]})
        state["recent_messages"] = messages[-max_messages:]
        state["context_revision"] = int(state.get("context_revision", 0)) + 1
        state["last_message_role"] = role
        state["updated_at_ms"] = int(time.time() * 1000)
        self.put("context", key, state)
        return state

    def context(self, key):
        state = self.get("context", key)
        return state if isinstance(state, dict) else {}


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
                 default="pi", bot_user_id=900000000001):
        self.entries = {}
        for entry in entries:
            key, name, owners = entry.get("id"), entry.get("name"), entry.get("owner_user_ids")
            if (not isinstance(key,str) or not AGENT_ID.fullmatch(key) or key in self.entries or
                not isinstance(name,str) or not name.strip() or len(name.encode()) > 120 or
                any(ord(c)<32 for c in name) or
                not isinstance(owners,list) or any(type(uid) is not int or uid<=0 for uid in owners)):
                raise ValueError("invalid Agent registry entry")
            if key not in adapters:
                raise ValueError("missing Agent adapter")
            self.entries[key] = dict(entry)
        if default not in self.entries or len(entries)>12:
            raise ValueError("invalid default Agent or registry size")
        self.adapters, self.store, self.default = adapters, store, default
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
        saved = self.store.get("selection", session)
        key = self.bot_agents[self._identity(session)[1]] if self.bot_agents else saved.get("agent_id",self.default)
        if key not in visible:
            raise ValueError("当前 Agent 已不可访问，请发送 /agent 重新选择")
        return key, saved

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
                    self.store.put("selection",session,{"agent_id":key,"revision":revision})
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
             context_start_seq=0,retry=False,on_progress=None):
        self._visible(session_id)
        started=time.monotonic()
        with self.locks.hold(session_id,timeout_s):
            key,_ = self._selected(session_id)
            context = self.store.append_context(session_id, "user", message)
            if on_progress:
                on_progress(self.entries[key]["name"] + " · 正在处理")
            text, metadata = self.adapters[key].chat(message,on_delta,
                max(0,timeout_s-(time.monotonic()-started)),None,None,session_id,0,retry=retry,on_progress=on_progress)
            context = self.store.append_context(session_id, "assistant", text)
            data = self._decorate(key,{"text":text})
            return data["text"], {**metadata,"agent_state":data["agent_state"],
                                   "context_revision":context["context_revision"],
                                   "context_message_count":len(context["recent_messages"])}

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
    adapters={}
    for entry in config["agents"]:
        runtime=entry.get("runtime")
        if runtime=="pi_rpc" and entry["id"]=="pi":
            adapters[entry["id"]]=pi
        elif runtime=="hermes_http":
            adapters[entry["id"]]=HermesHttpAdapter(entry,store)
        else:
            raise ValueError("unsupported Agent runtime")
    return AgentRouter(config["agents"],adapters,store,config.get("default_agent","pi"),config.get("bot_user_id",900000000001))
