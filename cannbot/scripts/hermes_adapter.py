"""Hermes Agent API transport, not a direct model completion adapter."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
import socket
import struct
import threading
from urllib.parse import quote
import time
import sys
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler

from agent_events import agent_event, command_card, public_model


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def windows_host():
    for line in Path('/proc/net/route').read_text().splitlines()[1:]:
        fields=line.split()
        if fields[1]=='00000000' and int(fields[3],16)&2:
            return socket.inet_ntoa(struct.pack('<I',int(fields[2],16)))
    raise RuntimeError('Windows host route unavailable')


class HermesHttpAdapter:
    def __init__(self, config, store):
        self.config, self.store = dict(config), store
        self.namespace='hermes:'+config['id']
        parsed=urlparse(config['base_url'])
        if (parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or
            parsed.password or parsed.query or parsed.fragment):
            raise ValueError('invalid Hermes Agent URL')
        self.base_url=config['base_url'].rstrip('/')
        # Resolve the WSL NAT host on every service start, not a hardcoded IP.
        if parsed.hostname=='windows-host':
            self.base_url=self.base_url.replace('windows-host',windows_host(),1)
        values={}
        credential=Path(config['credential_env_file'])
        for line in credential.read_text(encoding='utf-8').splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key,value=line.split('=',1)
                values[key.strip()]=value.strip().strip('\"').strip("'")
        self.key=values.get(config.get('credential_env_key','API_SERVER_KEY'),'')
        if not self.key or any(ord(c)<32 for c in self.key):
            raise ValueError('Hermes credential is missing or invalid')
        self.catalog_lock=threading.Lock()
        self.catalog=None
        self.catalog_time=0
        self.restart_lock=threading.Lock()
        self.relay_pool=None
        if config.get('transport')=='windows_stdio':
            from windows_agent_transport import WindowsRelayPool
            self.relay_pool=WindowsRelayPool(config['windows_python'])
        self.opener=build_opener(ProxyHandler({}),NoRedirect())

    def _open(self, path, payload=None, headers=None, timeout=10):
        url=(self.base_url.removesuffix('/v1') if path.startswith('/api/') else self.base_url)+path
        if self.config.get('transport')=='windows_stdio':
            from windows_agent_transport import WindowsResponse
            return WindowsResponse(self.config['windows_python'],url,payload,
                {'Authorization':'Bearer '+self.key,'Content-Type':'application/json',**(headers or {})},timeout,pool=self.relay_pool)
        body=None if payload is None else json.dumps(payload,ensure_ascii=False).encode('utf-8')
        request=Request(url,data=body,headers={
            'Authorization':'Bearer '+self.key,'Content-Type':'application/json',**(headers or {})})
        try:
            return self.opener.open(request,timeout=max(0.1,timeout))
        except HTTPError as exc:
            # Never relay upstream bodies: they can contain credentials or private paths.
            raise RuntimeError(f'Hermes Agent API 返回 HTTP {exc.code}') from None
        except (URLError,OSError):
            raise RuntimeError('Hermes Agent API 连接失败，请检查 Windows technical 服务') from None

    def stop(self):
        if self.relay_pool:
            self.relay_pool.close()

    def _json(self,path,payload=None,timeout=20):
        with self._open(path,payload,timeout=timeout) as response:
            raw=response.read(1024*1024+1)
        if len(raw)>1024*1024:
            raise RuntimeError('Hermes response exceeds limit')
        return json.loads(raw)

    def _catalog(self,refresh=False):
        with self.catalog_lock:
            if self.catalog and not refresh and time.monotonic()-self.catalog_time<60:
                return self.catalog
            payload=self._json('/api/model/options')
            entries={}
            current=None
            for row in payload.get('providers',[]):
                if row.get('authenticated') is False:
                    continue
                slug=row.get('slug')
                if not isinstance(slug,str) or not slug:
                    continue
                # The channel command grammar reserves colon for provider:model.
                # Keep custom:* identifiers private and map to a stable opaque token.
                token=slug if ':' not in slug else 'hermes-'+re.sub('[^a-z0-9-]','-',slug.lower())[:40]+'-'+hashlib.sha256(slug.encode()).hexdigest()[:6]
                for model_id in row.get('models',[]):
                    if model_id in (row.get('unavailable_models') or []):
                        continue
                    model=public_model({'provider':token,'id':model_id,'name':model_id,
                        'reasoning':row.get('capabilities',{}).get(model_id,{}).get('reasoning') is True})
                    if model:
                        entries[(token,model_id)]={'public':model,'provider':slug}
                        if model_id==payload.get('model') and (slug.lower()==str(payload.get('provider','')).lower() or row.get('is_current')):
                            current=entries[(token,model_id)]
            if not entries or not current:
                raise ValueError('Hermes 当前模型或凭据不可用，请检查 technical 配置')
            # Put the active provider first when a very large catalog is truncated.
            entries=dict(sorted(entries.items(),key=lambda item: item[1]['provider']!=current['provider']))
            self.catalog={'entries':entries,'default':current}
            self.catalog_time=time.monotonic()
            return self.catalog

    def available_models(self):
        return [entry['public'] for entry in self._catalog()['entries'].values()]

    def is_ready(self):
        try:
            # /v1/models is an identity probe, never the actual model picker.
            return any(m.get('id')==self.config['expected_model']
                       for m in self._json('/models',timeout=3).get('data',[]))
        except Exception:
            return False

    def _scope(self,session):
        return 'spark-im-'+hashlib.sha256((self.namespace+'\0'+session).encode()).hexdigest()

    def _upstream(self,session,state):
        return state.get('upstream_session',self._scope(session)+'-'+str(state['context_start_seq']))

    def _ensure_session(self,session,state):
        upstream=self._upstream(session,state)
        if not state.get('upstream_session'):
            # Deterministic create: HTTP 409 after a lost response means it
            # already exists. Read only that exact session before continuing.
            try:
                self._json('/api/sessions',{'id':upstream})
            except RuntimeError:
                self._json('/api/sessions/'+quote(upstream,safe=''))
        return upstream

    def _lock_model(self,session,state,entry):
        upstream=self._ensure_session(session,state)
        path='/api/sessions/'+quote(upstream,safe='')+'/model'
        ack=self._json(path,{'provider':entry['provider'],'model':entry['public']['id'],'require_model_lock':True})
        runtime=ack.get('runtime',{})
        if (ack.get('session_id')!=upstream or runtime.get('model_lock')!='accepted' or
            runtime.get('model')!=entry['public']['id'] or runtime.get('provider')!=entry['provider']):
            raise RuntimeError('Hermes 未确认模型选择')
        state.update(upstream_session=upstream,selection=entry,model=entry['public']['id'])

    def _state(self,session):
        return self.store.get(self.namespace,session) or {'context_start_seq':0,'model':self.config['expected_model']}

    def _metadata(self,state):
        model=state.get('selection',{}).get('public') or (self.catalog or {}).get('default',{}).get('public')
        model=model or public_model({'provider':'hermes','id':state['model']})
        return {'context_start_seq':state['context_start_seq'],
            'model_state':{'override':bool(state.get('selection')),'model':model}}

    def control(self,request,timeout_s=30):
        session,operation=request['session_id'],request.get('operation')
        state=self._state(session)
        card=command_card('Hermes 会话操作',['model','new','retry','restart','status','help'])
        if operation in {'list_models','set_model','reset_model'}:
            catalog=self._catalog(refresh=request.get('refresh') is True)
            if operation!='list_models':
                entry=(catalog['default'] if operation=='reset_model' else
                       catalog['entries'].get((request.get('target_provider'),request.get('target_model'))))
                if entry is None:
                    raise ValueError('该 Hermes 模型不可用，请刷新模型列表')
                revision=request.get('command_seq')
                if type(revision) is not int or not state['context_start_seq']<revision<2**63:
                    raise ValueError('模型命令属于旧上下文')
                if revision<state.get('model_revision',0) or (revision==state.get('model_revision') and entry!=state.get('selection')):
                    raise ValueError('模型选择已过期')
                if revision!=state.get('model_revision'):
                    self._lock_model(session,state,entry)
                    state['model_revision']=revision
                    self.store.put(self.namespace,session,state)
            selected=self._metadata(state)['model_state']['model']
            text=('当前 Hermes 模型：' if operation=='list_models' else '已切换 Hermes 模型：')+selected['id']
            if operation=='reset_model':
                text+='（采用 profile 当前默认值）'
            if operation=='list_models':
                models=[]
                size=0
                for entry in catalog['entries'].values():
                    model=entry['public']
                    size+=len(json.dumps(model,ensure_ascii=False).encode())+2
                    if size>90000 or len(models)>=512:
                        break
                    models.append(model)
                card={'kind':'model_picker','current':selected,'models':models,
                      'truncated':len(models)<len(catalog['entries'])}
        elif operation=='new_session':
            revision=request.get('command_seq')
            if type(revision) is not int or not 0<revision<2**63 or revision<state['context_start_seq']:
                raise ValueError('新建命令已过期')
            if revision>state['context_start_seq']:
                # /new clears the transcript, retaining the user's model choice.
                state={k:v for k,v in state.items() if k in {'model','selection','model_revision'}}
                state['context_start_seq']=revision
                self.store.put(self.namespace,session,state)
            text='已创建新的 Hermes 上下文；模型选择、聊天记录和长期记忆保留。'
        elif operation=='new':
            text='确认后将开启新的 Hermes 对话；保留模型选择、聊天记录和 profile 长期记忆。'
            card=command_card('新建 Hermes 上下文',['new_confirm','cancel'])
        elif operation=='retry':
            if state.get('last_user'):
                text='重新发送当前 Hermes 上下文的上一条用户消息；工具可能再次执行。'
                card=command_card('重试 Hermes 消息',['retry_confirm','cancel'])
            else:
                text='当前 Hermes 上下文没有可重试消息，请先发送一条消息。'
        elif operation=='restart':
            text='确认后重启 Windows Hermes technical 服务。此 profile 的所有渠道会短暂断开，正在执行的任务可能中断；聊天上下文、模型选择和长期记忆保留。'
            card=command_card('重启 Hermes technical 服务',['restart_confirm','cancel'])
        elif operation=='restart_agent':
            text=self._restart(request,state,timeout_s)
        elif operation in {'list_reasoning','set_reasoning'}:
            text='当前适配尚未开放 Hermes 会话思考等级切换，沿用 technical profile 配置；此次未修改。'
        elif operation in {'status','help'}:
            ready=self.is_ready()
            selected=self._metadata(state)['model_state']['model']
            model_label=selected['id'] if selected['provider']!='hermes' else '跟随 technical profile 默认配置'
            text='Hermes · '+self.config.get('profile','')+('\n服务：可连接' if ready else '\n服务：暂不可连接')+'\n模型：'+model_label+'\n上下文编号：'+str(state['context_start_seq'])
            text+='\n/model 选择模型\n/new 新建上下文\n/retry 重试上一条消息（可能重复执行工具）\n/restart 重启 technical 服务\n/status 会话状态\n/help 命令菜单\n/agent 打开其他 Agent 对话'
        else:
            raise ValueError('此 Hermes Adapter 不支持该操作')
        return agent_event('assistant_final',{'text':text,**self._metadata(state),'presentation':card})

    def _restart(self,request,state,timeout_s):
        from hermes_restart import restart_technical
        revision=request.get('command_seq')
        if type(revision) is not int or not state['context_start_seq']<revision<2**63:
            raise ValueError('重启命令已过期')
        session=request['session_id']
        with self.restart_lock:
            previous=self.store.get(self.namespace+':restart',session)
            if revision<=previous.get('revision',0):
                return previous.get('text','重启请求已提交，结果尚未确认。请用 /status 检查，勿重复提交。')
            # Durable intent BEFORE process mutation; replay never repeats a restart.
            self.store.put(self.namespace+':restart',session,{'revision':revision})
            try:
                deadline=time.monotonic()+timeout_s
                restart_technical(self.config,timeout=min(timeout_s-4,80))
                # A running PID precedes HTTP readiness during Hermes startup.
                ready=self.is_ready()
                while not ready and time.monotonic()<deadline-3:
                    time.sleep(0.5)
                    ready=self.is_ready()
                if not ready:
                    raise RuntimeError('service not ready')
                text='Hermes technical 服务已重启并通过连接检查；当前上下文和模型选择保留。'
            except Exception:
                text='重启请求已提交，但服务恢复尚未确认。请稍后用 /status 检查；此请求不会自动重复执行。'
            self.store.put(self.namespace+':restart',session,{'revision':revision,'text':text})
            return text

    def chat(self,message,on_delta,timeout_s,provider=None,model=None,session_id='',context_start_seq=0,retry=False,on_progress=None):
        started=time.monotonic()
        trace=uuid.uuid4().hex[:12]
        try:
            if on_progress:
                on_progress('Hermes · 正在连接 Agent 服务')
            state=self._state(session_id)
            if retry:
                message=state.get('last_user','')
                if not message:
                    raise ValueError('当前 Hermes 上下文没有可重试消息')
            # No IM history from Pi is sent to Hermes. Continuity is explicit and
            # uses a different namespace even for identical text from two users.
            scope=self._scope(session_id)
            upstream=state.get('upstream_session',scope+'-'+str(state['context_start_seq']))
            state['last_user']=message
            self.store.put(self.namespace,session_id,state)
            deadline=time.monotonic()+timeout_s
            parts=[]
            finish=None
            done=False
            total=0
            last_progress=started
            first_delta=None
            path='/chat/completions'
            payload={'model':state['model'],'stream':True,'messages':[{'role':'user','content':message}]}
            if state.get('selection'):
                # Explicit provider+model is honored by the installed OpenAI-compatible
                # endpoint; provider resolution fails closed. The Browser stream has
                # an upstream custom:* alias vs runtime "custom" comparison bug.
                payload.update(model=state['selection']['public']['id'],provider=state['selection']['provider'])
            with self._open(path,payload,
                    {'X-Hermes-Session-Id':upstream,'X-Hermes-Session-Key':scope},timeout=timeout_s) as response:
                connected=time.monotonic()
                if on_progress:
                    on_progress('Hermes · 服务已连接，等待模型回答')
                actual=response.headers.get('X-Hermes-Session-Id',upstream)
                if len(actual)>240 or any(ord(c)<33 for c in actual):
                    raise RuntimeError('Hermes returned invalid session identity')
                state['upstream_session']=actual
                self.store.put(self.namespace,session_id,state)
                event=''
                data=[]
                for raw in iter(lambda: response.readline(1024*1024+1), b''):
                    total+=len(raw)
                    if total>16*1024*1024 or len(raw)>1024*1024:
                        raise RuntimeError('Hermes stream exceeds limit')
                    if time.monotonic()>deadline:
                        raise TimeoutError('Hermes turn deadline exceeded')
                    line=raw.decode('utf-8').rstrip('\r\n')
                    if line.startswith(':'):
                        if on_progress and time.monotonic()-last_progress>=15:
                            on_progress('Hermes 正在处理，连接正常')
                            last_progress=time.monotonic()
                        continue
                    if line.startswith('event:'):
                        event=line[6:].strip()
                    elif line.startswith('data:'):
                        data.append(line[5:].lstrip())
                    elif not line:
                        value='\n'.join(data)
                        data=[]
                        kind,event=event,''
                        if value=='[DONE]':
                            done=True
                            break
                        if not value:
                            continue
                        payload=json.loads(value)
                        if payload.get('error') or kind=='error':
                            raise RuntimeError('Hermes 执行失败，未将部分输出标记为完成')
                        if kind=='hermes.tool.progress':
                            if on_progress:
                                tool=str(payload.get('tool') or payload.get('tool_name') or payload.get('toolName') or '工具')[:80]
                                on_progress('Hermes · '+tool+' · '+str(payload.get('status') or '执行中')[:40])
                            continue
                        for choice in payload.get('choices',[]):
                            if choice.get('index',0)!=0:
                                continue
                            delta=choice.get('delta',{}).get('content')
                            if isinstance(delta,str):
                                if delta and first_delta is None:
                                    first_delta=time.monotonic()
                                parts.append(delta)
                                if on_delta:
                                    on_delta(delta)
                            if choice.get('finish_reason') is not None:
                                finish=choice['finish_reason']
            text=''.join(parts)
            if not done or finish!='stop' or not text.strip():
                raise RuntimeError('Hermes 未正常完成回复；请检查状态后再重试')
            timing={'trace_id':trace,'connect_ms':round((connected-started)*1000),
                    'first_delta_ms':round((first_delta-started)*1000) if first_delta else None,
                    'total_ms':round((time.monotonic()-started)*1000)}
            print('hermes_timing '+json.dumps(timing),file=sys.stderr,flush=True)
            return text,{**self._metadata(state),'timing':timing}
        except Exception as exc:
            print('hermes_timing '+json.dumps({'trace_id':trace,'error_type':type(exc).__name__,
                'total_ms':round((time.monotonic()-started)*1000)}),file=sys.stderr,flush=True)
            raise
