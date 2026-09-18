"""Hermes Agent API transport, not a direct model completion adapter."""
from __future__ import annotations

import hashlib
import json
import os
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


def _text_metrics(value):
    if not isinstance(value, str):
        return {"bytes": 0, "chars": 0}
    return {"bytes": len(value.encode("utf-8")), "chars": len(value)}


def _length_audit_enabled():
    return os.environ.get("SPARK_PUSH_LENGTH_AUDIT", "").lower() in {"1", "true"}


def _length_audit(event):
    # Keep this diagnostic deliberately metadata-only: prompts, answers,
    # credentials and account identifiers never enter the log line.
    if not _length_audit_enabled():
        return
    print("hermes_length_audit " + json.dumps(event, ensure_ascii=False,
                                              separators=(",", ":")),
          file=sys.stderr, flush=True)


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
        self.artifact_store = None
        self.namespace='hermes:'+config['id']
        # Android gets a provider-first picker to keep the first Compose frame
        # small. Legacy PC/Telegram Hermes keeps its existing full model card.
        self.provider_first = (config.get('provider_first') is True or
                               config.get('id') == 'hermes-android')
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
            provider_info={}
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
                label=row.get('name')
                if not isinstance(label,str) or not label.strip():
                    label=slug[7:] if slug.startswith('custom:') else slug
                label=''.join(c for c in label.strip() if ord(c)>=32)[:160] or token
                provider_info[token]={'id':token,'name':label,'model_count':0,'current':False}
                for model_id in row.get('models',[]):
                    if model_id in (row.get('unavailable_models') or []):
                        continue
                    model=public_model({'provider':token,'id':model_id,'name':model_id,
                        'reasoning':row.get('capabilities',{}).get(model_id,{}).get('reasoning') is True})
                    if model:
                        key=(token,model_id)
                        if key not in entries:
                            entries[key]={'public':model,'provider':slug}
                            provider_info[token]['model_count']+=1
                        if model_id==payload.get('model') and (slug.lower()==str(payload.get('provider','')).lower() or row.get('is_current')):
                            current=entries[key]
            if not entries or not current:
                raise ValueError('Hermes 当前模型或凭据不可用，请检查 technical 配置')
            # Put the active provider first when a very large catalog is truncated.
            entries=dict(sorted(entries.items(),key=lambda item: item[1]['provider']!=current['provider']))
            active_provider=current['public']['provider']
            providers=[]
            for item in provider_info.values():
                if item['model_count']<=0:
                    continue
                providers.append({**item,'current':item['id']==active_provider})
            providers.sort(key=lambda item:(not item['current'],item['name'].lower(),item['id']))
            self.catalog={'entries':entries,'default':current,'providers':providers}
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
        state=self.store.get(self.namespace,session)
        if not isinstance(state,dict):
            state={}
        # Technical sessions are writing sessions by default. Older sessions
        # may predate creative_mode; normalize them before every turn.
        if self.config.get('id') == 'hermes-android':
            state.setdefault('creative_mode','write')
        state.setdefault('context_start_seq',0)
        state.setdefault('model',self.config['expected_model'])
        return state

    def _metadata(self,state):
        model=state.get('selection',{}).get('public') or (self.catalog or {}).get('default',{}).get('public')
        model=model or public_model({'provider':'hermes','id':state['model']})
        return {'context_start_seq':state['context_start_seq'],
            'model_state':{'override':bool(state.get('selection')),'model':model}}

    def _creative_prompt(self, state, message):
        """Build a bounded, inspectable prompt projection for technical mode."""
        if not state.get('creative_mode'):
            return None
        sections=['technical 创作模式：'+('剧情跑团' if state.get('creative_mode')=='roleplay' else '写作协作')]
        for label,key in [('角色设定','character'),('用户人设','persona'),('当前场景','scene'),('当前分支','active_branch')]:
            if state.get(key): sections.append(label+'：'+str(state[key])[:3000])
        facts=state.get('pinned_facts',[])
        if facts: sections.append('固定剧情事实：\n- '+'\n- '.join(str(x)[:1000] for x in facts[-20:]))
        lore=[]
        for entry in state.get('lorebook',[]):
            if isinstance(entry,dict) and any(str(k).lower() in message.lower() for k in entry.get('keys',[])):
                lore.append(str(entry.get('content',''))[:1500])
        if lore: sections.append('本轮激活世界书：\n- '+'\n- '.join(lore[:12]))
        return '\n\n'.join(sections)

    def control(self,request,timeout_s=30):
        session,operation=request['session_id'],request.get('operation')
        state=self._state(session)
        creative={'roleplay':'剧情跑团','write':'写作协作','scene':'当前场景','character':'角色设定','world':'世界书','memory':'记忆状态','remember':'保存固定事实','forget':'删除固定事实','persona':'用户人设','branch':'创建剧情分支','branches':'查看剧情分支','canon':'设为主线','export':'导出当前剧情'}
        if operation in creative:
            if operation in {'roleplay','write'}:
                state['creative_mode']=operation
                self.store.put(self.namespace,session,state)
                text='已切换 technical 模式：'+creative[operation]+'。后续请求将使用该创作模式。'
            elif operation=='persona':
                argument=request.get('argument','').strip()
                if argument: state['persona']=argument[:4000]; self.store.put(self.namespace,session,state); text='已更新用户人设。'
                else: text='当前用户人设：'+state.get('persona','尚未设置')
            elif operation in {'character','world'}:
                argument=request.get('argument','').strip()
                if operation=='character':
                    if argument: state['character']=argument[:4000]; self.store.put(self.namespace,session,state); text='已更新角色设定。'
                    else: text='当前角色设定：'+state.get('character','尚未设置')
                elif argument:
                    parts=argument.split('::',1)
                    if len(parts)!=2 or not parts[0].strip() or not parts[1].strip(): text='用法：/world 关键词1,关键词2 :: 世界观内容'
                    else:
                        lore=state.get('lorebook',[]); lore.append({'keys':[x.strip() for x in parts[0].split(',') if x.strip()][:12],'content':parts[1].strip()[:3000]}); state['lorebook']=lore[-100:]; self.store.put(self.namespace,session,state); text='已添加世界书条目（当前共 '+str(len(lore))+' 条）。'
                else: text='当前世界书条目：'+str(len(state.get('lorebook',[])))+' 条'
            elif operation=='scene':
                argument=request.get('argument','').strip()
                if argument:
                    state['scene']=argument[:2000]
                    self.store.put(self.namespace,session,state)
                    text='已更新当前场景：'+state['scene']
                else:
                    text='当前场景：'+state.get('scene','尚未设置')
            elif operation=='remember':
                argument=request.get('argument','').strip()
                if not argument:
                    text='用法：/remember <要固定的剧情事实>'
                else:
                    facts=state.get('pinned_facts',[])
                    if not isinstance(facts,list): facts=[]
                    facts.append(argument[:2000]); state['pinned_facts']=facts[-50:]
                    self.store.put(self.namespace,session,state)
                    text='已保存固定事实（当前共 '+str(len(state['pinned_facts']))+' 条）。'
            elif operation=='forget':
                argument=request.get('argument','').strip()
                facts=state.get('pinned_facts',[])
                if argument and isinstance(facts,list):
                    facts=[x for x in facts if x!=argument]
                    state['pinned_facts']=facts; self.store.put(self.namespace,session,state)
                    text='已删除匹配的固定事实（当前共 '+str(len(facts))+' 条）。'
                else:
                    text='当前固定事实：'+('；'.join(facts) if facts else '暂无')
            elif operation=='memory':
                text='technical 当前模式：'+({'roleplay':'剧情跑团','write':'写作协作'}.get(state.get('creative_mode'),'默认写作'))+'；固定事实 '+str(len(state.get('pinned_facts',[])))+' 条；场景：'+state.get('scene','未设置')
            elif operation=='branch':
                title=request.get('argument','').strip() or '未命名分支'
                branches=state.get('branches',[]); item={'id':'branch-'+uuid.uuid4().hex[:8],'title':title[:120],'canon':not branches}
                branches.append(item); state['branches']=branches[-50:]; state['active_branch']=item['id']; self.store.put(self.namespace,session,state); text='已创建剧情分支：'+item['title']
            elif operation=='branches':
                branches=state.get('branches',[]); text='当前剧情分支：'+('；'.join(x.get('title','')+(' [主线]' if x.get('canon') else '') for x in branches) if branches else '暂无')
            elif operation=='canon':
                target=request.get('argument','').strip(); branches=state.get('branches',[])
                for item in branches: item['canon']=bool(target and target in {item.get('id'),item.get('title')})
                if branches and not any(x['canon'] for x in branches): branches[-1]['canon']=True
                state['branches']=branches; self.store.put(self.namespace,session,state); text='已更新剧情主线。'
            elif operation=='export':
                lines=['# technical 剧情导出','', '## 模式', str(state.get('creative_mode','write'))]
                for label,key in [('角色设定','character'),('用户人设','persona'),('当前场景','scene')]:
                    if state.get(key): lines += ['', '## '+label, str(state[key])[:12000]]
                facts=state.get('pinned_facts',[])
                if facts: lines += ['', '## 固定事实'] + ['- '+str(x)[:2000] for x in facts[:50]]
                branches=state.get('branches',[])
                if branches: lines += ['', '## 剧情分支'] + ['- '+str(x.get('title',''))+('（主线）' if x.get('canon') else '') for x in branches[:50]]
                lore=state.get('lorebook',[])
                if lore: lines += ['', '## 世界书'] + ['- '+', '.join(x.get('keys',[]))+': '+str(x.get('content',''))[:2000] for x in lore[:100]]
                artifact_text='\n'.join(lines)+'\n'
                text='已生成 technical 剧情 Markdown 导出。'
                artifact={'name':'technical-story.md','mime':'text/markdown','text':artifact_text[:240000]}
                if self.artifact_store is not None:
                    saved=self.artifact_store.put_text(session, artifact['name'], artifact['text'], artifact['mime'])
                    artifact={**saved, 'text':artifact['text']}
            else:
                text='technical '+creative[operation]+'面板将在下一阶段开放；当前命令已记录到独立会话。'
            mode=state.get('creative_mode','write')
            actions=['roleplay','write','scene','character','persona','world','memory','branch','branches','export']
            data={'text':text,'creative_state':{'mode':mode},
                'presentation':{'kind':'creative_workspace','title':'technical 创作工作台 · '+mode,
                    'state':{'mode':mode,'character':state.get('character',''),'persona':state.get('persona',''),
                             'scene':state.get('scene',''),'active_branch':state.get('active_branch',''),
                             'pinned_facts_count':len(state.get('pinned_facts',[])),
                             'lorebook_count':len(state.get('lorebook',[])),
                             'branch_count':len(state.get('branches',[]))},
                    'actions':[{'id':key,'label':creative[key],'selected':key==mode} for key in actions]}}
            if operation=='export': data['artifact']=artifact
            if operation=='export': data['presentation']['artifact']=artifact
            return agent_event('assistant_final',data)
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
                provider_filter=request.get('provider_filter')
                if provider_filter is not None and (not isinstance(provider_filter,str) or not provider_filter or ':' in provider_filter or len(provider_filter)>128):
                    raise ValueError('provider 参数无效，请重新打开 /model')
                active_provider=selected['provider']
                providers=[{**item,'current':item['id']==active_provider} for item in catalog['providers']]
                if provider_filter:
                    models=[entry['public'] for entry in catalog['entries'].values()
                            if entry['public']['provider']==provider_filter]
                    if not models:
                        raise ValueError('该 provider 当前没有可用模型，请刷新 /model')
                    selected_provider=provider_filter
                    truncated=False
                else:
                    if self.provider_first:
                        # Android's first response is an inexpensive provider
                        # index. Models are loaded only after a provider is
                        # selected, so a large catalog cannot block Compose.
                        models=[]
                        selected_provider=None
                        truncated=False
                    else:
                        # Preserve the established PC/Telegram response shape.
                        models=[entry['public'] for entry in catalog['entries'].values()]
                        selected_provider=None
                        truncated=False
                source_models=list(models)
                models=[]
                size=0
                for model in source_models:
                    size+=len(json.dumps(model,ensure_ascii=False).encode())+2
                    if size>90000 or len(models)>=512:
                        truncated=True
                        break
                    models.append(model)
                card={'kind':'model_picker','current':selected,'models':models,
                      'providers':providers,'truncated':truncated}
                if selected_provider:
                    card['selected_provider']=selected_provider
        elif operation=='new_session':
            revision=request.get('command_seq')
            if type(revision) is not int or not 0<revision<2**63 or revision<state['context_start_seq']:
                raise ValueError('新建命令已过期')
            if revision>state['context_start_seq']:
                # /new clears the transcript, retaining the user's model choice.
                state={k:v for k,v in state.items() if k in {
                    'model','selection','model_revision','creative_mode',
                    'persona','character','lorebook','pinned_facts',
                }}
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
            levels=['low','high','xhigh','max']
            if operation=='set_reasoning':
                target=request.get('target') or request.get('level') or request.get('argument','')
                if target not in levels:
                    raise ValueError('思考深度必须是 low、high、xhigh 或 max')
                state['reasoning_level']=target; self.store.put(self.namespace,session,state)
                text='已切换 Hermes 思考深度：'+target+'。后续请求立即生效；未修改 technical profile 文件。'
            else:
                text='当前 Hermes 思考深度：'+str(state.get('reasoning_level') or 'profile 默认')
            card={'kind':'reasoning_picker','current':state.get('reasoning_level'),'levels':levels}
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

    def chat(self,message,on_delta,timeout_s,provider=None,model=None,session_id='',context_start_seq=0,retry=False,on_progress=None,request_id=''):
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
            chunk_count=0
            chunk_bytes=0
            chunk_chars=0
            last_progress=started
            first_delta=None
            path='/chat/completions'
            messages=[]
            creative=self._creative_prompt(state,message)
            if creative: messages.append({'role':'system','content':creative})
            messages.append({'role':'user','content':message})
            prompt_bytes=sum(_text_metrics(item.get('content','')).get('bytes',0)
                             for item in messages if isinstance(item,dict))
            prompt_chars=sum(_text_metrics(item.get('content','')).get('chars',0)
                             for item in messages if isinstance(item,dict))
            payload={'model':state['model'],'stream':True,'messages':messages}
            if state.get('reasoning_level'):
                payload['reasoning_effort']=state['reasoning_level']
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
                                if delta:
                                    metrics=_text_metrics(delta)
                                    chunk_count+=1
                                    chunk_bytes+=metrics['bytes']
                                    chunk_chars+=metrics['chars']
                                parts.append(delta)
                                if on_delta:
                                    on_delta(delta)
                            if choice.get('finish_reason') is not None:
                                finish=choice['finish_reason']
            text=''.join(parts)
            output_metrics=_text_metrics(text)
            selected=state.get('selection') if isinstance(state.get('selection'),dict) else {}
            public=selected.get('public') if isinstance(selected.get('public'),dict) else {}
            audit={
                'trace_id':trace,
                'provider':selected.get('provider') or 'profile-default',
                'public_provider':public.get('provider') or 'profile-default',
                'model':public.get('id') or state.get('model'),
                'profile':self.config.get('profile',''),
                'stream':True,
                'max_tokens':None,
                'max_output_tokens':None,
                'temperature':None,
                'stop':None,
                'timeout_s':round(float(timeout_s),3),
                'context_start_seq':state.get('context_start_seq',0),
                'body_message_count':len(messages),
                'system_prompt_bytes':_text_metrics(creative)['bytes'] if creative else 0,
                'system_prompt_chars':_text_metrics(creative)['chars'] if creative else 0,
                'prompt_bytes':prompt_bytes,
                'prompt_chars':prompt_chars,
                'chunk_count':chunk_count,
                'provider_chunk_bytes':chunk_bytes,
                'provider_chunk_chars':chunk_chars,
                'provider_output_bytes':output_metrics['bytes'],
                'provider_output_chars':output_metrics['chars'],
                'finish_reason':finish,
                'done':done,
            }
            _length_audit(audit)
            if not done or finish!='stop' or not text.strip():
                raise RuntimeError('Hermes 未正常完成回复；请检查状态后再重试')
            timing={'trace_id':trace,'connect_ms':round((connected-started)*1000),
                    'first_delta_ms':round((first_delta-started)*1000) if first_delta else None,
                    'total_ms':round((time.monotonic()-started)*1000),
                    'chunk_count':chunk_count,'finish_reason':finish,
                    'provider_output_bytes':output_metrics['bytes'],
                    'provider_output_chars':output_metrics['chars']}
            print('hermes_timing '+json.dumps(timing),file=sys.stderr,flush=True)
            metadata={**self._metadata(state),'timing':timing}
            # Keep the normal Hermes response metadata/protocol unchanged.
            # The detailed length ledger is emitted only during an explicit
            # audit run and is never persisted into ordinary chat messages.
            if _length_audit_enabled():
                metadata['length_audit']=audit
            return text,metadata
        except Exception as exc:
            _length_audit({
                'trace_id':trace,
                'provider':(state.get('selection') or {}).get('provider','profile-default') if 'state' in locals() else 'unknown',
                'model':(state.get('model') if 'state' in locals() else None),
                'stream':True,
                'max_tokens':None,
                'max_output_tokens':None,
                'chunk_count':chunk_count if 'chunk_count' in locals() else 0,
                'provider_output_bytes':_text_metrics(''.join(parts) if 'parts' in locals() else '')['bytes'],
                'provider_output_chars':_text_metrics(''.join(parts) if 'parts' in locals() else '')['chars'],
                'finish_reason':finish if 'finish' in locals() else None,
                'done':done if 'done' in locals() else False,
                'error_type':type(exc).__name__,
            })
            print('hermes_timing '+json.dumps({'trace_id':trace,'error_type':type(exc).__name__,
                'total_ms':round((time.monotonic()-started)*1000)}),file=sys.stderr,flush=True)
            raise
