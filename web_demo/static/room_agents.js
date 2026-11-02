(function(global){
'use strict';
class RoomAgents {
  constructor(panel,input,choices,api,append,options={}){
    Object.assign(this,{panel,input,choices,api,append,options,room:0,version:0,agents:[],selected:new Set(),streams:new Map(),completed:new Set()});
    panel.querySelector('[data-agents-refresh]').onclick=()=>this.refresh();
    input.addEventListener('input',()=>this.suggest());
  }
  async select(room){
    this.room=Number(room);this.version++;this.agents=[];this.selected.clear();this.completed.clear();
    for(const stream of this.streams.values())stream.node.remove();this.streams.clear();
    this.choices.replaceChildren();this.panel.querySelector('[data-agents-list]').replaceChildren();this.panel.querySelector('[data-agent-requests]').replaceChildren();this.panel.querySelector('[data-room-tasks]')?.replaceChildren();
    this.notice('');this.renderSelected();if(this.room)await this.refresh();
  }
  notice(text){this.panel.querySelector('[data-agents-error]').textContent=text;}
  async invoke(path,body){
    const version=this.version;
    try {const data=await this.api('/api/chatroom/agents/'+path,{room_id:this.room,...body});return version===this.version?data:null;}
    catch(error){if(version===this.version){this.notice(error.message);if(path==='sources'&&error.code===403)this.options.sourceDenied?.(this.room);}return null;}
  }
  async refresh(){
    if(!this.room)return;
    const refreshId=this.refreshId=(this.refreshId||0)+1;
    const data=await this.invoke('list',{});if(!data||refreshId!==this.refreshId)return;
    this.agents=Array.isArray(data.agents)?data.agents:[];this.notice('');const list=this.panel.querySelector('[data-agents-list]');list.replaceChildren();
    for(const agent of this.agents){
      const row=document.createElement('div');row.textContent=`${agent.runtime==='pi_rpc'?'π':'H'} · ${agent.name} · ${agent.installed?'已安装':'未安装'} · ${agent.enabled?'只读':'未开放'}`;
      if(data.can_manage && (agent.enabled || agent.installed)){
        const button=document.createElement('button');button.textContent=agent.installed?'停用':'安装';button.dataset.agentId=agent.agent_id;
        button.onclick=async()=>{if(await this.invoke(agent.installed?'uninstall':'install',{agent_id:agent.agent_id}))await this.refresh();};row.appendChild(button);
      }
      list.appendChild(row);
    }
    for(const id of this.selected)if(!this.agents.some(a=>a.agent_id===id&&a.installed&&a.enabled))this.selected.delete(id);
    this.renderSelected();this.renderRequests(data.requests||[]);await this.loadTasks();
    if(refreshId===this.refreshId)this.options.loaded?.();
  }
  renderRequests(requests){
    const list=this.panel.querySelector('[data-agent-requests]');list.replaceChildren();
    for(const request of requests){
      if(request.agent_id==='cann')continue;
      const row=document.createElement('div');row.dataset.requestId=request.request_id;
      row.textContent=`${request.agent_name} · ${this.options.name?.(request.actor_user_id)||('发起人 '+request.actor_user_id)} · ${this.options.status?.(request.status)||request.status}`;
      if(request.result && request.result.text){const detail=document.createElement('pre');detail.style.whiteSpace='pre-wrap';detail.textContent=request.result.text;row.appendChild(detail);}
      this.sourcesButton(row,request.request_id);
      const query=document.createElement('button');query.textContent='查询执行状态';
      query.onclick=async()=>{
        const state=await this.invoke('control',{request_id:request.request_id,operation:'status'});if(!state)return;
        query.textContent='执行：'+state.status;
        if(state.capabilities?.stop && !['completed','failed','cancelled'].includes(state.status) && !row.querySelector('[data-stop]')){
          const stop=document.createElement('button');stop.dataset.stop='';stop.textContent='请求停止';
          stop.onclick=async()=>{const result=await this.invoke('control',{request_id:request.request_id,operation:'stop'});if(result)this.notice(result.message||('执行：'+result.status));};row.appendChild(stop);
        }
      };row.appendChild(query);
      if(request.status==='completed'){
        const save=document.createElement('button');save.textContent='保存为协作任务';save.onclick=()=>this.createTask(request);row.appendChild(save);
        const proposal=this.proposal(request.result?.text||'');
        for(const task of (Array.isArray(proposal.tasks)?proposal.tasks:[]).slice(0,30)){
          if(!task||typeof task.title!=='string')continue;
          const button=document.createElement('button');button.textContent='保存任务 · '+task.title;
          button.onclick=()=>this.createTask(request,task);row.appendChild(button);
        }
      }
      if(request.status==='unknown'){
        const recover=document.createElement('button');recover.textContent='恢复原请求';
        recover.onclick=async()=>{
          const state=await this.invoke('control',{request_id:request.request_id,operation:'status'});if(!state)return;
          if(!['completed','failed','cancelled','interrupted'].includes(state.status)){
            this.notice('原执行状态：'+state.status+'。请稍后查询；结果未知时不会新建任务。');return;
          }
          if(await this.invoke('control',{request_id:request.request_id,operation:'recover'}))this.notice('已排队恢复原请求 ID；不会创建新的任务。');
        };row.appendChild(recover);
      }
      if(request.status==='failed'){
        const button=document.createElement('button');button.textContent='重新提问（新请求）';
        button.onclick=()=>{this.selected=new Set([request.agent_id]);this.input.value='请根据上一条失败情况重新回答：';this.renderSelected();this.input.focus();};row.appendChild(button);
      }
      list.appendChild(row);
    }
  }
  async taskApi(action,body){
    const version=this.version;
    try{const result=await this.api('/api/chatroom/tasks/'+action,{room_id:this.room,...body});return version===this.version?result:null;}
    catch(error){if(version===this.version)this.notice(error.message);return null;}
  }
  proposal(text){try{const value=JSON.parse(text.slice(text.indexOf('{'),text.lastIndexOf('}')+1));return value&&typeof value==='object'&&!Array.isArray(value)?value:{};}catch(_){return {};}}
  timelineSources(parent,request){
    if(!this.options.openSource||typeof request!=='string'||!request||request.length>121)return;
    let box=parent.querySelector('[data-message-sources]');
    if(box&&box.dataset.sourceVersion===String(this.version)&&box.dataset.requestId===request)return;
    if(!box){box=document.createElement('div');box.dataset.messageSources='';parent.append(box);}else box.replaceChildren();
    box.dataset.sourceVersion=String(this.version);box.dataset.requestId=request;this.sourcesButton(box,request);
  }
  sourcesButton(parent,request,ids){
    if(!this.options.openSource)return;
    const button=document.createElement('button');button.textContent=ids?'查看任务来源':'查看读取来源';
    const box=document.createElement('div');box.hidden=true;box.dataset.sourceList='';
    let after='',loading=false;const version=this.version;
    const load=async more=>{
      if(loading||version!==this.version)return;loading=true;
      if(!more)box.replaceChildren();
      try{
        const page=await this.invoke('sources',{request_id:request,...(ids?{source_ids:ids}:more?{after}:{})});if(!page)return;
        if(!more){box.replaceChildren();const hint=document.createElement('p');hint.textContent=ids?'任务保存的来源，点击后重新核对可用性。':'服务端记录的读取来源，不代表结论已经核实。';box.append(hint);}
        for(const source of page.items){
          const row=document.createElement('div'),link=document.createElement('button');
          link.textContent=source.available?source.name:'来源已失效或已撤回';link.disabled=!source.available;link.dataset.sourceId=source.source_id;
          if(source.available){const ref=source.reference,anchor=ref.anchor||{};
            if(ref.kind==='image_metadata')link.textContent+='（图片信息）';
            if(ref.kind==='media')link.textContent+='（资料目录）';
            if(anchor.page)link.textContent+=' · 第 '+anchor.page+' 页';
            else if(anchor.start_ms!==undefined||anchor.timestamp_ms!==undefined){const seconds=Math.floor((anchor.start_ms??anchor.timestamp_ms)/1000);link.textContent+=' · '+Math.floor(seconds/60)+':'+String(seconds%60).padStart(2,'0');}
            else if(ref.line_start)link.textContent+=' · 第 '+ref.line_start+' 行';
          }
          link.onclick=()=>{if(version===this.version)this.options.openSource(request,source.source_id);};row.append(link);box.append(row);
        }
        if(!page.items.length&&!more){const empty=document.createElement('p');empty.textContent='此请求没有可显示的资料读取回执。';box.append(empty);}
        after=page.next_after;next.hidden=!page.has_more;box.append(next);
      }finally{loading=false;}
    };
    const next=document.createElement('button');next.textContent='更多来源';next.hidden=true;next.onclick=()=>load(true);
    button.onclick=()=>{box.hidden=!box.hidden;if(!box.hidden)load(false);};parent.append(button,box);
  }
  static taskDraft(candidate){
    const proposal={title:typeof candidate.title==='string'?candidate.title:'',description:typeof candidate.description==='string'?candidate.description:'',
      steps:Array.isArray(candidate.steps)?candidate.steps.filter(value=>typeof value==='string').slice(0,30):[]};
    // Keep review evidence in the existing editable/persisted description.
    // Content uncertainty is not the Runtime's execution "unknown" state.
    const review=[];
    if(candidate.needs_confirmation===true)review.push('待确认：请核对来源中的姓名、术语与任务内容后再执行。');
    for(const [key,label] of [['verbatim_quote','模型摘录（待核对）'],['interpretation','模型解释'],['reason','确认说明']]){
      if(typeof candidate[key]==='string'&&candidate[key].trim())review.push(label+'：'+candidate[key].trim());
    }
    if(review.length)proposal.description=[...review,proposal.description].filter(Boolean).join('\n\n');
    return proposal;
  }
  async createTask(request,selected){
    if(this.savingTask)return;this.savingTask=true;
    try{
    const version=this.version;
    const text=request.result?.text||'',candidate=selected||this.proposal(text);
    const sourceIds=candidate.source_ids??[...new Set((selected?JSON.stringify(selected):text).match(/\bsrc_[a-f0-9]{64}\b/g)||[])];
    if(!Array.isArray(sourceIds)||sourceIds.length>32||sourceIds.some(id=>typeof id!=='string'||!/^src_[a-f0-9]{64}$/.test(id))){this.notice('任务来源格式无效或超过 32 项，请选择单项任务建议。');return;}
    const proposal=RoomAgents.taskDraft(candidate);
    let title,description,steps;
    if(this.options.editTask){const edited=await this.options.editTask(request,proposal);if(!edited||version!==this.version)return;({title,description,steps}=edited);}
    else {
      title=global.prompt('任务标题',typeof proposal.title==='string'?proposal.title:'协作任务');if(!title)return;
      description=global.prompt('任务描述',typeof proposal.description==='string'?proposal.description:text);if(description===null)return;
      steps=global.prompt('建议步骤（每行一项）',Array.isArray(proposal.steps)?proposal.steps.filter(x=>typeof x==='string').join('\n'):'');if(steps===null)return;
    }
    if(await this.taskApi('create',{request_id:request.request_id,title,description,steps:steps.split('\n').filter(x=>x.trim()),source_ids:[...new Set(sourceIds)]}))await this.loadTasks();
    }finally{this.savingTask=false;}
  }
  async loadTasks(){
    const target=this.panel.querySelector('[data-room-tasks]');if(!target||!this.room)return;
    const taskLoad=this.taskLoad=(this.taskLoad||0)+1;
    const data=await this.taskApi('list',{});if(!data||taskLoad!==this.taskLoad)return;target.replaceChildren();
    for(const task of data.tasks||[]){
      const detail=document.createElement('details');detail.dataset.taskId=task.task_id;
      const title=document.createElement('summary');title.textContent=`${task.title} · ${this.options.status?.(task.status)||task.status} · ${task.assignee_user_id?'负责人 '+(this.options.name?.(task.assignee_user_id)||task.assignee_user_id):'待接手'}`;
      const body=document.createElement('pre');body.style.whiteSpace='pre-wrap';body.textContent=`${task.description}\n步骤：\n${task.steps.join('\n')}\n交接：${task.summary}\n进度由成员更新，不代表测试已通过。`;
      detail.append(title,body);
      if(task.source_ids?.length)this.sourcesButton(detail,task.request_id,task.source_ids);
      const claim=document.createElement('button');claim.textContent='接手任务';claim.onclick=async()=>{if(await this.taskApi('update',{task_id:task.task_id,revision:task.revision,claim:true,status:'running'}))await this.loadTasks();};detail.appendChild(claim);
      const update=document.createElement('button');update.textContent='更新进度 / 交接';update.onclick=async()=>{
        const version=this.version;let summary,status;
        if(this.options.editProgress){const edited=await this.options.editProgress(task);if(!edited||version!==this.version)return;({summary,status}=edited);}
        else {summary=global.prompt('交接摘要',task.summary);if(summary===null)return;status=global.prompt('状态：queued / running / completed / failed / cancelled / unknown',task.status);if(status===null)return;}
        if(await this.taskApi('update',{task_id:task.task_id,revision:task.revision,summary,status}))await this.loadTasks();
      };detail.appendChild(update);
      const history=document.createElement('pre');history.style.whiteSpace='pre-wrap';history.textContent='变更历史\n'+(task.history||[]).map(h=>`版本 ${h.revision} · ${this.options.name?.(h.last_actor_user_id)||('用户 '+h.last_actor_user_id)} · ${this.options.status?.(h.status)||h.status} · ${h.summary}`).join('\n');detail.appendChild(history);target.appendChild(detail);
    }
  }
  suggest(){
    this.choices.replaceChildren();if(!/@[^\s@]*$/.test(this.input.value))return;
    const prefix=this.input.value.match(/@([^\s@]*)$/)[1].toLowerCase();
    for(const agent of this.agents.filter(a=>a.enabled&&a.installed&&(a.name.toLowerCase().startsWith(prefix)||a.agent_id.startsWith(prefix)))){
      const button=document.createElement('button');button.textContent='@'+agent.name;button.dataset.agentId=agent.agent_id;
      button.onclick=()=>{this.selected.add(agent.agent_id);this.input.value=this.input.value.replace(/@[^\s@]*$/,'@'+agent.name+' ');this.choices.replaceChildren();this.renderSelected();this.input.focus();};
      this.choices.appendChild(button);
    }
  }
  renderSelected(){
    const list=this.options.selectedHost||this.panel.querySelector('[data-agent-selected]');list.replaceChildren();
    for(const id of this.selected){const button=document.createElement('button');button.textContent='@'+(this.agents.find(a=>a.agent_id===id)?.name||id)+' ×';button.onclick=()=>{this.selected.delete(id);this.renderSelected();};list.appendChild(button);}
  }
  mentions(){return [...this.selected].sort();}
  sent(){this.selected.clear();this.renderSelected();this.choices.replaceChildren();}
  identity(message){
    const sender=Number(message.sender_id||message.from_user_id||0);
    return this.agents.find(a=>a.agent_id===message.agent_id&&Number(a.bot_user_id)===sender);
  }
  delta(message){
    if(message.session_id!=='r_'+this.room||!this.identity(message)||this.completed.has(message.request_id))return;
    const index=message.delta_index;if(!Number.isSafeInteger(index)||index<0)return;
    let stream=this.streams.get(message.request_id);
    if(!stream){
      if(this.streams.size>=20){const key=this.streams.keys().next().value;this.streams.get(key).node.remove();this.streams.delete(key);}
      const node=this.append('',false,`${message.agent_id.includes('pi')?'π':'H'} · ${message.agent_name} · ${this.options.name?.(message.actor_user_id)||('发起人 '+message.actor_user_id)}`,false,'',{statusText:'正在回答'});
      node.dataset.agentRequest=message.request_id;node.dataset.sourceMessage=message.source_msg_id;
      stream={node,pieces:new Map()};this.streams.set(message.request_id,stream);
    }
    if(stream.pieces.has(index)||stream.pieces.size>=8192)return;
    stream.pieces.set(index,message.progress?'':String(message.delta||''));
    if(message.progress)stream.node.querySelector('.msg-status').textContent=message.delta;
    stream.node.querySelector('.msg-body').textContent=[...stream.pieces].sort((a,b)=>a[0]-b[0]).map(p=>p[1]).join('');
  }
  final(message){
    if(!this.identity(message)||message.session_id!=='r_'+this.room)return;
    if(this.completed.has(message.request_id))return;
    this.completed.add(message.request_id);if(this.completed.size>1000)this.completed.delete(this.completed.values().next().value);
    const stream=this.streams.get(message.request_id);if(stream)stream.node.remove();this.streams.delete(message.request_id);this.refresh();
  }
}
global.RoomAgents=RoomAgents;
})(typeof window==='undefined'?globalThis:window);
