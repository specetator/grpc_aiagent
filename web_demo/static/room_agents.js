(function(global){
'use strict';
class RoomAgents {
  constructor(panel,input,choices,api,append){
    Object.assign(this,{panel,input,choices,api,append,room:0,version:0,agents:[],selected:new Set(),streams:new Map(),completed:new Set()});
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
    catch(error){if(version===this.version)this.notice(error.message);return null;}
  }
  async refresh(){
    if(!this.room)return;
    const data=await this.invoke('list',{});if(!data)return;
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
  }
  renderRequests(requests){
    const list=this.panel.querySelector('[data-agent-requests]');list.replaceChildren();
    for(const request of requests){
      if(request.agent_id==='cann')continue;
      const row=document.createElement('div');row.dataset.requestId=request.request_id;
      row.textContent=`${request.agent_name} · 发起人 ${request.actor_user_id} · ${request.status}`;
      if(request.result && request.result.text){const detail=document.createElement('pre');detail.style.whiteSpace='pre-wrap';detail.textContent=request.result.text;row.appendChild(detail);}
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
  async createTask(request){
    let proposal={};const text=request.result?.text||'';
    try{const raw=text.slice(text.indexOf('{'),text.lastIndexOf('}')+1);proposal=JSON.parse(raw);}catch(_){}
    const title=global.prompt('任务标题',typeof proposal.title==='string'?proposal.title:'协作任务');if(!title)return;
    const description=global.prompt('任务描述',typeof proposal.description==='string'?proposal.description:text);if(description===null)return;
    const steps=global.prompt('建议步骤（每行一项）',Array.isArray(proposal.steps)?proposal.steps.filter(x=>typeof x==='string').join('\n'):'');if(steps===null)return;
    if(await this.taskApi('create',{request_id:request.request_id,title,description,steps:steps.split('\n').filter(x=>x.trim())}))await this.loadTasks();
  }
  async loadTasks(){
    const target=this.panel.querySelector('[data-room-tasks]');if(!target||!this.room)return;
    if(!this.agents.length){target.replaceChildren();return;}
    const data=await this.taskApi('list',{});if(!data)return;target.replaceChildren();
    for(const task of data.tasks||[]){
      const detail=document.createElement('details');detail.dataset.taskId=task.task_id;
      const title=document.createElement('summary');title.textContent=`${task.title} · ${task.status} · ${task.assignee_user_id?'负责人 '+task.assignee_user_id:'待接手'}`;
      const body=document.createElement('pre');body.style.whiteSpace='pre-wrap';body.textContent=`${task.description}\n步骤：\n${task.steps.join('\n')}\n交接：${task.summary}\n进度由成员更新，不代表测试已通过。`;
      detail.append(title,body);
      const claim=document.createElement('button');claim.textContent='接手任务';claim.onclick=async()=>{if(await this.taskApi('update',{task_id:task.task_id,revision:task.revision,claim:true,status:'running'}))await this.loadTasks();};detail.appendChild(claim);
      const update=document.createElement('button');update.textContent='更新进度 / 交接';update.onclick=async()=>{
        const summary=global.prompt('交接摘要',task.summary);if(summary===null)return;
        const status=global.prompt('状态：queued / running / completed / failed / cancelled / unknown',task.status);if(status===null)return;
        if(await this.taskApi('update',{task_id:task.task_id,revision:task.revision,summary,status}))await this.loadTasks();
      };detail.appendChild(update);
      const history=document.createElement('pre');history.style.whiteSpace='pre-wrap';history.textContent='变更历史\n'+(task.history||[]).map(h=>`版本 ${h.revision} · 用户 ${h.last_actor_user_id} · ${h.status} · ${h.summary}`).join('\n');detail.appendChild(history);target.appendChild(detail);
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
    const list=this.panel.querySelector('[data-agent-selected]');list.replaceChildren();
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
      const node=this.append('',false,`${message.agent_id.includes('pi')?'π':'H'} · ${message.agent_name} · 发起人 ${message.actor_user_id}`,false,'',{statusText:'正在回答'});
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
    this.completed.add(message.request_id);if(this.completed.size>1000)this.completed.delete(this.completed.values().next().value);
    const stream=this.streams.get(message.request_id);if(stream)stream.node.remove();this.streams.delete(message.request_id);this.refresh();
  }
}
global.RoomAgents=RoomAgents;
})(typeof window==='undefined'?globalThis:window);
