/* Shared navigation and forms. Authentication, message delivery and cursors stay
 * in the existing chat client; all permissions stay in Logic. */
(function(global){
'use strict';
const statusLabel=value=>({queued:'排队中',running:'进行中',completed:'已完成',failed:'失败',cancelled:'已取消',unknown:'结果待核实',interrupted:'已中断'}[value]||value);
class SparkCollaborationUI {
  constructor(client){
    this.client=client;this.version=0;this.navigation=0;this.listVersion=0;this.spaces=[];this.members=[];this.room=0;
    const root=document.createElement('div');root.id='collaboration-ui';this.root=root;
    root.innerHTML=`
      <aside id="collaboration-panel" hidden aria-label="协作详情">
        <header><strong>协作详情</strong><button type="button" data-close-panel aria-label="关闭协作详情">×</button></header>
        <nav class="detail-tabs" aria-label="协作工具">
          <button data-tab="agents">Agent</button><button data-tab="tasks">任务</button><button data-tab="workspace">工作区</button><button data-tab="members">成员</button>
        </nav>
        <section id="room-agents">
          <div data-section="agents"><h3>房间 Agent</h3><p class="help">安装后，在消息框输入 @ 选择。普通讨论不会调用模型。</p><button data-agents-refresh>刷新执行状态</button><div data-agents-list></div><div data-agent-requests></div></div>
          <div data-section="tasks" hidden><h3>协作任务</h3><p class="help">在 Agent 已完成的回复中保存任务，成员可以接手并更新进度。</p><div data-room-tasks></div></div>
          <p data-agents-error class="form-error" role="status"></p>
        </section>
        <section data-section="workspace" hidden><h3>项目工作区</h3><p class="help">从管理员发布的项目中选择。公共 Agent 仅能读取授权代码。</p>
          <form data-workspace-form><label>授权项目<select name="project_id" aria-label="授权项目"></select></label><button type="submit">保存工作区</button></form>
          <p data-workspace-status role="status"></p>
        </section>
        <section data-section="members" hidden><h3>成员与权限</h3><div data-space-members></div>
          <form data-invite-space><label>邀请已有账号<input name="account" required autocomplete="off" placeholder="输入对方完整账号"></label><button type="submit">发送邀请</button></form>
          <p data-space-status role="status"></p><button data-refresh-spaces>刷新成员</button><button data-leave-space class="danger">退出空间</button>
        </section>
        <details id="room-agent-panel"><summary>CANN 算子协作</summary><p class="help">保留原有算子任务、授权开发与执行回执。</p><button data-action="refresh">刷新</button><button data-action="configure">配置任务</button><button data-action="checkpoint">更新交接</button><pre data-task></pre><pre data-streams></pre><p data-error role="status"></p></details>
      </aside>
      <dialog id="spaces-dialog" aria-labelledby="spaces-title"><header><h2 id="spaces-title">协作空间</h2><button data-close-dialog aria-label="关闭">×</button></header>
        <section><h3>创建空间</h3><form data-create-space><label>名称<input name="name" required maxlength="128" placeholder="例如：项目开发组"></label><label>描述<textarea name="description" maxlength="2000"></textarea></label><label>访问方式<select name="visibility"><option value="private">私有 · 邀请加入</option><option value="public">公开 · 登录用户可加入</option></select></label><button type="submit">创建空间</button></form></section>
        <section><h3>待接受的邀请</h3><div data-invitations></div></section>
        <section><h3>发现公开空间</h3><div data-public-spaces></div><button data-discover-more hidden>更多空间</button></section>
        <p data-directory-error class="form-error" role="status"></p>
      </dialog>
      <dialog id="collaboration-form" aria-labelledby="collaboration-form-title"><form><header><h2 id="collaboration-form-title"></h2><button type="button" data-close-dialog aria-label="关闭">×</button></header><div data-fields></div><footer><button type="button" data-cancel>取消</button><button type="submit">保存</button></footer></form></dialog>`;
    document.body.append(root);
    this.panel=root.querySelector('#collaboration-panel');
    this.directory=root.querySelector('#spaces-dialog');
    this.form=root.querySelector('#collaboration-form');
    root.querySelectorAll('[data-close-dialog]').forEach(button=>button.onclick=()=>button.closest('dialog').close());
    root.querySelector('[data-close-panel]').onclick=()=>this.togglePanel(false);
    root.querySelectorAll('[data-tab]').forEach(button=>button.onclick=()=>this.tab(button.dataset.tab));
    this.spacePanel=new SpacePanel(root,client.api,client.user,()=>this.refresh(),room=>client.invalidate(room),{
      accounts:true,notice:text=>{if(this.directory.open)this.root.querySelector('[data-directory-error]').textContent=text;},
      created:async data=>{this.directory.close();const space=this.spaces.find(s=>Number(s.room_id)===Number(data.room_id));if(space)await this.openRoom(space);},
      members:members=>{this.members=members;client.membersLoaded?.();}
    });
    const selected=document.createElement('div');selected.dataset.agentSelected='';selected.id='room-agent-selected';
    root.querySelector('#room-agents').append(selected);
    // Keep selected mentions beside the composer, while the details may be folded.
    document.getElementById('input-area').before(selected);
    const choices=document.createElement('div');choices.id='room-mention-choices';choices.setAttribute('aria-label','可用 Agent');selected.before(choices);
    const agentPanel=root.querySelector('#room-agents');
    this.agents=new RoomAgents(agentPanel,document.getElementById('msg-text'),choices,client.api,client.append,{
      selectedHost:selected,name:id=>this.memberName(id),status:statusLabel,loaded:client.agentsLoaded,
      editTask:(request,proposal)=>this.edit('保存协作任务',[
        {name:'title',label:'任务标题',value:proposal.title||'协作任务',required:true,max:255},
        {name:'description',label:'任务描述',value:proposal.description||request.result?.text||'',type:'textarea'},
        {name:'steps',label:'建议步骤（每行一项）',value:Array.isArray(proposal.steps)?proposal.steps.join('\n'):'',type:'textarea'}]),
      editProgress:task=>this.edit('更新进度与交接',[
        {name:'status',label:'任务状态',value:task.status,choices:['queued','running','completed','failed','cancelled','unknown'].map(value=>({value,label:statusLabel(value)}))},
        {name:'summary',label:'交接摘要',value:task.summary,type:'textarea'}])
    });
    this.cann=new RoomAgentChannel(root.querySelector('#room-agent-panel'),client.api);
    root.querySelector('[data-workspace-form]').onsubmit=e=>{e.preventDefault();this.saveWorkspace();};
    document.getElementById('manage-spaces-btn').onclick=()=>this.openDirectory();
    document.getElementById('collaboration-details-btn').onclick=()=>this.togglePanel();
    document.getElementById('sidebar-toggle').onclick=()=>document.body.classList.toggle('show-navigation');
    root.querySelector('[data-discover-more]').onclick=()=>this.discover(true);
    this.tab('agents');
  }
  memberName(id){return this.members.find(member=>Number(member.user_id)===Number(id))?.name||this.members.find(member=>Number(member.user_id)===Number(id))?.account||'成员';}
  reset(){
    this.version++;this.navigation++;this.listVersion++;this.room=0;this.spaces=[];this.members=[];
    this.spacePanel.select(0);this.agents.select(0);this.cann.select(0);this.togglePanel(false);
    this.directory.close();this.form.close();document.getElementById('space-list').replaceChildren();
    this.root.querySelector('[data-invitations]').replaceChildren();this.root.querySelector('[data-public-spaces]').replaceChildren();
    this.root.querySelector('[data-workspace-form]').reset();this.root.querySelector('[data-workspace-status]').textContent='';
  }
  async refresh(){
    if(!this.client.user())return;const version=++this.listVersion;
    try{
      const data=await this.client.api('/api/spaces/list',{});if(version!==this.listVersion)return;
      this.spaces=data.spaces||[];this.renderSpaces();
      const invitations=await this.client.api('/api/spaces/invitations',{});if(version!==this.listVersion)return;
      const count=(invitations.invitations||[]).length;
      document.getElementById('manage-spaces-btn').textContent=count?'创建 / 加入 · '+count+' 个邀请':'创建 / 加入';
    }catch(error){if(version===this.listVersion)this.client.notice(error.message);}
  }
  renderSpaces(){
    const host=document.getElementById('space-list');host.replaceChildren();
    for(const space of this.spaces){
      const button=document.createElement('button');button.type='button';button.className='session-item';button.dataset.sid=space.session_id;
      button.classList.toggle('active',space.session_id===this.client.active());
      const icon=document.createElement('span');icon.className='space-icon';icon.textContent=space.visibility==='private'?'◇':'#';
      const name=document.createElement('span');name.textContent=space.name;button.append(icon,name);
      button.onclick=()=>this.openRoom(space);host.append(button);
    }
    if(!this.spaces.length){const empty=document.createElement('p');empty.className='help';empty.textContent='创建一个空间，邀请队友开始讨论。';host.append(empty);}
  }
  async openRoom(space){
    const navigation=++this.navigation,access=this.client.access(space.room_id);
    try{
      const data=await this.client.api('/api/spaces/members',{room_id:Number(space.room_id)});
      if(navigation!==this.navigation||access!==this.client.access(space.room_id))return;
      this.members=data.members||[];
      const role=this.members.find(m=>Number(m.user_id)===Number(this.client.user()))?.role;
      if(!role)throw Object.assign(new Error('你已不是该空间的成员'),{code:403});
      this.client.open(0,space.session_id,{...space,kind:'room',role});
      document.body.classList.remove('show-navigation');
    }catch(error){if(navigation!==this.navigation)return;if(error.code===403||error.code===404)this.client.invalidate(space.room_id);this.client.notice(error.message);}
  }
  opened(conversation){
    const room=conversation?.kind==='room'?Number(conversation.room_id):0;this.room=room;this.version++;
    this.directory.close();this.form.close();
    document.getElementById('collaboration-details-btn').hidden=!room;
    if(!room){this.navigation++;this.members=[];this.togglePanel(false);}
    this.spacePanel.select(room,conversation?.role);this.agents.select(room);this.cann.select(room);
    if(room)this.loadWorkspace();
  }
  togglePanel(open){this.panel.hidden=open===undefined?!this.panel.hidden:!open;document.body.classList.toggle('details-open',!this.panel.hidden);}
  tab(name){this.root.querySelectorAll('[data-section]').forEach(section=>section.hidden=section.dataset.section!==name);this.root.querySelectorAll('[data-tab]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.tab===name)));}
  async openDirectory(){this.directory.showModal();this.root.querySelector('[data-directory-error]').textContent='';await this.spacePanel.run(()=>this.spacePanel.invitations());await this.discover(false);}
  async discover(more){
    const version=this.version;const host=this.root.querySelector('[data-public-spaces]');if(!more){host.replaceChildren();this.publicOffset=0;}
    try{
      const data=await this.client.api('/api/spaces/discover',{offset:this.publicOffset||0,limit:20});if(version!==this.version)return;
      for(const space of data.spaces||[]){const row=document.createElement('div');row.textContent=space.name+' ';const button=document.createElement('button');button.textContent='加入空间';
        button.onclick=async()=>{button.disabled=true;try{await this.client.api('/api/chatroom/join',{user_id:this.client.user(),room_id:space.room_id});await this.refresh();await this.openRoom(space);}catch(error){this.root.querySelector('[data-directory-error]').textContent=error.message;}finally{button.disabled=false;}};
        row.append(button);host.append(row);
      }
      this.publicOffset=(this.publicOffset||0)+(data.spaces||[]).length;this.root.querySelector('[data-discover-more]').hidden=(data.spaces||[]).length<20;
      if(!host.children.length)host.textContent='暂无公开空间，或直接创建私有空间。';
    }catch(error){if(version===this.version)this.root.querySelector('[data-directory-error]').textContent=error.message;}
  }
  async loadWorkspace(){
    const version=this.version,room=this.room;if(!room)return;
    const form=this.root.querySelector('[data-workspace-form]'),status=this.root.querySelector('[data-workspace-status]');
    form.hidden=true;status.textContent='正在读取工作区…';
    try{
      const data=await this.client.api('/api/chatroom/workspace/list',{room_id:room});if(version!==this.version)return;
      this.workspace=data.workspace;form.hidden=false;const select=form.elements.project_id;select.replaceChildren();
      for(const project of [{project_id:'',name:'未授权项目'},...(data.projects||[])]){const option=document.createElement('option');option.value=project.project_id;option.textContent=project.name;select.append(option);}
      select.value=data.workspace?.project_id||'';select.disabled=!data.can_manage;form.querySelector('button').disabled=!data.can_manage;
      status.textContent=data.can_manage?'只读授权。变更后，后续工具读取会重新校验。':'由房主管理项目授权。';
    }catch(error){if(version===this.version)status.textContent=error.message;}
  }
  async saveWorkspace(){
    const version=this.version,form=this.root.querySelector('[data-workspace-form]');
    try{await this.client.api('/api/chatroom/workspace/set',{room_id:this.room,project_id:form.elements.project_id.value,generation:this.workspace?.generation||0});if(version===this.version)await this.loadWorkspace();}
    catch(error){if(version===this.version)this.root.querySelector('[data-workspace-status]').textContent=error.message;}
  }
  edit(title,fields){
    if(this.form.open)this.form.close();const dialog=this.form,form=dialog.querySelector('form'),host=dialog.querySelector('[data-fields]');host.replaceChildren();dialog.querySelector('h2').textContent=title;
    for(const field of fields){const label=document.createElement('label');label.textContent=field.label;const input=document.createElement(field.choices?'select':field.type==='textarea'?'textarea':'input');input.name=field.name;input.required=!!field.required;if(field.max)input.maxLength=field.max;
      for(const choice of field.choices||[]){const option=document.createElement('option');option.value=choice.value;option.textContent=choice.label;input.append(option);}input.value=field.value||'';label.append(input);host.append(label);}
    return new Promise(resolve=>{let result=null;form.onsubmit=e=>{e.preventDefault();result=Object.fromEntries(new FormData(form));dialog.close();};dialog.querySelector('[data-cancel]').onclick=()=>dialog.close();dialog.onclose=()=>{dialog.onclose=null;resolve(result);};dialog.showModal();});
  }
}
global.SparkCollaborationUI=SparkCollaborationUI;
})(window);
