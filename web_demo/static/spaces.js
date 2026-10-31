(function(global) {
  'use strict';
  class SpacePanel {
    constructor(root, api, user, changed, left, options={}) {
      this.root=root; this.api=api; this.user=user; this.changed=changed; this.left=left;
      this.room=0; this.role='';this.version=0;this.options=options;
      root.querySelector('[data-create-space]')?.addEventListener('submit',e=>{e.preventDefault();this.run(async()=>{
        const version=this.version;
        const form=e.currentTarget;
        const data=await api('/api/spaces/create',{name:form.elements.name.value,description:form.elements.description.value,visibility:form.elements.visibility.value});
        if(version!==this.version)return;
        form.reset(); await changed(); this.notice('空间已创建');await options.created?.(data);
      });});
      root.querySelector('[data-invite-space]').onsubmit=e=>{e.preventDefault();this.run(async()=>{
        const room=this.room,version=this.version,form=e.currentTarget;
        const target=options.accounts?await api('/api/users/lookup',{account:form.elements.account.value}):{user_id:Number(form.elements.user_id.value)};
        if(version!==this.version)return;
        await api('/api/spaces/invite',{room_id:room,user_id:target.user_id});
        if(version!==this.version)return;form.reset();
        this.notice('邀请已发出，等待对方接受。');
      });};
      root.querySelector('[data-refresh-spaces]').onclick=()=>this.run(async()=>{await changed();await this.refresh();});
      root.querySelector('[data-leave-space]').onclick=()=>this.run(async()=>{
        const room=this.room; await api('/api/spaces/leave',{room_id:room}); this.left(room); await changed();
      });
    }
    notice(text) { this.root.querySelector('[data-space-status]').textContent=text;this.options.notice?.(text); }
    async run(fn) { const version=this.version;try { await fn(); } catch(error) { if(version===this.version)this.notice(error.message); } }
    select(room,role) { this.version++;this.room=Number(room)||0;this.role=role||'';this.notice('');return this.run(()=>this.refresh()); }
    async invitations() {
      const user=this.user();
      const data=await this.api('/api/spaces/invitations',{});
      if(user!==this.user()) return;
      const list=this.root.querySelector('[data-invitations]');list.replaceChildren();
      for(const invitation of data.invitations) {
        const item=document.createElement('div');item.textContent=invitation.name+' ';
        const button=document.createElement('button');button.type='button';button.textContent='接受邀请';
        button.onclick=()=>this.run(async()=>{await this.api('/api/spaces/accept',{room_id:invitation.room_id});await this.changed();await this.invitations();});
        item.append(button);list.append(item);
      }
      if(!data.invitations.length) list.textContent='暂无待接受邀请';
    }
    async refresh() {
      const room=this.room,version=this.version;
      this.root.querySelector('[data-space-members]').replaceChildren();
      this.root.querySelector('[data-invite-space]').hidden=!room || !['owner','admin'].includes(this.role);
      this.root.querySelector('[data-leave-space]').disabled=!room || this.role==='owner';
      if(!room) return;
      const data=await this.api('/api/spaces/members',{room_id:room});
      if(room!==this.room || version!==this.version) return;
      this.options.members?.(data.members);
      // Roles used for display come from this authenticated response. Server
      // repeats every permission check for all mutations.
      this.role=data.members.find(m=>Number(m.user_id)===Number(this.user()))?.role || '';
      this.root.querySelector('[data-invite-space]').hidden=!['owner','admin'].includes(this.role);
      this.root.querySelector('[data-leave-space]').disabled=this.role==='owner';
      const list=this.root.querySelector('[data-space-members]');list.replaceChildren();
      for(const member of data.members) {
        const row=document.createElement('div');row.textContent=`${member.name || member.account} · ${{owner:'房主',admin:'管理员',member:'成员'}[member.role] || member.role} `;
        if(this.role==='owner' && member.role!=='owner') {
          const select=document.createElement('select');select.setAttribute('aria-label','成员角色');
          for(const role of ['member','admin']) { const option=document.createElement('option');option.value=role;option.textContent=role;select.append(option); }
          select.value=member.role;select.onchange=()=>this.run(async()=>{await this.api('/api/spaces/role',{room_id:room,user_id:member.user_id,role:select.value});await this.refresh();});row.append(select);
        }
        if(Number(member.user_id)!==Number(this.user()) && member.role!=='owner' &&
           (this.role==='owner' || (this.role==='admin' && member.role==='member'))) {
          const button=document.createElement('button');button.type='button';button.textContent='移除';
          button.onclick=()=>this.run(async()=>{await this.api('/api/spaces/remove',{room_id:room,user_id:member.user_id});await this.refresh();});row.append(button);
        }
        list.append(row);
      }
    }
  }
  global.SpacePanel=SpacePanel;
})(typeof window==='undefined'?globalThis:window);
