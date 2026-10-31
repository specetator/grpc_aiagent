(function(global) {
  'use strict';
  class SpacePanel {
    constructor(root, api, user, changed, left) {
      this.root=root; this.api=api; this.user=user; this.changed=changed; this.left=left;
      this.room=0; this.role='';
      root.querySelector('[data-create-space]').onsubmit=e=>{e.preventDefault();this.run(async()=>{
        const form=e.currentTarget;
        const data=await api('/api/spaces/create',{name:form.elements.name.value,description:form.elements.description.value,visibility:form.elements.visibility.value});
        form.reset(); await changed(); this.notice('空间已创建，ID：'+data.room_id);
      });};
      root.querySelector('[data-invite-space]').onsubmit=e=>{e.preventDefault();this.run(async()=>{
        await api('/api/spaces/invite',{room_id:this.room,user_id:Number(e.currentTarget.elements.user_id.value)});
        this.notice('邀请已发出，等待对方接受。');
      });};
      root.querySelector('[data-refresh-spaces]').onclick=()=>this.run(async()=>{await changed();await this.refresh();});
      root.querySelector('[data-leave-space]').onclick=()=>this.run(async()=>{
        const room=this.room; await api('/api/spaces/leave',{room_id:room}); this.left(room); await changed();
      });
    }
    notice(text) { this.root.querySelector('[data-space-status]').textContent=text; }
    async run(fn) { try { await fn(); } catch(error) { this.notice(error.message); } }
    select(room,role) { this.room=Number(room)||0;this.role=role||'';return this.run(()=>this.refresh()); }
    async invitations() {
      const user=this.user();
      const data=await this.api('/api/spaces/invitations',{});
      if(user!==this.user()) return;
      const list=this.root.querySelector('[data-invitations]');list.replaceChildren();
      for(const invitation of data.invitations) {
        const item=document.createElement('div');item.textContent=invitation.name+' · '+invitation.room_id+' ';
        const button=document.createElement('button');button.type='button';button.textContent='接受邀请';
        button.onclick=()=>this.run(async()=>{await this.api('/api/spaces/accept',{room_id:invitation.room_id});await this.changed();await this.invitations();});
        item.append(button);list.append(item);
      }
      if(!data.invitations.length) list.textContent='暂无待接受邀请';
    }
    async refresh() {
      const room=this.room;
      this.root.querySelector('[data-space-members]').replaceChildren();
      this.root.querySelector('[data-invite-space]').hidden=!room || !['owner','admin'].includes(this.role);
      this.root.querySelector('[data-leave-space]').disabled=!room || this.role==='owner';
      if(!room) return;
      const data=await this.api('/api/spaces/members',{room_id:room});
      if(room!==this.room) return;
      // Roles used for display come from this authenticated response. Server
      // repeats every permission check for all mutations.
      this.role=data.members.find(m=>Number(m.user_id)===Number(this.user()))?.role || '';
      this.root.querySelector('[data-invite-space]').hidden=!['owner','admin'].includes(this.role);
      this.root.querySelector('[data-leave-space]').disabled=this.role==='owner';
      const list=this.root.querySelector('[data-space-members]');list.replaceChildren();
      for(const member of data.members) {
        const row=document.createElement('div');row.textContent=`${member.name || member.account} (${member.user_id}) · ${member.role} `;
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
