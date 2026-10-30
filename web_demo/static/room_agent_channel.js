(function (global) {
  'use strict';
  class RoomAgentChannel {
    constructor(panel, api) {
      this.panel = panel; this.api = api; this.room = 0; this.task = null;
      this.streams = new Map();
      this.completed = new Set();
      panel.querySelector('[data-action=refresh]').onclick = () => this.refresh();
      panel.querySelector('[data-action=configure]').onclick = () => this.configure();
      panel.querySelector('[data-action=checkpoint]').onclick = () => this.checkpoint();
    }
    select(room) {
      this.room = Number(room); this.task = null; this.streams.clear(); this.completed.clear(); this.renderStreams();
      this.panel.querySelector('[data-task]').textContent = '加载共享任务…';
      return this.refresh();
    }
    async invoke(path, body) {
      try { return await this.api(path, body); }
      catch (error) { if (body.room_id === this.room) this.panel.querySelector('[data-error]').textContent = error.message; return null; }
    }
    async refresh() {
      if (!this.room) return;
      const room = this.room;
      const data = await this.invoke('/api/chatroom/agent/task', {room_id:room});
      if (!data || room !== this.room) return;
      this.task = data.task;
      this.panel.querySelector('[data-error]').textContent = '';
      const task = data.task;
      const requests = data.requests || [];
      this.panel.querySelector('[data-task]').textContent = task ?
        `${task.title} · ${task.status} · 版本 ${task.revision}\n规格：${task.spec}\n交接：${task.summary || '暂无'}\n下一步：${task.next_steps || '暂无'}\n权限：${data.can_develop ? '可开发' : '只读提问'}\n` +
        requests.map(r => `用户 ${r.actor_user_id} · ${r.status}` +
          (r.result && r.result.workspace_checkpoint ? ` · 代码 ${r.result.workspace_checkpoint.code_revision.slice(0,12)} · 测试 ${r.result.workspace_checkpoint.tests.map(t=>t.passed?'通过':'未通过').join(',') || '未执行'}` : '')).join('\n') :
        '房间尚未绑定算子任务。房主可以创建任务；成员默认只读，开发权限需房主授予。';
    }
    async configure() {
      if (!this.room) return;
      const room = this.room;
      const input = {room_id:room};
      if (this.task) input.revision = this.task.revision;
      else {
        input.title = global.prompt('算子任务名称'); if (!input.title) return;
        input.spec = global.prompt('规格（shape/dtype、CANN 版本、目标设备、精度要求）') || '';
      }
      const ids = global.prompt('获授权开发成员的用户 ID，用英文逗号分隔；空表示仅房主可开发',
        this.task ? this.task.developer_user_ids.join(',') : '');
      if (ids === null) return;
      input.developer_user_ids = ids.trim() ? ids.split(',').map(x => Number(x.trim())) : [];
      if (input.developer_user_ids.some(x => !Number.isSafeInteger(x) || x<=0)) {
        this.panel.querySelector('[data-error]').textContent = '用户 ID 必须为正整数'; return;
      }
      if (await this.invoke('/api/chatroom/agent/configure', input)) await this.refresh();
    }
    async checkpoint() {
      if (!this.task) return;
      const room = this.room, revision = this.task.revision;
      const summary = global.prompt('交接摘要（人工进度说明，不代表测试通过）', this.task.summary);
      if (summary === null) return;
      const next = global.prompt('下一步', this.task.next_steps); if (next === null) return;
      if (await this.invoke('/api/chatroom/agent/checkpoint',
          {room_id:room, revision, patch:{summary, next_steps:next}})) await this.refresh();
    }
    delta(message) {
      if (message.session_id !== 'r_' + this.room) return;
      if (this.completed.has(message.request_id)) return;
      const index = message.delta_index;
      if (!Number.isSafeInteger(index) || index < 0 || !message.request_id) return;
      let stream = this.streams.get(message.request_id);
      if (!stream) {
        if (this.streams.size >= 20) this.streams.delete(this.streams.keys().next().value);
        stream = {pieces:new Map(), progress:'', actor:message.actor_user_id};
        this.streams.set(message.request_id, stream);
      }
      if (stream.pieces.has(index)) return;
      stream.pieces.set(index, message.progress ? '' : String(message.delta || ''));
      if (message.progress) stream.progress = String(message.delta || '');
      // Bound ephemeral previews; final persisted answer remains authoritative.
      if (stream.pieces.size > 8192) stream.pieces.delete(stream.pieces.keys().next().value);
      this.renderStreams();
    }
    final(requestId) {
      this.completed.add(requestId);
      if (this.completed.size > 1000) this.completed.delete(this.completed.values().next().value);
      this.streams.delete(requestId); this.renderStreams(); this.refresh();
    }
    renderStreams() {
      this.panel.querySelector('[data-streams]').textContent = Array.from(this.streams.values()).map(s =>
        `Kernel Agent → 用户 ${s.actor}\n${s.progress}\n` + Array.from(s.pieces.entries()).sort((a,b)=>a[0]-b[0]).map(x=>x[1]).join('')).join('\n\n');
    }
  }
  global.RoomAgentChannel = RoomAgentChannel;
})(typeof window === 'undefined' ? globalThis : window);
