/* User-owned Agent connections. Secrets stay in this form until submission. */
(function (root) {
  'use strict';
  const TYPES = {dsh:'DeepSeek Harness', codex:'Codex', claude_code:'Claude Code', hermes:'Hermes', openclaw:'OpenClaw'};
  function mount({button, getToken, apiBase, openChat}) {
    const dialog = document.createElement('dialog');
    dialog.className = 'agent-connections-dialog';
    dialog.setAttribute('aria-labelledby', 'agent-connections-title');
    // This template is static. All API-controlled values use textContent/value.
    dialog.innerHTML = `<div class="agent-connections-header"><div><h2 id="agent-connections-title">我的 Agent</h2><p>连接你自己的 Agent，在这里继续对话。</p></div><button type="button" data-close aria-label="关闭">×</button></div>
      <div data-connections></div>
      <form><h3>添加或更新连接</h3><label>Agent 类型<select name="runtime"><option value="auto">自动识别</option></select></label>
      <label>Connector 地址<input name="endpoint" type="url" placeholder="https://agent.example.com" required maxlength="2048"></label>
      <label>连接密钥<input name="token" type="password" autocomplete="off" maxlength="4096" placeholder="填写 Connector 的连接密钥"></label>
      <p class="agent-connections-hint">先在 Agent 所在设备启动 Spark Agent Connector，再填写服务可访问的地址。每种 Agent 可保存一个连接。</p>
      <div class="agent-connections-actions"><button type="button" data-probe>检测连接</button><button type="submit" class="btn-primary">验证并保存</button></div></form>
      <p data-status role="status" aria-live="polite"></p>`;
    document.body.appendChild(dialog);
    const form = dialog.querySelector('form'), status = dialog.querySelector('[data-status]');
    const list = dialog.querySelector('[data-connections]');
    const type = form.elements.runtime, endpoint = form.elements.endpoint, key = form.elements.token;
    let connections = [], busy = false, generation = 0;
    for (const [value, label] of Object.entries(TYPES)) {
      const option = document.createElement('option'); option.value=value; option.textContent=label; type.appendChild(option);
    }
    async function request(body) {
      const token = getToken();
      if (!token) throw new Error('请先登录');
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 12000);
      try {
        const resp = await fetch(apiBase + '/api/agent/connections', {method:'POST', signal:controller.signal,
          headers:{'Content-Type':'application/json', Authorization:'Bearer '+token}, body:JSON.stringify(body)});
        const data = await resp.json();
        if (getToken() !== token) throw new Error('登录状态已改变，请重新打开配置');
        if (!resp.ok || data.code !== 0) throw new Error(data.message || '连接服务不可用');
        return data.data;
      } finally { clearTimeout(timer); }
    }
    function pending(value) {
      busy=value;
      for (const el of dialog.querySelectorAll('input, select, button:not([data-close])')) el.disabled=value;
    }
    async function run(task) {
      if (busy) return;
      const turn=generation;
      pending(true); status.textContent='正在处理…';
      try { await task(); }
      catch (error) { if (turn===generation) status.textContent=error.name==='AbortError' ? '连接超时，请重试' : error.message; }
      finally { pending(false); }
    }
    function edit(row) {
      type.value=row.runtime; endpoint.value=row.endpoint; key.value='';
      key.placeholder=row.has_key ? '留空保留已保存的密钥（地址不变时）' : '填写 Connector 的连接密钥';
    }
    function render() {
      list.replaceChildren();
      for (const row of connections) {
        const item=document.createElement('div'); item.className='agent-connection-row';
        const title=document.createElement('strong'); title.textContent=TYPES[row.runtime] || row.runtime;
        const detail=document.createElement('span'); detail.textContent=row.configured ? '已配置' : '未连接';
        const editBtn=document.createElement('button'); editBtn.type='button'; editBtn.textContent=row.configured ? '编辑' : '连接'; editBtn.onclick=()=>edit(row);
        item.append(title, detail, editBtn);
        if (row.configured) {
          const chat=document.createElement('button'); chat.type='button'; chat.textContent='打开对话';
          chat.onclick=()=>{ openChat(row.bot_user_id, '我的 '+TYPES[row.runtime]); dialog.close(); };
          const remove=document.createElement('button'); remove.type='button'; remove.textContent='断开';
          remove.onclick=()=>run(async()=>{
            if (!confirm('断开此 Agent？已有聊天记录保留。')) { status.textContent=''; return; }
            await request({action:'delete', runtime:row.runtime, revision:row.revision});
            await refresh(); key.value=''; status.textContent='已断开连接，聊天记录保留。';
          });
          item.append(chat, remove);
        }
        list.appendChild(item);
      }
    }
    async function refresh() {
      const data=await request({action:'list'}); connections=data.connections; render();
    }
    function payload(action) {
      const old=connections.find(row=>row.runtime===type.value);
      return {action, runtime:type.value, endpoint:endpoint.value.trim(), token:key.value, revision:old?.revision || ''};
    }
    dialog.querySelector('[data-probe]').onclick=()=>run(async()=>{
      if (!form.reportValidity()) { status.textContent='请填写有效的连接地址。'; return; }
      const result=await request(payload('probe'));
      type.value=result.runtime;
      status.textContent='已识别 '+result.name+'，连接可用。检测不会发送模型请求。';
    });
    form.onsubmit=event=>{
      event.preventDefault();
      run(async()=>{
        const result=await request(payload('save'));
        key.value=''; type.value=result.runtime;
        await refresh();
        status.textContent='已保存 '+TYPES[result.runtime]+'，可以打开对话。';
      });
    };
    type.onchange=()=>{ const row=connections.find(row=>row.runtime===type.value); if (row) edit(row); };
    button.onclick=()=>{
      if (!getToken() || busy) return;
      generation++; connections=[]; list.replaceChildren(); form.reset(); key.value='';
      dialog.showModal(); run(async()=>{await refresh(); status.textContent='请选择 Agent，或填写地址自动识别。';});
    };
    dialog.querySelector('[data-close]').onclick=()=>{ key.value=''; dialog.close(); };
    dialog.addEventListener('close', ()=>{ key.value=''; });
    return {dialog};
  }
  root.SparkAgentConnections={mount};
})(window);
