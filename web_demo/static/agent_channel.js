/* Spark IM channel presentation: consumes AgentEvent, never Pi RPC objects. */
(function (root) {
  'use strict';
  function validModel(model) {
    return model && typeof model.provider === 'string' && typeof model.id === 'string' &&
      typeof model.name === 'string' && model.provider && model.id && !model.provider.includes(':') &&
      new TextEncoder().encode(model.provider + ':' + model.id).length <= 128 &&
      !/[\s\x00-\x1f\x7f]/u.test(model.provider + model.id);
  }
  const levels = ['off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'];
  function validAgentContact(value) {
    return typeof value === 'string' && /^[1-9][0-9]{0,15}$/.test(value) && Number.isSafeInteger(Number(value));
  }
  const commands = Object.freeze({agent: '/agent', model: '/model', reasoning: '/reasoning', new: '/new', retry: '/retry',
    restart: '/restart', restart_confirm: '/restart now', status: '/status', help: '/help', new_confirm: '/new now', retry_confirm: '/retry now'});
  function commandForAction(action) {
    if (!action || typeof action.kind !== 'string') return null;
    if (action.kind === 'agent_set') return typeof action.value === 'string' && /^[a-z][a-z0-9_-]{0,47}$/.test(action.value) ? '/agent ' + action.value : null;
    if (action.kind === 'reasoning_set') return levels.includes(action.value) ? '/reasoning ' + action.value : null;
    if (Object.prototype.hasOwnProperty.call(commands, action.kind)) return commands[action.kind];
    if (action.kind === 'list_models') return '/model';
    if (action.kind === 'refresh_models') return '/model refresh';
    if (action.kind === 'reset_model') return '/model default';
    if (action.kind === 'select_model' && validModel({provider: action.provider, id: action.modelId, name: ''}))
      return '/model ' + action.provider + ':' + action.modelId;
    return null;
  }
  function actionLabel(action) {
    if (action && action.kind === 'agent_open' && validAgentContact(action.value)) return '打开 Agent 对话';
    if (!commandForAction(action)) return null;
    if (action.kind === 'agent_set') return '选择 Agent：' + action.value;
    if (action.kind === 'reasoning_set') return '选择思考深度：' + action.value;
    if (action.kind === 'select_model') return '选择模型：' + action.provider + ':' + action.modelId;
    return ({agent: '查看 Agent 列表', model: '查看模型列表', list_models: '查看模型列表', refresh_models: '刷新模型列表', reset_model: '恢复默认模型',
      reasoning: '查看思考深度', new: '查看新建上下文选项', retry: '查看重试选项',
      new_confirm: '确认新建上下文', retry_confirm: '确认重试上一条消息',
      restart: '查看重启选项', restart_confirm: '确认重启 Hermes technical', status: '查看会话状态', help: '打开命令菜单'})[action.kind] || null;
  }
  function displayAction(content) {
    if (!content || !content.agent_action) return null;
    const action = content.agent_action;
    // Metadata is display-only. It must exactly match the authenticated text
    // command; neither arbitrary display strings nor actions execute here.
    if (commandForAction(action) !== content.text) return null;
    return actionLabel(action);
  }
  function renderCommands(bubble, ui, sendAction) {
    if (typeof ui.title !== 'string' || ui.title.length > 320 || !Array.isArray(ui.actions) ||
        !ui.actions.length || ui.actions.length > 16 || !ui.actions.every(action => action &&
          typeof action.label === 'string' && action.label.length > 0 && action.label.length <= 160 &&
          (action.id === 'cancel' || (action.id === 'agent_open' && validAgentContact(action.value)) || commandForAction({kind: action.id, value: action.value})))) return false;
    const doc = bubble.ownerDocument;
    const card = doc.createElement('section');
    card.className = 'agent-command-card agent-model-picker';
    card.setAttribute('aria-label', ui.title);
    const title = doc.createElement('strong'); title.textContent = ui.title; card.appendChild(title);
    const actions = doc.createElement('div'); actions.className = 'agent-command-actions'; card.appendChild(actions);
    const status = doc.createElement('div'); status.className = 'agent-model-status';
    status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite'); card.appendChild(status);
    let pending = false;
    for (const action of ui.actions) {
      const button = doc.createElement('button'); button.type = 'button';
      button.textContent = (action.selected === true ? '✓ ' : '') + action.label;
      button.disabled = action.selected === true;
      button.setAttribute('aria-pressed', String(action.selected === true));
      if (action.selected === true) button.className = 'selected';
      button.onclick = () => {
        if (pending) return;
        if (action.id === 'cancel') { actions.remove(); status.textContent = '已取消，当前上下文未改变。'; return; }
        if (sendAction({kind: action.id, value: action.value, label: action.label}) !== true) {
          status.textContent = '未发送，请检查连接并回到此会话后重试。'; return;
        }
        pending = true;
        for (const item of actions.querySelectorAll('button')) item.disabled = true;
        status.textContent = (actionLabel({kind: action.id, value: action.value}) || action.label) + ' · 等待确认';
      };
      actions.appendChild(button);
    }
    const old = bubble.querySelector('.agent-model-picker'); if (old) old.remove();
    bubble.appendChild(card); return true;
  }
  function render(bubble, event, sendAction) {
    if (!bubble || !event || event.schema !== 'sparkpush.agent_event.v1' ||
        event.type !== 'assistant_final' || !event.data) return false;
    const ui = event.data.presentation;
    if (ui && ui.kind === 'command_card') return renderCommands(bubble, ui, sendAction);
    if (!ui || ui.kind !== 'model_picker' || !Array.isArray(ui.models) || ui.models.length > 512 ||
        !validModel(ui.current) || !ui.models.every(validModel)) return false;
    const doc = bubble.ownerDocument;
    const make = (tag, cls, text) => {
      const element = doc.createElement(tag);
      if (cls) element.className = cls;
      if (text !== undefined) element.textContent = text;
      return element;
    };
    const old = bubble.querySelector('.agent-model-picker');
    if (old) old.remove();
    const card = make('section', 'agent-model-picker');
    card.setAttribute('aria-label', '选择 Agent 模型');
    const title = make('strong', '', '选择模型');
    card.appendChild(title);
    const current = ui.current.provider + ':' + ui.current.id;
    card.appendChild(make('div', 'agent-model-current', '当前 · ' + current));
    const search = make('input', 'agent-model-search');
    search.type = 'search'; search.placeholder = '搜索模型或服务商';
    search.setAttribute('aria-label', '搜索模型或服务商');
    card.appendChild(search);
    const list = make('div', 'agent-model-options');
    card.appendChild(list);
    const status = make('div', 'agent-model-status');
    status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
    const navigation = make('div', 'agent-model-navigation');
    const previous = make('button', '', '上一页');
    const pageLabel = make('span');
    const next = make('button', '', '下一页');
    for (const node of [previous, pageLabel, next]) navigation.appendChild(node);
    card.appendChild(navigation);
    const actions = make('div', 'agent-model-actions');
    const reset = make('button', '', '恢复默认');
    const refresh = make('button', '', '刷新列表');
    actions.appendChild(reset); actions.appendChild(refresh); card.appendChild(actions);
    card.appendChild(status);
    let page = 0, pending = false;
    const again = make('button', '', '重新选择模型');
    again.type = 'button'; again.hidden = true; card.appendChild(again);
    again.onclick = () => sendAction({kind: 'list_models'});
    const submit = action => {
      if (pending && !['list_models', 'refresh_models'].includes(action.kind)) return;
      if (sendAction(action) !== true) {
        status.textContent = '未发送，请检查连接并回到此会话后重试。';
        return;
      }
      if (['select_model', 'reset_model'].includes(action.kind)) {
        card.applyModelState({pending: true});
      } else {
        status.textContent = '正在刷新模型列表…';
      }
    };
    function draw() {
      const query = search.value.trim().toLocaleLowerCase();
      const matches = ui.models.filter(model =>
        (model.provider + ' ' + model.id + ' ' + model.name).toLocaleLowerCase().includes(query));
      const pages = Math.max(1, Math.ceil(matches.length / 8));
      page = Math.min(page, pages - 1);
      list.replaceChildren();
      for (const model of matches.slice(page * 8, page * 8 + 8)) {
        const selected = model.provider + ':' + model.id === current;
        const button = make('button', 'agent-model-option' + (selected ? ' selected' : ''));
        button.type = 'button'; button.disabled = pending || selected;
        button.setAttribute('aria-pressed', String(selected));
        button.appendChild(make('span', 'agent-model-name', (selected ? '✓ ' : '') + model.name));
        button.appendChild(make('small', '', model.provider + ' · ' + model.id));
        button.onclick = () => submit({kind: 'select_model', provider: model.provider, modelId: model.id});
        list.appendChild(button);
      }
      if (!matches.length) list.appendChild(make('div', 'agent-model-empty', '没有匹配的可用模型'));
      previous.disabled = page === 0; next.disabled = page + 1 >= pages;
      pageLabel.textContent = (page + 1) + ' / ' + pages + ' · ' + matches.length + ' 个';
    }
    card.applyModelState = state => {
      pending = state.pending === true;
      const success = state.ok === true && validModel(state.model);
      title.textContent = pending ? '正在切换模型…' : success ? '模型修改成功' : '模型切换失败';
      search.hidden = list.hidden = navigation.hidden = pending || success;
      actions.hidden = success;
      again.hidden = !success;
      reset.disabled = pending;
      if (success) {
        for (const node of [...bubble.childNodes]) if (node !== card) node.remove();
        card.querySelector('.agent-model-current').textContent = '已切换为 · ' + state.model.id;
        status.textContent = '✓ 已保存，此对话的后续消息将使用该模型。';
      } else {
        status.textContent = pending ? '正在等待服务端确认，请稍候。' : '未能确认修改成功，请查看回复原因或刷新列表后重试。';
      }
      draw();
    };
    search.oninput = () => { page = 0; draw(); };
    previous.onclick = () => { --page; draw(); };
    next.onclick = () => { ++page; draw(); };
    reset.onclick = () => submit({kind: 'reset_model'});
    refresh.onclick = () => submit({kind: 'refresh_models'});
    for (const button of [reset, refresh, previous, next]) button.type = 'button';
    if (ui.truncated) status.textContent = '列表较长，仅显示部分模型；也可输入 /model provider:model。';
    draw(); bubble.appendChild(card);
    return true;
  }
  const api = {render, validModel, validAgentContact, commandForAction, actionLabel, displayAction};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.SparkAgentChannel = api;
})(typeof window !== 'undefined' ? window : globalThis);
