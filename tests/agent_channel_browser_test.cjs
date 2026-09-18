// Optional real-browser test. Install Playwright outside the project and set
// SPARK_PLAYWRIGHT_MODULE to its module directory; all page traffic is mocked.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const {chromium} = require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage({viewport: {width: 1150, height: 900}});
    const errors = [];
    let historyData = [];
    page.on('pageerror', error => errors.push(error.message));
    const assets = path.resolve(__dirname, '../web_demo/static');
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      const name = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      if (url.origin === 'http://spark.test' && ['index.html', 'auth.js', 'auth_style.css', 'safe_markdown.js', 'agent_channel.js', 'agent_channel.css'].includes(name)) {
        await route.fulfill({body: await fs.readFile(path.join(assets, name)),
          contentType: name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' : 'text/html'});
      } else if (url.pathname === '/api/session/history') {
        await route.fulfill({status: 200, json: {code: 0, data: {messages: historyData}}});
      } else await route.fulfill({status: 200, json: {code: 404, message: 'isolated browser fixture'}});
    });
    await page.goto('http://spark.test/');
    await page.evaluate(() => {
      currentUserId = 7; currentPeerId = hermesBotUserId;
      currentSessionId = 's_7_' + hermesBotUserId;
      historyReady = true;
      window.sentActions = [];
      ws = {readyState: WebSocket.OPEN, bufferedAmount: 0,
        send: payload => window.sentActions.push(JSON.parse(payload))};
      updateUIState();
      document.getElementById('empty-state').classList.add('hidden');
      document.getElementById('chat-window').classList.remove('hidden');
      document.getElementById('msg-text').value = '保留我的草稿';
      const models = Array.from({length: 10}, (_, i) => ({provider: 'demo', id: 'model-' + i,
        name: i === 3 ? '<img src=x onerror=alert(1)>' : '模型 ' + i}));
      window.fixtureMessage = {session_id: currentSessionId, from_user_id: hermesBotUserId,
        client_msg_id: 'hermes:request-1', msg_id: currentSessionId + '-2', msg_seq: 2,
        content: {text: '当前模型：demo:model-0', agent_event: {schema: 'sparkpush.agent_event.v1',
          type: 'assistant_final', data: {text: '当前模型：demo:model-0',
            presentation: {kind: 'model_picker', current: models[0], models}}}}};
      renderSingleChatMessage(window.fixtureMessage);
    });
    assert.equal(await page.locator('.agent-model-option').count(), 8);
    assert.equal(await page.locator('.agent-model-option.selected').count(), 1);
    assert.equal(await page.locator('.agent-model-picker img').count(), 0, 'model label executed as HTML');
    await page.getByRole('button', {name: '下一页'}).click();
    assert.equal(await page.locator('.agent-model-option').count(), 2);
    await page.getByRole('searchbox', {name: '搜索模型或服务商'}).fill('model-9');
    assert.equal(await page.locator('.agent-model-option').count(), 1);
    await page.locator('.agent-model-option').click();
    assert.equal(await page.evaluate(() => sentActions[0].content.text), '/model demo:model-9');
    assert.equal(await page.evaluate(() => sentActions[0].content.agent_action.kind), 'select_model');
    assert.equal(await page.evaluate(() => getMsgText(sentActions[0])), '选择模型：demo:model-9');
    assert.equal(await page.locator('.msg-bubble').filter({hasText: /^\/model demo:model-9$/}).count(), 0);
    await page.evaluate(() => {
      const echo = {...sentActions[0], from_user_id: currentUserId, session_id: currentSessionId, msg_id: 'echo', msg_seq: 3};
      renderSingleChatMessage(echo);
      const historical = {content: {content: echo.content}};
      if (getMsgText(historical) !== '选择模型：demo:model-9') throw new Error('history action label lost');
      if (SparkAgentChannel.displayAction({text:'/new now', agent_action:{kind:'reasoning_set',value:'high'}}) !== null)
        throw new Error('mismatching metadata hid actual command');
    });
    assert.equal(await page.locator('#msg-text').inputValue(), '保留我的草稿');
    assert.match(await page.locator('.agent-model-current').innerText(), /demo:model-0/, 'selection shown before confirmation');
    assert.equal(await page.locator('.agent-model-option').isDisabled(), true);
    assert.equal(await page.locator('.agent-model-options').isVisible(), false);
    assert.match(await page.locator('.agent-model-picker strong').innerText(), /正在切换/);
    await page.getByRole('button', {name: '刷新列表'}).click();
    assert.equal(await page.evaluate(() => sentActions[1].content.text), '/model refresh');
    await page.evaluate(() => {
      renderSingleChatMessage({session_id:currentSessionId,from_user_id:hermesBotUserId,
        client_msg_id:'hermes:failed-model-action',msg_id:'failed-model-receipt',msg_seq:4,
        content:{text:'模型暂不可用',agent_command_result:{command:'model',
          client_msg_id:sentActions[0].client_msg_id,ok:false}}});
    });
    assert.equal(await page.locator('.agent-model-options').isVisible(), true);
    assert.match(await page.locator('.agent-model-picker strong').innerText(), /切换失败/);
    // Duplicate and historical messages use the same renderer and remain one card.
    await page.evaluate(() => renderSingleChatMessage(fixtureMessage));
    assert.equal(await page.locator('.agent-model-picker').count(), 1);
    await page.getByRole('button', {name: '恢复默认'}).click();
    assert.equal(await page.evaluate(() => sentActions[2].content.text), '/model default');
    await page.evaluate(() => { renderSingleChatMessage(fixtureMessage); currentSessionId = 'another-session'; });
    await page.getByRole('button', {name: '刷新列表'}).click();
    assert.equal(await page.evaluate(() => sentActions.length), 3, 'old card sent to another session');
    await page.evaluate(() => {
      currentSessionId = fixtureMessage.session_id;
      handleWebSocketMessage({type:'error',session_id:currentSessionId,client_msg_id:sentActions[2].client_msg_id,message:'fixture rejected'});
      renderSingleChatMessage(fixtureMessage);
      ws.readyState = WebSocket.CLOSED;
    });
    await page.locator('.agent-model-option').nth(1).click();
    assert.equal(await page.evaluate(() => sentActions.length), 3, 'disconnected action sent');
    assert.equal(await page.locator('.agent-model-option').nth(1).isDisabled(), false);
    await page.evaluate(() => {
      const fake = {...fixtureMessage, from_user_id: 123, client_msg_id: 'hermes:spoof', msg_id: 'spoof'};
      renderSingleChatMessage(fake);
    });
    assert.equal(await page.locator('.agent-model-picker').count(), 1, 'human message forged a model control card');
    historyData = [await page.evaluate(() => ({msg_id: fixtureMessage.msg_id,
      client_msg_id: fixtureMessage.client_msg_id, msg_seq: 2, sender_id: hermesBotUserId,
      timestamp_ms: 1750000000000, msg_type: 'text',
      content: {session_id: currentSessionId, content: fixtureMessage.content}}))];
    await page.evaluate(async () => {
      renderedMessageRows.clear(); document.getElementById('messages').replaceChildren();
      ws.readyState = WebSocket.OPEN;
      historyLoading = false; historyAnchorSeq = 0; historyHasMore = true;
      await loadHistory(true);
    });
    assert.equal(await page.locator('.agent-model-picker').count(), 1, 'history card did not render');
    await page.getByRole('button', {name: '刷新列表'}).click();
    assert.equal(await page.evaluate(() => sentActions[3].content.text), '/model refresh', 'history card not bound to query session');
    // All common commands use the same authenticated WS send path, including history cards.
    await page.evaluate(() => {
      window.showCommandFixture = (actions, title = '常用命令') => {
        const message = structuredClone(fixtureMessage);
        message.msg_id = 'command-card'; message.client_msg_id = 'hermes:command-card'; message.msg_seq = 10;
        message.content.agent_event.data.presentation = {kind: 'command_card', title, actions};
        renderSingleChatMessage(message);
      };
      showCommandFixture([{id: 'reasoning_set', label: 'max', value: 'max'}, {id: 'new', label: '新建上下文'}]);
    });
    await page.locator('.agent-command-card button').first().click();
    assert.equal(await page.evaluate(() => sentActions.at(-1).content.text), '/reasoning max');
    assert.equal(await page.evaluate(() => getMsgText(sentActions.at(-1))), '选择思考深度：max');
    assert.equal(await page.locator('.agent-command-card button').last().isDisabled(), true);
    await page.evaluate(() => showCommandFixture([{id: 'new_confirm', label: '确认新建'}, {id: 'cancel', label: '取消'}]));
    const countBeforeCancel = await page.evaluate(() => sentActions.length);
    await page.locator('.agent-command-card').getByRole('button', {name: '取消'}).click();
    assert.equal(await page.evaluate(() => sentActions.length), countBeforeCancel);
    await page.evaluate(() => showCommandFixture([{id: 'agent_set', label: 'Hermes · technical', value: 'hermes-technical'}]));
    await page.getByRole('button', {name: 'Hermes · technical', exact: true}).click();
    assert.equal(await page.evaluate(() => sentActions.at(-1).content.text), '/agent hermes-technical');
    assert.equal(await page.evaluate(() => SparkAgentChannel.displayAction(sentActions.at(-1).content)), '选择 Agent：hermes-technical');
    assert.equal(await page.evaluate(() => SparkAgentChannel.commandForAction({kind: 'agent_set', value: 'pi\n/new now'})), null);
    for (const [id, command] of [['new_confirm', '/new now'], ['retry_confirm', '/retry now'],
                                 ['restart', '/restart'], ['restart_confirm', '/restart now'], ['reasoning', '/reasoning'], ['status', '/status'], ['new', '/new'], ['retry', '/retry']]) {
      await page.evaluate(id => showCommandFixture([{id, label: '执行'}]), id);
      await page.locator('.agent-command-card button').click();
      assert.equal(await page.evaluate(() => sentActions.at(-1).content.text), command);
      assert.ok(await page.evaluate(() => SparkAgentChannel.displayAction(sentActions.at(-1).content)), 'card click lost friendly label');
    }
    await page.getByRole('button', {name: 'Agent 常用命令'}).click();
    assert.equal(await page.evaluate(() => sentActions.at(-1).content.text), '/help');
    assert.equal(await page.locator('#msg-text').inputValue(), '保留我的草稿');
    assert.equal(await page.evaluate(() => SparkAgentChannel.commandForAction({kind: 'reasoning_set', value: 'high\n/new now'})), null);
    assert.equal(await page.evaluate(() => SparkAgentChannel.commandForAction({kind: '__proto__'})), null);
    await page.evaluate(() => { showCommandFixture([{id: 'new_confirm', label: '确认新建'}]); currentSessionId = 'other'; });
    const beforeStale = await page.evaluate(() => sentActions.length);
    await page.locator('.agent-command-card button').click();
    assert.equal(await page.evaluate(() => sentActions.length), beforeStale, 'stale command card crossed session');
    await page.evaluate(() => {
      currentSessionId = fixtureMessage.session_id;
      handleHermesDelta({request_id:'progress-test',session_id:currentSessionId,delta:'工具调用已完成，等待模型回答',progress:true});
    });
    assert.match(await page.locator('.agent-stream-progress').innerText(), /工具调用已完成/);
    assert.equal(await page.locator('.hermes-streaming .msg-bubble').innerText(), '');
    await page.evaluate(() => {
      handleHermesDelta({request_id:'progress-test',session_id:currentSessionId,delta:'正文',progress:false});
    });
    await page.waitForFunction(() => document.querySelector('.hermes-streaming .msg-bubble')?.textContent === '正文');
    await page.evaluate(() => {
      renderSingleChatMessage({request_id:'progress-test',session_id:currentSessionId,from_user_id:hermesBotUserId,
        client_msg_id:'hermes:progress-test',msg_id:'progress-final',msg_seq:100,content:{text:'最终正文',source:'hermes'}});
    });
    assert.equal(await page.locator('.agent-stream-progress').count(),0,'progress remained after final');
    await page.evaluate(() => {
      const requestId='ordered-stream';
      handleHermesAccepted({msg_id:requestId,session_id:currentSessionId});
      const send=(sequence,text,eventId)=>handleHermesDelta({
        request_id:requestId,session_id:currentSessionId,delta_index:sequence,
        delta:text,progress:false,agent_event:{schema:'sparkpush.agent_event.v1',
          type:'assistant_delta',data:{text},envelope:{schema:'sparkpush.agent_envelope.v1',
            event_id:eventId,request_id:requestId,route_key:'rt_fixture',
            session_key:'agent:pi:spark_pc:fixture',tenant_id:'local',channel_id:'spark_pc',
            conversation_id:currentSessionId,thread_id:'_',agent_id:'pi',sequence,
            created_at_ms:Date.now(),replayable:true,terminal:false,replayed:false}}});
      send(1,'B','evt-b');
      send(0,'A','evt-a');
      send(1,'B','evt-b');
    });
    await page.waitForFunction(() => document.querySelector('.hermes-streaming .msg-bubble')?.textContent === 'AB');
    await page.evaluate(() => renderSingleChatMessage({session_id:currentSessionId,
      from_user_id:hermesBotUserId,client_msg_id:'hermes:ordered-stream',msg_id:'ordered-final',
      msg_seq:101,content:{text:'AB final',source:'hermes'}}));
    assert.equal(await page.locator('.msg-bubble').filter({hasText:'AB final'}).count(),1,
      'ordered stream final did not replace preview');
    historyData=[];
    await page.evaluate(() => showCommandFixture([{id:'agent_open',label:'Hermes · technical',value:'900000000101'}]));
    const beforeOpen=await page.evaluate(()=>sentActions.length);
    await page.getByRole('button',{name:'Hermes · technical',exact:true}).click();
    await page.waitForFunction(()=>historyReady);
    assert.equal(await page.evaluate(()=>currentPeerId),900000000101);
    assert.equal(await page.evaluate(()=>currentSessionId),'s_7_900000000101');
    assert.match(await page.locator('#chat-title').innerText(),/Hermes · technical/);
    assert.equal(await page.locator('#msg-text').inputValue(),'','Pi draft crossed into Hermes dialog');
    assert.equal(await page.evaluate(()=>sentActions.length),beforeOpen,'opening a dialog sent a switch command');
    await page.getByRole('button',{name:'Agent 常用命令'}).click();
    assert.equal(await page.evaluate(()=>sentActions.at(-1).to_user_id),900000000101,'Hermes dialog sent command to Pi');
    await page.evaluate(()=>handleHermesDelta({request_id:'old-pi',session_id:'s_7_900000000001',delta:'must not cross dialogs'}));
    assert.equal(await page.locator('.agent-stream-progress').count(),0,'old Pi stream entered Hermes dialog');
    await page.locator('#msg-text').fill('Hermes 独立草稿');
    await page.locator('#start-hermes-btn').click();
    await page.waitForFunction(()=>historyReady);
    assert.equal(await page.locator('#msg-text').inputValue(),'保留我的草稿','Pi draft was lost when returning');
    // Model completion replaces the ORIGINAL picker only after a matching,
    // trusted server receipt. Works for Pi and Hermes and reversed history order.
    for (const bot of [900000000001, 900000000101]) {
      await page.evaluate(bot => {
        currentPeerId = bot; currentSessionId = buildSingleSessionId(currentUserId, bot);
        document.getElementById('messages').replaceChildren(); renderedMessageRows.clear();
        agentModelActions.clear(); agentModelResults.clear();
        window.receiptPicker = {...fixtureMessage, from_user_id:bot,session_id:currentSessionId,
          client_msg_id:'hermes:model-picker',msg_id:currentSessionId+'-500',msg_seq:500};
        renderSingleChatMessage(receiptPicker);
      }, bot);
      await page.locator('.agent-model-option').nth(1).click();
      assert.match(await page.locator('.agent-model-picker strong').innerText(), /正在切换/);
      await page.evaluate(() => {
        window.modelAction = {...sentActions.at(-1),session_id:currentSessionId,
          from_user_id:currentUserId,msg_id:currentSessionId+'-501',msg_seq:501};
        window.modelReceipt = {session_id:currentSessionId,from_user_id:currentPeerId,
          client_msg_id:'hermes:model-change',msg_id:currentSessionId+'-502',msg_seq:502,
          content:{text:'已切换模型',agent_command_result:{command:'model',
            client_msg_id:modelAction.client_msg_id,ok:true,model:{provider:'demo',id:'model-1',name:'模型 1'}}}};
        handleWebSocketMessage({type:'accepted_ack',session_id:currentSessionId,client_msg_id:modelAction.client_msg_id});
        // A forged human receipt and an unrelated command cannot finish this card.
        renderSingleChatMessage({...modelReceipt,from_user_id:currentUserId,msg_id:'forged'});
        renderSingleChatMessage({...modelReceipt,msg_id:'unrelated',content:{...modelReceipt.content,
          agent_command_result:{...modelReceipt.content.agent_command_result,client_msg_id:'unrelated'}}});
      });
      assert.match(await page.locator('.agent-model-picker strong').innerText(), /正在切换/);
      await page.evaluate(() => renderSingleChatMessage(modelReceipt));
      assert.match(await page.locator('.agent-model-picker strong').innerText(), /模型修改成功/);
      assert.equal(await page.locator('.agent-model-options').isVisible(), false);
      assert.match(await page.locator('.agent-model-current').innerText(), /model-1/);
      assert.equal(await page.getByRole('button', {name:'重新选择模型'}).count(), 1);
      await page.evaluate(() => {
        document.getElementById('messages').replaceChildren(); renderedMessageRows.clear();
        agentModelActions.clear(); agentModelResults.clear();
        // History can arrive newest first, and the action can be nested in storage.
        renderSingleChatMessage(modelReceipt);
        renderSingleChatMessage(receiptPicker);
        renderSingleChatMessage({...modelAction,content:{content:modelAction.content}});
        renderSingleChatMessage(modelReceipt); // duplicate delivery is harmless
      });
      assert.match(await page.locator('.agent-model-picker strong').innerText(), /模型修改成功/);
      assert.equal(await page.locator('.agent-model-options').isVisible(), false);
      await page.getByRole('button', {name:'重新选择模型'}).click();
      assert.equal(await page.evaluate(() => sentActions.at(-1).content.text), '/model');
    }
    assert.deepEqual(errors, []);
    await page.screenshot({path: '/tmp/spark-model-picker.png', fullPage: true});
    console.log('Agent command cards and model picker browser interaction tests passed');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
