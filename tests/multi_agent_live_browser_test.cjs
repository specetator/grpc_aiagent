// Opt-in deployment smoke: creates an independent test user; never calls a model.
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright');
(async()=>{
  if(process.env.SPARK_LIVE_TEST!=='1') throw new Error('Set SPARK_LIVE_TEST=1 to create a live test account');
  const browser=await chromium.launch({headless:true});
  try{
    const page=await browser.newPage();
    await page.goto('http://127.0.0.1:9010/index.html');
    await page.locator('[data-mode="register"]').click();
    await page.locator('#account').fill('agent_routing_check_'+Date.now());
    await page.locator('#password').fill(crypto.randomBytes(24).toString('base64url'));
    await page.locator('#nickname').fill('Agent 路由自检');
    await page.locator('#register-btn').click();
    await page.waitForFunction(()=>currentUserId&&ws?.readyState===WebSocket.OPEN);
    await page.locator('#start-hermes-btn').click();
    await page.waitForFunction(()=>historyReady);
    await page.evaluate(()=>{
      window.agentReplies=[];
      ws.addEventListener('message',e=>{
        try{const m=JSON.parse(e.data);if(m.type==='single_chat'&&isAgentPeer(m.from_user_id))agentReplies.push(m);}catch{}
      });
    });
    const send=async text=>{
      const count=await page.evaluate(()=>agentReplies.length);
      await page.locator('#msg-text').fill(text);await page.locator('#send-btn').click();
      await page.waitForFunction(n=>agentReplies.length>n,count,{timeout:45000});
      return page.evaluate(()=>agentReplies.at(-1));
    };
    let reply=await send('/agent');
    const actions=reply.content.agent_event.data.presentation.actions;
    assert(reply.content.text.startsWith('当前对话：'), 'Agent catalog request failed');
    assert.deepEqual(actions.map(a=>a.value),['900000000001'],'personal technical profile leaked to another user');
    reply=await send('/agent pi');
    assert(reply.content.text.includes('当前对话：Pi Agent'));
    const finalId=reply.msg_id;
    const result=await page.evaluate(async id=>{
      const response=await fetch(logicBase+'/api/session/history',{method:'POST',
        headers:{'Content-Type':'application/json',Authorization:'Bearer '+token},
        body:JSON.stringify({session_id:currentSessionId,anchor_seq:0,limit:50})});
      const history=(await response.json()).data.messages;
      const entry=history.find(m=>m.msg_id===id);
      return {persisted:!!entry,hasAgentCard:JSON.stringify(entry).includes('agent_open')};
    },finalId);
    assert(result.persisted && result.hasAgentCard);
    await page.reload();
    await page.waitForFunction(()=>currentUserId&&ws?.readyState===WebSocket.OPEN);
    await page.locator('#start-hermes-btn').click();await page.waitForFunction(()=>historyReady);
    await page.getByText('打开 Agent 对话',{exact:true}).first().waitFor();
    const piSession=await page.evaluate(()=>currentSessionId);
    await page.evaluate(()=>showChatWindow(900000000101,buildSingleSessionId(currentUserId,900000000101)));
    await page.waitForFunction(()=>historyReady);
    assert.notEqual(await page.evaluate(()=>currentSessionId),piSession);
    await page.evaluate(()=>{
      window.agentReplies=[];
      ws.addEventListener('message',e=>{try{const m=JSON.parse(e.data);if(m.type==='single_chat'&&isAgentPeer(m.from_user_id))agentReplies.push(m);}catch{}});
    });
    reply=await send('/status');
    assert.equal(reply.from_user_id,900000000101);
    assert(reply.content.text.includes('未成功'),'unprivileged account unexpectedly accessed personal profile');
    await page.locator('#start-hermes-btn').click();await page.waitForFunction(()=>historyReady);
    assert.equal(await page.evaluate(()=>currentSessionId),piSession);
    assert.equal(await page.locator('#messages').getByText('未成功',{exact:false}).count(),0,'Hermes message appeared in Pi history');
    // Exercise the real command receipt through Kafka, Logic persistence and WS.
    // This changes only the independent test account's Pi model; no prompt is run.
    reply=await send('/model');
    assert.equal(reply.content.agent_event.data.presentation.kind,'model_picker');
    await page.locator('.agent-model-option:not(:disabled)').first().click();
    await page.getByText('模型修改成功',{exact:true}).waitFor({timeout:45000});
    assert.equal(await page.locator('.agent-model-options:visible').count(),0,'successful picker stayed expanded');
    await page.reload();
    await page.waitForFunction(()=>currentUserId&&ws?.readyState===WebSocket.OPEN);
    await page.locator('#start-hermes-btn').click();await page.waitForFunction(()=>historyReady);
    await page.getByText('模型修改成功',{exact:true}).waitFor({timeout:15000});
    assert.equal(await page.locator('.agent-model-options:visible').count(),0,'history lost model confirmation');
    console.log('Live independent Agent dialogs, owner filtering, separate reply identities, history, model receipt and reload passed');
  }finally{await browser.close();}
})().catch(e=>{console.error(e.message);process.exitCode=1;});
