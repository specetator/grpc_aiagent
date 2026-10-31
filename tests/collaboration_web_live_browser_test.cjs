// Real Web acceptance runner. Invoked by collaboration_web_e2e.py against its
// owned Compose stack. No page routes, fake WS, identity injection or mock LLM.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const net=require('node:net');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE||'playwright');
const args=process.argv.slice(2),arg=name=>args[args.indexOf(name)+1];
const base=arg('--base'),out=arg('--out'),control=arg('--control'),screenshots=arg('--screenshots'),agents=args.includes('--agents');
const receipt={ok:false,mode:'real three-browser Web UI '+(agents?'with actual public Pi/Hermes':'with Agents disabled'),checks:[]};
const browsers=[],pages=[],contexts=[],errors=[];
const bobFrames=[];
const bobTraffic=[],carolFrames=[];
const taskCreates=[];
function observeBob(page){page.on('websocket',socket=>{
 for(const direction of ['framereceived','framesent'])socket.on(direction,frame=>{try{const value=JSON.parse(frame.payload);bobTraffic.push({direction,frame:value});if(direction==='framereceived')bobFrames.push(value);}catch{}});
});}
const relaySockets=new Set();let relay,relayAvailable=true,relayBase;
const check=text=>{receipt.checks.push(text);console.log(text);};
const wait=async(fn,timeout=180000)=>{const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await fn())return;await new Promise(r=>setTimeout(r,150));}throw Error('acceptance condition timed out');};
const prefix='web_'+crypto.randomBytes(5).toString('hex'),password=crypto.randomBytes(16).toString('hex');
const users=['Alice','Bob','Carol'].map(name=>({name,account:prefix+'_'+name.toLowerCase()}));
async function login(page,user,register=false){
 await page.goto((page.acceptanceBase||base)+'/');
 if(register)await page.locator('.auth-tab[data-mode=register]').click();
 await page.locator('#account').fill(user.account);await page.locator('#password').fill(password);
 if(register)await page.locator('#nickname').fill(user.name);
 await page.locator(register?'#register-btn':'#login-btn').click();
 await page.locator('#nav-panel:not(.hidden)').waitFor({timeout:30000});await page.getByText('已连接',{exact:true}).waitFor({timeout:30000});
}
async function openRoom(page){await page.locator('#space-list').getByRole('button',{name:/项目开发组/}).click();await page.waitForFunction(()=>document.getElementById('chat-title').textContent==='项目开发组'&&currentSessionId?.startsWith('r_')&&historyReady);}
async function details(page,tab){if(!await page.locator('#collaboration-panel').isVisible())await page.locator('#collaboration-details-btn').click();await page.locator('[data-tab='+tab+']').click();}
async function send(page,text){await page.locator('#msg-text').fill(text);await page.locator('#send-btn').click();}
async function mention(page,id,text){await page.locator('#msg-text').fill('@');await page.locator('#room-mention-choices [data-agent-id="'+id+'"]').click();await page.locator('#msg-text').fill(text);await page.locator('#send-btn').click();}
async function timeline(page){return page.locator('#messages .msg-row[data-message-seq]').evaluateAll(nodes=>nodes.map(n=>({seq:Number(n.dataset.messageSeq),id:n.dataset.messageId,text:n.querySelector('.msg-bubble').textContent})));}
async function faultControl(name){await fs.writeFile(path.join(control,name+'.request'),'requested');await wait(async()=>{try{await fs.access(path.join(control,name+'.ready'));return true;}catch{return false;}},30000);}
async function history(page,allowMissing=false){return page.evaluate(async allowMissing=>{const response=await fetch(logicBase+'/api/session/history',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify({session_id:currentSessionId,limit:100})});const result=await response.json();if(allowMissing&&result.code===404)return [];if(result.code!==0)throw Error('history denied during recovery check');return result.data.messages.map(message=>({id:message.msg_id,seq:message.msg_seq}));},allowMissing);}
(async()=>{
 try{
  // Transparent byte relay solely for a real transport failure on Bob's
  // connection. No HTTP/WS payload is parsed, mocked or altered.
  const upstream=new URL(base);
  relay=net.createServer(socket=>{
   if(!relayAvailable){socket.destroy();return;}
   const target=net.connect(Number(upstream.port||80),upstream.hostname);
   for(const connection of [socket,target]){relaySockets.add(connection);connection.on('error',()=>{socket.destroy();target.destroy();});connection.on('close',()=>relaySockets.delete(connection));}
   socket.pipe(target);target.pipe(socket);socket.on('close',()=>target.destroy());target.on('close',()=>socket.destroy());
  });
  await new Promise(resolve=>relay.listen(0,'127.0.0.1',resolve));relayBase='http://127.0.0.1:'+relay.address().port;
  for(let i=0;i<3;i++){
   const browser=await chromium.launch({headless:true});browsers.push(browser);
   const context=await browser.newContext({viewport:{width:1440,height:1000}});contexts.push(context);
   const page=await context.newPage();page.acceptanceBase=i===1?relayBase:base;pages.push(page);page.on('pageerror',e=>errors.push({user:i,message:e.message}));if(i===1)observeBob(page);
   if(i===2)page.on('websocket',socket=>socket.on('framereceived',frame=>{try{carolFrames.push(JSON.parse(frame.payload));}catch{}}));
   await login(page,users[i],true);
  }
  check('three independent browser processes register and log in through the UI');
  const [a,b,c]=pages;
  a.on('request',request=>{if(new URL(request.url()).pathname==='/api/chatroom/tasks/create')taskCreates.push(request.postDataJSON());});
  await a.locator('#manage-spaces-btn').click();await a.locator('[data-create-space] [name=name]').fill('项目开发组');await a.locator('[data-create-space] [name=description]').fill('讨论消息可靠性，与 Agent 一起审查代码。');await a.getByRole('button',{name:'创建空间',exact:true}).click();
  await a.waitForFunction(()=>currentSessionId?.startsWith('r_')&&historyReady);
  assert.equal(await a.locator('#conversation-subtitle').innerText(),'私有协作空间');
  await details(a,'members');
  for(const user of users.slice(1)){await a.locator('[data-invite-space] [name=account]').fill(user.account);await a.getByRole('button',{name:'发送邀请',exact:true}).click();await a.getByText('邀请已发出，等待对方接受。',{exact:true}).waitFor();await a.waitForFunction(()=>document.querySelector('[data-invite-space] input').value==='');}
  for(const page of [b,c]){await page.locator('#manage-spaces-btn').click();await page.getByRole('button',{name:'接受邀请',exact:true}).click();await page.locator('#spaces-dialog [data-close-dialog]').click();await openRoom(page);}
  check('Alice creates a private space, invites Bob and Carol by account, both accept without numeric IDs');
  await c.locator('#manage-spaces-btn').click();await c.locator('[data-create-space] [name=name]').fill('公开交流测试');await c.locator('[data-create-space] [name=visibility]').selectOption('public');await c.getByRole('button',{name:'创建空间',exact:true}).click();await c.waitForFunction(()=>document.getElementById('chat-title').textContent==='公开交流测试'&&historyReady);
  for(const page of [a,b]){await page.locator('#manage-spaces-btn').click();await page.locator('[data-public-spaces]>div').filter({hasText:'公开交流测试'}).getByRole('button',{name:'加入空间',exact:true}).click();await page.waitForFunction(()=>document.getElementById('chat-title').textContent==='公开交流测试'&&historyReady);}
  await send(c,'公开空间交流，与私有讨论隔离。');await wait(async()=>{for(const page of pages)if(!(await timeline(page)).some(row=>row.text==='公开空间交流，与私有讨论隔离。'))return false;return true;});
  for(const page of pages)await openRoom(page);
  for(const page of pages)assert(!(await timeline(page)).some(row=>row.text==='公开空间交流，与私有讨论隔离。'));
  check('public discovery and join work through the UI, public and private timelines remain isolated');
  const texts=['我们需要确认 gRPC 请求进入 Kafka 后才返回接受回执。','消息重投应该按 client_msg_id 去重，持久化不能遗漏。','我负责补充断线恢复与历史顺序测试。'];
  await Promise.all(pages.map((page,i)=>send(page,texts[i])));
  await wait(async()=>{const lists=await Promise.all(pages.map(timeline));return lists.every(list=>texts.every(text=>list.some(row=>row.text===text)));});
  let rows=await timeline(a);assert.deepEqual(await timeline(b),rows);assert.deepEqual(await timeline(c),rows);
  assert.equal(new Set(rows.map(row=>row.id)).size,rows.length);check('three users send concurrently and see the same ordered, deduplicated message timeline');
  if(await a.locator('#collaboration-panel').isVisible())await a.locator('[data-close-panel]').click();
  await a.locator('#to-user').fill(users[1].account);await a.locator('#start-chat-btn').click();await a.waitForFunction(()=>currentSessionId?.startsWith('s_')&&historyReady);
  await send(a,'私聊确认：协作空间保持独立。');
  await b.locator('#to-user').fill(users[0].account);await b.locator('#start-chat-btn').click();await b.waitForFunction(()=>currentSessionId?.startsWith('s_')&&historyReady);await b.getByText('私聊确认：协作空间保持独立。',{exact:true}).waitFor();
  await openRoom(a);await openRoom(b);check('account lookup opens real direct messages; switching preserves the room timeline');
  if(agents){
   await details(a,'workspace');await a.getByLabel('授权项目').selectOption('spark-push');await a.getByRole('button',{name:'保存工作区',exact:true}).click();await a.waitForFunction(()=>collaboration.workspace?.project_id==='spark-push');
   await details(a,'agents');for(const id of ['public-pi','public-hermes']){const install=a.locator('[data-agents-list] [data-agent-id="'+id+'"]');if(await install.innerText()==='安装')await install.click();await a.waitForFunction(id=>collaboration.agents.agents.some(agent=>agent.agent_id===id&&agent.installed),id);}
   // Refresh catalog through its normal button on each browser.
   for(const page of [b,c]){await details(page,'agents');await page.locator('[data-agents-refresh]').click();await page.waitForFunction(()=>collaboration.agents.agents.filter(agent=>agent.installed).length>=2);}
   await mention(a,'public-pi','@Pi 请总结刚才关于 gRPC 与 Kafka 消息投递的讨论，并整理为协作任务。请只返回 JSON 对象，包含 title、description、steps（字符串数组），覆盖接受回执、幂等、断线恢复三点。');
   await a.locator('#messages [data-agent-identity=public-pi]').waitFor({timeout:300000});
   await mention(b,'public-hermes','@Hermes 请实际读取授权项目的 probe.cpp 和 logic/message_reservation.cpp。回答必须包含 probe.cpp 注释中的完整验收 nonce，Reserve 函数的真实行为、源码路径与只读代码审查建议。不要执行 shell 或修改文件。');
   await b.locator('#messages [data-agent-identity=public-hermes]').waitFor({timeout:300000});
   await wait(async()=>{for(const page of pages)if(await page.locator('#messages [data-agent-identity]').count()<2)return false;return true;},300000);
   const hermesText=await b.locator('#messages [data-agent-identity=public-hermes] .msg-bubble').innerText();assert(hermesText.includes(arg('--nonce')),'Hermes did not return the actual authorized source nonce');assert(hermesText.includes('message_reservation.cpp')&&/Reserve/.test(hermesText));
   assert.equal(await b.locator('#messages [data-agent-identity=public-hermes] .source-message-link').count(),1);
   const finals=['public-pi','public-hermes'].map(id=>bobFrames.find(frame=>frame.type==='chatroom'&&frame.agent_id===id));
   for(let i=0;i<finals.length;i++){assert.equal(finals[i]?.agent_status,'completed');assert(!finals[i]?.content?.error);assert.equal(finals[i].actor_user_id,await pages[i].evaluate(()=>Number(currentUserId)));assert(finals[i].request_id&&finals[i].source_msg_id);}
   receipt.agent_requests=finals.map(final=>({request_id:final.request_id,agent_id:final.agent_id,actor_user_id:final.actor_user_id,source_msg_id:final.source_msg_id,session_id:final.session_id}));
   const piText=finals[0].content.text,proposal=JSON.parse(piText.slice(piText.indexOf('{'),piText.lastIndexOf('}')+1));
   await details(a,'agents');await a.locator('[data-agents-refresh]').click();await a.locator('[data-agent-requests]>div[data-request-id="'+finals[0].request_id+'"]').getByRole('button',{name:'保存为协作任务',exact:true}).click();await a.locator('#collaboration-form').waitFor();
   assert.notEqual(await a.getByLabel('任务标题').inputValue(),'协作任务','Pi did not return a structured task proposal');await a.locator('#collaboration-form button[type=submit]').click();
   await wait(async()=>taskCreates.length>0);assert.equal(taskCreates[0].request_id,finals[0].request_id);assert.equal(taskCreates[0].title,proposal.title);assert.equal(taskCreates[0].description,proposal.description);assert.deepEqual(taskCreates[0].steps,proposal.steps);
   await details(c,'agents');await c.locator('[data-agents-refresh]').click();await details(c,'tasks');await c.locator('[data-task-id] summary').first().click();await c.getByRole('button',{name:'接手任务',exact:true}).first().click();await c.waitForFunction(()=>document.querySelector('[data-room-tasks]').textContent.includes('负责人 Carol'));
   await c.locator('[data-task-id] summary').first().click();await c.getByRole('button',{name:'更新进度 / 交接',exact:true}).first().click();await c.getByLabel('交接摘要').fill('Carol 已接手：准备核对消息投递代码与恢复测试。');await c.locator('#collaboration-form button[type=submit]').click();
   await c.waitForFunction(()=>document.querySelector('[data-room-tasks]').textContent.includes('Carol 已接手'));
   check('actual Pi creates a structured task; isolated Hermes reads authorized C++ files; Carol claims and persists handoff');
  }
  if(agents){
   const task=c.locator('[data-task-id]').first();
   if(!await task.evaluate(node=>node.open))await task.locator('summary').click();
   await c.locator('#messages').evaluate(node=>{node.scrollTop=node.scrollHeight;});
  }
  await a.screenshot({path:path.join(screenshots,'alice-space.png'),fullPage:true});await c.screenshot({path:path.join(screenshots,'carol-tasks.png'),fullPage:true});
  receipt.tail_recovery=[];
  for(const kind of ['room','direct']){
   if(kind==='direct'){
    for(const [page,user] of [[b,users[2]],[c,users[1]]]){await page.locator('#to-user').fill(user.account);await page.locator('#start-chat-btn').click();await page.waitForFunction(()=>currentSessionId?.startsWith('s_')&&historyReady);}
    assert.equal(await b.evaluate(()=>workspaceFor(currentSessionId).exists),false,'first-message fault starts with an absent SQL session');
   }
   const session=await b.evaluate(()=>currentSessionId),before=await timeline(b),cursor=before.at(-1)?.seq||0;
   await b.getByText('已连接',{exact:true}).waitFor();
   await faultControl(kind+'.job-pause');
   relayAvailable=false;for(const socket of relaySockets)socket.destroy();await b.getByText('连接已断开，正在重连…',{exact:true}).waitFor();
   const frameStart=carolFrames.length,trafficStart=bobTraffic.length,text='SQL 延迟落库恢复 '+kind;
   await send(c,text);
   await wait(async()=>carolFrames.slice(frameStart).some(frame=>frame.type==='accepted_ack'&&frame.session_id===session));
   const accepted=carolFrames.slice(frameStart).find(frame=>frame.type==='accepted_ack'&&frame.session_id===session);
   const sqlBefore=await history(c,kind==='direct');assert(!sqlBefore.some(row=>row.id===accepted.msg_id),'Job was not paused before persistence');
   const directoryWait=kind==='direct'?b.waitForResponse(response=>new URL(response.url()).pathname==='/api/session/list_single'):null;
   relayAvailable=true; // From here onward only observe automatic recovery.
   await b.getByText('已连接',{exact:true}).waitFor({timeout:30000});
   if(kind==='room')await wait(async()=>bobTraffic.slice(trafficStart).some(event=>event.direction==='framereceived'&&event.frame.type==='sync_end'&&event.frame.session_id===session&&event.frame.after_seq===cursor&&event.frame.next_seq===cursor));
   else{const directory=await (await directoryWait).json();assert.equal(directory.code,0);assert(!directory.data.sessions.some(item=>item.session_id===session),'session must still be absent from authoritative directory');}
   assert(!(await timeline(b)).some(row=>row.text===text));assert(!(await history(c,kind==='direct')).some(row=>row.id===accepted.msg_id));
   const resumeStart=bobTraffic.length;
   await faultControl(kind+'.job-resume');
   await wait(async()=>(await history(c,kind==='direct')).some(row=>row.id===accepted.msg_id),30000);
   await wait(async()=>(await timeline(b)).some(row=>row.text===text),20000);
   await wait(async()=>(await timeline(c)).some(row=>row.text===text),20000);
   const after=await timeline(b);assert.deepEqual(after,await timeline(c));assert.equal(new Set(after.map(row=>row.id)).size,after.length);
   const recoverySync=bobTraffic.slice(resumeStart).filter(event=>event.frame.type==='sync'||event.frame.type==='sync_end').map(event=>({direction:event.direction,type:event.frame.type,after_seq:event.frame.after_seq,next_seq:event.frame.next_seq,safe_seq:event.frame.safe_seq,session_id:event.frame.session_id}));
   // With explicit Agent-room/direct routes, the offline recipient is absent
   // from the persisted fanout. Recovery must come from a new cursor request.
   if(agents||kind==='direct')assert(recoverySync.some(event=>event.direction==='framesent'&&event.session_id===session&&event.after_seq===cursor),'tail arrived without exercising periodic sync');
   receipt.tail_recovery.push({kind,session_id:session,old_cursor:cursor,message_id:accepted.msg_id,sql_before:sqlBefore,sql_after:await history(c),recovery_sync:recoverySync,directory_absent_before:kind==='direct'});
   if(kind==='direct'){await openRoom(b);await openRoom(c);}
  }
  check('paused real Job proves room empty reconnect sync and absent first-DM directory; later SQL tails recover automatically with one ordered copy');
  // Close A's page entirely; B loses its network while C continues ordinary IM.
  await a.close();
  for(let cycle=0;cycle<3;cycle++){
   await b.getByText('已连接',{exact:true}).waitFor({timeout:15000});
   relayAvailable=false;for(const socket of relaySockets)socket.destroy();
   await b.getByText('连接已断开，正在重连…',{exact:true}).waitFor({timeout:15000});
   const text='网络恢复后应补齐这条消息。 '+cycle;
   await send(c,text);await c.getByText(text,{exact:true}).waitFor();relayAvailable=true;
   // Exercise one manual retry and two automatic reconnects. Never reset a
   // live connection from an assertion poll.
   if(cycle===0)await b.locator('#reconnect-btn').click({timeout:1000}).catch(()=>{});
   await wait(async()=>(await timeline(b)).some(row=>row.text===text),60000);
   await b.getByText('已连接',{exact:true}).waitFor({timeout:15000});
   const recovered=await timeline(b),expected=await timeline(c);
   assert.deepEqual(recovered,expected,'reconnect must recover the complete ordered timeline before the next fault');
   assert.equal(new Set(recovered.map(row=>row.id)).size,recovered.length);
  }
  check('closing Alice and taking Bob offline does not block Carol; Bob recovers the persisted message');
  rows=await timeline(c);await fs.writeFile(path.join(control,'restart.request'),'restart');
  await wait(async()=>{try{await fs.access(path.join(control,'restart.ready'));return true;}catch{return false;}},240000);
  // Fresh processes and profiles rule out old DOM, localStorage and IM caches
  // masking missing server persistence after the restart.
  await Promise.all(browsers.map(browser=>browser.close()));
  for(let i=0;i<3;i++){
   const browser=await chromium.launch({headless:true});browsers.push(browser);
   const context=await browser.newContext({viewport:{width:1440,height:1000}});contexts[i]=context;
   const page=await context.newPage();page.acceptanceBase=i===1?relayBase:base;pages[i]=page;page.on('pageerror',e=>errors.push({user:i,message:e.message}));if(i===1)observeBob(page);
   await login(page,users[i]);await openRoom(page);
   await wait(async()=>{const list=await timeline(page);return rows.every(row=>list.some(item=>item.id===row.id&&item.text===row.text));});
   if(agents){
    await details(page,'tasks');
    await page.waitForFunction(()=>[...document.querySelectorAll('[data-task-id] pre')].some(node=>node.textContent.startsWith('变更历史')&&node.textContent.includes('Carol 已接手：准备核对消息投递代码与恢复测试。')));
   }
  }
  assert.deepEqual(await timeline(pages[0]),await timeline(pages[1]));assert.deepEqual(await timeline(pages[1]),await timeline(pages[2]));
  check('full Compose restart and fresh browser logins preserve timeline, membership and task handoff');
  for(const i of [0,1]){const page=pages[i];await page.locator('#to-user').fill(users[1-i].account);await page.locator('#start-chat-btn').click();await page.getByText('私聊确认：协作空间保持独立。',{exact:true}).waitFor();assert(!(await timeline(page)).some(row=>row.text===texts[2]));await openRoom(page);}
  await pages[2].locator('#space-list').getByRole('button',{name:/公开交流测试/}).click();await pages[2].getByText('公开空间交流，与私有讨论隔离。',{exact:true}).waitFor();await openRoom(pages[2]);
  check('direct and public conversations recover from server history in fresh browser profiles');
  const privateRoom=await pages[1].evaluate(()=>Number(currentConversation.room_id));
  await details(pages[0],'members');const bobRow=pages[0].locator('[data-space-members]>div').filter({hasText:'Bob'});await bobRow.getByRole('button',{name:'移除',exact:true}).click();
  await pages[1].locator('#chat-window').waitFor({state:'hidden',timeout:30000});assert.equal(await pages[1].locator('#messages').textContent(),'');
  const denied=await pages[1].evaluate(async room=>{
   const results={};for(const [endpoint,body] of [['/api/session/history',{session_id:'r_'+room}],['/api/spaces/members',{room_id:room}],['/api/chatroom/agents/list',{room_id:room}],['/api/chatroom/tasks/list',{room_id:room}]]){
    const response=await fetch(logicBase+endpoint,{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify(body)});results[endpoint]=(await response.json()).code;
   }return results;
  },privateRoom);
  assert.equal(denied['/api/session/history'],403);assert.equal(denied['/api/spaces/members'],403);
  if(agents){assert.equal(denied['/api/chatroom/agents/list'],403);assert.equal(denied['/api/chatroom/tasks/list'],403);}
  receipt.revoked_api_codes=denied;
  const removedMarker='成员移除后的新消息 '+crypto.randomBytes(4).toString('hex');await send(pages[2],removedMarker);await wait(async()=>(await timeline(pages[0])).some(row=>row.text===removedMarker));
  assert(!bobFrames.some(frame=>frame.type==='chatroom'&&JSON.stringify(frame).includes(removedMarker)));
  await pages[1].reload();await pages[1].waitForFunction(()=>currentUserId);await wait(async()=>await pages[1].locator('#space-list').getByRole('button',{name:/公开交流测试/}).count()===1);assert.equal(await pages[1].locator('#space-list').getByRole('button',{name:/项目开发组/}).count(),0);
  check('revocation clears Bob cache, denies real history/member APIs, blocks live delivery and survives reload');
  await pages[2].setViewportSize({width:390,height:844});if(await pages[2].locator('#collaboration-panel').isVisible())await pages[2].locator('[data-close-panel]').click();await pages[2].waitForFunction(()=>document.getElementById('sidebar').getBoundingClientRect().right<=1);assert(await pages[2].evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await pages[2].screenshot({path:path.join(screenshots,'mobile-space.png'),fullPage:true,animations:'disabled'});
  assert.deepEqual(errors,[]);check('desktop and mobile layouts are usable without uncaught JavaScript errors');receipt.ok=true;
 }catch(error){
  receipt.errors=errors;
  receipt.failure=String(error.message||error).split('\n')[0];
  for(let i=0;i<pages.length;i++)if(!pages[i].isClosed()){
   const status=await pages[i].locator('#login-status').textContent().catch(()=>'');
   if(status)console.error('browser '+i+' login status: '+status);
   await pages[i].screenshot({path:path.join(screenshots,'failure-'+i+'.png'),fullPage:true}).catch(()=>{});
   receipt.failure_state=receipt.failure_state||[];
   receipt.failure_state.push(await pages[i].evaluate(async()=>{
    const state={session:currentSessionId,cursors:sessionCursors,pending:[...pendingCursorSync],seen:Object.fromEntries(Object.entries(seenSessionSequences).map(([id,set])=>[id,[...set]])),historyReady,ws:ws?.readyState};
    if(currentSessionId&&token){try{const response=await fetch(logicBase+'/api/session/history',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify({session_id:currentSessionId,limit:100})});const data=await response.json();state.history={code:data.code,messages:data.data?.messages?.map(message=>({id:message.msg_id,seq:message.msg_seq}))};}catch{state.history='unavailable';}}
    return state;
   }).catch(()=>({unavailable:true})));
  }
  receipt.bob_recent_frames=bobFrames.slice(-30).map(frame=>({type:frame.type,session_id:frame.session_id,msg_seq:frame.msg_seq,after_seq:frame.after_seq,next_seq:frame.next_seq,safe_seq:frame.safe_seq,message:frame.type==='error'?frame.message:undefined}));
  throw error;
 }finally{await Promise.all(browsers.map(browser=>browser.close()));for(const socket of relaySockets)socket.destroy();if(relay)await new Promise(resolve=>relay.close(resolve));await fs.writeFile(out,JSON.stringify(receipt,null,2)+'\n');}
})().catch(error=>{console.error(error);process.exitCode=1;});
