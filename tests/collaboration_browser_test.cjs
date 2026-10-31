// Browser contract tests with controlled HTTP/WS fixtures, NOT real service E2E.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
let chromium;
try{({chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE||'playwright'));}
catch{console.log('SKIP: install Playwright and set SPARK_PLAYWRIGHT_MODULE for browser contracts');process.exit(77);}
(async()=>{
 const browser=await chromium.launch({headless:true});let checks=0;
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[],calls=[];
  page.on('pageerror',e=>errors.push(e.message));
  let spaces=[],revoked=false,workspace={project_id:'',generation:0},requests=[],tasks=[],history=[];
  let releaseAttachment=null,delayedAttachment=false;
  const agents=[{agent_id:'pi-public',bot_user_id:101,name:'Pi',runtime:'pi_rpc',enabled:true,installed:false},{agent_id:'hermes-public',bot_user_id:102,name:'Hermes',runtime:'hermes_native',enabled:true,installed:true}];
  await page.addInitScript(()=>{
   window.socketSends=[];window.testSockets=[];
   window.WebSocket=class{
    static OPEN=1;static CLOSED=3;
    constructor(){this.readyState=1;this.bufferedAmount=0;testSockets.push(this);setTimeout(()=>this.onopen?.(),0);}
    send(text){socketSends.push(JSON.parse(text));}
    close(){this.readyState=3;this.onclose?.();}
   };
   window.emit=message=>testSockets.at(-1).onmessage({data:JSON.stringify(message)});
  });
  await page.route('**/*',async route=>{
   const url=new URL(route.request().url());
   if(!url.pathname.startsWith('/api/')){
    const name=url.pathname==='/'?'index.html':path.basename(url.pathname);
    try{return await route.fulfill({body:await fs.readFile(path.join(__dirname,'../web_demo/static',name)),contentType:name.endsWith('.js')?'text/javascript':name.endsWith('.css')?'text/css':'text/html'});}catch{return route.fulfill({status:404,body:''});}
   }
   const body=route.request().postDataJSON(),endpoint=url.pathname;calls.push({endpoint,body});let code=0,data={};
   if(endpoint==='/api/login')data={user_id:body.account==='alice'?7:9,name:body.account==='alice'?'Alice':'Carol',token:body.account+'-fixture'};
   else{
    assert.match(route.request().headers().authorization||'',/^Bearer (alice|carol)-fixture$/);
    if(endpoint==='/api/session/list_single')data={sessions:[{session_id:'s_7_8',peer_user_id:8,peer_name:'Bob',last_msg_seq:0,unread_count:2}]};
    else if(endpoint==='/api/users/lookup'){assert.equal(body.account,'bob');data={user_id:8,name:'Bob',account:'bob'};}
    else if(endpoint==='/api/spaces/list')data={spaces:revoked?[]:spaces};
    else if(endpoint==='/api/spaces/invitations')data={invitations:[]};
    else if(endpoint==='/api/spaces/discover')data={spaces:[{room_id:52,session_id:'r_52',name:'公开交流',visibility:'public'}]};
    else if(endpoint==='/api/chatroom/join'){assert.deepEqual(body,{room_id:52,user_id:7});spaces.push({room_id:52,session_id:'r_52',name:'公开交流',visibility:'public'});}
    else if(endpoint==='/api/spaces/create'){assert.equal(body.visibility,'private');spaces=[{room_id:51,session_id:'r_51',name:body.name,visibility:'private',role:'owner'}];data={room_id:51,session_id:'r_51'};}
    else if(endpoint==='/api/spaces/members'){code=revoked?403:0;data={members:[{user_id:7,name:'Alice',account:'alice',role:'owner'},{user_id:8,name:'Bob',account:'bob',role:'member'}]};}
    else if(endpoint==='/api/chatroom/agents/list')data={agents,requests,can_manage:true};
    else if(endpoint==='/api/chatroom/agents/install')agents.find(a=>a.agent_id===body.agent_id).installed=true;
    else if(endpoint==='/api/chatroom/agent/task')data={task:null,requests:[]};
    else if(endpoint==='/api/chatroom/workspace/list')data={projects:[{project_id:'fixture',name:'示例 C++ 项目'}],workspace,can_manage:true};
    else if(endpoint==='/api/chatroom/workspace/set'){assert.equal(body.generation,workspace.generation);workspace={project_id:body.project_id,generation:workspace.generation+1};data={workspace};}
    else if(endpoint==='/api/chatroom/tasks/list')data={tasks};
    else if(endpoint==='/api/chatroom/tasks/create'){tasks=[{...body,task_id:1,status:'queued',summary:'',history:[],revision:1}];data={task:tasks[0]};}
    else if(endpoint==='/api/chatroom/tasks/update'){tasks[0]={...tasks[0],...body,revision:tasks[0].revision+1,assignee_user_id:body.claim?7:tasks[0].assignee_user_id};data={task:tasks[0]};}
    else if(endpoint==='/api/session/history')data={messages:body.session_id==='r_51'?history:[]};
    else if(endpoint==='/api/session/unread')data={unread:0};
    else if(endpoint==='/api/attachment/upload'){if(delayedAttachment)await new Promise(resolve=>releaseAttachment=resolve);data={id:'image-fixture',mime:'image/png',name:'test.png'};}
   }
   await route.fulfill({json:{code,data,message:code?'membership revoked':'ok'}});
  });
  const login=async account=>{await page.locator('#account').fill(account);await page.locator('#password').fill('fixture-password');await page.locator('#login-btn').click();await page.locator('#nav-panel:not(.hidden)').waitFor();await page.getByText('已连接',{exact:true}).waitFor();};
  const emit=message=>page.evaluate(value=>emit(value),message);
  const roomMessage=(seq,text,extra={})=>({type:'chatroom',session_id:'r_51',group_id:51,from_user_id:8,sender:{name:'Bob'},msg_id:'r_51-'+seq,msg_seq:seq,client_msg_id:'human-'+seq,content:{text},...extra});
  const bubbles=()=>page.locator('#messages .msg-bubble').allTextContents();
  await page.goto('http://collaboration.test/');await login('alice');
  await page.locator('#manage-spaces-btn').click();await page.locator('[data-create-space] [name=name]').fill('项目开发组');await page.getByRole('button',{name:'创建空间',exact:true}).click();
  await page.getByText('项目开发组',{exact:true}).last().waitFor();await page.waitForFunction(()=>currentSessionId==='r_51'&&historyReady);
  assert.equal(await page.locator('#conversation-subtitle').innerText(),'私有协作空间');checks++;
  await page.locator('#manage-spaces-btn').click();await page.locator('[data-public-spaces]').getByRole('button',{name:'加入空间',exact:true}).click();await page.waitForFunction(()=>currentSessionId==='r_52'&&historyReady);
  assert.equal(await page.locator('#conversation-subtitle').innerText(),'公开协作空间');await page.locator('#space-list [data-sid=r_51]').click();await page.waitForFunction(()=>currentSessionId==='r_51');checks++;
  await page.locator('#collaboration-details-btn').click();await page.locator('[data-tab=members]').click();
  await page.locator('[data-invite-space] [name=account]').fill('bob');await page.getByRole('button',{name:'发送邀请',exact:true}).click();await page.getByText('邀请已发出，等待对方接受。',{exact:true}).waitFor();
  assert(calls.some(c=>c.endpoint==='/api/spaces/invite'&&c.body.user_id===8));assert.equal(await page.locator('[data-space-members]').innerText().then(s=>s.includes('(8)')),false);checks++;
  await page.locator('[data-tab=workspace]').click();await page.getByLabel('授权项目').selectOption('fixture');await page.getByRole('button',{name:'保存工作区',exact:true}).click();await page.waitForFunction(()=>collaboration.workspace?.generation===1);checks++;
  await page.locator('[data-tab=agents]').click();await page.locator('[data-agent-id=pi-public]').click();await page.waitForFunction(()=>collaboration.agents.agents.find(a=>a.agent_id==='pi-public')?.installed);
  await page.locator('#msg-text').fill('@P');await page.locator('#room-mention-choices [data-agent-id=pi-public]').click();await page.locator('#msg-text').fill('@Pi 总结讨论');await page.locator('#send-btn').click();
  const sent=await page.evaluate(()=>socketSends.filter(m=>m.type==='chatroom').at(-1));assert.deepEqual(sent.agent_mentions,['pi-public']);assert.equal(sent.group_id,51);checks++;
  await emit({type:'accepted_ack',session_id:'r_51',client_msg_id:sent.client_msg_id});
  assert.match(await page.locator('#messages').innerText(),/已接收/);
  await emit(roomMessage(3,'第三条'));await emit(roomMessage(1,'第一条'));await emit(roomMessage(2,sent.content.text,{from_user_id:7,client_msg_id:sent.client_msg_id,sender:{name:'Alice'}}));await emit(roomMessage(1,'第一条'));
  assert.deepEqual(await bubbles(),['第一条','@Pi 总结讨论','第三条']);assert.equal(await page.evaluate(()=>sessionCursors.r_51),3);checks++;
  await page.locator('#msg-text').fill('普通讨论');await page.locator('#send-btn').click();
  const normal=await page.evaluate(()=>socketSends.filter(m=>m.type==='chatroom').at(-1));assert.equal(normal.agent_mentions,undefined);checks++;
  await emit({type:'error',session_id:'r_51',client_msg_id:normal.client_msg_id,message:'temporary fixture failure'});
  await page.getByRole('button',{name:'重发此消息',exact:true}).click();assert.deepEqual(await page.evaluate(()=>socketSends.filter(m=>m.type==='chatroom').at(-1)),normal);checks++;
  await page.locator('#to-user').fill('bob');await page.locator('#start-chat-btn').click();await page.waitForFunction(()=>currentSessionId==='s_7_8'&&historyReady);
  assert.equal(await page.locator('#chat-title').innerText(),'Bob');
  await emit(roomMessage(4,'后台收到的消息'));assert.equal(await page.evaluate(()=>sessionCursors.r_51),4);
  await page.locator('#space-list [data-sid=r_51]').click();await page.waitForFunction(()=>currentSessionId==='r_51'&&historyReady);
  assert((await bubbles()).includes('后台收到的消息'));checks++;
  const final=roomMessage(5,'总结结果',{from_user_id:101,client_msg_id:'hermes:request-1',agent_id:'pi-public',agent_name:'Pi',actor_user_id:7,request_id:'request-1',source_msg_id:'r_51-2',agent_status:'completed'});
  await emit({...final,type:'hermes_delta',delta_index:0,delta:'临时',msg_seq:undefined});assert.equal(await page.evaluate(()=>sessionCursors.r_51),4);checks++;
  requests=[{request_id:'request-1',agent_id:'pi-public',agent_name:'Pi',actor_user_id:7,status:'completed',result:{text:JSON.stringify({title:'核实消息投递',description:'检查消息持久化',steps:['阅读代码','执行回归']})}}];
  await emit(final);await page.locator('[data-agent-identity=pi-public]').waitFor();assert.equal(await page.locator('#messages [data-agent-request=request-1]').count(),1);
  await page.locator('#collaboration-details-btn').click();if(!await page.locator('#collaboration-panel').isVisible())await page.locator('#collaboration-details-btn').click();
  await page.locator('[data-tab=agents]').click();await page.getByRole('button',{name:'保存为协作任务',exact:true}).click();
  await page.locator('#collaboration-form').waitFor();assert.equal(await page.getByLabel('任务标题').inputValue(),'核实消息投递');await page.locator('#collaboration-form button[type=submit]').click();await page.locator('[data-tab=tasks]').click();await page.locator('[data-task-id="1"] summary').click();
  await page.getByRole('button',{name:'接手任务',exact:true}).click();await page.waitForFunction(()=>document.querySelector('[data-task-id="1"]').textContent.includes('负责人 Alice'));
  await page.locator('[data-task-id="1"] summary').click();await page.getByRole('button',{name:'更新进度 / 交接',exact:true}).click();await page.getByLabel('任务状态').selectOption('completed');await page.getByLabel('交接摘要').fill('交接记录不等于测试回执');await page.locator('#collaboration-form button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-task-id="1"]').textContent.includes('交接记录不等于测试回执'));checks++;
  // The server HTTP upload may finish after a conversation switch. It must not
  // send the original room's attachment or text to the newly selected peer.
  await page.locator('[data-close-panel]').click();delayedAttachment=true;
  await page.locator('#image-file').setInputFiles({name:'first.png',mimeType:'image/png',buffer:Buffer.from('first')});await page.waitForFunction(()=>pendingImages.length===1);
  await page.locator('#msg-text').fill('本次要发送的图片说明');await page.locator('#send-btn').click();while(!releaseAttachment)await new Promise(resolve=>setTimeout(resolve,10));
  await page.locator('#msg-text').fill('上传期间编辑的新草稿');await page.locator('#image-file').setInputFiles({name:'next.png',mimeType:'image/png',buffer:Buffer.from('next')});await page.waitForFunction(()=>pendingImages.length===2);
  releaseAttachment();await page.waitForFunction(()=>!uploadingMessage);
  assert.equal(await page.locator('#msg-text').inputValue(),'上传期间编辑的新草稿');assert.equal(await page.evaluate(()=>pendingImages[0].name),'next.png');
  assert.equal(await page.evaluate(()=>socketSends.filter(m=>m.type==='chatroom').at(-1).content.text),'本次要发送的图片说明');checks++;
  await page.locator('#pending-images button').click();releaseAttachment=null;
  for(const id of ['pi-public','hermes-public']){await page.locator('#msg-text').fill('@');await page.locator('#room-mention-choices [data-agent-id="'+id+'"]').click();}
  await page.locator('#image-file').setInputFiles({name:'mention.png',mimeType:'image/png',buffer:Buffer.from('mention')});await page.waitForFunction(()=>pendingImages.length===1);
  await page.locator('#msg-text').fill('只更改目标选择');await page.locator('#send-btn').click();while(!releaseAttachment)await new Promise(resolve=>setTimeout(resolve,10));
  await page.locator('#room-agent-selected').getByRole('button',{name:'@Pi ×',exact:true}).click();releaseAttachment();await page.waitForFunction(()=>!uploadingMessage);
  assert.deepEqual(await page.evaluate(()=>collaboration.agents.mentions()),['hermes-public'],'mention-only editing survives upload completion');checks++;
  await page.locator('#room-agent-selected').getByRole('button',{name:'@Hermes ×',exact:true}).click();releaseAttachment=null;
  await page.locator('#image-file').setInputFiles({name:'test.png',mimeType:'image/png',buffer:Buffer.from('fixture')});await page.waitForFunction(()=>pendingImages.length===1);
  await page.locator('#msg-text').fill('留在原会话的草稿');const sendsBefore=await page.evaluate(()=>socketSends.filter(m=>['single_chat','chatroom'].includes(m.type)).length);
  await page.locator('#send-btn').click();while(!releaseAttachment)await new Promise(resolve=>setTimeout(resolve,10));
  await page.locator('#session-list [data-sid=s_7_8]').click();releaseAttachment();await page.waitForFunction(()=>!uploadingMessage);
  assert.equal(await page.evaluate(()=>socketSends.filter(m=>['single_chat','chatroom'].includes(m.type)).length),sendsBefore);
  await page.locator('#space-list [data-sid=r_51]').click();await page.waitForFunction(()=>currentSessionId==='r_51');assert.equal(await page.locator('#msg-text').inputValue(),'留在原会话的草稿');checks++;
  revoked=true;await emit({type:'room_revoked',room_id:51});await page.locator('#chat-window').waitFor({state:'hidden'});
  assert.equal(await page.evaluate(()=>sessionWorkspaces.has('r_51')||conversationDrafts.has('r_51')||conversationPendingImages.has('r_51')),false);
  await emit(roomMessage(6,'撤销后的延迟消息'));assert.equal(await page.evaluate(()=>sessionWorkspaces.has('r_51')),false);checks++;
  revoked=false;history=[roomMessage(40,'历史已落库 40',{sender_id:8}),roomMessage(42,'历史已落库 42',{sender_id:8})];
  await page.reload();await page.locator('#space-list [data-sid=r_51]').click();await page.waitForFunction(()=>historyReady);
  assert.equal(await page.evaluate(()=>sessionCursors.r_51),40,'SQL history max must not seal an unsettled gap');
  await emit(roomMessage(41,'延迟落库 41'));assert.equal(await page.evaluate(()=>sessionCursors.r_51),42);assert.deepEqual(await bubbles(),['历史已落库 40','延迟落库 41','历史已落库 42']);checks++;
  await page.locator('#session-list [data-sid=s_7_8]').click();await page.waitForFunction(()=>currentSessionId==='s_7_8');
  await page.evaluate(()=>{for(let seq=43;seq<=2047;seq++)emit({type:'chatroom',session_id:'r_51',group_id:51,from_user_id:8,msg_id:'overflow-'+seq,msg_seq:seq,content:{text:'背景 '+seq}});});
  assert.equal(await page.evaluate(()=>sessionWorkspaces.get('r_51').pending.length),2000);
  assert.equal(await page.evaluate(()=>sessionWorkspaces.get('r_51').recoveryFrom),42);
  await page.locator('#space-list [data-sid=r_51]').click();await page.waitForFunction(()=>currentSessionId==='r_51');
  assert.equal(await page.evaluate(()=>socketSends.filter(m=>m.type==='sync'&&m.session_id==='r_51').at(-1).after_seq),42,'overflow replays from the pre-buffer cursor');
  await page.evaluate(()=>{for(let seq=43;seq<=2047;seq++)emit({type:'chatroom',session_id:'r_51',group_id:51,from_user_id:8,msg_id:'overflow-'+seq,msg_seq:seq,content:{text:'背景 '+seq}});});
  assert.equal(await page.locator('#messages .msg-row').count(),2008);assert.equal(await page.evaluate(()=>sessionCursors.r_51),2047);checks++;
  await page.locator('.logout-btn').click();await login('carol');assert.equal(await page.evaluate(()=>Boolean(sessionWorkspaces.has('r_51')||conversationDrafts.size||pendingImages.length)),false);checks++;
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await page.locator('#sidebar-toggle').click();assert.equal(await page.locator('#manage-spaces-btn').isVisible(),true);checks++;
  assert.deepEqual(errors,[]);checks++;
  console.log(JSON.stringify({kind:'mock_browser_contract',checks,passed:true}));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
