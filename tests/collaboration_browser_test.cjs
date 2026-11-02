// Browser contract tests with controlled HTTP/WS fixtures, NOT real service E2E.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
let chromium;
try{({chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE||'playwright'));}
catch{console.log('SKIP: install Playwright and set SPARK_PLAYWRIGHT_MODULE for browser contracts');process.exit(77);}
(async()=>{
 const browser=await chromium.launch({headless:true});let checks=0;
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[],calls=[];
  const previewJpeg=await page.evaluate(()=>{const canvas=document.createElement('canvas');canvas.width=canvas.height=1;return canvas.toDataURL('image/jpeg').split(',')[1];});
  page.on('pageerror',e=>errors.push(e.message));
  let spaces=[],revoked=false,workspace={project_id:'',generation:0},requests=[],tasks=[],history=[];
  let releaseAttachment=null,delayedAttachment=false,releaseSearch=null;
  const sourceId='src_'+'a'.repeat(64);let sourceAvailable=true;
  const documentSource={name:'design.pdf',reference:{kind:'fragment',asset_id:'material-fixture',revision:'1'.repeat(64),index:0,sha256:'2'.repeat(64),anchor:{page:1}}};
  let selectedSource=documentSource;
  let material=null,dropChunkReceipt=true;const materialParts=new Map();
  const recording=Buffer.alloc(44+32000*2);recording.write('RIFF');recording.writeUInt32LE(recording.length-8,4);recording.write('WAVEfmt ',8);
  recording.writeUInt32LE(16,16);recording.writeUInt16LE(1,20);recording.writeUInt16LE(1,22);recording.writeUInt32LE(16000,24);
  recording.writeUInt32LE(32000,28);recording.writeUInt16LE(2,32);recording.writeUInt16LE(16,34);recording.write('data',36);recording.writeUInt32LE(recording.length-44,40);
  const audioMaterial={id:'recording-fixture',name:'meeting.wav',mime:'audio/wav',bytes:recording.length,sha256:crypto.createHash('sha256').update(recording).digest('hex'),state:'queued'};
  const agents=[{agent_id:'pi-public',bot_user_id:101,name:'Pi',runtime:'pi_rpc',enabled:true,installed:false},{agent_id:'hermes-public',bot_user_id:102,name:'Hermes',runtime:'hermes_native',enabled:true,installed:true}];
  await page.addInitScript(()=>{
   window.socketSends=[];window.testSockets=[];
   window.revokedUrls=[];const revoke=URL.revokeObjectURL.bind(URL);
   URL.revokeObjectURL=url=>{revokedUrls.push(url);revoke(url);};
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
    else if(endpoint==='/api/chatroom/agents/sources'){code=revoked?403:0;data={items:[{source_id:sourceId,available:sourceAvailable,...(sourceAvailable?selectedSource:{})}],has_more:false,next_after:sourceId};}
    else if(endpoint==='/api/chatroom/agents/install')agents.find(a=>a.agent_id===body.agent_id).installed=true;
    else if(endpoint==='/api/chatroom/agent/task')data={task:null,requests:[]};
    else if(endpoint==='/api/chatroom/workspace/list')data={projects:[{project_id:'fixture',name:'示例 C++ 项目'}],workspace,can_manage:true};
    else if(endpoint==='/api/chatroom/workspace/set'){assert.equal(body.generation,workspace.generation);workspace={project_id:body.project_id,generation:workspace.generation+1};data={workspace};}
    else if(endpoint==='/api/chatroom/tasks/list')data={tasks};
    else if(endpoint==='/api/chatroom/tasks/create'){assert.deepEqual(body.source_ids,[sourceId]);tasks=[{...body,task_id:1,status:'queued',summary:'',history:[],revision:1}];data={task:tasks[0]};}
    else if(endpoint==='/api/chatroom/tasks/update'){tasks[0]={...tasks[0],...body,revision:tasks[0].revision+1,assignee_user_id:body.claim?7:tasks[0].assignee_user_id};data={task:tasks[0]};}
    else if(endpoint==='/api/session/history')data={messages:body.session_id==='r_51'?history:[]};
    else if(endpoint==='/api/session/unread')data={unread:0};
    else if(endpoint==='/api/attachment/upload'){if(delayedAttachment)await new Promise(resolve=>releaseAttachment=resolve);data={id:'image-fixture',mime:'image/png',name:'test.png'};}
    else if(endpoint.startsWith('/api/room/media/')){
      const op=endpoint.split('/').at(-1);
      if(revoked)code=403;
      else if(op==='list')data={items:material?.state==='ready'?[material,audioMaterial]:[],has_more:false,next_before:0};
      else if(op==='uploads')data={items:material?.state==='uploading'?[material]:[]};
      else if(op==='init'){if(material)assert.equal(body.client_upload_id,material.client_upload_id);else material={...body,id:'material-fixture',state:'uploading',chunks:[],ordinal:1};data=material;}
      else if(op==='chunk'){materialParts.set(body.index,Buffer.from(body.data,'base64'));material.chunks=[...materialParts].map(([index,bytes])=>({index,bytes:bytes.length}));if(dropChunkReceipt){dropChunkReceipt=false;code=503;}}
      else if(op==='finish'){assert.equal(Buffer.concat([...materialParts.values()]).length,material.bytes);material.state='ready';data=material;}
      else if(op==='processing')data=body.id===audioMaterial.id?{state:'queued'}:{state:'ready',revision:'1'.repeat(64)};
      else if(op==='status'){assert.equal(body.id,audioMaterial.id);data=audioMaterial;}
      else if(op==='read'){assert.equal(body.id,audioMaterial.id);data={offset:body.offset,total_bytes:recording.length,sha256:audioMaterial.sha256,data:recording.subarray(body.offset,body.offset+body.length).toString('base64')};}
      else if(op==='search'){
        if(body.query==='slow-search')await new Promise(resolve=>releaseSearch=resolve);
        data={items:[{id:'material-fixture',name:'design.pdf',revision:'1'.repeat(64),index:7,sha256:'2'.repeat(64),content:{text:'Kafka tail decision <script>untrusted</script>',anchor:{page:8}}}],has_more:true};
      }
      else if(op==='fragments')data={items:[{index:body.offset||0,sha256:'2'.repeat(64),content:body.id===audioMaterial.id?{text:'Meeting decision to verify',anchor:{start_ms:1000,end_ms:1800}}:{text:body.offset===7?'Kafka tail decision':'Kafka delivery design material',anchor:{page:body.offset===7?8:1}}}],next_offset:(body.offset||0)+1,has_more:false};
      else if(op==='artifacts')data={items:[{index:0,anchor:{page:1}}],has_more:false};
      else if(op==='artifact')data={data:previewJpeg,metadata:{sha256:'2'.repeat(64)}};
    }
   }
   await route.fulfill({json:{code,data,message:code?'membership revoked':'ok'}});
  });
  const login=async account=>{await page.locator('#account').fill(account);await page.locator('#password').fill('fixture-password');await page.locator('#login-btn').click();await page.locator('#nav-panel:not(.hidden)').waitFor();await page.getByText('已连接',{exact:true}).waitFor();};
  const emit=message=>page.evaluate(value=>emit(value),message);
  const roomMessage=(seq,text,extra={})=>({type:'chatroom',session_id:'r_51',group_id:51,from_user_id:8,sender:{name:'Bob'},msg_id:'r_51-'+seq,msg_seq:seq,client_msg_id:'human-'+seq,content:{text},...extra});
  const bubbles=()=>page.locator('#messages .msg-bubble').allTextContents();
  await page.goto('http://localhost/');await login('alice');
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
  await page.getByRole('button',{name:'房间资料',exact:true}).click();
  const sample={name:'design.pdf',mimeType:'application/pdf',buffer:Buffer.alloc(1024*1024+32,65)};
  const beforeMedia=await page.evaluate(()=>socketSends.length);
  await page.locator('[data-media-file]').setInputFiles(sample);await page.waitForFunction(()=>!collaboration.media.busy&&document.querySelector('[data-media-status]').textContent.includes('继续'));
  await page.locator('[data-media-file]').setInputFiles(sample);await page.getByRole('button',{name:'查看内容',exact:true}).waitFor();
  assert.equal(calls.filter(c=>c.endpoint==='/api/room/media/chunk'&&c.body.index===0).length,1,'resume preserves acknowledged server chunks');
  const accepted=await page.locator('[data-media-file]').getAttribute('accept');
  assert(accepted.includes('.pdf')&&accepted.includes('.png')&&!accepted.includes('.wav')&&!accepted.includes('.mp4'));
  const legacy=page.locator('[data-media-id="'+audioMaterial.id+'"]');
  assert.equal(await legacy.getByRole('button',{name:'下载原文件',exact:true}).count(),1);
  assert.equal(await legacy.getByRole('button',{name:/播放|分析/}).count(),0);
  const initCount=calls.filter(call=>call.endpoint==='/api/room/media/init').length;
  for(const file of [{name:'new.wav',mimeType:'audio/wav',buffer:recording},{name:'new.mp4',mimeType:'video/mp4',buffer:Buffer.from('unsupported')}]){
    await page.locator('[data-media-file]').setInputFiles(file);
    await page.waitForFunction(()=>document.querySelector('[data-media-status]').textContent.includes('暂不支持音视频'));
  }
  assert.equal(calls.filter(call=>call.endpoint==='/api/room/media/init').length,initCount,'deferred formats do not initiate uploads');checks++;
  await page.getByRole('button',{name:'查看内容',exact:true}).click();await page.getByText('Kafka delivery design material',{exact:true}).waitFor();
  await page.locator('[data-media-preview]').getByRole('button',{name:'查看第 1 页',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('[data-media-preview] img')?.naturalWidth===1);
  const previewUrl=await page.locator('[data-media-preview] img').getAttribute('src');
  assert.equal(await page.locator('[data-media-preview] img').getAttribute('alt'),'design.pdf 第 1 页');
  await page.getByRole('button',{name:'关闭预览',exact:true}).click();
  assert(await page.evaluate(url=>revokedUrls.includes(url),previewUrl));checks++;
  await page.locator('[data-media-query]').fill('Kafka');await page.locator('[data-media-search] button[type=submit]').click();
  await page.locator('[data-media-matches] [data-media-match]').waitFor();assert.equal(await page.locator('[data-media-matches] script').count(),0);
  await page.getByRole('button',{name:'查看匹配片段',exact:true}).click();await page.getByText('Kafka tail decision',{exact:true}).waitFor();
  assert(calls.some(c=>c.endpoint==='/api/room/media/fragments'&&c.body.offset===7),'search opens the matching fragment, not the first page');checks++;
  await page.locator('[data-media-query]').fill('slow-search');await page.locator('[data-media-search] button[type=submit]').click();
  while(!releaseSearch)await new Promise(resolve=>setTimeout(resolve,10));
  await page.locator('[data-media-search-clear]').click();
  const searchDone=page.waitForResponse(r=>r.url().endsWith('/api/room/media/search'));releaseSearch();await searchDone;
  await page.evaluate(()=>new Promise(requestAnimationFrame));assert.equal(await page.locator('[data-media-matches]').innerText(),'');checks++;

  await page.getByRole('button',{name:'请 Agent 分析',exact:true}).click();
  assert.deepEqual(await page.getByLabel('分析目标').locator('option').evaluateAll(options=>options.map(option=>option.value)),['summary','tasks']);
  await page.getByLabel('分析目标').selectOption('tasks');
  await page.locator('[data-media-preview]').getByRole('button',{name:'@Hermes',exact:true}).click();
  assert.deepEqual(await page.evaluate(()=>collaboration.agents.mentions()),['hermes-public']);assert.match(await page.locator('#msg-text').inputValue(),/design.pdf/);
  const analysis=await page.locator('#msg-text').inputValue();
  assert(analysis.includes(JSON.stringify({asset_id:'material-fixture',revision:'1'.repeat(64),name:'design.pdf'})));
  assert.match(analysis,/tasks:.*source_ids/);assert.match(analysis,/不按姓名分配系统账号/);
  assert.equal(await page.evaluate(()=>socketSends.length),beforeMedia,'upload and preparing a mention do not trigger model messages');
  await page.evaluate(()=>{collaboration.agents.sent();document.getElementById('msg-text').value='';});checks++;
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
  requests=[{request_id:'request-1',agent_id:'pi-public',agent_name:'Pi',actor_user_id:7,status:'completed',result:{text:JSON.stringify({title:'核实消息投递',description:'检查消息持久化',steps:['阅读代码','执行回归'],needs_confirmation:true,verbatim_quote:'QBAC Pressure Review <script>untrusted</script>',interpretation:'现有上下文不足以确定该术语含义。',reason:'需要核对原录音。',source_ids:[sourceId]})}}];
  await emit(final);await page.locator('[data-agent-identity=pi-public]').waitFor();assert.equal(await page.locator('#messages [data-agent-request=request-1]').count(),1);
  assert.equal(await page.locator('#messages [data-message-sources]').count(),1);await emit(final);assert.equal(await page.locator('#messages [data-message-sources]').count(),1);
  await page.locator('#collaboration-details-btn').click();if(!await page.locator('#collaboration-panel').isVisible())await page.locator('#collaboration-details-btn').click();
  await page.locator('[data-tab=agents]').click();await page.locator('#messages').getByRole('button',{name:'查看读取来源',exact:true}).click();
  await page.locator('#messages [data-source-list] [data-source-id]').click();await page.getByText('Kafka delivery design material',{exact:true}).waitFor();
  assert(calls.filter(c=>c.endpoint==='/api/chatroom/agents/sources').length>=3,'source opened through fresh pre/post lookup');checks++;
  selectedSource={...documentSource,reference:{...documentSource.reference,kind:'image'}};
  await page.locator('#messages [data-source-list] [data-source-id]').click();
  await page.waitForFunction(()=>document.querySelector('[data-media-preview] img')?.naturalWidth===1);
  const sourceUrl=await page.locator('[data-media-preview] img').getAttribute('src');
  assert.equal(await page.locator('[data-media-preview] img').getAttribute('alt'),'design.pdf');
  await page.getByRole('button',{name:'关闭预览',exact:true}).click();
  assert(await page.evaluate(url=>revokedUrls.includes(url),sourceUrl));checks++;
  selectedSource={name:audioMaterial.name,reference:{...documentSource.reference,asset_id:audioMaterial.id,anchor:{start_ms:1000,end_ms:1800}}};
  await page.locator('#messages [data-source-list] [data-source-id]').click();
  await page.getByText('Meeting decision to verify',{exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:/播放原文件/}).count(),0,'historical sources remain readable without reopening deferred playback');checks++;
  selectedSource=documentSource;
  await page.locator('[data-tab=agents]').click();await page.getByRole('button',{name:'保存为协作任务',exact:true}).click();
  await page.locator('#collaboration-form').waitFor();assert.equal(await page.getByLabel('任务标题').inputValue(),'核实消息投递');
  const reviewDescription=await page.getByLabel('任务描述').inputValue();
  assert.match(reviewDescription,/待确认/);assert.match(reviewDescription,/模型摘录（待核对）：QBAC Pressure Review <script>untrusted<\/script>/);
  assert.match(reviewDescription,/模型解释：现有上下文不足/);assert.match(reviewDescription,/确认说明：需要核对原录音/);
  assert.equal(await page.locator('#collaboration-form script').count(),0);
  await page.locator('#collaboration-form button[type=submit]').click();await page.locator('[data-tab=tasks]').click();await page.locator('[data-task-id="1"] summary').click();
  assert.equal(tasks[0].description,reviewDescription,'review notes reach the existing task API');
  await page.getByRole('button',{name:'接手任务',exact:true}).click();await page.waitForFunction(()=>document.querySelector('[data-task-id="1"]').textContent.includes('负责人 Alice'));
  await page.locator('[data-task-id="1"] summary').click();await page.getByRole('button',{name:'更新进度 / 交接',exact:true}).click();await page.getByLabel('任务状态').selectOption('completed');await page.getByLabel('交接摘要').fill('交接记录不等于测试回执');await page.locator('#collaboration-form button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-task-id="1"]').textContent.includes('交接记录不等于测试回执'));checks++;
  await page.locator('[data-task-id="1"] summary').click();
  sourceAvailable=false;await page.getByRole('button',{name:'查看任务来源',exact:true}).click();
  await page.getByRole('button',{name:'来源已失效或已撤回',exact:true}).waitFor();
  assert(await page.getByRole('button',{name:'来源已失效或已撤回',exact:true}).isDisabled());checks++;
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
