// Focused extension of the existing three-browser runner. All writes use the
// product UI; authenticated reads below inspect the resulting server records.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');

module.exports=async function run(context){
 const {a,b,c,pages,browsers,contexts,users,receipt,errors,taskCreates,bobFrames,
  base,relayBase,control,screenshots,arg,check,wait,details,timeline,login,openRoom,chromium}=context;
 const video=arg('--media-case')==='video',name=video?'incident.mp4':'design.pdf';
 const expected=JSON.parse(await fs.readFile(path.join(arg('--media-fixtures'),'collaboration-expectations.json'),'utf8'));
 const read=async(page,endpoint,body)=>page.evaluate(async({endpoint,body})=>{
  const response=await fetch(logicBase+endpoint,{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify(body)});
  return response.json();
 },{endpoint,body});
 const room=await a.evaluate(()=>Number(currentConversation.room_id));
 const taskList=async page=>{const result=await read(page,'/api/chatroom/tasks/list',{room_id:room});assert.equal(result.code,0);return result.data.tasks;};
 const file=path.join(arg('--media-fixtures'),name),bytes=await fs.readFile(file);
 receipt.media={case:video?'video':'pdf',scope:video?'real browser video upload, native audiovisual analysis, source playback and recovery':'real browser PDF upload, native analysis, source navigation, task persistence and revocation',
  input_sha256:crypto.createHash('sha256').update(bytes).digest('hex')};
 await details(a,'workspace');await a.getByLabel('授权项目').selectOption('spark-push');
 await a.getByRole('button',{name:'保存工作区',exact:true}).click();await a.waitForFunction(()=>collaboration.workspace?.project_id==='spark-push');
 await details(a,'agents');
 const install=a.locator('[data-agents-list] [data-agent-id="public-hermes"]');
 if(await install.innerText()==='安装')await install.click();
 await a.waitForFunction(()=>collaboration.agents.agents.some(agent=>agent.agent_id==='public-hermes'&&agent.installed));
 await details(a,'media');await a.locator('[data-media-file]').setInputFiles(file);
 const row=a.locator('[data-media-list] [data-media-id]').filter({has:a.locator('strong',{hasText:name})});
 await row.waitFor({timeout:60000});await a.waitForFunction(()=>!collaboration.media.busy);
 receipt.media.asset_id=await row.getAttribute('data-media-id');
 // Refresh using the same product control a member uses while parsing runs.
 await wait(async()=>{if(await row.getByRole('button',{name:'请 Agent 分析',exact:true}).count())return true;
  await a.getByRole('button',{name:'刷新资料',exact:true}).click();return false;},180000);
 for(const page of [b,c]){
  await details(page,'media');await page.getByRole('button',{name:'刷新资料',exact:true}).click();
  await page.locator('[data-media-id="'+receipt.media.asset_id+'"]').waitFor();
 }
 const before=await read(a,'/api/chatroom/agents/list',{room_id:room});assert.equal(before.code,0);assert.deepEqual(before.data.requests,[]);
 await row.getByRole('button',{name:'查看内容',exact:true}).click();
 if(video)await a.waitForFunction(()=>document.querySelector('[data-media-preview] pre')?.textContent.length>0);
 else await a.waitForFunction(nonce=>document.querySelector('[data-media-preview]').textContent.includes(nonce),expected.pdf_nonce);
 const state=await read(a,'/api/room/media/processing',{id:receipt.media.asset_id});assert.equal(state.code,0);assert.equal(state.data.state,'ready');receipt.media.revision=state.data.revision;
 check(name+' uploaded through the UI is parsed by the real worker, shared with three members and readable without a model call');
 await row.getByRole('button',{name:'请 Agent 分析',exact:true}).click();
 await a.locator('[data-media-analysis]').selectOption(video?'video':'tasks');
 await a.locator('[data-media-preview]').getByRole('button',{name:'@Hermes',exact:true}).click();
 const draft=await a.locator('#msg-text').inputValue();assert(draft.includes(receipt.media.asset_id));
 assert(!draft.includes(expected.pdf_nonce));assert(!draft.includes(arg('--nonce')));
 const ready=await read(a,'/api/chatroom/agents/list',{room_id:room});assert.deepEqual(ready.data.requests,[],'draft selection must not invoke the model');
 // Additional user instructions ask for values from actual files without
 // supplying those values, and retain the normal generated task/source schema.
 const instruction=video?' Read two actual frames on opposite sides of the transition. Return ONLY JSON: frames:[{timestamp_ms,panel_color,marker_color,marker_shape,source_id}] in time order, transition_window_ms:[before_frame_ms,after_frame_ms], audio_summary:string in English. Use English color and shape names, describe the nonspoken visible markers, and bound the transition by actual sampled frames.':
  ' 另请实际读取授权项目 probe.cpp 与 logic/message_reservation.cpp；在同一 JSON 中增加 document_nonce（PDF 内的 Design nonce）、code_nonce（probe.cpp 注释值）、code_summary（Reserve 的实际行为）。至少提出一项核对文档中最终消息持久化要求的任务，引用真实文档片段。只返回 JSON，不执行或创建任务。';
 await a.locator('#msg-text').fill(draft+instruction);
 await a.locator('#send-btn').click();
 await wait(async()=>bobFrames.some(frame=>frame.type==='chatroom'&&frame.agent_id==='public-hermes'&&frame.request_id),300000);
 const final=bobFrames.find(frame=>frame.type==='chatroom'&&frame.agent_id==='public-hermes'&&frame.request_id);
 assert.equal(final.agent_status,'completed');assert(!final.content?.error);
 assert.equal(final.actor_user_id,await a.evaluate(()=>Number(currentUserId)));
 receipt.agent_requests=[{request_id:final.request_id,agent_id:final.agent_id,actor_user_id:final.actor_user_id,source_msg_id:final.source_msg_id,session_id:final.session_id}];
 const text=final.content.text,proposal=JSON.parse(text.slice(text.indexOf('{'),text.lastIndexOf('}')+1));
 receipt.media.proposal=proposal;
 if(video){await require('./collaboration_web_video_analysis.cjs')({...context,read,room,final,proposal});return;}
 assert.equal(proposal.document_nonce,expected.pdf_nonce);assert.equal(proposal.code_nonce,arg('--nonce'));assert(proposal.code_summary);
 assert(Array.isArray(proposal.tasks)&&proposal.tasks.length>0);
 for(const page of pages)await page.locator('#messages [data-agent-request="'+final.request_id+'"]').waitFor();
 await wait(async()=>JSON.stringify(await timeline(a))===JSON.stringify(await timeline(b))&&JSON.stringify(await timeline(a))===JSON.stringify(await timeline(c)));
 check('explicit UI mention reaches isolated Hermes; actual PDF and code facts appear in the same attributed timeline for all three members');
 const sources=await read(a,'/api/chatroom/agents/sources',{room_id:room,request_id:final.request_id});assert.equal(sources.code,0);
 // Resolve the task's own citation. Native may read a document as page images
 // or extracted text; both are supported product paths, with exact hashes.
 receipt.media.sources=sources.data.items;
 let candidate,citation,sourceText;
 for(const task of proposal.tasks){
  for(const id of (Array.isArray(task.source_ids)?task.source_ids:[])){
   const source=sources.data.items.find(item=>item.source_id===id&&item.available&&['fragment','image'].includes(item.reference.kind)&&item.reference.asset_id===receipt.media.asset_id);
   if(!source)continue;
   const ref=source.reference;
   if(ref.kind==='fragment'){
    const content=await read(a,'/api/room/media/fragments',{id:ref.asset_id,revision:ref.revision,offset:ref.index,limit:1});assert.equal(content.code,0);
    const item=content.data.items[0];assert.equal(item?.sha256,ref.sha256);
    if(!item.content.text.includes('Persist final message only'))continue;
    assert(typeof task.verbatim_quote==='string'&&task.verbatim_quote.length>0&&item.content.text.includes(task.verbatim_quote),'task quote must occur in its cited fragment');
    sourceText=item.content.text;
   }else{
    // The fixture's second page contains this exact requirement. The parent
    // harness also verifies this image hash in actual Native tool/provider IO.
    if(ref.anchor?.page!==2)continue;
    assert.equal(task.verbatim_quote,'Persist final message only');
   }
   candidate=task;citation=source;break;
  }
  if(candidate)break;
 }
 assert(candidate,'no task cites the actual PDF requirement');receipt.media.citation=citation;
 const verifyPreview=async page=>{
  if(citation.reference.kind==='fragment'){
   await page.waitForFunction(text=>document.querySelector('[data-media-preview]').textContent.includes(text),sourceText);
  }else{
   await page.waitForFunction(()=>{const image=document.querySelector('[data-media-preview] img');return image?.complete&&image.naturalWidth>0;});
   const hash=await page.locator('[data-media-preview] img').evaluate(async image=>{
    const bytes=await (await fetch(image.src)).arrayBuffer();
    return [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(value=>value.toString(16).padStart(2,'0')).join('');
   });assert.equal(hash,citation.reference.sha256);
  }
 };
 await details(a,'agents');await a.locator('[data-agents-refresh]').click();
 const requestRow=a.locator('[data-agent-requests]>div[data-request-id="'+final.request_id+'"]');
 await requestRow.getByRole('button',{name:'保存任务 · '+candidate.title,exact:true}).waitFor({timeout:30000});
 await requestRow.getByRole('button',{name:'查看读取来源',exact:true}).click();
 await requestRow.locator('[data-source-id="'+citation.source_id+'"]').click();await verifyPreview(a);
 await details(a,'agents');await requestRow.getByRole('button',{name:'保存任务 · '+candidate.title,exact:true}).click();
 await a.locator('#collaboration-form').waitFor();const description=await a.getByLabel('任务描述').inputValue();
 assert.equal(await a.getByLabel('任务标题').inputValue(),candidate.title);
 await a.locator('#collaboration-form button[type=submit]').click();
 await wait(async()=>taskCreates.length===1);assert(taskCreates[0].source_ids.includes(citation.source_id));
 await wait(async()=>(await taskList(a)).length===1);
 let saved=(await taskList(a))[0];assert.equal(saved.description,description);assert.deepEqual(saved.source_ids,taskCreates[0].source_ids);
 for(const page of [b,c]){await details(page,'agents');await page.locator('[data-agents-refresh]').click();await details(page,'tasks');await page.locator('[data-task-id="'+saved.task_id+'"]').waitFor();}
 const task=()=>c.locator('[data-task-id="'+saved.task_id+'"]');
 await task().locator('summary').click();await task().getByRole('button',{name:'接手任务',exact:true}).click();
 await c.waitForFunction(()=>document.querySelector('[data-room-tasks]').textContent.includes('负责人 Carol'));
 if(!await task().evaluate(node=>node.open))await task().locator('summary').click();
 await task().getByRole('button',{name:'更新进度 / 交接',exact:true}).click();
 const handoff='Carol 已接手文档任务，后续执行结果仍待验证。';
 await c.getByLabel('交接摘要').fill(handoff);await c.locator('#collaboration-form button[type=submit]').click();
 await wait(async()=>(await taskList(a))[0].summary===handoff);saved=(await taskList(a))[0];receipt.media.task=saved;
 assert.equal(saved.assignee_user_id,await c.evaluate(()=>Number(currentUserId)));assert.equal(saved.status,'running');
 check('source preview opens exact PDF content; Alice saves the actual model proposal and Carol claims it with a persistent handoff');
 await c.screenshot({path:path.join(screenshots,'media-analysis-task.png'),fullPage:true});
 const rows=await timeline(a);
 await fs.writeFile(path.join(control,'restart.request'),'restart');
 await wait(async()=>{try{await fs.access(path.join(control,'restart.ready'));return true;}catch{return false;}},240000);
 await Promise.all(browsers.map(browser=>browser.close()));
 for(let i=0;i<3;i++){
  const browser=await chromium.launch({headless:true});browsers.push(browser);
  contexts[i]=await browser.newContext({viewport:{width:1440,height:1000}});
  const page=await contexts[i].newPage();page.acceptanceBase=i===1?relayBase:base;pages[i]=page;
  page.on('pageerror',error=>errors.push({user:i,message:error.message}));
  await login(page,users[i]);await openRoom(page);
  await wait(async()=>JSON.stringify(await timeline(page))===JSON.stringify(rows));
  await details(page,'tasks');const restored=page.locator('[data-task-id="'+saved.task_id+'"]');await restored.waitFor();
  const value=(await taskList(page)).find(task=>task.task_id===saved.task_id);assert(value);
  for(const field of ['title','description','steps','source_ids','summary','assignee_user_id','status','revision'])assert.deepEqual(value[field],saved[field],field);
  await restored.locator('summary').click();await restored.getByRole('button',{name:'查看任务来源',exact:true}).click();
  await restored.locator('[data-source-id="'+citation.source_id+'"]').click();await verifyPreview(page);
 }
 check('full service restart and fresh browser profiles recover the same timeline, task content/assignee/status/revision and source navigation');
 await details(pages[0],'members');await pages[0].locator('[data-space-members]>div').filter({hasText:'Bob'}).getByRole('button',{name:'移除',exact:true}).click();
 await pages[1].locator('#chat-window').waitFor({state:'hidden',timeout:30000});
 assert.equal(await pages[1].locator('[data-media-preview]').textContent(),'');
 const denials={};
 for(const [endpoint,body] of [
  ['/api/room/media/read',{id:receipt.media.asset_id,offset:0,length:1}],
  ['/api/chatroom/agents/sources',{room_id:room,request_id:final.request_id,source_ids:[citation.source_id]}],
  ['/api/chatroom/tasks/list',{room_id:room}]
 ]){denials[endpoint]=(await read(pages[1],endpoint,body)).code;assert.equal(denials[endpoint],403);}
 receipt.media.revoked_api_codes=denials;
 check('membership revocation clears source preview and denies original PDF, Agent sources and persisted task access');
};
