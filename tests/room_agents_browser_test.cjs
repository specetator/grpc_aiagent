// Component contract only: API responses below are fixtures, not E2E evidence.
const assert=require('node:assert/strict');
const path=require('node:path');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
 const browser=await chromium.launch({headless:true});
 try{
  const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<section id="panel"><button data-agents-refresh>刷新</button><div data-agents-list></div><div data-agent-selected></div><div data-agents-error></div><div data-agent-requests></div><div data-room-tasks></div></section><input id="input"><div id="choices"></div><div id="msgs"></div>');
  await page.addScriptTag({path:path.join(__dirname,'../web_demo/static/room_agents.js')});
  await page.evaluate(async()=>{
   window.calls=[];window.fixture={agents:[{agent_id:'pi-stable',bot_user_id:101,name:'Pi',runtime:'pi_rpc',enabled:true,installed:true},{agent_id:'hermes-test',name:'Hermes',runtime:'hermes_native',enabled:true,installed:true}],requests:[],can_manage:false};
   window.ui=new RoomAgents(document.querySelector('#panel'),document.querySelector('#input'),document.querySelector('#choices'),async(p,b)=>{calls.push([p,b]);return p.endsWith('/tasks/list')?{tasks:[]}:fixture;},(text,me,name)=>{const n=document.createElement('div');const title=document.createElement('span');title.textContent=name;n.append(title);n.insertAdjacentHTML('beforeend','<span class="msg-body"></span><span class="msg-status"></span>');document.querySelector('#msgs').append(n);return n;});
   await ui.select(7);
  });
  assert.equal(await page.evaluate(()=>Boolean(ui.identity({agent_id:'pi-stable',sender_id:7,from_user_id:101}))),false,'legacy human payload cannot impersonate Catalog bot');
  await page.locator('#input').fill('@P');await page.getByRole('button',{name:'@Pi',exact:true}).click();
  assert.deepEqual(await page.evaluate(()=>ui.mentions()),['pi-stable']);
  await page.evaluate(()=>{
   const d=(id,index,text,room=7)=>ui.delta({session_id:'r_'+room,request_id:id,agent_id:'pi-stable',from_user_id:101,agent_name:'<img src=x onerror=evil()>',actor_user_id:8,source_msg_id:'source',delta_index:index,delta:text});
   d('one',1,'B');d('one',0,'A');d('one',1,'duplicate');d('two',0,'separate');d('wrong-room',0,'forbidden',9);
  });
  assert.deepEqual(await page.locator('#msgs .msg-body').allTextContents(),['AB','separate']);assert.equal(await page.locator('#msgs img').count(),0);
  await page.evaluate(()=>{ui.final({session_id:'r_7',request_id:'one',agent_id:'pi-stable',from_user_id:101});ui.delta({session_id:'r_7',request_id:'one',agent_id:'pi-stable',from_user_id:101,delta_index:2,delta:'late'});});
  assert.deepEqual(await page.locator('#msgs .msg-body').allTextContents(),['separate']);
  await page.evaluate(async()=>{fixture.requests=[{request_id:'failed',agent_id:'pi-stable',from_user_id:101,agent_name:'Pi',actor_user_id:8,status:'failed',result:{text:'Clear failure'}}];await ui.refresh();});
  await page.getByRole('button',{name:'重新提问（新请求）'}).click();assert.deepEqual(await page.evaluate(()=>ui.mentions()),['pi-stable']);
  await page.evaluate(async()=>{
   fixture.requests[0].status='unknown';window.actualState='running';const previous=ui.api;
   ui.api=async(p,b)=>{if(p.endsWith('/control')){calls.push([p,b]);return {status:actualState};}return previous(p,b);};await ui.refresh();
  });
  await page.getByRole('button',{name:'恢复原请求',exact:true}).click();
  assert.equal(await page.evaluate(()=>calls.filter(([,b])=>b.operation==='recover').length),0,'running native turn is queried, not resubmitted');
  await page.evaluate(()=>actualState='completed');await page.getByRole('button',{name:'恢复原请求',exact:true}).click();
  assert.equal(await page.evaluate(()=>calls.filter(([,b])=>b.operation==='recover').length),1,'terminal result recovery uses original request');
  await page.evaluate(async()=>{ui.sent();await ui.select(0);});
  assert.equal(await page.locator('#msgs').textContent(),'');assert.deepEqual(await page.evaluate(()=>ui.mentions()),[]);
  // A response from a previous room must never render into the newly selected one.
  await page.evaluate(async()=>{ui.api=()=>new Promise(resolve=>window.resolveOld=resolve);ui.room=7;window.pending=ui.refresh();await ui.select(0);resolveOld(fixture);await pending;});
  assert.equal(await page.locator('[data-agents-list]').textContent(),'');assert.deepEqual(errors,[]);
  console.log('RoomAgents component contract passed: structured IDs, separated streams, order/dedup, safe text, failure, revoke, stale responses');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
