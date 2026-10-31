// Real backends, separate browser storage. Routing only supplies static assets
// and maps the demo's fixed development ports to the owned acceptance stack.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const http=require('node:http');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
  const staticServer=http.createServer(async(req,res)=>{
    const file=path.basename(new URL(req.url,'http://localhost').pathname)||'chatroom.html';
    try{const body=await fs.readFile(path.join(__dirname,'../web_demo/static',file));res.writeHead(200,{'Content-Type':file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html'});res.end(body);}
    catch{res.writeHead(404);res.end();}
  });
  await new Promise(resolve=>staticServer.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+staticServer.address().port;
  const browser=await chromium.launch({headless:true});
  const pages=[],errors=[],credentials=[],agents=JSON.parse(process.env.SPARK_TEST_AGENTS);
  const name='Browser team '+crypto.randomBytes(5).toString('hex');
  try {
    for(let i=0;i<3;i++){
      const context=await browser.newContext({viewport:{width:1440,height:1100}});
      await context.addInitScript(url=>{
        const Native=window.WebSocket;
        window.WebSocket=class extends Native{constructor(address,protocols){super(url+'/ws'+new URL(address).search,protocols);}};
      },process.env.SPARK_TEST_WS);
      const page=await context.newPage();pages.push(page);
      page.on('pageerror',e=>errors.push(e.message));
      page.on('websocket',socket=>socket.on('socketerror',error=>console.error('WebSocket failed',String(error).replace(/([?&]token=)[^\s'"]+/g,'$1[redacted]'))));
      await page.route('**/api/**',async route=>{
        const url=new URL(route.request().url());
        if(url.pathname.startsWith('/api/')){
          const response=await route.fetch({url:process.env.SPARK_TEST_HTTP+url.pathname});
          const data=await response.json().catch(()=>null);if(data?.code)console.error('API failed',url.pathname,data.code,data.message);
          return route.fulfill({response,headers:{...response.headers(),'access-control-allow-origin':'*','access-control-allow-headers':'Authorization, Content-Type','access-control-allow-methods':'POST, GET, OPTIONS'}});
        }
        const file=path.basename(url.pathname)||'chatroom.html';
        try{return route.fulfill({body:await fs.readFile(path.join(__dirname,'../web_demo/static',file)),contentType:file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html'});}
        catch{return route.fulfill({status:404,body:''});}
      });
      await page.goto(origin+'/chatroom.html');
      await page.locator('[data-mode=register]').click();
      const login={account:'browser_'+crypto.randomBytes(8).toString('hex'),password:crypto.randomBytes(16).toString('hex')};credentials.push(login);
      await page.locator('#account').fill(login.account);
      await page.locator('#nickname').fill('Browser '+String.fromCharCode(65+i));
      await page.locator('#password').fill(login.password);
      await page.locator('#register-btn').click();
      await page.locator('#room-nav').waitFor({state:'visible'});
      await page.locator('#space-panel summary').click();
    }
    const [a,b,c]=pages;
    await a.locator('[data-create-space] [name=name]').fill(name);
    await a.getByRole('button',{name:'创建空间',exact:true}).click();
    await a.locator('.room-item').filter({hasText:name}).click();
    for(const member of [b,c]){
      const id=await member.evaluate(()=>uid); // observe real login, never inject identity
      await a.locator('[data-invite-space] input').fill(String(id));
      await a.getByRole('button',{name:'邀请成员',exact:true}).click();
      await a.getByText('邀请已发出，等待对方接受。').waitFor();
      await member.locator('[data-refresh-spaces]').click();
      await member.getByRole('button',{name:'接受邀请',exact:true}).click();
      await member.locator('.room-item').filter({hasText:name}).click();
    }
    for(const page of pages)await page.waitForFunction(()=>ws?.readyState===WebSocket.OPEN&&roomHistoryReady);
    for(const id of agents){
      await a.locator(`[data-agents-list] button[data-agent-id="${id}"]`).click();
      await a.locator(`[data-agents-list] button[data-agent-id="${id}"]`).filter({hasText:'停用'}).waitFor();
    }
    for(const page of pages)await page.locator('[data-agents-refresh]').click();
    await Promise.all(pages.map((page,i)=>page.locator('#msg-input').fill('Kafka browser discussion '+i)));
    await Promise.all(pages.map(page=>page.locator('#send-btn').click()));
    for(const page of pages)for(let i=0;i<3;i++)await page.locator('#msgs .msg-body').filter({hasText:'Kafka browser discussion '+i}).waitFor();
    // A and B independently choose installed identities in the @ picker.
    for(let i=0;i<agents.length;i++){
      const page=pages[i];await page.locator('#msg-input').fill('@');
      await page.locator(`#agent-mention-choices [data-agent-id="${agents[i]}"]`).click();
      await page.locator('#msg-input').fill('Summarize our Kafka discussion in one sentence.');
      await page.locator('#send-btn').click();
    }
    for(const page of pages){
      await page.waitForFunction(ids=>ids.every(id=>[...renderedRoomMessages.values()].some(node=>node.dataset.agentIdentity===id)),agents,{timeout:120000});
      assert.equal(await page.locator('#msgs [data-agent-request]').count(),0,'final replaces ephemeral preview');
    }
    const timelines=await Promise.all(pages.map(page=>page.locator('#msgs .msg-body').allTextContents()));
    assert.deepEqual(timelines[0],timelines[1]);assert.deepEqual(timelines[0],timelines[2]);
    await a.getByText('请求与结果',{exact:true}).click();
    await a.locator('[data-agents-refresh]').click();
    const answers=['Kafka investigation','Persisted browser task','Inspect outbox\nVerify recovery'];
    a.on('dialog',dialog=>dialog.accept(answers.shift()||''));
    await a.getByRole('button',{name:'保存为协作任务',exact:true}).first().click();
    await a.getByText('协作任务与交接',{exact:true}).click();
    await a.locator('[data-room-tasks] summary').filter({hasText:'Kafka investigation'}).waitFor();
    await b.locator('[data-agents-refresh]').click();await b.getByText('协作任务与交接',{exact:true}).click();
    await b.locator('[data-room-tasks] summary').click();
    await b.getByRole('button',{name:'接手任务',exact:true}).click();
    await b.locator('[data-room-tasks] summary').filter({hasText:'running'}).waitFor();
    await c.locator('.room-logout-link').click();await c.reload();
    await c.locator('#account').fill(credentials[2].account);await c.locator('#password').fill(credentials[2].password);await c.locator('#login-btn').click();
    await c.locator('.room-item').filter({hasText:name}).click();
    await c.getByText('协作任务与交接',{exact:true}).click();
    await c.locator('[data-room-tasks] summary').filter({hasText:'running'}).waitFor();
    await c.waitForFunction(ids=>ids.every(id=>[...renderedRoomMessages.values()].some(node=>node.dataset.agentIdentity===id)),agents);
    assert.deepEqual(errors,[]);
    console.log(JSON.stringify({ok:true,browsers:3,backend:'real HTTP/WS',checks:['registration','private creation','invitations','three-user messaging','structured mentions','both agent finals','same timeline','task persistence','claim','logout/login recovery']}));
  }catch(error){for(const page of pages)console.error('UI state',await page.locator('[data-space-status]').textContent(),await page.locator('#room-list').textContent(),await page.evaluate(()=>({socket:ws?.readyState,history:roomHistoryReady,room:currentRoom})));throw error;}finally{await browser.close();await new Promise(resolve=>staticServer.close(resolve));}
})().catch(e=>{console.error(e);process.exit(1);});
