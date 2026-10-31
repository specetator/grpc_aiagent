// Isolated browser test: no live accounts, Agent processes or real credentials.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true, executablePath:process.env.SPARK_CHROMIUM_EXECUTABLE});
  try {
    const page=await browser.newPage({viewport:{width:1100,height:850}});
    const errors=[], requests=[];
    let configured=false, revision='', probeFailure=false;
    const types=['dsh','codex','claude_code','hermes','openclaw'];
    const rows=()=>types.map((runtime,i)=>({runtime,name:runtime,bot_user_id:900000000301+i,
      configured:runtime==='codex'&&configured, endpoint:runtime==='codex'&&configured?'https://customer.example':'',
      has_key:runtime==='codex'&&configured,revision:runtime==='codex'?revision:''}));
    page.on('pageerror',e=>errors.push(e.message));
    await page.route('**/*',async route=>{
      const url=new URL(route.request().url());
      if (url.pathname==='/api/agent/connections') {
        const req=route.request().postDataJSON(); requests.push(req);
        assert.equal(route.request().headers().authorization,'Bearer test-login');
        assert.equal(req.actor_user_id,undefined);
        if(req.action==='list') return route.fulfill({json:{code:0,data:{connections:rows()}}});
        if(req.action==='probe') return route.fulfill({json:probeFailure?{code:400,message:'连接检测失败'}:{code:0,data:{runtime:'codex',name:'Codex',capabilities:{chat:true}}}});
        if(req.action==='save') {
          assert.equal(req.token,'fake-test-key'); configured=true; revision='revision-1';
          return route.fulfill({json:{code:0,data:rows()[1]}});
        }
        if(req.action==='delete') { configured=false; revision=''; return route.fulfill({json:{code:0,data:rows()[1]}}); }
      }
      if(url.pathname==='/api/session/history') return route.fulfill({json:{code:0,data:{messages:[]}}});
      if(url.pathname==='/api/session/list_single') return route.fulfill({json:{code:0,data:{sessions:[]}}});
      if(url.origin==='http://spark.test') {
        const name=url.pathname==='/'?'index.html':path.basename(url.pathname);
        try { return route.fulfill({body:await fs.readFile(path.join(__dirname,'../web_demo/static',name)),
          contentType:name.endsWith('.js')?'text/javascript':name.endsWith('.css')?'text/css':'text/html'}); }
        catch {}
      }
      return route.fulfill({status:404,body:''});
    });
    await page.goto('http://spark.test/');
    assert.equal(await page.locator('#my-agents-btn').isVisible(),false,'configuration exposed before login');
    await page.evaluate(()=>{currentUserId=7;token='test-login';currentUserName='test';updateUIState();});
    await page.locator('#my-agents-btn').click();
    await page.getByText('请选择 Agent，或填写地址自动识别。').waitFor();
    assert.equal(await page.locator('.agent-connection-row').count(),5);
    await page.locator('[name=endpoint]').fill('https://customer.example');
    await page.locator('[name=token]').fill('fake-test-key');
    await page.getByRole('button',{name:'检测连接',exact:true}).click();
    await page.getByText('已识别 Codex，连接可用。检测不会发送模型请求。').waitFor();
    assert.equal(await page.locator('[name=runtime]').inputValue(),'codex');
    await page.getByRole('button',{name:'验证并保存',exact:true}).click();
    await page.getByText('已保存 Codex，可以打开对话。').waitFor();
    if (process.env.SPARK_AGENT_SCREENSHOT) await page.screenshot({path:process.env.SPARK_AGENT_SCREENSHOT});
    assert.equal(await page.locator('[name=token]').inputValue(),'');
    assert.equal(await page.evaluate(()=>JSON.stringify(localStorage).includes('fake-test-key')),false);
    await page.getByRole('button',{name:'打开对话',exact:true}).click();
    assert.equal(await page.evaluate(()=>currentSessionId),'s_7_900000000302');
    assert.equal(await page.locator('#chat-title').textContent(),'与 我的 Codex 对话中');
    await page.locator('#my-agents-btn').click();
    await page.getByRole('button',{name:'编辑',exact:true}).click();
    assert.equal(await page.locator('[name=token]').inputValue(),'');
    page.once('dialog',d=>d.accept());
    await page.getByRole('button',{name:'断开',exact:true}).click();
    await page.getByText('已断开连接，聊天记录保留。').waitFor();
    assert.equal(await page.getByRole('button',{name:'打开对话',exact:true}).count(),0);
    probeFailure=true;
    await page.locator('[name=endpoint]').fill('https://unavailable.example');
    await page.locator('[name=token]').fill('fake-test-key');
    await page.getByRole('button',{name:'检测连接',exact:true}).click();
    await page.getByText('连接检测失败',{exact:true}).waitFor();
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.locator('.agent-connections-dialog').evaluate(el=>el.scrollWidth<=el.clientWidth),true);
    await page.getByRole('button',{name:'关闭',exact:true}).click();
    assert.equal(await page.locator('[name=token]').inputValue(),'');
    assert.deepEqual(errors,[]);
    assert(requests.some(r=>r.action==='save')&&requests.some(r=>r.action==='delete'));
    console.log('Agent connections browser test passed: login visibility, detect/save/open/delete, errors, secrets, mobile');
  } finally { await browser.close(); }
})().catch(e=>{console.error(e);process.exit(1);});
