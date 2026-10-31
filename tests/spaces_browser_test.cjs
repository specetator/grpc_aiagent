const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');
const {chromium}=require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    const page=await browser.newPage();const errors=[],calls=[];
    let spaces=[],pending=[{room_id:2,name:'邀请空间'}],members=[{user_id:7,name:'Owner',role:'owner'},{user_id:8,name:'<img src=x onerror=evil()>',role:'member'}];
    page.on('pageerror',e=>errors.push(e.message));
    await page.route('**/*',async route=>{
      const url=new URL(route.request().url());
      if(url.pathname.startsWith('/api/')) {
        const body=route.request().postDataJSON();calls.push([url.pathname,body]);
        assert.equal(route.request().headers().authorization,'Bearer space-test');
        let data={};
        if(url.pathname==='/api/spaces/list')data={spaces};
        else if(url.pathname==='/api/spaces/invitations')data={invitations:pending};
        else if(url.pathname==='/api/spaces/create') {assert.equal(body.visibility,'private');spaces=[{room_id:1,session_id:'r_1',name:body.name,visibility:body.visibility,role:'owner'}];data={room_id:1};}
        else if(url.pathname==='/api/spaces/accept'){pending=[];spaces.push({room_id:2,session_id:'r_2',name:'邀请空间',role:'member',visibility:'private'});}
        else if(url.pathname==='/api/spaces/members')data={members};
        else if(url.pathname==='/api/spaces/role'){members[1].role=body.role;}
        else if(url.pathname==='/api/spaces/remove'){members=members.filter(m=>m.user_id!==body.user_id);}
        else if(url.pathname==='/api/session/history')data={messages:[]};
        else if(url.pathname==='/api/chatroom/agent/task')data={task:null,requests:[],can_develop:false};
        else if(url.pathname==='/api/chatroom/online_count')data={online_count:1};
        return route.fulfill({json:{code:0,data}});
      }
      try {const file=path.basename(url.pathname);return route.fulfill({body:await fs.readFile(path.join(__dirname,'../web_demo/static',file)),contentType:file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html'});}catch{return route.fulfill({status:404,body:''});}
    });
    await page.goto('http://spark.test/chatroom.html');
    assert.equal(await page.locator('#space-panel').isVisible(),false);
    await page.evaluate(()=>{uid=7;token='space-test';toggleUI(true);loadMyRooms();});
    await page.locator('#space-panel summary').click();
    await page.locator('[data-create-space] [name=name]').fill("O'Reilly <script>evil()</script>");
    await page.locator('[data-create-space] [name=description]').fill('collaboration');
    await page.getByRole('button',{name:'创建空间',exact:true}).click();
    await page.locator('.room-item[data-rid="1"]').waitFor();
    await page.locator('.room-item[data-rid="1"]').click();
    await page.locator('[data-space-members]').getByText('<img src=x onerror=evil()>',{exact:false}).waitFor();
    assert.equal(await page.locator('[data-space-members] img').count(),0,'member names must be text');
    await page.evaluate(()=>{
      renderRoomMessage({type:'chatroom',group_id:1,client_msg_id:'optimistic-b',content:{text:'second by server'}},{optimistic:true});
      renderRoomMessage({type:'chatroom',group_id:1,from_user_id:8,msg_id:'r_1-1',msg_seq:1,content:{text:'first by server'}});
      renderRoomMessage({type:'chatroom',group_id:1,from_user_id:7,msg_id:'r_1-2',msg_seq:2,client_msg_id:'optimistic-b',content:{text:'second by server'}});
    });
    assert.deepEqual(await page.locator('#msgs .msg-body').allTextContents(),['first by server','second by server'],'authoritative sequence reorders optimistic rows');
    await page.locator('[data-invite-space] input').fill('9');
    await page.getByRole('button',{name:'邀请成员',exact:true}).click();
    await page.getByText('邀请已发出，等待对方接受。').waitFor();
    await page.locator('[data-space-members] select').selectOption('admin');
    await page.getByText('<img src=x onerror=evil()> · 管理员',{exact:false}).waitFor();
    await page.getByRole('button',{name:'移除',exact:true}).click();
    await page.waitForFunction(()=>!document.querySelector('[data-space-members]').textContent.includes('<img'));
    assert.equal(await page.locator('[data-leave-space]').isDisabled(),true,'owner cannot leave');
    await page.getByRole('button',{name:'接受邀请',exact:true}).click();
    await page.locator('.room-item[data-rid="2"]').waitFor();
    await page.evaluate(()=>handleRoomSocketMessage({type:'room_revoked',room_id:1}));
    assert.equal(await page.evaluate(()=>currentRoom),null);
    assert.equal(await page.locator('#msgs').textContent(),'');
    assert(calls.some(([p,b])=>p==='/api/spaces/invite'&&b.user_id===9));
    assert(calls.some(([p,b])=>p==='/api/spaces/role'&&b.role==='admin'));
    await page.evaluate(()=>doLogout());
    assert.equal(await page.locator('#space-panel').isVisible(),false);
    assert.equal(await page.locator('[data-invitations]').textContent(),'');
    assert.deepEqual(errors,[]);
    console.log('spaces browser passed: create/default privacy, list, invitations, roles, removal, safe text, revocation');
  }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
