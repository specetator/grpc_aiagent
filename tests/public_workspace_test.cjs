// Uses Pi's installed native read implementation; authority responses are a contract fixture.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const pi = process.env.SPARK_PI_PACKAGE || '/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent';
const {createJiti} = require(path.join(pi,'node_modules/jiti'));
const jiti = createJiti(__filename,{alias:{'@earendil-works/pi-coding-agent':path.join(pi,'dist/index.js')}});
(async()=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'spark-public-read-'));
  const originalFetch=global.fetch;
  try {
    const content='int queued() { return 3; }\n';
    fs.writeFileSync(path.join(root,'queue.cpp'),content);
    const hash=crypto.createHash('sha256').update(content).digest('hex');
    const manifest={files:[{path:'queue.cpp',sha256:hash}],index_sha256:''};
    const scope={actor_user_id:7,project_id:'sample',request_id:'ra_'+'a'.repeat(64)};
    fs.writeFileSync(path.join(root,'manifest.json'),JSON.stringify(manifest));
    Object.assign(process.env,{SPARK_PUBLIC_SNAPSHOT:root,SPARK_PUBLIC_SCOPE:JSON.stringify(scope),
      SPARK_PUBLIC_SCOPE_CANONICAL:JSON.stringify(scope),SPARK_PUBLIC_MANIFEST_PATH:path.join(root,'manifest.json'),
      SPARK_WORKSPACE_AUTH_ORIGIN:'http://authority.invalid',SPARK_PUSH_WORKSPACE_AUTH_KEY:'k'.repeat(48)});
    let calls=0,denyAt=Infinity;
    global.fetch=async(url,options)=>{
      calls++;
      assert.equal(url,'http://authority.invalid/internal/room-agent/authorize');
      assert.deepEqual(JSON.parse(options.body),{request_id:scope.request_id});
      return {ok:true,text:async()=>JSON.stringify({code:calls>=denyAt?403:0,data:{request_id:scope.request_id,collaboration:scope}})};
    };
    const extension=await jiti.import('../cannbot/pi/extensions/public-workspace.ts');
    const tools={};extension.default({registerTool(tool){tools[tool.name]=tool;}});
    assert.deepEqual(Object.keys(tools),['read']);
    assert(extension.approvedFile(root,manifest,'queue.cpp'));
    for(const bad of ['/etc/passwd','../queue.cpp','manifest.json','.env','queue.cpp/../../x'])
      assert(!extension.approvedFile(root,manifest,bad));
    const result=await tools.read.execute('read-1',{path:'queue.cpp'},undefined,undefined,{});
    assert(result.content.some(item=>item.text?.includes('int queued()')));
    assert.equal(calls,2,'read must be authorized both before and after the native tool');
    denyAt=calls+2;
    await assert.rejects(tools.read.execute('read-2',{path:'queue.cpp'},undefined,undefined,{}));
    denyAt=Infinity;
    await assert.rejects(tools.read.execute('read-3',{path:'../outside'},undefined,undefined,{}));
    fs.writeFileSync(path.join(root,'queue.cpp'),'tampered');
    await assert.rejects(tools.read.execute('read-4',{path:'queue.cpp'},undefined,undefined,{}));
    console.log('public Pi native read: allowed bytes, before/after authorization, escape and tamper checks passed');
  } finally {global.fetch=originalFetch;fs.rmSync(root,{recursive:true,force:true});}
})().catch(error=>{console.error(error);process.exitCode=1;});
