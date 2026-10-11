const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const pi = process.env.SPARK_PI_PACKAGE || '/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent';
const {createJiti} = require(path.join(pi,'node_modules/jiti'));
const jiti = createJiti(__filename,{alias:{typebox:require.resolve('typebox',{paths:[pi]})}});
(async()=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'spark-room-guard-'));
  try {
    const module=await jiti.import('../cannbot/pi/extensions/room-workspace.ts');
    assert(module.permittedPath(root,'operator.cpp'));
    assert(!module.permittedPath(root,'../secret'));
    assert(!module.permittedPath(root,'/etc/passwd'));
    assert(!module.permittedPath(root,'.pi/extensions/evil.ts'));
    fs.symlinkSync('/definitely-not-existing-parent/sensitive',path.join(root,'dangling'));
    assert(!module.permittedPath(root,'dangling'));
    fs.symlinkSync('/tmp',path.join(root,'link'));
    assert(!module.permittedPath(root,'link/escape.cpp'));
    process.env.SPARK_ROOM_WORKSPACE=root;
    for(const developer of [false,true]) {
      process.env.SPARK_ROOM_CAN_DEVELOP=developer?'1':'0';
      let hook,resultHook;let next=0;const tools={};
      module.default({on(name,fn){if(name==='tool_call')hook=fn;if(name==='tool_result')resultHook=fn;},registerTool(tool){tools[tool.name]=tool;}});
      const handler=async event=>{const toolCallId='tool-'+next++;const result=await hook({...event,toolCallId});await resultHook({toolCallId});return result;};
      assert(!(await handler({toolName:'read',input:{path:'operator.cpp'}})));
      assert.equal(Boolean((await handler({toolName:'write',input:{path:'operator.cpp'}}))?.block),!developer);
      assert((await handler({toolName:'bash',input:{command:'echo unsafe'}})).block);
      assert((await handler({toolName:'read',input:{path:'/etc/passwd'}})).block);
      assert(!(await handler({toolName:'cann_knowledge_search',input:{query:'Tiling'}})));
      assert.equal(Boolean((await handler({toolName:'room_run_tests',input:{}}))?.block),!developer);
      delete process.env.SPARK_ROOM_TEST_RUNNER;
      assert((await tools.room_run_tests.execute()).content[0].text.includes('不能宣称测试通过'));
      if(developer) {
        await hook({toolName:'write',toolCallId:'writer',input:{path:'operator.cpp'}});
        let acquired=false;
        const test=hook({toolName:'room_run_tests',toolCallId:'runner',input:{}}).then(()=>acquired=true);
        await new Promise(resolve=>setImmediate(resolve));assert(!acquired,'test raced a file writer');
        await resultHook({toolCallId:'writer'});await test;assert(acquired);
        await resultHook({toolCallId:'runner'});
      }
    }
    console.log('room workspace guard: path, symlink, read-only/developer and fixed runner checks passed');
  } finally { fs.rmSync(root,{recursive:true,force:true}); }
})().catch(error=>{console.error(error);process.exit(1);});
