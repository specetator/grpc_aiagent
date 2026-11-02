// Owned HTTP integration driver: real Pi extension and Logic transport, no model.
// Credentials arrive on stdin and are never included in output or error messages.
const assert = require('node:assert/strict');
const path = require('node:path');
const crypto = require('node:crypto');
const pi = process.env.SPARK_PI_PACKAGE || '/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent';
const {createJiti} = require(path.join(pi, 'node_modules/jiti'));
const jiti = createJiti(__filename, {alias:{'@earendil-works/pi-coding-agent':path.join(pi,'dist/index.js')}});
(async () => {
  let raw = '';
  for await (const chunk of process.stdin) raw += chunk;
  const input = JSON.parse(raw);
  const extension = await jiti.import('../cannbot/pi/extensions/room-context.ts');
  const tools = Object.fromEntries(extension.createRoomTools(input.scope,input.origin,input.key).map(t=>[t.name,t]));
  const call = () => tools[input.tool].execute('http-integration',input.arguments,undefined,undefined,
    {model:{input:['text','image']}}); // Controlled capability, not a model E2E claim.
  if (input.revoked) await assert.rejects(call(), /revoked/);
  else {
    const result = await call();
    assert(result.details.source_ids.includes(input.source_id));
    if (input.sha256) {
      const item = result.content.find(c=>c.type==='image');
      assert.equal(item.mimeType,'image/jpeg');
      assert.equal(crypto.createHash('sha256').update(Buffer.from(item.data,'base64')).digest('hex'),input.sha256);
    }
  }
  console.log('Pi extension real Logic transport passed');
})().catch(error=>{console.error('Pi extension HTTP contract failed:',error.name);process.exitCode=1;});
