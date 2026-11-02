// Actual Pi extension loader and native result types; fixture authority, no model/SQL E2E.
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/room-context.json')));
const pi = process.env.SPARK_PI_PACKAGE || '/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent';
const {createJiti} = require(path.join(pi, 'node_modules/jiti'));
const jiti = createJiti(__filename, {alias: {'@earendil-works/pi-coding-agent': path.join(pi, 'dist/index.js')}});
(async () => {
  const extension = await jiti.import('../cannbot/pi/extensions/room-context.ts');
  const scope = fixture.scope;
  let requests = [], result = structuredClone(fixture.image), revoked = false, revokeAfter = false, raw;
  const transport = async (url, options) => {
    const args = JSON.parse(options.body); requests.push({url, args});
    assert.equal(args.request_id, scope.request_id);
    assert.equal(options.redirect, 'error');
    assert(options.signal instanceof AbortSignal);
    if (url.endsWith('/authorize')) return new Response(JSON.stringify({code: revoked ? 403 : 0,
      data: {request_id:scope.request_id, collaboration:scope}}));
    if (revokeAfter) revoked = true;
    return new Response(raw ?? JSON.stringify({code:0, data:result}));
  };
  const tools = Object.fromEntries(extension.createRoomTools(scope, 'http://authority.invalid', 'k'.repeat(48), transport).map(t => [t.name, t]));
  assert.deepEqual(Object.keys(tools).sort(), ['room_read', 'room_read_media', 'room_search']);
  for (const tool of Object.values(tools)) {
    for (const key of ['request_id','room_id','actor_user_id','path','runtime_url'])
      assert(!Object.hasOwn(tool.parameters.properties, key));
  }
  const args = {asset_id:'a'.repeat(32), revision:'b'.repeat(64), index:0};
  const context = {model:{input:['text','image']}};
  const read = () => tools.room_read_media.execute('tool-call', args, undefined, undefined, context);
  for (const unavailable of [undefined, {}, {model:{}}, {model:{input:['text']}}, {model:{input:'image'}}])
    await assert.rejects(tools.room_read_media.execute('unsupported', args, undefined, undefined, unavailable), /Image input is unavailable/);
  assert.equal(requests.length, 0); // Unsupported/unknown models do not fetch protected image bytes.
  const value = await read();
  assert.equal(value.content[1].type, 'image');
  assert.equal(value.content[1].mimeType, 'image/jpeg');
  assert.equal(value.content[1].data, fixture.image.items[0].data);
  assert(!value.content[0].text.includes(fixture.image.items[0].data));
  assert(!JSON.stringify(value.details).includes(fixture.image.items[0].data));
  assert.equal(requests.length, 3);
  requests = [];
  for (const input of [{request_id:scope.request_id},{room_id:8},{limit:true},{limit:1.5},{limit:-1},
      {query:'中'.repeat(81)},{query:'\0'},{path:'/private'},{before_seq:2**53}])
    await assert.rejects(tools.room_search.execute('bad',input));
  assert.equal(requests.length, 0);
  revokeAfter = true; await assert.rejects(read(), /revoked/); revokeAfter = revoked = false;
  for (const change of [v=>v.request_id='other',v=>v.context_generation=true,
      v=>v.items[0].reference.room_id=8,v=>v.items[0].reference.asset_id='other',
      v=>v.items[0].reference.revision='e'.repeat(64),v=>v.items[0].reference.index=1,
      v=>v.items[0].mime='image/png',v=>v.items[0].data='not base64!',
      v=>v.items[0].metadata.sha256='e'.repeat(64)]) {
    result = structuredClone(fixture.image); change(result); await assert.rejects(read());
  }
  result = structuredClone(fixture.image);
  for (const source of ['private body not JSON', 'x'.repeat(2*1024*1024+1)]) {
    raw = source; await assert.rejects(read(), error => !error.message.includes('private body'));
  }
  raw = undefined; result = {...fixture.image,items:[]};
  const empty = await tools.room_search.execute('empty',{});
  assert.deepEqual(JSON.parse(empty.content[0].text).items,[]);
  for (const key of ['context_generation','workspace_generation','project_id','source_msg_id']) {
    const legacy = {...scope}; delete legacy[key];
    assert.throws(()=>extension.createRoomTools(legacy,'http://authority.invalid','k'.repeat(48),transport));
  }
  console.log('Pi room tools: native image shape, bounded transport, trusted identity, post-read revocation, malformed responses and legacy denial passed (contract fixtures).');
})().catch(error => { console.error(error); process.exitCode = 1; });
