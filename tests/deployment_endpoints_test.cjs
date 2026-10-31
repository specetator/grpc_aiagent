const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../web_demo/static/endpoints.js'), 'utf8');
for (const [protocol, host, sameOrigin, apiBase, websocket] of [
  ['http:', 'localhost:9010', true, 'http://localhost:9010', 'ws://localhost:9010/ws'],
  ['https:', 'team.example.org', true, 'https://team.example.org', 'wss://team.example.org/ws'],
  ['http:', 'localhost:9010', false, 'http://localhost:9101', 'ws://localhost:9000/ws']
]) {
  const url = new URL(protocol + '//' + host);
  const context = {window: {SparkEndpointConfig: {sameOrigin}}, location: url};
  vm.runInNewContext(source, context);
  assert.equal(context.window.SparkEndpoints.apiBase, apiBase);
  assert.equal(context.window.SparkEndpoints.websocket, websocket);
}
console.log('deployment endpoints: same origin HTTP/HTTPS and WSL compatibility passed');
