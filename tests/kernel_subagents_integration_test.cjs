/** Real Pi SDK sessions + a loopback OpenAI-compatible fixture, no live API key. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { once } = require('node:events');

const pi = process.env.SPARK_PI_PACKAGE || path.join(os.homedir(), '.local/node/lib/node_modules/@earendil-works/pi-coding-agent');
if (!fs.existsSync(path.join(pi, 'dist/index.js'))) {
  console.log('SKIP: Pi SDK is not installed; set SPARK_PI_PACKAGE to its package directory.');
  process.exit(77);
}
const { createJiti } = require(path.join(pi, 'node_modules/jiti'));
const jiti = createJiti(__filename, { alias: {
  '@earendil-works/pi-coding-agent': path.join(pi, 'dist/index.js'),
  typebox: require.resolve('typebox', { paths: [pi] }),
} });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const decodeText = value => typeof value === 'string' ? value : (value || []).map(item => item.text || '').join('');

(async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kernel-sdk-integration-'));
  const workspace = path.join(root, 'workspace');
  const agentDir = path.join(root, 'agent-home');
  fs.mkdirSync(workspace); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(workspace, 'input.cpp'), 'int kernel_input = 7;\n');
  fs.writeFileSync(path.join(root, 'outside.txt'), 'private outside fixture');
  const previousHome = process.env.PI_CODING_AGENT_DIR;
  const previousRoom = process.env.SPARK_ROOM_WORKSPACE;
  const previousEnabled = process.env.KERNEL_AGENT_SUBAGENTS_ENABLED;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.KERNEL_AGENT_SUBAGENTS_ENABLED = 'true';
  delete process.env.SPARK_ROOM_WORKSPACE;
  const requests = [];
  const held = new Set();
  const sockets = new Set();
  let cleanupExtension = async () => {};
  let checks = 0;
  const check = (condition, message) => { assert(condition, message); checks++; };
  const server = http.createServer(async (request, response) => {
    try {
      let raw = '';
      for await (const chunk of request) raw += chunk;
      const body = JSON.parse(raw);
      requests.push(body);
      assert.equal(request.headers.authorization, 'Bearer kernel-fixture-only');
      assert.equal(request.url, '/v1/chat/completions');
      const users = body.messages.filter(item => item.role === 'user');
      const task = decodeText(users.at(-1)?.content).match(/KERNEL_TEST:([a-z_]+)/)?.[1] || 'unknown';
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      const frame = (delta, finishReason = null) => response.write(`data: ${JSON.stringify({
        id: 'kernel-fixture', object: 'chat.completion.chunk', created: 1, model: 'kernel-fixture',
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      })}\n\n`);
      const callTool = (name, args, id) => {
        frame({ role: 'assistant', tool_calls: [{ index: 0, id,
          type: 'function', function: { name, arguments: JSON.stringify(args) } }] });
        frame({}, 'tool_calls');
      };
      if (task === 'hold') {
        held.add(response);
        response.on('close', () => held.delete(response));
        frame({ role: 'assistant', content: '' });
        return;
      }
      const last = body.messages.at(-1);
      if (task === 'parent_orchestration') {
        const previousCall = body.messages.flatMap(item => item.tool_calls || [])
          .find(item => item.id === last?.tool_call_id);
        if (last?.role === 'user') {
          callTool('spawn_agent', { role: 'researcher', message: 'KERNEL_TEST:echo' }, 'parent_spawn');
        } else if (previousCall?.function.name === 'spawn_agent') {
          const child = JSON.parse(decodeText(last.content));
          callTool('wait_agent', { ids: [child.id], timeout_ms: 15000 }, 'parent_wait');
        } else if (previousCall?.function.name === 'wait_agent') {
          const result = JSON.parse(decodeText(last.content));
          assert.equal(result.agents[0].status, 'completed');
          assert.equal(result.agents[0].output, 'child-complete:echo');
          callTool('close_agent', { id: result.agents[0].id }, 'parent_close');
        } else {
          assert.equal(previousCall?.function.name, 'close_agent');
          frame({ role: 'assistant', content: 'parent-collected-real-child-result' });
          frame({}, 'stop');
        }
      } else if (last?.role === 'tool') {
        frame({ role: 'assistant', content: `tool-result: ${decodeText(last.content)}` });
        frame({}, 'stop');
      } else if (task.startsWith('write_') || task === 'read_outside') {
        const tool = task === 'read_outside' ? 'read' : 'write';
        const file = task === 'write_allowed' ? 'allowed.cpp'
          : task === 'read_outside' ? '../outside.txt' : 'forbidden.cpp';
        const args = tool === 'read' ? { path: file } : { path: file, content: 'int child_value = 42;\n' };
        callTool(tool, args, `call_${task}`);
      } else {
        frame({ role: 'assistant', content: `child-complete:${task}` });
        frame({}, 'stop');
      }
      response.end('data: [DONE]\n\n');
    } catch (error) {
      response.destroy(error);
    }
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const sdk = await import(pathToFileURL(path.join(pi, 'dist/index.js')).href);
    const { default: extension } = await jiti.import('../cannbot/pi/extensions/kernel-subagents.ts');
    const runtime = await sdk.ModelRuntime.create({ authPath: path.join(agentDir, 'auth.json'),
      modelsPath: null, refreshOnCreate: false, allowModelNetwork: false });
    runtime.registerProvider('kernel-fixture', { baseUrl: `http://127.0.0.1:${server.address().port}/v1`,
      api: 'openai-completions', models: [{ id: 'kernel-fixture', name: 'Kernel local fixture',
        reasoning: false, input: ['text'], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 32768, maxTokens: 2048 }] });
    // Deliberately runtime-only: child auth inheritance must not depend on auth.json.
    await runtime.setRuntimeApiKey('kernel-fixture', 'kernel-fixture-only');
    const registry = new sdk.ModelRegistry(runtime);
    const model = runtime.getModel('kernel-fixture', 'kernel-fixture');
    assert(model);
    const hooks = new Map();
    const tools = new Map();
    const activeTools = ['read', 'write', 'edit', 'bash', 'spawn_agent', 'send_input', 'wait_agent', 'close_agent', 'list_agents'];
    extension({ registerTool(tool) { tools.set(tool.name, tool); },
      on(event, callback) { if (!hooks.has(event)) hooks.set(event, []); hooks.get(event).push(callback); },
      getActiveTools() { return activeTools; }, getThinkingLevel() { return 'off'; } });
    check(tools.size === 5, 'All five lifecycle tools must register');
    const emit = async (event, ctx) => { for (const callback of hooks.get(event) || []) await callback({ type: event }, ctx); };
    let parentAbort = new AbortController();
    let owner = 'parent-one';
    const context = () => ({ cwd: workspace, model, modelRegistry: registry, thinkingLevel: 'off',
      sessionManager: { getSessionId: () => owner }, signal: parentAbort.signal,
      ui: {}, hasUI: false, mode: 'rpc', isProjectTrusted: () => true });
    cleanupExtension = () => emit('session_shutdown', context());
    const invoke = async (name, params = {}, ctx = context()) => {
      const result = await tools.get(name).execute(`fixture_${name}`, params, ctx.signal, undefined, ctx);
      const value = JSON.parse(result.content[0].text);
      if (result.isError) throw new Error(`${name}: ${value.error}`);
      return value;
    };
    const finished = async id => {
      const result = await invoke('wait_agent', { ids: [id], timeout_ms: 15000 });
      check(!result.timed_out, 'Local SDK request should complete');
      check(result.agents[0].status === 'completed', JSON.stringify(result));
      return result.agents[0];
    };
    const spawn = (task, options = {}) => invoke('spawn_agent', { message: `KERNEL_TEST:${task}`, ...options });
    let child = await spawn('echo');
    check((await finished(child.id)).output === 'child-complete:echo', 'Real SDK child completion reaches parent');
    check(requests.length > 0, 'The real SDK called the local HTTP fixture');
    check(requests[0].tools.every(item => !['bash', 'spawn_agent', 'send_input'].includes(item.function.name)),
      'Children must not inherit shell or delegation');
    await invoke('send_input', { id: child.id, message: 'KERNEL_TEST:follow_up' });
    check((await finished(child.id)).output === 'child-complete:follow_up', 'Follow-up uses the existing child');
    check(requests.at(-1).messages.some(item => decodeText(item.content).includes('child-complete:echo')),
      'Child follow-up retains its own history');
    await invoke('close_agent', { id: child.id });

    child = await spawn('write_allowed', { role: 'developer', allowed_writes: ['allowed.cpp'] });
    await finished(child.id);
    check(fs.readFileSync(path.join(workspace, 'allowed.cpp'), 'utf8') === 'int child_value = 42;\n',
      'Actual Pi write tool may edit its explicitly delegated file');
    await invoke('close_agent', { id: child.id });
    child = await spawn('write_forbidden', { role: 'developer', allowed_writes: ['allowed.cpp'] });
    check((await finished(child.id)).output.includes('allowed_writes'), 'Runtime guard rejects undelegated write');
    check(!fs.existsSync(path.join(workspace, 'forbidden.cpp')), 'Rejected write has no file effect');
    await invoke('close_agent', { id: child.id });
    child = await spawn('read_outside');
    const deniedRead = await finished(child.id);
    check(!deniedRead.output.includes('private outside fixture'), 'Child cannot read outside parent workspace');
    check(deniedRead.output.includes('escapes'), 'Read denial is returned as real tool feedback');
    await invoke('close_agent', { id: child.id });

    child = await spawn('hold');
    for (let count = 0; count < 200 && !held.size; count++) await delay(10);
    check(held.size > 0, 'Cancellation test reached an active model request');
    const pending = await invoke('wait_agent', { ids: [child.id], timeout_ms: 1 });
    check(pending.timed_out && pending.agents[0].status === 'running', 'Wait timeout does not cancel');
    await emit('session_before_switch', context());
    owner = 'parent-two'; parentAbort = new AbortController();
    check((await invoke('list_agents')).agents.length === 0, 'Session switch clears previous parent children');
    await assert.rejects(() => invoke('send_input', { id: child.id, message: 'KERNEL_TEST:echo' }), /Unknown child/);
    checks++;
    for (let count = 0; count < 100 && held.size; count++) await delay(10);
    check(held.size === 0, 'Parent switch aborts actual child HTTP stream');
    child = await spawn('echo');
    await finished(child.id);
    check(!requests.at(-1).messages.some(item => decodeText(item.content).includes('child-complete:follow_up')),
      'New parent does not receive previous child history');
    await cleanupExtension();

    // Complete parent -> spawn -> child -> wait -> close -> parent flow using
    // actual Pi tool dispatch and actual lifecycle hooks, not the capture API.
    const settings = sdk.SettingsManager.inMemory({ defaultProjectTrust: 'never',
      retry: { enabled: false }, compaction: { enabled: false }, enableAnalytics: false, enableInstallTelemetry: false });
    const loader = new sdk.DefaultResourceLoader({ cwd: workspace, agentDir, settingsManager: settings,
      noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [extension], systemPrompt: 'Exercise the Kernel Agent tools using the fixture instructions.' });
    await loader.reload();
    check(loader.getExtensions().errors.length === 0, 'Real parent SDK loads the extension');
    const { session: parent } = await sdk.createAgentSession({ cwd: workspace, agentDir, model,
      modelRuntime: runtime, thinkingLevel: 'off', settingsManager: settings, resourceLoader: loader,
      tools: ['read', 'spawn_agent', 'send_input', 'wait_agent', 'close_agent', 'list_agents'],
      sessionManager: sdk.SessionManager.inMemory(workspace) });
    try {
      await parent.prompt('KERNEL_TEST:parent_orchestration');
      const answer = parent.messages.filter(item => item.role === 'assistant').at(-1);
      check(answer.stopReason === 'stop' && answer.content.some(item => item.text === 'parent-collected-real-child-result'),
        'Actual parent dispatches, waits, closes and receives child evidence');
      const executed = parent.messages.filter(item => item.role === 'toolResult').map(item => item.toolName);
      check(['spawn_agent', 'wait_agent', 'close_agent'].every(name => executed.includes(name)),
        'Parent transcript records real lifecycle tool results');
    } finally { await parent.abort(); parent.dispose(); }
    console.log(`Kernel subagent real Pi SDK integration: ${checks} checks passed`);
  } finally {
    await cleanupExtension();
    for (const response of held) response.destroy();
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
    if (previousHome === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousHome;
    if (previousRoom === undefined) delete process.env.SPARK_ROOM_WORKSPACE; else process.env.SPARK_ROOM_WORKSPACE = previousRoom;
    if (previousEnabled === undefined) delete process.env.KERNEL_AGENT_SUBAGENTS_ENABLED; else process.env.KERNEL_AGENT_SUBAGENTS_ENABLED = previousEnabled;
    fs.rmSync(root, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
