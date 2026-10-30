const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const pi = process.env.SPARK_PI_PACKAGE || path.join(os.homedir(), '.local/node/lib/node_modules/@earendil-works/pi-coding-agent');
if (!fs.existsSync(path.join(pi, 'node_modules/jiti'))) {
  console.log('SKIP: installed Pi SDK is unavailable; set SPARK_PI_PACKAGE');
  process.exit(77);
}
const { createJiti } = require(path.join(pi, 'node_modules/jiti'));
const jiti = createJiti(__filename, { alias: {
  typebox: require.resolve('typebox', { paths: [pi] }),
  '@earendil-works/pi-coding-agent': path.join(pi, 'dist/index.js'),
} });
const tick = () => new Promise(resolve => setImmediate(resolve));
const gate = () => { let resolve; const promise = new Promise(done => resolve = done); return { promise, resolve }; };

class FakeSession {
  messages = []; listeners = []; disposed = false; prompts = []; aborts = 0; abortGate;
  prompt(text) { this.prompts.push(text); this.pending = gate(); return this.pending.promise; }
  subscribe(fn) { this.listeners.push(fn); return () => this.listeners.splice(this.listeners.indexOf(fn), 1); }
  complete(text = 'verified evidence', stopReason = 'stop') {
    this.messages.push({ role: 'assistant', stopReason, content: [{ type: 'text', text }] });
    this.pending?.resolve();
  }
  async abort() { this.aborts++; await this.abortGate?.promise; this.complete('', 'aborted'); }
  dispose() { this.disposed = true; }
}

(async () => {
  const { AgentControl, scopedPath, OUTPUT_LIMIT, MAX_TOOL_CALLS } = await jiti.import('../cannbot/pi/extensions/lib/kernel-agent-control.ts');
  const extension = await jiti.import('../cannbot/pi/extensions/kernel-subagents.ts');
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kernel-subagents-unit-'));
  const controls = [];
  let checks = 0;
  const ok = (value, message) => { assert(value, message); checks++; };
  const throws = (fn, regex) => { assert.throws(fn, regex); checks++; };
  const rejects = async (fn, regex) => { await assert.rejects(fn, regex); checks++; };
  function make(extra = {}) {
    const sessions = []; const specs = [];
    const control = new AgentControl({ owner: 'parent-a', root, parentTools: ['read', 'write', 'edit', 'bash', 'cann_knowledge_search'],
      factory: async spec => { specs.push(spec); const session = new FakeSession(); sessions.push(session); return session; }, ...extra });
    controls.push(control); return { control, sessions, specs };
  }
  try {
    fs.writeFileSync(path.join(root, 'input.cpp'), 'int main() {}');
    fs.symlinkSync('/tmp', path.join(root, 'escape'));
    fs.symlinkSync('/not-existing-sensitive', path.join(root, 'dangling'));
    fs.linkSync(path.join(root, 'input.cpp'), path.join(root, 'hardlink.cpp'));
    for (const name of ['../outside', '/etc/passwd', '.env', 'escape/file', 'dangling', 'hardlink.cpp', '.', 'sub/.hidden/file'])
      throws(() => scopedPath(root, name), /Path|Hidden|Linked|path|files/);
    fs.unlinkSync(path.join(root, 'hardlink.cpp'));
    ok(scopedPath(root, 'new.cpp') === path.join(root, 'new.cpp'), 'new file scoped');

    const { control: c, sessions, specs } = make({ maxChildren: 2 });
    const a = c.spawn({ role: 'researcher', message: 'inspect input.cpp' });
    const b = c.spawn({ role: 'reviewer', message: 'review input.cpp' });
    throws(() => c.spawn({ message: 'third' }), /slots/);
    ok(a.status === 'starting' && b.status === 'starting', 'reserve before async factory');
    await tick();
    ok(specs[0].tools.join(',') === 'read,cann_knowledge_search', 'role subset excludes shell/writes');
    ok(sessions[0] !== sessions[1], 'separate sessions');
    ok((await c.wait([a.id, b.id], 0)).timed_out, 'wait timeout does not cancel');
    const waiter = c.wait([a.id, b.id], 1000);
    sessions[0].complete('first answer');
    const first = await waiter;
    ok(!first.timed_out && first.agents[0].output === 'first answer' && first.agents[1].status === 'running', 'wait-any');
    throws(() => c.spawn({ message: 'full until close' }), /slots/);
    await rejects(() => c.send(b.id, 'do more'), /running/);
    await c.send(a.id, 'follow-up');
    ok(sessions[0].prompts.length === 2, 'same child context reused');
    sessions[0].complete('x'.repeat(OUTPUT_LIMIT + 100));
    const repeated = await c.wait([a.id], 1000);
    ok(repeated.agents[0].output.includes('[truncated;') && repeated.agents[0].output.length < OUTPUT_LIMIT + 100, 'bounded output');
    await c.close(a.id);
    ok(sessions[0].disposed && c.list()[0].status === 'closed', 'close releases session');
    await rejects(() => c.wait(['not-this-parents-child'], 0), /Unknown child/);
    const other = make({ owner: 'parent-b' }).control;
    await rejects(() => other.close(b.id), /Unknown child/);
    const cancellation = new AbortController();
    const cancelledWait = c.wait([b.id], 1000, cancellation.signal);
    cancellation.abort();
    await rejects(() => cancelledWait, /cancelled/);
    ok(c.list().find(row => row.id === b.id).status === 'running', 'wait cancellation alone is not mistaken for successful output');

    const writer = make({ maxChildren: 3 });
    throws(() => writer.control.spawn({ role: 'developer', message: 'edit' }), /allowed_writes/);
    throws(() => writer.control.spawn({ role: 'reviewer', message: 'edit', allowed_writes: ['x.cpp'] }), /Only private/);
    const w = writer.control.spawn({ role: 'developer', message: 'edit', allowed_writes: ['x.cpp'] });
    await tick();
    ok(writer.specs[0].tools.includes('write') && !writer.specs[0].tools.includes('bash'), 'developer restricted tools');
    throws(() => writer.control.spawn({ role: 'developer', message: 'conflict', allowed_writes: ['x.cpp'] }), /overlaps/);
    throws(() => writer.control.spawn({ role: 'developer', message: 'ancestor conflict', allowed_writes: ['x.cpp/child'] }), /overlaps/);
    writer.sessions[0].abortGate = gate();
    const closing = writer.control.close(w.id);
    throws(() => writer.control.spawn({ role: 'developer', message: 'close race', allowed_writes: ['x.cpp'] }), /overlaps/);
    writer.sessions[0].abortGate.resolve(); await closing;
    const nextWriter = writer.control.spawn({ role: 'developer', message: 'after close', allowed_writes: ['x.cpp'] });
    ok(nextWriter.status === 'starting', 'reuse only after cleanup');
    const readonly = make({ parentTools: ['read'] }).control;
    throws(() => readonly.spawn({ role: 'developer', message: 'unauthorized', allowed_writes: ['x.cpp'] }), /no file mutation/);
    const room = make({ room: true });
    throws(() => room.control.spawn({ role: 'developer', message: 'room write', allowed_writes: ['x.cpp'] }), /Only private/);
    room.control.spawn({ role: 'developer', message: 'room review' }); await tick();
    ok(!room.specs[0].tools.includes('write'), 'room roles never get write tools');

    let guard;
    extension.childGuard(writer.specs[0])({ on(name, handler) { if (name === 'tool_call') guard = handler; } });
    ok(guard({ toolName: 'write', input: { path: 'x.cpp' } }).block, 'closed child tools are revoked');
    extension.childGuard(writer.specs[1])({ on(name, handler) { if (name === 'tool_call') guard = handler; } });
    ok(!guard({ toolName: 'write', input: { path: 'x.cpp' } }), 'allowed exact write');
    ok(guard({ toolName: 'write', input: { path: 'other.cpp' } }).block, 'other write blocked');
    ok(guard({ toolName: 'bash', input: { command: 'unsafe' } }).block, 'shell blocked');
    ok(guard({ toolName: 'read', input: { path: '/etc/passwd' } }).block, 'read escape blocked');
    for (let i = 4; i < MAX_TOOL_CALLS; i++) guard({ toolName: 'read', input: { path: 'input.cpp' } });
    ok(guard({ toolName: 'write', input: { path: 'x.cpp' } }).block, 'budget blocks before 49th side effect');

    const interrupted = make();
    const active = interrupted.control.spawn({ message: 'long task' }); await tick();
    interrupted.sessions[0].abortGate = gate();
    const follow = interrupted.control.send(active.id, 'replacement', true);
    await rejects(() => interrupted.control.send(active.id, 'racing replacement', true), /operation is in progress/);
    interrupted.sessions[0].abortGate.resolve(); await follow;
    ok(interrupted.sessions[0].prompts.length === 2 && interrupted.sessions[0].prompts[1] === 'replacement', 'interrupt then follow-up');
    interrupted.sessions[0].complete('done'); await interrupted.control.wait([active.id], 1000);
    await interrupted.control.send(active.id, 'round3'); interrupted.sessions[0].complete(); await interrupted.control.wait([active.id], 1000);
    await interrupted.control.send(active.id, 'round4'); interrupted.sessions[0].complete(); await interrupted.control.wait([active.id], 1000);
    await rejects(() => interrupted.control.send(active.id, 'round5'), /follow-up limit/);

    const failed = make({ maxChildren: 1, factory: async () => { throw new Error('secret-provider-error'); } }).control;
    failed.spawn({ message: 'fail init' }); await tick();
    ok(failed.list()[0].status === 'failed' && !JSON.stringify(failed.list()).includes('secret-provider'), 'failed init is sanitized');
    ok(failed.spawn({ message: 'replacement' }).status === 'starting', 'failed init releases slot');
    const timeout = make({ timeoutMs: 20 });
    const slow = timeout.control.spawn({ message: 'slow' });
    const timed = await timeout.control.wait([slow.id], 1000); await tick();
    ok(timed.agents[0].status === 'timed_out' && timeout.sessions[0].disposed, 'deadline aborts and disposes');
    const lateFactory = gate(); const lateSession = new FakeSession();
    const late = make({ factory: async () => { await lateFactory.promise; return lateSession; } }).control;
    late.spawn({ message: 'starting' }); await late.dispose(); lateFactory.resolve(); await tick();
    ok(lateSession.disposed && !lateSession.prompts.length, 'late factory cannot run after parent disposal');
    const badAbort = make({ timeoutMs: 20, maxChildren: 1 });
    const badAbortId = badAbort.control.spawn({ message: 'abort fails' }).id; await tick();
    badAbort.sessions[0].abort = async () => { throw new Error('abort transport failed'); };
    const unconfirmed = await badAbort.control.wait([badAbortId], 1000); await tick();
    ok(badAbort.control.list()[0].status === 'failed' && badAbort.sessions[0].disposed, 'abort failure is contained');
    throws(() => badAbort.control.spawn({ message: 'unsafe reuse' }), /slots/);
    badAbort.sessions[0].complete('settled eventually'); await tick();
    ok(badAbort.control.spawn({ message: 'safe reuse' }).status === 'starting', 'failed abort releases only after prompt settles');
    await c.dispose();
    ok(sessions[1].disposed && c.list().length === 0, 'parent disposal cancels remaining children');
    throws(() => c.spawn({ message: 'new turn cannot reuse old registry' }), /closed/);
    const bad = make(); const badId = bad.control.spawn({ message: 'failed answer' }).id; await tick();
    bad.sessions[0].complete('not success', 'error');
    ok((await bad.control.wait([badId], 1000)).agents[0].status === 'failed', 'non-stop terminal is not success');
    console.log(`Kernel subagent lifecycle, permission and race tests: ${checks} checks passed`);
  } finally {
    await Promise.allSettled(controls.map(control => control.dispose()));
    fs.rmSync(root, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exit(1); });
