const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../web_demo/static/index.html'), 'utf8');
const start = source.indexOf('    function advanceSessionCursor(');
const end = source.indexOf('    function renderSingleChatMessage(', start);
const context = {
  sessionCursors: {}, seenSessionSequences: {}, pendingCursorSync: new Set(),
  requests: [], timers: [],
  sendCursorSync(sid, after) { this.requests.push([sid, after]); },
  setTimeout(callback) { context.timers.push(callback); }
};
// Functions in a VM call plain globals with the VM global as their receiver.
context.sendCursorSync = (sid, after) => context.requests.push([sid, after]);
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);
context.advanceSessionCursor('s', 2);
context.handleSyncEnd({session_id:'s', after_seq:0, next_seq:2, has_more:false, safe_seq:0});
context.advanceSessionCursor('s', 3);
assert.equal(context.sessionCursors.s, 0, 'missing 1 must remain a gap');
context.handleSyncEnd({session_id:'s', after_seq:2, next_seq:3, safe_seq:3});
assert.equal(context.sessionCursors.s, 0, 'a later page cannot settle a prior unseen range');
context.advanceSessionCursor('s', 1);
assert.equal(context.sessionCursors.s, 3);
context.advanceSessionCursor('t', 2);
context.handleSyncEnd({session_id:'t', after_seq:0, next_seq:2, safe_seq:2});
assert.equal(context.sessionCursors.t, 2, 'server can prove a sealed unused reservation');
context.advanceSessionCursor('u', 2);
context.handleSyncEnd({session_id:'u', after_seq:0, next_seq:2, has_more:true, safe_seq:0});
assert.deepEqual(context.requests.at(-1), ['u', 2], 'pagination cursor differs from safe recovery cursor');
console.log('gap, settled prefix and pagination regression checks passed');
