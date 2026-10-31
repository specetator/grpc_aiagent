const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../web_demo/static/index.html'), 'utf8');
const start = source.indexOf('    function advanceSessionCursor(');
const end = source.indexOf('    function renderSingleChatMessage(', start);
const context = {
  sessionCursors: {}, seenSessionSequences: {}, pendingCursorSync: new Set(),cursorSyncStarted:new Map(),
  authEpoch:0,wsGeneration:0,
  revokedRooms:new Set(),roomAccessEpoch:new Map(),
  clearCursorSync(sid) { for(const key of context.pendingCursorSync)if(key.startsWith(sid+':'))context.pendingCursorSync.delete(key); },
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
const requestsBefore=context.requests.length;
context.authEpoch++;
context.timers.forEach(timer=>timer());
assert.equal(context.requests.length,requestsBefore,'old login gap retry cannot target a new account');
context.revokedRooms.add(7);
context.handleSyncEnd({session_id:'r_7',after_seq:0,next_seq:10,safe_seq:10,has_more:true});
assert.equal(context.sessionCursors.r_7,undefined,'revoked room sync completion cannot restore cursors');
console.log('gap, settled prefix and pagination regression checks passed');

// Exercise the actual recovery scheduler with a deterministic clock. An empty
// reconnect result must not permanently stop recovery of a later SQL tail.
let now=100,visibleHandler;
const recovery={sessionCursors:{r_7:7},seenSessionSequences:{},pendingCursorSync:new Set(),cursorSyncStarted:new Map(),
 sessionWorkspaces:new Map(),revokedRooms:new Set(),roomAccessEpoch:new Map(),authEpoch:1,wsGeneration:1,
 token:'fixture',currentSessionId:'r_7',historyReady:true,document:{hidden:false,addEventListener(name,fn){assert.equal(name,'visibilitychange');visibleHandler=fn;}},
 WebSocket:{OPEN:1},ws:{readyState:1,send(value){recovery.sent.push(JSON.parse(value));}},
 Date:{now:()=>now},sent:[],intervals:[],setInterval(fn,delay){recovery.intervals.push({fn,delay});},setTimeout(){}};
vm.createContext(recovery);
vm.runInContext(source.slice(source.indexOf('    function clearCursorSync('),end),recovery);
assert.equal(recovery.intervals.length,1);assert.equal(recovery.intervals[0].delay,5000);
const tick=recovery.intervals[0].fn;
tick();assert.equal(recovery.sent.length,1);assert.equal(recovery.sent[0].after_seq,7);
recovery.sendCursorSync('r_7',8);tick();assert.equal(recovery.sent.length,1,'same-session gap and periodic sync are serialized');
recovery.handleSyncEnd({session_id:'r_7',after_seq:7,next_seq:7,safe_seq:7});
now+=5000;tick();assert.equal(recovery.sent.length,2,'an empty result must not end ongoing tail reconciliation');
now+=15000;tick();assert.equal(recovery.sent.length,3,'lost sync_end has a bounded recovery path');
recovery.advanceSessionCursor('r_7',8);recovery.handleSyncEnd({session_id:'r_7',after_seq:7,next_seq:8,safe_seq:8});
assert.equal(recovery.sessionCursors.r_7,8);
recovery.document.hidden=true;tick();assert.equal(recovery.sent.length,3);
recovery.document.hidden=false;visibleHandler();assert.equal(recovery.sent.length,4,'becoming visible immediately reconciles');
recovery.currentSessionId='s_7_8';recovery.reconcileCurrentConversation();assert.equal(recovery.sent.at(-1).session_id,'s_7_8');
const count=recovery.sent.length;
recovery.token=null;now+=20000;tick();assert.equal(recovery.sent.length,count,'logout stops periodic requests');
recovery.token='next';recovery.currentSessionId='r_7';recovery.revokedRooms.add(7);tick();assert.equal(recovery.sent.length,count,'revoked room cannot be reconciled');
recovery.currentSessionId='s_7_8';recovery.ws.readyState=3;tick();assert.equal(recovery.sent.length,count,'offline timer sends nothing');
recovery.ws.readyState=1;recovery.historyReady=false;tick();assert.equal(recovery.sent.length,count,'history establishes the initial cursor before periodic sync');
console.log('ongoing tail recovery, pending expiry, visibility and authorization regression checks passed');
recovery.currentSessionId='r_9';recovery.historyReady=true;recovery.sessionCursors.r_9=7;
tick();recovery.advanceSessionCursor('r_9',8);now+=15000;tick();
const lateCount=recovery.sent.length;
recovery.handleSyncEnd({session_id:'r_9',after_seq:7,next_seq:9,safe_seq:7,has_more:true});
assert.equal(recovery.sent.length,lateCount,'expired different page cannot resume old pagination');
assert.equal(recovery.cursorSyncStarted.get('r_9').after,8,'old page cannot release newer pending request');
assert.equal(recovery.sessionCursors.r_9,8);
now+=15000;tick();assert.equal(recovery.sent.length,lateCount+1);
recovery.advanceSessionCursor('r_9',9);recovery.advanceSessionCursor('r_9',9);
recovery.handleSyncEnd({session_id:'r_9',after_seq:8,next_seq:9,safe_seq:9});
assert.equal(recovery.sessionCursors.r_9,9,'same-page retry end is equivalent and never regresses the cursor');
assert.equal(recovery.cursorSyncStarted.has('r_9'),false);
assert.equal(recovery.sent.length,lateCount+1,'same-page completion needs no immediate duplicate sync');
console.log('late different-page and equivalent-page completion regression checks passed');
(async()=>{
 const discoveries=[],deadlines=[];
 recovery.pendingConversationDiscovery=null;recovery.AbortController=AbortController;
 recovery.setTimeout=fn=>{deadlines.push(fn);return deadlines.length;};recovery.clearTimeout=()=>{};
 recovery.loadSingleSessionList=({signal})=>new Promise(resolve=>{discoveries.push({resolve,signal});signal.addEventListener('abort',resolve);});
 recovery.currentSessionId='s_8_9';recovery.sessionWorkspaces.set('s_8_9',{exists:false});
 const sent=recovery.sent.length;
 tick();tick();assert.equal(discoveries.length,1);assert.equal(recovery.sent.length,sent,'absent session is only discovered, never synthesized');
 deadlines.at(-1)();await Promise.resolve();assert.equal(recovery.pendingConversationDiscovery,null,'timed-out discovery releases its gate');
 tick();assert.equal(discoveries.length,2);
 recovery.sessionWorkspaces.get('s_8_9').exists=true;discoveries[1].resolve();await Promise.resolve();
 assert.equal(recovery.sent.length,sent+1);assert.equal(recovery.sent.at(-1).after_seq,0,'directory metadata must not advance the message cursor');
 tick();assert.equal(discoveries.length,2,'an established session stops directory discovery');
 recovery.currentSessionId='s_9_10';recovery.sessionWorkspaces.set('s_9_10',{exists:false});tick();
 const old=discoveries[2];recovery.authEpoch++;recovery.pendingConversationDiscovery=null;
 recovery.currentSessionId='s_11_12';recovery.sessionWorkspaces.set('s_11_12',{exists:false});tick();
 const active=recovery.pendingConversationDiscovery;old.resolve();await Promise.resolve();
 assert.equal(recovery.pendingConversationDiscovery,active,'old-account completion cannot clear new-account discovery');
 assert.equal(recovery.sent.length,sent+1,'old-account discovery cannot start sync for a new identity');
 active.controller.abort();await Promise.resolve();
 console.log('first-session discovery, timeout and account-epoch regression checks passed');
})().catch(error=>{console.error(error);process.exitCode=1;});
