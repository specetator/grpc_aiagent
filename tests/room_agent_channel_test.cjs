// DOM-contract test: ephemeral output, safe text, request/room boundaries.
const assert=require('node:assert/strict');
require('../web_demo/static/room_agent_channel.js');
(async()=>{
  const nodes={};
  const panel={querySelector(selector){return nodes[selector] ||= {textContent:''};}};
  let pending;
  const channel=new RoomAgentChannel(panel,async(path,body)=>{
    if(body.room_id===3) return new Promise(resolve=>pending=resolve);
    return {task:{title:'<img src=x onerror=evil()>',spec:'spec',status:'developing',revision:1,
      summary:'A implemented',next_steps:'B test',developer_user_ids:[]},requests:[],can_develop:false};
  });
  await channel.select(1);
  assert(nodes['[data-task]'].textContent.includes('<img src=x onerror=evil()>'));
  assert(!('innerHTML' in nodes['[data-task]']));
  channel.delta({session_id:'r_1',request_id:'req',actor_user_id:7,delta_index:2,delta:'C'});
  channel.delta({session_id:'r_1',request_id:'req',actor_user_id:7,delta_index:0,delta:'A'});
  channel.delta({session_id:'r_1',request_id:'req',actor_user_id:7,delta_index:1,delta:'B'});
  channel.delta({session_id:'r_1',request_id:'req',actor_user_id:7,delta_index:1,delta:'duplicate'});
  assert(nodes['[data-streams]'].textContent.endsWith('ABC'));
  channel.delta({session_id:'r_2',request_id:'other',delta_index:0,delta:'wrong room'});
  assert(!nodes['[data-streams]'].textContent.includes('wrong room'));
  channel.final('req');
  channel.delta({session_id:'r_1',request_id:'req',delta_index:3,delta:'late'});
  assert.equal(nodes['[data-streams]'].textContent,'');
  const stale=channel.select(3);
  await channel.select(4);
  pending({task:{title:'STALE_ROOM'},requests:[]});await stale;
  assert(!nodes['[data-task]'].textContent.includes('STALE_ROOM'));
  console.log('room Agent UI: safe text, attribution, ordering, deduplication, final authority and stale-room checks passed');
})().catch(error=>{console.error(error);process.exit(1);});
