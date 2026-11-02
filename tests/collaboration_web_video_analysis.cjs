// Continue the shared Web media upload/mention path with actual video sources.
// Semantic facts are checked by the existing Python validate_video contract;
// this module exercises visible source navigation and original-file playback.
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const path=require('node:path');

module.exports=async function run({a,b,c,pages,browsers,contexts,users,receipt,errors,base,relayBase,control,
 screenshots,check,wait,details,timeline,login,openRoom,chromium,read,room,final,proposal}){
 assert(Array.isArray(proposal.frames)&&proposal.frames.length===2);
 for(const page of pages)await page.locator('#messages [data-agent-request="'+final.request_id+'"]').waitFor();
 await wait(async()=>JSON.stringify(await timeline(a))===JSON.stringify(await timeline(b))&&JSON.stringify(await timeline(a))===JSON.stringify(await timeline(c)));
 check('three browsers see the same attributed video analysis; ordinary upload and draft selection did not invoke a model');
 const ids=proposal.frames.map(frame=>frame.source_id);
 assert.equal(new Set(ids).size,2);
 const sources=await read(a,'/api/chatroom/agents/sources',{room_id:room,request_id:final.request_id,source_ids:ids});assert.equal(sources.code,0);
 receipt.media.sources=sources.data.items;
 const citations=ids.map(id=>sources.data.items.find(source=>source.source_id===id));
 for(const citation of citations){
  assert(citation?.available);assert.equal(citation.reference.kind,'image');
  assert.equal(citation.reference.asset_id,receipt.media.asset_id);assert.equal(citation.reference.revision,receipt.media.revision);
 }
 receipt.media.citations=citations;
 const open=async(page,citation)=>{
  // The durable final itself exposes source navigation; use its normal button.
  const message=page.locator('#messages [data-agent-request="'+final.request_id+'"]');
  const sourceList=message.locator('[data-source-list]');
  if(!await sourceList.isVisible())await message.getByRole('button',{name:'查看读取来源',exact:true}).click();
  await message.locator('[data-source-id="'+citation.source_id+'"]').click();
  await page.waitForFunction(()=>{const image=document.querySelector('[data-media-preview] img');return image?.complete&&image.naturalWidth>0;});
  const hash=await page.locator('[data-media-preview] img').evaluate(async image=>{
   const bytes=await (await fetch(image.src)).arrayBuffer();
   return [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(value=>value.toString(16).padStart(2,'0')).join('');
  });assert.equal(hash,citation.reference.sha256);
 };
 const play=async(page,citation)=>{
  await open(page,citation);
  await page.locator('[data-media-preview]').getByRole('button',{name:/播放原文件/}).click();
  const seconds=citation.reference.anchor.timestamp_ms/1000;
  await page.waitForFunction(seconds=>{const video=document.querySelector('[data-media-preview] video');return video&&video.readyState>=1&&Math.abs(video.currentTime-seconds)<.15;},seconds,{timeout:60000});
  const player=page.locator('[data-media-preview] video');assert.equal(await player.evaluate(video=>video.paused),true);
  await player.scrollIntoViewIfNeeded();await player.press('Space');
  await page.waitForFunction(seconds=>{const video=document.querySelector('[data-media-preview] video');return video&&!video.error&&!video.paused&&video.currentTime>seconds+.15;},seconds);
  await player.press('Space');assert.equal(await player.evaluate(video=>video.paused),true);
 };
 for(const citation of citations)await open(a,citation);
 const later=citations.reduce((a,b)=>a.reference.anchor.timestamp_ms>b.reference.anchor.timestamp_ms?a:b);
 await play(b,later);await open(c,citations[0]);
 receipt.media.playback_timestamp_ms=later.reference.anchor.timestamp_ms;
 check('cited frames open with exact hashes; the source action seeks the real video without autoplay and native controls advance playback');
 await b.screenshot({path:path.join(screenshots,'video-source-playback.png'),fullPage:true});
 const rows=await timeline(a);
 await fs.writeFile(path.join(control,'restart.request'),'restart');
 await wait(async()=>{try{await fs.access(path.join(control,'restart.ready'));return true;}catch{return false;}},240000);
 await Promise.all(browsers.map(browser=>browser.close()));
 for(let i=0;i<3;i++){
  const browser=await chromium.launch({headless:true});browsers.push(browser);
  contexts[i]=await browser.newContext({viewport:{width:1440,height:1000}});
  const page=await contexts[i].newPage();page.acceptanceBase=i===1?relayBase:base;pages[i]=page;
  page.on('pageerror',error=>errors.push({user:i,message:error.message}));
  await login(page,users[i]);await openRoom(page);
  await wait(async()=>JSON.stringify(await timeline(page))===JSON.stringify(rows));
  await open(page,later);
 }
 await play(pages[1],later);
 check('full service restart and fresh browser profiles recover the same video final, fixed source images and timestamp playback');
 await details(pages[0],'members');await pages[0].locator('[data-space-members]>div').filter({hasText:'Bob'}).getByRole('button',{name:'移除',exact:true}).click();
 await pages[1].locator('#chat-window').waitFor({state:'hidden',timeout:30000});
 assert.equal(await pages[1].locator('[data-media-preview]').textContent(),'');
 const denials={};
 for(const [endpoint,body] of [
  ['/api/room/media/read',{id:receipt.media.asset_id,offset:0,length:1}],
  ['/api/room/media/artifact',{id:receipt.media.asset_id,revision:receipt.media.revision,index:later.reference.index}],
  ['/api/chatroom/agents/sources',{room_id:room,request_id:final.request_id,source_ids:ids}]
 ]){denials[endpoint]=(await read(pages[1],endpoint,body)).code;assert.equal(denials[endpoint],403);}
 receipt.media.revoked_api_codes=denials;
 check('removing the member clears his active player and denies original video, derived frame and Agent source reads');
};
