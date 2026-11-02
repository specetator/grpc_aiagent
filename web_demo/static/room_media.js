/* Room material library. Every read/write uses authenticated Logic APIs. */
(function(global){
'use strict';
const CHUNK=1024*1024, MAX=256*CHUNK;
const TYPES={txt:'text/plain',md:'text/markdown',pdf:'application/pdf',docx:'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  png:'image/png',jpg:'image/jpeg',jpeg:'image/jpeg',gif:'image/gif',webp:'image/webp',
  wav:'audio/wav',mp3:'audio/mpeg',flac:'audio/flac',ogg:'audio/ogg',m4a:'audio/mp4',mp4:'video/mp4',webm:'video/webm'};
// Current delivery scope: documents and images. Keep legacy formats/read paths
// for stored data; this presentation switch is not a server permission rule.
const AUDIO_VIDEO_ENABLED=false;
const UPLOAD_TYPES=Object.fromEntries(Object.entries(TYPES).filter(([,mime])=>AUDIO_VIDEO_ENABLED||!/^(audio|video)\//.test(mime)));
const LABEL={uploading:'待续传',pending:'等待处理',queued:'等待处理',running:'正在解析',processing:'正在解析',
  ready:'可供 Agent 阅读',failed:'处理失败',cancelled:'已取消',cancelling:'正在取消',expired:'已取消',unknown:'状态待核实'};
const hex=buffer=>Array.from(new Uint8Array(buffer),v=>v.toString(16).padStart(2,'0')).join('');
const sha=async buffer=>hex(await crypto.subtle.digest('SHA-256',buffer));
function encode(bytes){let value='';for(let i=0;i<bytes.length;i+=8192)value+=String.fromCharCode(...bytes.subarray(i,i+8192));return btoa(value);}
function decode(value){return Uint8Array.from(atob(value),c=>c.charCodeAt(0));}
function element(tag,text){const node=document.createElement(tag);if(text!==undefined)node.textContent=text;return node;}
function button(text,action){const node=element('button',text);node.type='button';node.onclick=action;return node;}
const size=value=>value<CHUNK?Math.ceil(value/1024)+' KB':(value/CHUNK).toFixed(1)+' MB';
function anchor(value){if(!value)return '';if(value.page)return '第 '+value.page+' 页';const ms=value.timestamp_ms??value.start_ms;
  if(ms!==undefined)return Math.floor(ms/60000)+':'+String(Math.floor(ms/1000)%60).padStart(2,'0');return value.section||'';}
const mediaType=item=>item.mime||TYPES[item.name.split('.').pop().toLowerCase()]||'';
const playable=item=>/^(audio|video)\//.test(mediaType(item));

class RoomMedia {
  constructor(root,api,agents,input,options={}){
    Object.assign(this,{root,api,agents,input,options,room:0,version:0,uploadVersion:0,urls:[],items:[],before:0,busy:false,controllers:new Set()});
    root.innerHTML=`<h3>房间资料</h3><p class="help">分享文本、Markdown、PDF、文档和图片。处理完成后，可请房间 Agent 阅读和分析。</p>
      <label class="media-upload">上传或继续上传<input type="file" data-media-file accept="${Object.keys(UPLOAD_TYPES).map(extension=>'.'+extension).join(',')}"></label>
      <p class="help">网页单文件上限 256 MB。中断后重新选择同一文件即可续传；上传本身不会调用模型。</p>
      <progress data-media-progress max="100" value="0" hidden></progress>
      <button type="button" data-media-pause hidden>暂停上传</button><button type="button" data-media-refresh>刷新资料</button>
      <p data-media-status role="status" aria-live="polite"></p><div data-media-drafts></div><div data-media-list></div>
      <button type="button" data-media-more hidden>更多资料</button>
      <form data-media-search><label>检索资料正文<input type="search" data-media-query placeholder="例如：Kafka、设计决策、错误日志" maxlength="240"></label>
        <button type="submit">检索</button><button type="button" data-media-search-clear>清除检索</button></form>
      <div data-media-matches aria-live="polite"></div>
      <section data-media-preview hidden aria-label="资料内容"></section>`;
    this.file=root.querySelector('[data-media-file]');this.file.disabled=true;
    this.file.onchange=()=>{const file=this.file.files[0];this.file.value='';if(file)this.upload(file);};
    root.querySelector('[data-media-refresh]').onclick=()=>this.refresh();
    root.querySelector('[data-media-more]').onclick=()=>this.refresh(true);
    root.querySelector('[data-media-pause]').onclick=()=>{this.uploadVersion++;this.notice('已暂停。重新选择同一文件可继续上传。');};
    this.query=root.querySelector('[data-media-query]');this.query.disabled=true;
    root.querySelector('[data-media-search]').onsubmit=event=>{event.preventDefault();this.search();};
    root.querySelector('[data-media-search-clear]').onclick=()=>{this.clearSearch();this.query.focus();};
  }
  notice(text){this.root.querySelector('[data-media-status]').textContent=text;}
  clearPreview(){this.urls.forEach(url=>URL.revokeObjectURL(url));this.urls=[];const box=this.root.querySelector('[data-media-preview]');
    for(const player of box.querySelectorAll('audio,video')){player.pause();player.removeAttribute('src');player.load();}
    box.replaceChildren();box.hidden=true;this.previewVersion=(this.previewVersion||0)+1;}
  clearSearch(){this.searchVersion=(this.searchVersion||0)+1;this.query.value='';this.root.querySelector('[data-media-matches]').replaceChildren();}
  select(room){this.version++;this.uploadVersion++;this.room=Number(room);this.items=[];this.before=0;this.busy=false;this.loading=false;
    for(const controller of this.controllers)controller.abort();this.controllers.clear();
    clearInterval(this.timer);this.clearPreview();this.clearSearch();this.file.disabled=!this.room;this.query.disabled=!this.room;
    this.root.querySelector('[data-media-list]').replaceChildren();this.root.querySelector('[data-media-drafts]').replaceChildren();
    this.root.querySelector('[data-media-more]').hidden=true;this.root.querySelector('[data-media-progress]').hidden=true;
    this.root.querySelector('[data-media-pause]').hidden=true;this.notice('');
    if(this.room){if(this.root.offsetParent)this.refresh();this.timer=setInterval(()=>{if(!document.hidden&&this.root.offsetParent&&!this.busy)this.refresh();},6000);}
  }
  async request(op,body,version=this.version){
    if(version!==this.version)throw Object.assign(new Error('视图已切换'),{stale:true});
    const controller=new AbortController();this.controllers.add(controller);const timer=setTimeout(()=>controller.abort(),45000);
    try{const value=await this.api(op.startsWith('/api/')?op:'/api/room/media/'+op,body,{signal:controller.signal});
      if(version!==this.version)throw Object.assign(new Error('视图已切换'),{stale:true});return value;
    }catch(error){if(error.name==='AbortError'&&version===this.version)throw new Error('请求超时，请刷新或继续上传。');throw error;}
    finally{clearTimeout(timer);this.controllers.delete(controller);}
  }
  failure(error,version){if(version!==this.version||error.stale)return;
    if(error.code===403){this.clearPreview();this.root.querySelector('[data-media-list]').replaceChildren();
      this.clearSearch();this.root.querySelector('[data-media-drafts]').replaceChildren();this.options.denied?.(this.room);}
    this.notice(error.message||'资料请求失败，请稍后刷新。');
  }
  async refresh(more=false){
    if(!this.room||this.loading)return;const version=this.version;this.loading=true;
    try{
      const [page,drafts]=await Promise.all([this.request('list',{room_id:this.room,limit:20,...(more&&this.before?{before:this.before}:{})},version),
        this.request('uploads',{room_id:this.room},version)]);
      this.items=more?[...this.items,...page.items]:page.items;this.before=page.next_before;
      const states=await Promise.all(this.items.map(async item=>{
        try{return await this.request('processing',{id:item.id},version);}catch(error){if(error.code===403||error.stale)throw error;return {state:item.state,error_code:'status_unavailable'};}
      }));
      if(version!==this.version)return;
      this.root.querySelector('[data-media-more]').hidden=!page.has_more;
      const list=this.root.querySelector('[data-media-list]');list.replaceChildren();
      if(!this.items.length)list.append(element('p','暂无共享资料。'));
      this.items.forEach((item,index)=>this.row(list,item,states[index]));
      const pending=this.root.querySelector('[data-media-drafts]');pending.replaceChildren();
      for(const item of drafts.items){const row=element('div');row.className='media-row';
        row.append(element('strong',item.name),element('p','未完成上传 · '+size(item.bytes)),
          button('选择原文件续传',()=>this.file.click()),button('取消上传',async()=>{
            try{await this.request('abort',{id:item.id},version);this.notice('未完成上传已取消。');this.refresh();}catch(error){this.failure(error,version);}
          }));pending.append(row);}
    }catch(error){this.failure(error,version);}finally{if(version===this.version)this.loading=false;}
  }
  row(list,item,state){
    const row=element('article');row.className='media-row';row.dataset.mediaId=item.id;
    row.append(element('strong',item.name),element('p',size(item.bytes)+' · '+(LABEL[state.state]||'等待处理')));
    if(!AUDIO_VIDEO_ENABLED&&playable(item)){
      row.append(element('p','音视频功能暂缓开放，已有原文件可下载。'),button('下载原文件',()=>this.download(item)));list.append(row);return;
    }
    if(state.error_code)row.append(element('p',state.error_code==='status_unavailable'?'暂时无法获取处理状态，请刷新。':
      state.error_code==='encrypted_document'?'加密文档暂不支持，请上传解密后的副本。':'解析未完成，请确认文件可以正常打开，或联系管理员检查处理服务。'));
    if(state.state==='ready'){
      row.append(button('查看内容',()=>this.preview(item,state.revision)),button('请 Agent 分析',()=>this.chooseAgent({...item,revision:state.revision})));
    }
    const type=mediaType(item);
    if(type.startsWith('audio/')||type.startsWith('video/'))row.append(button(type.startsWith('audio/')?'播放录音':'播放视频',()=>this.play(item,type)));
    row.append(button('下载原文件',()=>this.download(item)));list.append(row);
  }
  async search(){
    if(!this.room)return;const version=this.version,query=this.query.value.trim(),search=this.searchVersion=(this.searchVersion||0)+1;
    const box=this.root.querySelector('[data-media-matches]');box.replaceChildren();
    if(!query)return;
    if(new TextEncoder().encode(query).length>240){box.append(element('p','关键词过长，请缩短后重试。'));return;}
    box.append(element('p','正在检索已处理的资料…'));
    try{
      const page=await this.request('search',{room_id:this.room,query,limit:20},version);
      if(version!==this.version||search!==this.searchVersion)return;box.replaceChildren();
      if(!page.items.length)box.append(element('p','没有匹配片段。请更换关键词，或确认资料已处理完成。'));
      for(const item of page.items){
        const row=element('article');row.className='media-row';row.dataset.mediaMatch=item.id;
        const content=item.content,label=anchor(content.anchor)||(content.line_start?'第 '+content.line_start+' 行':'');
        row.append(element('strong',item.name),element('small',label),element('pre',Array.from(content.text||'').slice(0,600).join('')),
          button('查看匹配片段',()=>this.preview(item,item.revision,item.index)));
        if(AUDIO_VIDEO_ENABLED||!playable(item))row.append(button('请 Agent 分析此资料',()=>this.chooseAgent(item)));
        box.append(row);
      }
      if(page.has_more)box.append(element('p','这里只展示前 20 个匹配片段，请补充关键词缩小范围。'));
    }catch(error){if(version!==this.version||search!==this.searchVersion)return;box.replaceChildren();this.failure(error,version);}
  }
  async upload(file){
    if(this.busy||!this.room)return;
    const extension=file.name.split('.').pop().toLowerCase();
    if(!AUDIO_VIDEO_ENABLED&&/^(audio|video)\//.test(TYPES[extension]||'')){this.notice('本阶段暂不支持音视频上传，请选择文本、PDF/DOCX 或图片。');return;}
    const version=this.version,upload=++this.uploadVersion;
    const active=()=>{if(version!==this.version||upload!==this.uploadVersion)throw Object.assign(new Error('上传已暂停'),{stale:true});};
    const progress=this.root.querySelector('[data-media-progress]'),pause=this.root.querySelector('[data-media-pause]');
    this.busy=true;this.file.disabled=true;progress.hidden=false;pause.hidden=false;
    try{
      if(!file.size||file.size>MAX)throw new Error('请选择 1 字节至 256 MB 的文件。');
      const mime=TYPES[file.name.split('.').pop().toLowerCase()];if(!mime)throw new Error('暂不支持此文件格式。');
      if(!global.crypto?.subtle)throw new Error('浏览器无法安全校验文件，请通过 HTTPS 或本机地址打开页面。');
      this.notice('正在校验文件…');progress.value=0;
      let bytes=await file.arrayBuffer();active();const hash=await sha(bytes);bytes=null;active();
      const key=await sha(new TextEncoder().encode(JSON.stringify([file.name,mime,file.size,hash])));active();
      const media=await this.request('init',{room_id:this.room,client_upload_id:'web_'+key,name:file.name,mime,bytes:file.size,sha256:hash},version);active();
      if(media.state==='uploading'){
        const saved=new Set((media.chunks||[]).map(chunk=>chunk.index));
        for(let index=0,offset=0;offset<file.size;index++,offset+=CHUNK){active();
          if(!saved.has(index)){const part=new Uint8Array(await file.slice(offset,offset+CHUNK).arrayBuffer());active();
            await this.request('chunk',{id:media.id,index,data:encode(part)},version);active();}
          progress.value=Math.min(100,Math.round((offset+CHUNK)/file.size*100));this.notice('正在上传 '+file.name+' · '+progress.value+'%');
        }
        this.notice('正在确认文件完整性…');await this.request('finish',{id:media.id},version);active();
      }else if(media.state==='expired'||media.state==='cancelling')throw new Error('此上传已取消，请重命名文件后重新上传。');
      this.notice(media.state==='ready'?'该资料已共享并处理完成。':'资料已共享，正在等待解析。完成后可请 Agent 分析。');progress.value=100;
    }catch(error){if(!error.stale){this.failure(error,version);if(version===this.version&&error.code!==403)this.notice((error.message||'上传中断')+'；可重新选择同一文件继续。');}}
    finally{if(version===this.version){this.busy=false;this.file.disabled=!this.room;pause.hidden=true;this.refresh();}}
  }
  appendImage(parent,data,alt){
    const url=URL.createObjectURL(new Blob([decode(data)],{type:'image/jpeg'}));this.urls.push(url);
    const node=element('img');node.src=url;node.alt=alt;node.style.maxWidth='100%';parent.append(node);
  }
  async preview(item,revision,start=0){
    this.clearPreview();const version=this.version,view=this.previewVersion,box=this.root.querySelector('[data-media-preview]');
    box.hidden=false;box.append(element('h3',item.name),button('关闭预览',()=>this.clearPreview()));
    const text=element('div'),pictures=element('div');box.append(text,pictures);
    const current=()=>version===this.version&&view===this.previewVersion;
    let offset=start;
    const load=async()=>{try{const page=await this.request('fragments',{id:item.id,revision,offset,limit:10},version);if(!current())return;
      for(const fragment of page.items){const value=fragment.content;const label=anchor(value.anchor)||(value.line_start?'第 '+value.line_start+' 行':'');
        text.append(element('small',label),element('pre',value.text||''));
        this.playbackLink(text,item,value.anchor);}
      offset=page.next_offset;more.hidden=!page.has_more;
    }catch(error){this.failure(error,version);}};
    const more=button('更多片段',load);box.append(more);await load();
    try{const page=await this.request('artifacts',{id:item.id,revision,offset:0,limit:20},version);if(!current())return;
      for(const entry of page.items){pictures.append(button('查看'+(anchor(entry.anchor)||'图片'),async()=>{
        try{const image=await this.request('artifact',{id:item.id,revision,index:entry.index},version);if(!current())return;
          this.appendImage(pictures,image.data,item.name+' '+anchor(entry.anchor));
        }catch(error){this.failure(error,version);}
      }));}
      if(page.has_more)pictures.append(element('p','此处展示前 20 个画面入口；Agent 可按资料工具继续读取。'));
    }catch(error){this.failure(error,version);}
  }
  async openSource(request,source){
    this.clearPreview();const version=this.version,view=this.previewVersion,room=this.room;
    const current=()=>version===this.version&&view===this.previewVersion;
    const lookup=async()=>{
      const page=await this.request('/api/chatroom/agents/sources',{room_id:room,request_id:request,source_ids:[source]},version);
      const item=page.items[0];if(!item?.available)throw new Error('来源已失效或上下文已变更，不能作为当前依据打开。');return item;
    };
    this.notice('正在核对来源…');
    try{
      const item=await lookup(),ref=item.reference;let content='',picture=null;
      if(ref.kind==='message')content=item.text;
      else if(ref.kind==='fragment'){
        const page=await this.request('fragments',{id:ref.asset_id,revision:ref.revision,offset:ref.index,limit:1},version);
        const fragment=page.items[0];if(!fragment||fragment.index!==ref.index||fragment.sha256!==ref.sha256)throw new Error('来源片段已变更。');
        content=fragment.content.text||'';
      }else if(ref.kind==='image'||ref.kind==='image_metadata'){
        picture=await this.request('artifact',{id:ref.asset_id,revision:ref.revision,index:ref.index},version);
        if(picture.metadata.sha256!==ref.sha256)throw new Error('来源图片已变更。');
      }else content='这是资料目录的读取记录；它证明 Agent 查到了该资料，不代表已经阅读其中的正文或图片。';
      const again=await lookup();if(!current())return;
      if(JSON.stringify(again.reference)!==JSON.stringify(ref))throw new Error('来源版本已变更。');
      const box=this.root.querySelector('[data-media-preview]');box.hidden=false;
      box.append(element('h3',item.name),element('p',anchor(ref.anchor)||(ref.line_start?'第 '+ref.line_start+' 行':'原请求读取的资料')),
        button('关闭预览',()=>this.clearPreview()));
      if(ref.kind==='image_metadata')box.append(element('p','原请求仅读取了图片信息；下图是你当前查看的对应图片。'));
      if(picture)this.appendImage(box,picture.data,item.name);
      else box.append(element('pre',content));
      if(ref.asset_id)this.playbackLink(box,{id:ref.asset_id,name:item.name},ref.anchor,async()=>{
        const latest=await lookup();
        if(JSON.stringify(latest.reference)!==JSON.stringify(ref))throw new Error('来源版本已变更。');
      });
      this.notice('已按原请求的固定来源核对；不代表分析结论已经验证。');
    }catch(error){if(current())this.failure(error,version);}
  }
  chooseAgent(item){
    if(!AUDIO_VIDEO_ENABLED&&playable(item)){this.notice('本阶段暂不提供音视频分析。');return;}
    const installed=this.agents.agents.filter(agent=>agent.enabled&&agent.installed);
    if(!installed.length){this.notice('请先在 Agent 页安装一个可用 Agent。');return;}
    const box=this.root.querySelector('[data-media-preview]');this.clearPreview();box.hidden=false;
    box.append(element('h3','分析 '+item.name));
    const label=element('label','分析目标'),mode=element('select');mode.dataset.mediaAnalysis='';
    for(const [value,name] of [['summary','总结资料'],['tasks','整理决策与任务'],['video','视频故障分析']]){
      if(value==='video'&&!AUDIO_VIDEO_ENABLED)continue;
      const option=element('option',name);option.value=value;mode.append(option);
    }
    const extension=item.name.split('.').pop().toLowerCase();
    mode.value=['mp4','webm'].includes(extension)?'video':['wav','mp3','flac','ogg','m4a'].includes(extension)?'tasks':'summary';
    label.append(mode);box.append(label,element('p','选择 Agent 后可继续编辑草稿；任务建议需核对并保存。'));
    for(const agent of installed)box.append(button('@'+agent.name,()=>{
      if(!this.room||!this.agents.agents.some(a=>a.agent_id===agent.agent_id&&a.enabled&&a.installed))return;
      this.agents.selected.add(agent.agent_id);this.agents.renderSelected();
      const target=JSON.stringify({asset_id:item.id,revision:item.revision,name:item.name});
      const instructions={
        summary:'总结要点、结论与待确认事项，引用实际读取的 source_id 和页码或时间位置。',
        tasks:'阅读相关原文或会议转写，提取决策和后续任务。返回 JSON：{decisions:[{summary,source_ids:[string]}],tasks:[{title,description,steps:[string],owner,verbatim_quote,interpretation,needs_confirmation,reason,source_ids:[string]}]}。每项引用实际读取且支持该项的 source_id。verbatim_quote 保留逐字原文；解释与原文分开，只有上下文支持时才提出解释，否则明确无法确定。疑似转写错误、姓名、职责或结论不清楚时将 needs_confirmation 设为 true，并在 reason 中写明待确认点。不猜测、不按姓名分配系统账号，不静默改写原转写。仅提出建议，不创建或执行任务。',
        video:'结合音轨转写与实际视频帧解释故障过程。先查看画面目录，再用 room_read_media 读取故障前后图片；引用每幅图的 source_id 和时间。区分画面事实、音轨描述与推测，用相邻采样帧界定转变区间。没有真实图片输入时明确说明，不把 OCR 或元数据当作已看到画面。'
      };
      this.input.value+=(this.input.value.trim()?'\n':'')+'@'+agent.name+' 请通过房间资料工具读取所选资料 '+target+'。使用指定 asset_id 和 revision；不可用时说明原因，不替换成同名资料。'+instructions[mode.value];
      this.input.focus();this.notice('请求已填入消息框，点击发送后开始分析。');this.options.compose?.();
    }));
  }
  async original(item,version,active=()=>{}){
    if(!global.crypto?.subtle)throw new Error('请通过 HTTPS 或本机地址打开页面再读取原文件。');
    if(item.bytes>MAX)throw new Error('此文件超过网页读取上限 256 MB。');
    const chunks=[];
    for(let offset=0;offset<item.bytes;offset+=CHUNK){active();
      const part=await this.request('read',{id:item.id,offset,length:Math.min(CHUNK,item.bytes-offset)},version);active();
      if(part.sha256!==item.sha256||part.total_bytes!==item.bytes||part.offset!==offset)throw new Error('文件版本发生变化，请刷新。');
      chunks.push(decode(part.data));
    }
    const blob=new Blob(chunks,{type:'application/octet-stream'});
    if(blob.size!==item.bytes||await sha(await blob.arrayBuffer())!==item.sha256)throw new Error('文件校验失败，请重试。');
    active();return blob;
  }
  playbackLink(box,item,position,verify){
    if(!AUDIO_VIDEO_ENABLED)return;
    const ms=position?.timestamp_ms??position?.start_ms;
    if(!playable(item)||!Number.isSafeInteger(ms)||ms<0)return;
    const version=this.version,view=this.previewVersion;
    const current=()=>version===this.version&&view===this.previewVersion;
    box.append(button('播放原文件 '+anchor(position),async()=>{
      try{
        const original=await this.request('status',{id:item.id},version);
        if(verify)await verify();
        if(!current())return;
        if(!playable(original))throw new Error('此资料不支持音视频播放。');
        await this.play(original,mediaType(original),ms,verify);
      }catch(error){if(current())this.failure(error,version);}
    }));
  }
  async play(item,type,startMs=0,verify){
    this.clearPreview();const version=this.version,view=this.previewVersion,box=this.root.querySelector('[data-media-preview]');
    const current=()=>version===this.version&&view===this.previewVersion;
    const active=()=>{if(!current())throw Object.assign(new Error('视图已切换'),{stale:true});};
    box.hidden=false;box.append(element('h3',item.name),button('关闭预览',()=>this.clearPreview()));
    this.notice('正在读取并校验原文件，完成后即可播放…');
    try{
      const original=await this.original(item,version,active);
      if(verify)await verify();active();
      const url=URL.createObjectURL(new Blob([original],{type}));this.urls.push(url);
      const player=element(type.startsWith('video/')?'video':'audio');player.controls=true;player.preload='metadata';player.src=url;
      player.style.maxWidth='100%';player.setAttribute('aria-label','播放 '+item.name);
      player.addEventListener('loadedmetadata',()=>{
        if(!current())return;
        if(!Number.isFinite(player.duration)||startMs/1000>=player.duration){this.notice('该时间位置超出可播放范围，请手动核对原文件。');return;}
        player.currentTime=startMs/1000;
        this.notice(startMs?'已定位到 '+anchor({start_ms:startMs})+'，点击播放核对原文。':'原文件已校验，可使用播放进度条核对转写。');
      },{once:true});
      player.onerror=()=>{if(current())this.notice('浏览器无法播放此格式，请下载原文件后播放。');};
      box.append(player,element('p','转写可能存在姓名或术语错误，请对照原录音核对后再保存任务。'));
      this.notice('原文件已校验，可使用播放进度条核对转写。');
    }catch(error){if(current())this.failure(error,version);}
  }
  async download(item){
    if(this.downloading){this.notice('请等待当前文件下载完成。');return;}
    const version=this.version;this.downloading=true;
    try{this.notice('正在读取 '+item.name+'…');
      const blob=await this.original(item,version);
      if(version!==this.version)return;const url=URL.createObjectURL(blob);this.urls.push(url);const a=element('a');a.href=url;a.download=item.name;a.click();
      setTimeout(()=>{URL.revokeObjectURL(url);this.urls=this.urls.filter(value=>value!==url);},60000);
      this.notice('文件已校验并交给浏览器下载。');
    }catch(error){this.failure(error,version);}finally{this.downloading=false;}
  }
}
global.RoomMedia=RoomMedia;
})(typeof window==='undefined'?globalThis:window);
