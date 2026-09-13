// Uses the installed Pi TypeScript loader; no model or network requests.
const assert = require('node:assert/strict');
const path = require('node:path');
const pi = process.env.SPARK_PI_PACKAGE || '/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent';
const {createJiti} = require(path.join(pi, 'node_modules/jiti'));
const jiti = createJiti(__filename, {alias: {typebox: require.resolve('typebox', {paths:[pi]})}});
(async () => {
  const {fitContext, readContext} = await jiti.import('../cannbot/pi/extensions/cann-advisor.ts');
  const citation = {citation_id:'ref_1234567890abcdef',doc_id:'doc',chunk_id:'chunk',uri:'/knowledge.html?doc_id=doc'};
  const chunk = fitContext({content:'whole document '.repeat(10000),chunk:{content:'exact evidence',chunk_id:'chunk'},citation});
  assert.equal(chunk.content,'exact evidence');
  assert.equal(chunk.chunk.content,undefined);
  assert.deepEqual(chunk.citation,citation);
  const search = fitContext({results:Array.from({length:8},()=>({content:'x'.repeat(8000),snippet:'duplicate',citation})),evidence:{sufficient:true}});
  assert.equal(search.context_chars,6000);
  assert.equal(search.results.reduce((n,r)=>n+r.content.length,0),6000);
  assert(search.results.every(r=>r.content.length<=1600 && !('snippet' in r)));
  assert(search.truncated);
  assert.deepEqual(search.results[0].citation,citation);
  const doc=fitContext({content:'a'.repeat(10000),citation});
  assert.equal(doc.content.length,6000);
  // Put the critical qualifier beyond the former 6000-character cut.
  const evidence = '通用说明🙂\n'.repeat(1500) + '限制：仅适用于型号 A；长度必须 32 字节对齐。';
  const source = {doc_id:'doc',chunk:{chunk_id:'chunk',heading:'限制条件',content:evidence},
    content:'unrelated parent document',citation,source_revision:'revision-1'};
  let options = {length:997}, reconstructed = '', pages = 0;
  while (options) {
    const result = readContext(source, options);
    assert.equal(result.chunk.heading,'限制条件');
    assert.equal(result.source_revision,'revision-1');
    assert.equal(result.parent_read.doc_id,'doc');
    assert.equal(result.neighbors_read.chunk_id,'chunk');
    assert.equal(result.coverage.start,Array.from(reconstructed).length);
    reconstructed += result.content;
    options = result.next_read;
    ++pages;
  }
  assert(pages>6);
  assert.equal(reconstructed,evidence,'pagination lost or duplicated evidence');
  assert(reconstructed.endsWith('长度必须 32 字节对齐。'));
  const initial=readContext(source,{length:100});
  assert.throws(()=>readContext({...source,chunk:{...source.chunk,content:evidence+'changed'}}, initial.next_read), /内容已变化/);
  assert.throws(()=>readContext(source,{offset:100}), /expected_hash/);
  assert.throws(()=>readContext(source,{offset:-1}), /integers/);
  assert.throws(()=>readContext(source,{length:0}), /integers/);
  const neighbor = fitContext({results:[{snippet:'short preview',citation}]});
  assert.equal(neighbor.results[0].coverage.preview_only,true);
  assert.equal(neighbor.results[0].coverage.complete,false);
  assert.equal(neighbor.results[0].next_read.offset,0);
  const preview = fitContext({results:[{content:evidence,citation}]});
  const first = preview.results[0];
  const after = readContext({doc_id:citation.doc_id,chunk:{chunk_id:citation.chunk_id,content:evidence},citation},first.next_read);
  assert.equal(after.coverage.start,first.coverage.end);
  assert.equal(after.coverage.source_hash,first.coverage.source_hash);
  if (process.env.SPARK_CANN_LIVE_TEST === '1') {
    const {default: register} = await jiti.import('../cannbot/pi/extensions/cann-advisor.ts');
    const tools = {};
    register({on(){},registerTool(tool){tools[tool.name]=tool;}});
    const invoke = async (name,params) => {
      const result=JSON.parse((await tools[name].execute('pagination-test',params)).content[0].text);
      assert.notEqual(result.ok,false,result.error);
      return result;
    };
    const found=await invoke('cann_knowledge_search',{query:'DMA DataCopy 对齐 约束',top_k:3});
    assert(found.results.length>0);
    const target=found.results[0].read_ref;
    assert(target.doc_id && target.chunk_id);
    const {execFileSync}=require('node:child_process');
    const root=process.env.CANN_KNOWLEDGE_ROOT || process.env.SPARK_PUSH_CANN_KNOWLEDGE_ROOT || '/home/peco/cppcode/fenbushi/cann-agent-knowledge';
    const original=JSON.parse(execFileSync(path.join(root,'bin/cann-rag'),['--root',root,'get','--doc-id',target.doc_id,'--chunk-id',target.chunk_id],{encoding:'utf8',timeout:30000,maxBuffer:8*1024*1024}));
    let next={...target,length:Math.max(1,Math.ceil(Array.from(original.chunk.content).length/3))}, body='', count=0;
    while(next){
      assert(++count<20,'unexpected page count');
      const result=await invoke('cann_knowledge_get',next);
      body+=result.content;
      next=result.next_read;
    }
    assert(count>1);
    assert.equal(body,original.chunk.content);
    console.log(`Live registered CANN tools: ${count} pages reconstructed ${Array.from(body).length} code points exactly`);
  }
  console.log('CANN lossless pagination, tail qualifiers, Unicode, revision guard and context links passed');
})().catch(e=>{console.error(e.message);process.exitCode=1;});
