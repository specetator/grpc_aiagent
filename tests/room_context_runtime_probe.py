"""Opt-in real native room tools/models with an HTTP authority fixture.

This is a native integration probe, NOT Logic/MySQL/Kafka/Web acceptance.
Only explicitly authorized default-profile MODEL access is provisioned into
owned homes; private sessions/configuration/workspaces are never copied.
"""
import argparse
import base64
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import random
import re
import secrets
import subprocess
import sys
import tempfile
import threading
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'cannbot/scripts'))
from agent_router import AgentStore
from hermes_native_adapter import HermesNativeAdapter
from pi_gateway import PiRpcClient
from room_agent import PublicRoomPiFactory
from room_workspace import execution_namespace
from public_runtime_fixture import PublicHermesRuntime
from pi_media_capability_probe import png_fixture

PI_AUDIT = r'''
import fs from 'node:fs';
import crypto from 'node:crypto';
function images(value: any): string[] {
  if (typeof value === 'string' && /^data:image\/[a-z]+;base64,/.test(value))
    return [crypto.createHash('sha256').update(Buffer.from(value.split(',')[1],'base64')).digest('hex')];
  if (value && typeof value === 'object') {
    if (value.type==='image' && value.data) return [crypto.createHash('sha256').update(Buffer.from(value.data,'base64')).digest('hex')];
    return Object.values(value).flatMap(images);
  }
  return [];
}
function associations(payload: any) {
  const calls = new Map((payload?.input || []).filter((x: any)=>x.type==='function_call')
    .map((x: any)=>[x.call_id, x]));
  return (payload?.input || []).filter((x: any)=>x.type==='function_call_output' && images(x).length)
    .map((x: any)=>{
      const call: any = calls.get(x.call_id);
      const args = call ? JSON.parse(call.arguments) : {};
      const text = (x.output || []).find((p: any)=>p.type==='input_text')?.text;
      const refs = text ? JSON.parse(text).items.map((i: any)=>i.reference) : [];
      return {call_id:x.call_id, tool:call?.name, asset_id:args.asset_id,
        references:refs.map((r: any)=>({asset_id:r.asset_id,revision:r.revision,index:r.index,sha256:r.sha256})),hashes:images(x)};
    });
}
export default function(pi: any) {
  const record = (phase: string, value: any) => {
    const hashes=images(value);
    const markers=[...new Set(JSON.stringify(value).match(/probe-memory-[a-f0-9]{24}/g)||[])];
    const request_id=JSON.parse(process.env.SPARK_PUBLIC_SCOPE||'{}').request_id;
    const sources = phase==='provider' ? associations(value) : [];
    if (hashes.length || phase==='provider') fs.appendFileSync(process.env.SPARK_PROBE_AUDIT!,JSON.stringify({phase,hashes,markers,request_id,sources})+'\n');
  };
  pi.on('before_provider_request',(event: any)=>{record('provider',event.payload);});
  pi.on('tool_result',(event: any)=>{record('tool',event);});
}
'''

HERMES_AUDIT = r'''
import base64,hashlib,json,re,runpy,sys,threading
from pathlib import Path
scripts,audit,launcher=sys.argv[1:4];sys.path.insert(0,scripts)
import hermes_public_workspace
original_adapter=hermes_public_workspace.public_api_adapter
lock=threading.Lock()
def images(value):
    if isinstance(value,str) and value.startswith('data:image/') and ';base64,' in value:
        return [hashlib.sha256(base64.b64decode(value.split(',',1)[1],validate=True)).hexdigest()]
    if isinstance(value,dict):return [h for v in value.values() for h in images(v)]
    if isinstance(value,list):return [h for v in value for h in images(v)]
    return []
def markers(value):return sorted(set(re.findall(r'probe-memory-[a-f0-9]{24}',json.dumps(value))))
def record(phase,hashes,status=None,seen=None,request_id=None):
    context=hermes_public_workspace._tool_scope.get()
    request_id=request_id or (context['scope']['request_id'] if context else None)
    if hashes or phase in ('provider','todo'):
        with lock:
            with Path(audit).open('a') as output:output.write(json.dumps({'phase':phase,'hashes':hashes,'status':status,'markers':seen or [],'request_id':request_id})+'\n')
def observed_adapter(*args,**kwargs):
    # Called only AFTER the formal launcher cleans env and installs its owned home.
    from tools.registry import registry
    original_normalize=registry._normalize_handler_result
    def normalized(name,result):
        result=original_normalize(name,result);record('tool',images(result));return result
    registry._normalize_handler_result=normalized
    import httpx
    original_send=httpx.Client.send
    def send(client,request,*a,**kw):
        try:payload=json.loads(request.content)
        except Exception:payload={}
        response=original_send(client,request,*a,**kw)
        if isinstance(payload,dict) and 'model' in payload and ('input' in payload or 'messages' in payload):
            record('provider',images(payload),response.status_code,markers(payload))
        return response
    httpx.Client.send=send
    base=original_adapter(*args,**kwargs)
    class Observed(base):
        def _create_agent(self,*a,**kw):
            request_id=hermes_public_workspace._admission.get()['scope']['request_id']
            agent=super()._create_agent(*a,**kw)
            original=agent.run_conversation
            def conversation(*a,**kw):
                result=original(*a,**kw)
                record('todo',[],seen=markers(agent._todo_store.snapshot()),request_id=request_id)
                return result
            agent.run_conversation=conversation
            return agent
    return Observed
hermes_public_workspace.public_api_adapter=observed_adapter
sys.argv=[launcher,*sys.argv[4:]]
runpy.run_path(launcher,run_name='__main__')
'''


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


class ObservedHermes(PublicHermesRuntime):
    """Real Native launcher with hash-only tool/provider observations."""
    def launch(self):
        script = self.root / 'probe-bootstrap.py'
        script.write_text(HERMES_AUDIT)
        self.audit = self.root / 'image-audit.jsonl'
        if not hasattr(self, 'base_command'):
            self.base_command = list(self.command)
        self.command = [self.python, self.win(script), self.win(ROOT / 'cannbot/scripts'),
                        self.win(self.audit), *self.base_command[1:]]
        super().launch()


def run(args):
    report = {'kind':'real-native-tools-and-models-with-fixture-authority', 'passed':False,
              'scope':__doc__, 'checks':[], 'runtimes':{}}
    destination = args.out
    destination.parent.mkdir(parents=True, exist_ok=True)
    image_directory=destination.parent/(destination.stem+'-images')
    image_directory.mkdir(exist_ok=True)
    def save(): destination.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    def check(value, label, *, fatal=True):
        if not value:
            report.setdefault('failed_checks', []).append(label);save()
            if fatal: raise AssertionError(label)
            print('native probe failed:',label,flush=True)
            return
        report['checks'].append(label);save();print('native probe passed:',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='spark-room-media-probe-', dir='/mnt/f') as directory:
        root = Path(directory)
        grants, hits = {}, []
        key = secrets.token_hex(32)
        nonces = {7:secrets.token_hex(12), 8:secrets.token_hex(12)}
        memories = {runtime:'probe-memory-'+secrets.token_hex(12) for runtime in ('pi','hermes')}
        artifacts, expected = [], []
        snapshots = root/'snapshots';snapshots.mkdir()
        native = ObservedHermes(profile=args.hermes_model_profile, workspace={'key':key,'snapshots':str(snapshots),'context_tools':True})
        model, auth = native.model_credentials()
        report['model'] = {'provider': model.get('provider'), 'id': model['default']}
        replay = json.loads(args.fixtures_report.read_text()) if args.fixtures_report else None
        if replay:
            expected = next(iter(replay['runtimes'].values()))['expected']
            if len(expected) != 3 or len(replay['image_files']) != 3:
                raise ValueError('fixture replay requires three recorded images and answers')
            report['fixture_replay_sha256'] = hashlib.sha256(args.fixtures_report.read_bytes()).hexdigest()
        # Use the already-installed isolated-test encoder; no private workspace.
        for index in range(3):
            if replay:
                source = (args.fixtures_report.parent / replay['image_files'][index]).resolve()
                if not source.is_relative_to(args.fixtures_report.parent.resolve()):
                    raise ValueError('fixture image must remain within the report directory')
                data = source.read_bytes()
                if source.stem != hashlib.sha256(data).hexdigest():
                    raise ValueError('fixture image digest mismatch')
            else:
                png, answer = png_fixture(random.SystemRandom());expected.append(answer)
                source, jpeg = root/f'image-{index}.png', root/f'image-{index}.jpg'
                source.write_bytes(png)
                subprocess.run([native.python,'-B','-c',
                    'from PIL import Image;import sys;Image.open(sys.argv[1]).convert("RGB").save(sys.argv[2],"JPEG",quality=95)',
                    native.win(source),native.win(jpeg)],check=True,capture_output=True,timeout=20)
                data=jpeg.read_bytes()
            sha=hashlib.sha256(data).hexdigest()
            artifacts.append({'bytes':data,'sha256':sha})
            image_path=image_directory/(sha+'.jpg')
            if image_path.exists() and image_path.read_bytes()!=data:raise AssertionError('image artifact hash collision')
            image_path.write_bytes(data)
            report.setdefault('image_files',[]).append(str(Path(image_directory.name)/image_path.name))
        def grant(agent, runtime, room=7, actor=17, epoch=0):
            scope={'kind':'room_agent_v1','room_id':room,'agent_id':agent,'actor_user_id':actor,'owner_user_id':17,
                'bot_user_id':900000000402 if runtime=='hermes_native' else 900000000401,'generation':1,
                'workspace_generation':0,'context_generation':epoch,'project_id':'','policy':'read_only','runtime':runtime,
                'source_msg_id':f'r_{room}-9','request_id':'ra_'+secrets.token_hex(32)}
            grants[scope['request_id']]=scope
            return scope
        def reference(scope, kind, index):
            ref={'kind':kind,'room_id':scope['room_id'],'asset_id':f'{index+1:032x}','revision':digest(index),
                 'sha256':artifacts[index]['sha256']}
            if kind=='image':ref.update(index=0,anchor={'image':1})
            return ref
        def item(scope, ref, **fields):
            return {'source_id':'src_'+digest([scope['request_id'],ref]),'reference':ref,**fields}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                scope=grants.get(body.get('request_id')) if self.headers.get('Authorization')=='Bearer '+key else None
                code=0;data={}
                if not scope:code=403
                elif self.path.endswith('/authorize'):data={'request_id':scope['request_id'],'collaboration':scope}
                else:
                    operation=self.path.rsplit('/',1)[-1];hits.append({'scope':copy.deepcopy(scope),'operation':operation})
                    items=[]
                    if operation=='search':
                        if scope['room_id']==7:
                            items=[item(scope,reference(scope,'media',i),name=f'picture-{i}.jpg',mime='image/jpeg',image_count=1,fragment_count=0)
                                   for i in range(3)]
                    elif operation=='read' and body.get('kind')=='message' and body.get('msg_seq')==8:
                        text=nonces[scope['room_id']];offset=body.get('offset',0);text=text[offset:offset+body.get('limit',4096)]
                        ref={'kind':'message','room_id':scope['room_id'],'msg_id':f"r_{scope['room_id']}-8",'msg_seq':8,
                             'offset':offset,'length':len(text),'sha256':hashlib.sha256(text.encode()).hexdigest()}
                        items=[item(scope,ref,text=text)]
                    elif operation=='media' and scope['room_id']==7:
                        candidates=[i for i in range(3) if body.get('asset_id')==f'{i+1:032x}' and body.get('revision')==digest(i) and body.get('index')==0]
                        if not candidates:code=404
                        else:
                            i=candidates[0];ref=reference(scope,'image',i)
                            items=[item(scope,ref,mime='image/jpeg',data=base64.b64encode(artifacts[i]['bytes']).decode(),
                                metadata={'sha256':ref['sha256'],'mime':'image/jpeg','anchor':ref['anchor'],'width':256,'height':256})]
                    else:code=404
                    data={'request_id':scope['request_id'],'context_generation':scope['context_generation'],'items':items}
                encoded=json.dumps({'code':code,'data':data}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(encoded)));self.end_headers()
                self.wfile.write(encoded)
        server=ThreadingHTTPServer(('0.0.0.0',0),Handler)
        thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.02));thread.start()
        host=subprocess.check_output(['hostname','-I'],text=True).split()[0]
        origin=f'http://{host}:{server.server_port}';native.workspace['authority']=origin
        prompt=('Use room_search with type=media to find the three images. Read each using room_read_media, and read message msg_seq=8 '
            'using room_read. Return JSON only: {"nonce":"exact text of message 8","images":[...]} in picture-0,1,2 order. '
            'For each image return {"left":{"shape":"circle|square|triangle","color":"red|green|blue"},'
            '"right":{...},"lower":{...}}. Identify actual leftmost, rightmost and lowest shapes; metadata has no answer. No code project is authorized.')
        def examine(runtime, answer, audit_path, request_id):
            parsed=json.loads(re.sub(r'^```(?:json)?\s*|\s*```$','',answer.strip()))
            records=[json.loads(line) for line in audit_path.read_text().splitlines()]
            hashes={entry['sha256'] for entry in artifacts}
            sent={h for row in records if row['phase']=='provider' and row.get('request_id')==request_id and row.get('status',200)==200 for h in row['hashes']}
            tool={h for row in records if row['phase']=='tool' and row.get('request_id')==request_id for h in row['hashes']}
            report['runtimes'][runtime]={'answer':parsed,'expected':expected,'image_sha256':sorted(hashes),'audit':records}
            check(parsed.get('nonce')==nonces[7],runtime+' reads bounded room text without a code project')
            check(hashes<=tool,runtime+' native tool result contains all three actual images')
            check(hashes<=sent,runtime+' provider-bound request contains all three image payloads')
            if runtime == 'pi':
                pairs = [pair for row in records if row['phase']=='provider' and row.get('request_id')==request_id
                         for pair in row.get('sources',[])]
                check(len(pairs)>=3 and all(pair['tool']=='room_read_media' and len(pair['references'])==1 and
                      pair['references'][0]['asset_id']==pair['asset_id'] and
                      pair['hashes']==[pair['references'][0]['sha256']] for pair in pairs),
                      'pi provider function calls preserve image/source association')
            # Semantic failure remains a failed run, but should not hide the
            # independent Runtime/isolation checks for this or the other Agent.
            check(parsed.get('images')==expected,runtime+' answers three no-text images correctly',fatal=False)
        def audits(path):return [json.loads(line) for line in path.read_text().splitlines()]
        def isolated(runtime, path, start, request_id, expected_marker=None, todo=False):
            records=[r for r in audits(path)[start:] if r.get('request_id')==request_id]
            providers=[r for r in records if r['phase']=='provider']
            check(bool(providers),runtime+' isolation turn actually reaches the model')
            seen={m for r in providers for m in r.get('markers',[])}
            check((expected_marker in seen if expected_marker else not seen),runtime+' provider history has the expected scope markers')
            check(not (seen & {m for m in memories.values() if m != expected_marker}),runtime+' provider history excludes other room/Agent/generation markers')
            if todo:
                states=[r for r in records if r['phase']=='todo']
                check(bool(states) and set(states[-1]['markers'])==({expected_marker} if expected_marker else set()),
                      runtime+' actual native todo state matches the scoped history')
            report['runtimes'][runtime].setdefault('isolation',[]).append({'provider_markers':sorted(seen),'todo_checked':todo,
                'request_id':request_id,'room_id':grants[request_id]['room_id'],'context_generation':grants[request_id]['context_generation']})
            save()
        adapter=None
        try:
            if args.runtime in ('pi','both'):
                config=root/'models.json'
                config.write_text(json.dumps({'providers':{'openai-codex':{'baseUrl':'https://chatgpt.com/backend-api',
                    'api':'openai-codex-responses','apiKey':auth['providers']['openai-codex']['tokens']['access_token'],
                    'models':[{'id':model['default'],'name':model['default'],'reasoning':True,'input':['text','image'],
                               'contextWindow':32768,'maxTokens':4096}]}}}));config.chmod(0o600)
                audit=root/'pi-image-audit.jsonl';extension=root/'audit.ts';extension.write_text(PI_AUDIT)
                class ObservedPi(PiRpcClient):
                    def __init__(self,argv,cwd,env,session_dir):
                        super().__init__([*argv,'--extension',str(extension)],cwd,{**env,'SPARK_PROBE_AUDIT':str(audit)},session_dir)
                private=root/'prototype';private.mkdir()
                prototype=ObservedPi([str(args.pi),'--provider','openai-codex','--model',model['default']],private,
                    {'PATH':str(args.pi.parent)+os.pathsep+'/usr/bin:/bin'},private/'sessions')
                environment={'SPARK_WORKSPACE_AUTH_ORIGIN':origin,'SPARK_PUSH_WORKSPACE_AUTH_KEY':key,
                             'SPARK_PUSH_PUBLIC_PI_MODELS_FILE':str(config)}
                with patch.dict(os.environ,environment,clear=True):factory=PublicRoomPiFactory(prototype,root/'pi-public')
                scope=grant('public-pi','pi_rpc')
                first_prompt=prompt+(' Remember '+memories['pi']+' for later recall; keep the requested JSON shape unchanged.' if args.isolation else '')
                answer,_=factory.for_scope(scope).chat(first_prompt,None,240,session_id=execution_namespace(scope),request_id=scope['request_id'])
                examine('pi',answer,audit,scope['request_id'])
                if args.isolation:
                    for room,epoch,marker in ((7,0,memories['pi']),(8,0,None),(7,1,None)):
                        current=grant('public-pi','pi_rpc',room=room,actor=18,epoch=epoch)
                        start=len(audits(audit))
                        reply,_=factory.for_scope(current).chat('Without tools, return the exact probe-memory marker from previous conversation, or UNKNOWN if none.',
                            None,120,session_id=execution_namespace(current),request_id=current['request_id'])
                        report['runtimes']['pi'].setdefault('recall_answers',[]).append({
                            'request_id':current['request_id'],'room_id':room,'context_generation':epoch,
                            'answer':reply,'expected':marker or 'UNKNOWN'})
                        check(reply.strip()==(marker or 'UNKNOWN'),'Pi native process restart recalls only its room/epoch history',fatal=False)
                        isolated('pi',audit,start,current['request_id'],marker)
            if args.runtime in ('hermes','both'):
                entry=native.start();adapter=HermesNativeAdapter(entry,AgentStore(root/'native-store'))
                check(adapter._capabilities(15)['features'].get('spark_room_context_v1') is True,'Hermes advertises verified explicit room toolset')
                scope=grant(entry['id'],'hermes_native')
                first_prompt=prompt+(' Remember '+memories['hermes']+' for later recall; keep the requested JSON shape unchanged.' if args.isolation else '')
                answer,_=adapter.chat(first_prompt,None,240,session_id=execution_namespace(scope),request_id=scope['request_id'])
                examine('hermes',answer,native.audit,scope['request_id'])
                if args.isolation:
                    current=grant(entry['id'],'hermes_native',actor=18)
                    start=len(audits(native.audit))
                    adapter.chat('Use todo_list to save exactly one pending item whose content is the probe-memory marker from the previous conversation. Do not read room sources. Return DONE.',
                        None,120,session_id=execution_namespace(current),request_id=current['request_id'])
                    isolated('hermes',native.audit,start,current['request_id'],memories['hermes'],todo=True)
                    adapter.stop();adapter=None;native.stop_process();native.launch()
                    adapter=HermesNativeAdapter(entry,AgentStore(root/'native-store'))
                    for room,epoch,marker in ((7,0,memories['hermes']),(8,0,None),(7,1,None)):
                        current=grant(entry['id'],'hermes_native',room=room,actor=18,epoch=epoch)
                        start=len(audits(native.audit))
                        adapter.chat('Call todo_list with no arguments. Return only the contents of its items, or EMPTY if there are none. Do not create or change tasks or read other sources.',
                            None,120,session_id=execution_namespace(current),request_id=current['request_id'])
                        isolated('hermes',native.audit,start,current['request_id'],marker,todo=True)
        except Exception as error:
            report['error_type']=type(error).__name__
            # Preserve only sanitized assertion labels, never native/provider errors.
            if isinstance(error,AssertionError):report['failed_check']=str(error)
            for runtime, path in (('pi',root/'pi-image-audit.jsonl'),('hermes',getattr(native,'audit',root/'absent'))):
                if path.exists():report.setdefault('failure_audits',{})[runtime]=[json.loads(line) for line in path.read_text().splitlines()]
            if (native.root/'runtime.log').exists():
                log=(native.root/'runtime.log').read_text(errors='replace')
                report['native_error_classes']=sorted(set(re.findall(r'\b([A-Za-z]+(?:Error|Exception)):',log)))
            raise
        finally:
            try:
                if adapter:adapter.stop()
                native.close()
            finally:
                server.shutdown();server.server_close();thread.join(5)
                report['private_profile_hashes_unchanged']=all(hashlib.sha256((native.profile/p).read_bytes()).hexdigest()==h for p,h in native.hashes.items())
                report['tool_call_count']=len(hits);save()
        check(report['private_profile_hashes_unchanged'],'authorized private model profile remains byte-identical')
        if report.get('failed_checks'):
            report['error_type']='AssertionError';save()
            raise AssertionError('Native semantic checks failed; see failed_checks')
        report['passed']=True;save()
    return 0


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime',choices=['pi','hermes','both'],default='both')
    parser.add_argument('--isolation',action='store_true',help='Also inspect actual provider history/todo across rooms, epochs and process restart.')
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--fixtures-report',type=Path,help='Replay the exact saved JPEG bytes/answers from an earlier probe; failures remain separate reports.')
    parser.add_argument('--pi',type=Path,default=Path('/home/peco/.local/node/bin/pi'))
    parser.add_argument('--hermes-model-profile',type=Path,default=Path('/mnt/f/hermes'))
    try:raise SystemExit(run(parser.parse_args()))
    except Exception as error:
        print('Native probe failed:',type(error).__name__,'(inspect sanitized report)',flush=True)
        raise SystemExit(1)
