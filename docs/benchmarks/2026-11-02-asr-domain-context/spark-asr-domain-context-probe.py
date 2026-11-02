"""Bounded actual ASR diagnostic with generic technical context, not service E2E."""
import hashlib,json,os,subprocess,time,uuid,sys
from pathlib import Path
lab=Path('/tmp/spark-asr-checkpoint-ravzgwns')
script=lab/'domain-context-decoder.py'
prompt='This is a technical meeting about software development and distributed systems.'
script.write_text('''import json,wave,numpy as np,importlib.metadata
from faster_whisper import WhisperModel
with wave.open('/input.wav') as f:
    assert (f.getnchannels(),f.getsampwidth(),f.getframerate())==(1,2,16000)
    samples=np.frombuffer(f.readframes(f.getnframes()),dtype='<i2').astype(np.float32)/32768.0
model=WhisperModel('/model',device='cpu',compute_type='int8',cpu_threads=2,num_workers=1,local_files_only=True)
segments,info=model.transcribe(samples,beam_size=5,vad_filter=False,condition_on_previous_text=False,word_timestamps=True,initial_prompt='This is a technical meeting about software development and distributed systems.')
print(json.dumps({'faster_whisper_version':importlib.metadata.version('faster-whisper'),'language':info.language,'duration':info.duration,'segments':[{'text':s.text,'start':s.start,'end':s.end,'avg_logprob':s.avg_logprob} for s in segments]},ensure_ascii=False))
''')
name='spark-asr-domain-context-'+uuid.uuid4().hex
image=subprocess.check_output(['docker','image','inspect','spark-media:development','--format','{{.Id}}'],text=True).strip()
with Path('/tmp/spark-media-asr-turbo/model.bin').open('rb') as stream:model_hash=hashlib.file_digest(stream,'sha256').hexdigest()
report={'kind':__doc__,'image_id':image,'beam_size':5,'initial_prompt':prompt,
 'input_sha256':hashlib.sha256((lab/'first-30s.wav').read_bytes()).hexdigest(),
 'script_sha256':hashlib.sha256(script.read_bytes()).hexdigest(),'model_sha256':model_hash,
 'expected_names_or_terms_supplied':False,'completed':False,'accuracy_gate_passed':False,'production_adopted':False}
args=['docker','run','--rm','--name',name,'--network','none','--read-only','--user',f'{os.getuid()}:{os.getgid()}',
 '--cpus','2','--memory','4g','--memory-swap','4g','--pids-limit','128','--cap-drop=ALL','--security-opt','no-new-privileges',
 '--tmpfs','/tmp:rw,noexec,nosuid,size=256m','-v',str(script)+':/probe.py:ro','-v',str(lab/'first-30s.wav')+':/input.wav:ro',
 '-v','/tmp/spark-media-asr-turbo:/model:ro','--entrypoint','python3',image,'/probe.py']
started=time.monotonic()
try:
 result=subprocess.run(args,timeout=240,capture_output=True,text=True)
 Path('/tmp/spark-asr-domain-context-stderr.log').write_text(result.stderr)
 report['decoder_exit_code']=result.returncode;result.check_returncode()
 value=json.loads(result.stdout);report.update(completed=True,asr=value)
 text=''.join(segment['text'] for segment in value['segments'])
 normalized=''.join(c for c in text.lower() if c.isalnum())
 report['accuracy_gate_passed']='alice' in normalized and 'queuebackpressure' in normalized
finally:
 subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 report['seconds']=round(time.monotonic()-started,3)
 Path('/tmp/spark-asr-domain-context.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False))
sys.exit(0 if report['accuracy_gate_passed'] else 1)
