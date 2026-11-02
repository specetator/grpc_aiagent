"""One real ASR decoder diagnostic; no IM/Agent/pipeline success is implied."""
import hashlib,json,os,subprocess,time,uuid
from pathlib import Path
root=Path('/home/peco/cppcode/fenbushi/sparkpush_project')
lab=Path('/tmp/spark-asr-checkpoint-ravzgwns')
script=lab/'greedy-decoder.py'
script.write_text('''import json,wave,numpy as np
from faster_whisper import WhisperModel
with wave.open('/input.wav') as f:
    assert (f.getnchannels(),f.getsampwidth(),f.getframerate())==(1,2,16000)
    samples=np.frombuffer(f.readframes(f.getnframes()),dtype='<i2').astype(np.float32)/32768.0
model=WhisperModel('/model',device='cpu',compute_type='int8',cpu_threads=2,num_workers=1,local_files_only=True)
segments,info=model.transcribe(samples,beam_size=1,vad_filter=False,condition_on_previous_text=False,word_timestamps=True)
print(json.dumps({'language':info.language,'duration':info.duration,'segments':[{'text':s.text,'start':s.start,'end':s.end,'avg_logprob':s.avg_logprob} for s in segments]},ensure_ascii=False))
''')
name='spark-asr-greedy-'+uuid.uuid4().hex
image=subprocess.check_output(['docker','image','inspect','spark-media:development','--format','{{.Id}}'],text=True).strip()
report={'kind':__doc__,'image_id':image,'beam_size':1,'input_sha256':hashlib.sha256((lab/'first-30s.wav').read_bytes()).hexdigest(),'script_sha256':hashlib.sha256(script.read_bytes()).hexdigest(),'model_sha256':json.loads(Path('/tmp/spark-asr-large-v3-provision.json').read_text())['model_sha256'],'ok':False}
args=['docker','run','--rm','--name',name,'--network','none','--read-only','--user',f'{os.getuid()}:{os.getgid()}','--cpus','2','--memory','4g','--memory-swap','4g','--pids-limit','128','--cap-drop=ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,noexec,nosuid,size=256m','-v',str(script)+':/probe.py:ro','-v',str(lab/'first-30s.wav')+':/input.wav:ro','-v','/tmp/spark-media-asr-large-v3:/model:ro','--entrypoint','python3',image,'/probe.py']
started=time.monotonic()
try:
    result=subprocess.run(args,timeout=240,capture_output=True,text=True)
    Path('/tmp/spark-asr-greedy-stderr.log').write_text(result.stderr)
    report['exit_code']=result.returncode
    result.check_returncode()
    report.update(ok=True,asr=json.loads(result.stdout))
finally:
    subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    report['seconds']=round(time.monotonic()-started,3)
    (lab/'large-v3-greedy.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False))
