import json,wave,numpy as np
from faster_whisper import WhisperModel
with wave.open('/input.wav') as f:
    assert (f.getnchannels(),f.getsampwidth(),f.getframerate())==(1,2,16000)
    samples=np.frombuffer(f.readframes(f.getnframes()),dtype='<i2').astype(np.float32)/32768.0
model=WhisperModel('/model',device='cpu',compute_type='int8',cpu_threads=2,num_workers=1,local_files_only=True)
segments,info=model.transcribe(samples,beam_size=1,vad_filter=False,condition_on_previous_text=False,word_timestamps=True)
print(json.dumps({'language':info.language,'duration':info.duration,'segments':[{'text':s.text,'start':s.start,'end':s.end,'avg_logprob':s.avg_logprob} for s in segments]},ensure_ascii=False))
