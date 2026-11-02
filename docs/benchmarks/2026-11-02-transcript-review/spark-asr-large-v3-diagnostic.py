"""Fixed public candidate provisioning for a bounded diagnostic, not app config."""
import json,sys
from pathlib import Path
sys.path.insert(0,'/home/peco/cppcode/fenbushi/sparkpush_project')
from cannbot.media import provision_asr
provision_asr.MODELS['large-v3']={
    'repository':'Systran/faster-whisper-large-v3',
    'revision':'edaa852ec7e145841d8ffdb056a99866b5f0a478',
    'files':{'config.json','model.bin','tokenizer.json','vocabulary.json','preprocessor_config.json'},
    'weights_limit':4*1024**3,
}
result=provision_asr.provision(Path('/tmp/spark-media-asr-large-v3'), 'large-v3')
Path('/tmp/spark-asr-large-v3-provision.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'model':result['model'],'model_sha256':result['model_sha256'],'files':len(result['files'])}),flush=True)
