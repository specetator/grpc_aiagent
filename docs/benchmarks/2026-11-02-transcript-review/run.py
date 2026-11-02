import hashlib,json,sys,time,subprocess
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from cannbot.media import sandbox
lab=Path(__file__).parent;model=Path(sys.argv[2]);label=sys.argv[3]
source=lab/'first-30s.wav';destination=lab/(label+'-parsed')
image=subprocess.check_output(['docker','image','inspect','spark-media:development','--format','{{.Id}}'],text=True).strip()
model_sha=sandbox.model_fingerprint(model);parser_sha=sandbox.hash_file(root/'cannbot/media/parser.py')
started=time.monotonic()
report={'kind':'real isolated ASR diagnostic, no Agent/IM','input':json.loads((lab/'input.json').read_text()),'image_id':image,'model_sha256':model_sha,'parser_sha256':parser_sha,'ok':False}
try:
    manifest=sandbox.run(source,'audio/wav',destination,image_id=image,parser_sha=parser_sha,source_sha=sandbox.hash_file(source),model_root=model,model_sha=model_sha,timeout=240)
    report.update(ok=True,coverage=manifest['coverage'],fragments=manifest['fragments'])
finally:
    report['seconds']=round(time.monotonic()-started,3)
    (lab/(label+'.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False))
