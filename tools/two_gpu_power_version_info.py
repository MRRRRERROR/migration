import hashlib
import argparse
import importlib.metadata as metadata
import json
from pathlib import Path
import platform
import subprocess
import sys

parser=argparse.ArgumentParser()
parser.add_argument('--root',required=True)
args=parser.parse_args()
root=Path(args.root)
model=Path('/data/models/Qwen2.5-7B-Instruct')
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        while block:=f.read(8*1024*1024):h.update(block)
    return h.hexdigest()
packages={}
for name in ('vllm','torch','transformers','grpcio','aiohttp','psutil','redis'):
    try:packages[name]=metadata.version(name)
    except metadata.PackageNotFoundError:packages[name]=None
git=subprocess.run(['git','-C','/data/home/zylu/llumnix-v018-main','rev-parse','HEAD'],capture_output=True,text=True)
result={'python':platform.python_version(),'executable':sys.executable,'packages':packages,
    'git_commit':git.stdout.strip() if git.returncode==0 else None,
    'git_unavailable_reason':git.stderr.strip() if git.returncode else None,
    'serving_version_fingerprint':'version_manifest.json and FINAL_SHUTDOWN_ACCEPTANCE.json; frozen serving version checked before every deployment',
    'model_generation_config':json.loads((model/'generation_config.json').read_text()),
    'model_weights':[{'path':str(p),'size_bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(model.glob('*.safetensors'))],
    'result_directory':str(root)}
(root/'runtime_and_model_versions.json').write_text(json.dumps(result,indent=2))
print(json.dumps({k:result[k] for k in ('python','packages','git_commit')}),flush=True)
