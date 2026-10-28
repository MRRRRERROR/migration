"""Read-only serving environment inventory and tokenizer consistency audit."""
import hashlib
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

P=Path('/data/home/zylu/llumnix-v018-main')
R=P/'results/incremental_runtime_migration_20261027_01'
def run(args):
    r=subprocess.run(args,cwd=P,capture_output=True,text=True,timeout=30)
    return dict(argv=args,returncode=r.returncode,stdout=r.stdout,stderr=r.stderr)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(name,value):(R/'audit'/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str)+'\n')

def main():
    global R
    parser=argparse.ArgumentParser();parser.add_argument('--root',default=str(R));args=parser.parse_args();R=Path(args.root).resolve()
    if not R.is_relative_to(P/'results'):raise ValueError('Audit output must stay under project results')
    (R/'audit').mkdir(parents=True,exist_ok=True);(R/'inputs').mkdir(exist_ok=True)
    commands=[['git','rev-parse','HEAD'],['git','status','--porcelain'],['git','diff','--stat'],
        ['nvidia-smi','--query-gpu=index,uuid,name,memory.used,utilization.gpu','--format=csv'],
        ['nvidia-smi','--query-compute-apps=gpu_uuid,pid,process_name,used_memory','--format=csv']]
    write('environment.json',dict(time_ns=time.time_ns(),project=str(P),python=os.sys.executable,
        packages={k:importlib.metadata.version(k) for k in ['vllm','blade_kvt','llumnix','torch','transformers','aiohttp','grpcio']},
        commands=[run(c) for c in commands]))
    (R/'audit'/'tracked_changes.patch').write_text(run(['git','diff','--binary'])['stdout'])
    model=Path('/data/models/Qwen2.5-7B-Instruct')
    modelmeta=[dict(path=str(f),sha256=sha(f),size=f.stat().st_size) for f in model.iterdir() if f.suffix=='.json' or 'tokenizer' in f.name or f.name.endswith('.index.json')]
    write('model_tokenizer.json',modelmeta)
    write('generation_config.json',json.loads((model/'generation_config.json').read_text()))
    src=P/'results/migration_net_benefit_20261014_01/runtime_verified_01/inputs/prompts.jsonl'
    destination=R/'inputs/prompts.jsonl'
    if destination.exists():
        if sha(destination)!=sha(src):raise RuntimeError('Frozen prompt pool differs; do not overwrite')
    else:shutil.copy2(src,destination)
    from transformers import AutoTokenizer
    tokenizer=AutoTokenizer.from_pretrained(model,trust_remote_code=False)
    # U+2028/U+2029 are legal unescaped JSON string content, not JSONL delimiters.
    rows=[json.loads(l) for l in destination.read_text().split('\n') if l.strip()]
    checks=[];texts=set()
    for row in rows:
        encoded=tokenizer.apply_chat_template([dict(role='user',content=row['text'])],tokenize=True,add_generation_prompt=True)
        checks.append(dict(prompt_index=row['prompt_index'],matching=encoded==row['token_ids'],input_tokens=len(encoded),
            duplicate=row['text'] in texts,within_range=128<=len(encoded)<=4096))
        texts.add(row['text'])
    write('prompt_pool_checks.json',dict(source=str(src),source_sha256=sha(src),copy_sha256=sha(destination),
        encoding='target tokenizer apply_chat_template(single original user text, add_generation_prompt=True)',
        unique_texts=len(texts),total=len(rows),checks=checks,all_passed=all(c['matching'] and c['within_range'] and not c['duplicate'] for c in checks)))
    if not all(c['matching'] and c['within_range'] and not c['duplicate'] for c in checks):raise RuntimeError('Prompt pool audit failed')
    from vllm.v1.request import Request
    from vllm.sampling_params import SamplingParams
    from llumnix.compat.hybrid_connector_compat import req2corereq
    import copy,pickle
    req=Request('cpu_only_id',rows[0]['token_ids'],SamplingParams(max_tokens=4096,temperature=0),None,arrival_time=123.,queue_server_address='preserved')
    clone=copy.copy(req);clone.sampling_params=copy.deepcopy(req.sampling_params)
    dto=req2corereq(clone);dto.client_index=req.client_index
    decoded=pickle.loads(pickle.dumps(dto,protocol=5));roundtrip=Request.from_engine_core_request(decoded,None)
    ok=roundtrip.request_id==req.request_id and roundtrip.arrival_time==123. and roundtrip.queue_server_address=='preserved' and roundtrip.num_computed_tokens==0
    write('cpu_dto_roundtrip.json',dict(passed=ok,input_tokens=len(roundtrip.prompt_token_ids),sampling=str(roundtrip.sampling_params)))
    assert ok
    print('AUDIT_CPU_PASSED',len(rows),flush=True)

if __name__=='__main__':main()
