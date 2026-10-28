"""Finite 30-run phase; requires an explicit evidence-based pretrial gate."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import shutil
import sys
import traceback

from runner import Incremental,poisson,ROOT,base
from engine_control import atomic

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

async def main():
    p=argparse.ArgumentParser();p.add_argument('--experiment',default=str(ROOT));a=p.parse_args();experiment=Path(a.experiment)
    gate=json.loads((experiment/'pretrial_gate.json').read_text())
    if gate['decision']!='formal':raise RuntimeError('Pretrial gate does not authorize formal expansion')
    frozen=json.loads((experiment/'frozen_pretrial.json').read_text())
    root=experiment/'formal_execution_01'
    if root.exists():raise RuntimeError('Formal directory exists; inspect and resume explicitly, never overwrite')
    (root/'inputs').mkdir(parents=True)
    shutil.copy2(experiment/'inputs/prompts.jsonl',root/'inputs/prompts.jsonl')
    h=Incremental(root)
    if asdict(h.cfg)!=frozen['policy']:raise RuntimeError('Policy changed after pretrial freeze')
    # Pretrials and formal use identical engine hooks. Only controller fixes
    # resolving declared invalid rounds may precede a new freeze.
    before=experiment/'failures/01_event_name_parser/execution_sources'
    for name in ['engine_control.py','sitecustomize.py','policy.py']:
        if sha(before/name)!=sha(experiment/'execution_sources'/name):raise RuntimeError('Engine/policy hook differs from actually tested version: '+name)
    source=experiment/'execution_sources';shutil.copytree(source,root/'execution_sources')
    (root/'correctness_01').mkdir()
    shutil.copy2(experiment/'correctness_01/result.json',root/'correctness_01/result.json')
    atomic(root/'correctness_01/qualification_origin.json',dict(origin=str(experiment/'correctness_01'),reused_same_engine_hooks=True,native_hash_rechecked_before_replay=True))
    manifest={str(f.relative_to(source)):sha(f) for f in source.glob('*.py')}
    atomic(root/'frozen_code.json',manifest);atomic(root/'frozen_config.json',frozen)
    order=[]
    permutations=['ABC','BCA','CAB','ACB','CBA','BAC']
    for k,seed in enumerate(frozen['formal_seeds']):
        loads=['medium','high'] if k%2==0 else ['high','medium']
        for load in loads:
            rate=frozen['loads'][load];offset=0 if load=='medium' else 1
            trace=poisson(h.prompts,rate,seed,600,'formal_'+load)
            path=root/'inputs'/f'{load}_s{seed}.json';atomic(path,trace)
            for scheme in permutations[(k+offset)%len(permutations)]:
                order.append(dict(name=f'formal_{load}_s{seed}_{scheme}',mode=scheme,load=load,seed=seed,rate=rate,trace=str(path)))
    assert len(order)==30;atomic(root/'order.json',order)
    h.beat('formal_deploying',planned_rounds=30)
    try:
        await h.deploy(h.service,'kv')
        expected={r['path']:r['sha256'] for r in json.loads((experiment/'services_01/native_loaded.json').read_text())}
        loaded={r['path']:r['sha256'] for r in json.loads((h.service/'native_loaded.json').read_text())}
        atomic(root/'native_comparison.json',dict(expected=expected,actual=loaded,passed=expected==loaded))
        if expected!=loaded:raise RuntimeError('Formal native libraries differ from pretrials')
        await h.start_gateway()
        h.start('nvml',f'{sys.executable} -u tools/two_gpu_live_nvml.py --gpus 0,1 --output {h.service}/power.csv',h.service)
        h.start('prometheus',f'{sys.executable} -u {root}/execution_sources/metrics_monitor.py {root}',h.service)
        for row in order:await h.round(row['name'],row['mode'],json.loads(Path(row['trace']).read_text()))
        h.beat('formal_measurements_complete',planned_rounds=30)
        (root/'STOP_METRICS').touch();await h.gateway_runner.cleanup();await h.proxy_session.close()
        await h.cleanup(h.service)
    except BaseException as error:
        atomic(root/'run_failure.json',dict(error=repr(error),traceback=traceback.format_exc(),states=[h.state(i) for i in range(2)],preserve_scene=True))
        h.beat('blocked',error=repr(error));raise

if __name__=='__main__':asyncio.run(main())
