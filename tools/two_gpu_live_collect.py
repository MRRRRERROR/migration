"""Read-only evidence capture for completed rounds; no service or native changes."""
import argparse
import hashlib
import json
from pathlib import Path
import time

def write(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2))

def collect(root):
    project=Path('/data/home/zylu/llumnix-v018-main')
    rounds=[p for p in root.iterdir() if p.is_dir() and (p/'summary.json').exists()]
    pid_round={}
    for d in rounds:
        for role in ('S','D'):
            for p in (d/('state_'+role)).glob('*.json'):
                s=json.loads(p.read_text());pid_round[s['pid']]=(d,role)
    grouped={d:[] for d in rounds}
    trace=project/'kvt500_minimal_trace.jsonl'
    if trace.exists():
        with trace.open() as f:
            for line in f:
                try:row=json.loads(line)
                except json.JSONDecodeError:continue
                target=pid_round.get(row.get('pid'))
                if target:grouped[target[0]].append(row)
    for d,rows in grouped.items():
        with (d/'native_attempt_trace.jsonl').open('w') as f:
            for row in rows:f.write(json.dumps(row,ensure_ascii=False)+'\n')
        write(d/'native_attempt_trace_provenance.json',dict(source=str(trace),filtered_engine_pids=[pid for pid,(folder,_) in pid_round.items() if folder==d],records=len(rows),collected_wall_ns=time.time_ns()))
    release=project/'results/token_migration_20261008_01/delivery_20261009_route_01'
    verification=[]
    for row in json.loads((release/'manifest.json').read_text()):
        p=Path(row['path']);actual=hashlib.sha256(p.read_bytes()).hexdigest()
        verification.append(dict(path=str(p),expected=row['after_sha256'],actual=actual,unchanged=actual==row['after_sha256']))
    write(root/'delivery_integrity_after.json',verification)
    files=list((project/'tools').glob('two_gpu_live*.py'))+[project/'tools/live_load_observer/sitecustomize.py',project/'tools/test_two_gpu_live_metrics.py']
    write(root/'experiment_tools_manifest.json',[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files])
    sources=root/'experiment_sources';sources.mkdir(exist_ok=True)
    for p in files:(sources/p.name).write_bytes(p.read_bytes())
    print(json.dumps(dict(rounds=len(rounds),native_records={d.name:len(v) for d,v in grouped.items()},unchanged=all(x['unchanged'] for x in verification))))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();collect(Path(a.root))
