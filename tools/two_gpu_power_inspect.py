import argparse
import csv
import json
from pathlib import Path
import time

p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();root=Path(a.root)
print('HOST_CLOCK',time.monotonic(),time.time(),flush=True)
for log in root.glob('controller*.log'):
    lines=log.read_text(errors='replace').splitlines()
    print(log.name,'\n'.join([s for s in lines if s.startswith(('DEPLOY_READY','T0 ','SOURCE_EXIT','ROUND_DONE','CAPACITY','RuntimeError'))][-5:]),flush=True)
for f in root.glob('*blocked.json'):print('BLOCKED',f.read_text(),flush=True)
for f in root.glob('*/blocked.json'):print('BLOCKED',f.read_text(),flush=True)
rounds=sorted([p for p in root.iterdir() if p.is_dir() and (p/'round_config.json').exists()],key=lambda p:p.stat().st_mtime)
for folder in rounds[-2:]:
    results=[]
    f=folder/'request_results.jsonl'
    if f.exists():
        with f.open() as stream:results=[json.loads(s) for s in stream if s.strip()]
    print('ROUND',folder.name,'client_finished',len(results),'errors',sum(bool(r.get('error')) for r in results),flush=True)
    for role in ('S','D'):
        files=list((folder/('state_'+role)).glob('*.json'))
        if files:
            state=json.loads(files[0].read_text())
            print(role,{k:state.get(k) for k in ('monotonic_s','running','waiting','frontend_observed','migration_tasks','migrating_request_ids')},flush=True)
    if (folder/'summary.json').exists():
        summary=json.loads((folder/'summary.json').read_text());print('SUMMARY',{k:summary.get(k) for k in ('sent','completed','raw_counts','send_delay_s')},flush=True)
    if (folder/'observer_errors.jsonl').exists():print('OBSERVER_ERROR',folder.name,flush=True)
