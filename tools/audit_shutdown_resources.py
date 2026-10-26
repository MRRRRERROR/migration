"""Read-only audit restricted to processes identified by exit evidence."""
import argparse
import json
from pathlib import Path
import re
import time

import psutil


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    args=parser.parse_args()
    root=Path(args.root)
    cases=[]
    for result in sorted(root.glob('final_*/result.json')):
        pids={}
        for exit_path in result.parent.glob('exit_*.json'):
            for row in json.loads(exit_path.read_text())['before']:
                pids[row['pid']]=row['created']
        live=[]
        for pid,created in pids.items():
            try:
                proc=psutil.Process(pid)
                if proc.create_time()==created:
                    live.append({'pid':pid,'status':proc.status()})
            except psutil.NoSuchProcess:
                pass
        shared=[]
        for path in Path('/dev/shm').iterdir():
            name=path.name
            for pid in pids:
                if re.search(r'^(stats\.'+str(pid)+r'\.|SHM_BAREX_LOG_'+str(pid)+r'$|barexlock\..*\.'+str(pid)+r'$)',name):
                    shared.append(name)
        cases.append({'case':result.parent.name,'known_pids':list(pids),'matching_live_processes':live,
                      'matching_barex_shared_memory':shared,'pass':not live and not shared})
    report={'wall_ns':time.time_ns(),'cases':cases,'passed':len(cases)==6 and all(row['pass'] for row in cases),
            'scope':'Verified PID/create-time identities and PID-named Barex shared memory; unrelated host resources excluded; no deletions performed'}
    (root/'post_exit_resource_audit.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))
    if not report['passed']:
        raise SystemExit(2)


if __name__=='__main__':
    main()
