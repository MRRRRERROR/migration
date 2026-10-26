"""Resume the fixed coverage phase only after completed main measurement cleanup."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import psutil

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);a=p.parse_args();root=Path(a.root)
    while not (root/'main_measurements_complete_hold.json').exists():
        if (root/'blocked.json').exists() or (root/'semantic_blocked.json').exists():sys.exit(0)
        time.sleep(1)
    hold=json.loads((root/'main_measurements_complete_hold.json').read_text())
    deadline=time.monotonic()+15
    while True:
        try:
            previous=psutil.Process(hold['pid'])
            if previous.create_time()!=hold['created'] or previous.status()==psutil.STATUS_ZOMBIE:break
        except psutil.NoSuchProcess:break
        if time.monotonic()>deadline:raise RuntimeError('Previous coordinator still alive')
        time.sleep(.1)
    if (root/'blocked.json').exists() or (root/'semantic_blocked.json').exists():sys.exit(0)
    partial=root/'coverage_B'
    if partial.exists():
        if (partial/'owned_processes.json').exists():raise RuntimeError('Coverage already started; do not duplicate')
        archive=root/'setup_records';archive.mkdir(exist_ok=True);partial.rename(archive/'coverage_B_before_full_power_baseline')
    project=Path('/data/home/zylu/llumnix-v018-main')
    with (root/'coverage_controller.log').open('x') as f:
        controller=subprocess.Popen([sys.executable,'-u',str(project/'tools/two_gpu_live_coverage.py'),'--root',str(root),'--gpus',a.gpus],
            cwd=project,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    (root/'coverage_controller.pid').write_text(str(controller.pid))
    with (root/'coverage_semantic_guard.log').open('x') as f:
        guard=subprocess.Popen([sys.executable,'-u',str(project/'tools/two_gpu_live_guard.py'),'--root',str(root),
            '--controller-file','coverage_controller.pid','--identity-name','coverage_controller_identity.json'],cwd=project,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    (root/'coverage_semantic_guard.pid').write_text(str(guard.pid))
