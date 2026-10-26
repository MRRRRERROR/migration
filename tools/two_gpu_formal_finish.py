"""Bounded experiment completion handler: archive evidence, never expand the plan."""
import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
import subprocess
import tarfile
import time
import psutil
import os
from two_gpu_formal_report import report
from two_gpu_live_collect import collect
from two_gpu_live_nvml import NVML

def main(root,gpus):
    while not (root/'completed.json').exists() and not (root/'blocked.json').exists():time.sleep(5)
    for scenario in 'ALH':
        folder=root/scenario
        if folder.exists():collect(folder)
    report(root)
    selected=root/'current_gpu_selection.json'
    if selected.exists():gpus=json.loads(selected.read_text())['gpus']
    used=set(gpus)
    for path in root.rglob('gpu_mapping.json'):
        for row in json.loads(path.read_text()):used.add(row['gpu_index'])
    readings=[];nv=NVML()
    for i in range(2):
        if i:time.sleep(10)
        readings.append([nv.row(index,role) for index,role in zip(gpus,'SD')])
    idle=all(not json.loads(r['compute_pids']) and r['utilization_pct']==0 and r['memory_mib']<=100 for sample in readings for r in sample)
    all_used_readings=[nv.row(index,'used') for index in sorted(used)]
    identities=[]
    for path in root.rglob('owned_processes.json'):identities.extend(json.loads(path.read_text()))
    tree=root/'owned_tree_identity_samples.jsonl'
    if tree.exists():
        for line in tree.open():identities.extend(json.loads(line)['processes'])
    residuals={}
    for item in identities:
        pid=item['pid']
        if pid==os.getpid():continue
        try:
            process=psutil.Process(pid)
            if process.create_time()==item['created'] and process.status()!=psutil.STATUS_ZOMBIE:
                residuals[pid]=dict(pid=pid,created=process.create_time(),command=process.cmdline())
        except psutil.NoSuchProcess:pass
    guard=json.loads((root/'actual_old_revocation_protections.json').read_text())['files']
    guard_hashes=[dict(path=x['path'],expected=x['sha256'],actual=hashlib.sha256(Path(x['path']).read_bytes()).hexdigest()) for x in guard]
    (root/'finish_audit.json').write_text(json.dumps(dict(gpu_checks=readings,gpus_idle=idle,current_gpus=gpus,used_gpu_checks=all_used_readings,
        owned_residuals=list(residuals.values()),experiment_processes_released=not residuals,
        other_processes_on_previously_used_cards_do_not_imply_our_residual=True,old_protections=guard_hashes,
        old_protections_preserved=all(x['actual']==x['expected'] for x in guard_hashes),normal_unload_qualified=False,
        completed=json.loads((root/'completed.json').read_text()) if (root/'completed.json').exists() else None,
        blocked=json.loads((root/'blocked.json').read_text()) if (root/'blocked.json').exists() else None),indent=2))
    sources=root/'analysis_sources';sources.mkdir(exist_ok=True)
    for name in ('two_gpu_formal_load.py','two_gpu_formal_report.py','two_gpu_formal_finish.py'):
        p=Path(__file__).parent/name;shutil.copy2(p,sources/name)
    (root/'analysis_sources_manifest.json').write_text(json.dumps([dict(path=p.name,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sources.glob('*.py')],indent=2))
    # Small data-only archive for plotting with the existing local CPU runtime.
    with tarfile.open(root/'plot_data.tar.gz','w:gz') as archive:
        for p in root.glob('*/main_*/power.csv'):archive.add(p,arcname=str(p.relative_to(root)))
        for p in root.glob('*/main_*/timeline.json'):archive.add(p,arcname=str(p.relative_to(root)))
    (root/'report_ready.json').write_text(json.dumps(dict(ready=True,gpus_idle=idle,wall_ns=time.time_ns(),plots_pending_local_cpu=True)))
    print('REPORT_READY',str(root),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);a=p.parse_args()
    main(Path(a.root),[int(x) for x in a.gpus.split(',')])
