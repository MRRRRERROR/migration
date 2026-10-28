"""Read-only process/GPU evidence, with optional previously verified identities."""
import argparse
import csv
import io
import json
from pathlib import Path
import subprocess
import time

import psutil


PORTS={16579,18270,18271,41270,41271,41370,41371,59170,59171,46570,46578,18370}
UUIDS={'GPU-0fc3a8e6-93f3-9c89-f694-bf490b219907','GPU-9ce9c226-678f-8cb4-4cf6-47c18d5b638a'}


def main():
    p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('output');p.add_argument('--baseline');a=p.parse_args()
    root=Path(a.root).resolve();service=root/'services_01';known={}
    for row in json.loads((service/'owned_processes.json').read_text()):known[row['pid']]=row.copy()
    controller=json.loads((root/'paired_abc_01/controller_identity.json').read_text())
    known[controller['pid']]=dict(pid=controller['pid'],created=controller['created'],label='controller')
    if a.baseline:
        for row in json.loads(Path(a.baseline).read_text())['verified_identities']:known[row['pid']]=row.copy()
    records=[];verified=[]
    def capture(row):
        try:
            proc=psutil.Process(row['pid']);matches=proc.create_time()==row['created']
            status=proc.status();entry=dict(**row,exists=True,identity_matches=matches,status=status)
            if matches:
                entry.update(cmdline=proc.cmdline(),ppid=proc.ppid(),uid=proc.uids().real)
                verified.append(row.copy())
            records.append(entry);return proc if matches and status!=psutil.STATUS_ZOMBIE else None
        except psutil.NoSuchProcess:records.append(dict(**row,exists=False,identity_matches=False));return None
    for row in list(known.values()):
        proc=capture(row)
        if proc is not None and row['label'].startswith('vllm_'):
            for child in proc.children(recursive=True):
                if child.pid not in known:
                    detail=dict(pid=child.pid,created=child.create_time(),label='model_child',verified_parent=row['pid'])
                    known[child.pid]=detail;capture(detail)
    # The independent metrics reader has an unambiguous task-specific command.
    if not a.baseline:
        try:
            proc=psutil.Process(349780);cmd=proc.cmdline()
            if str(root) in cmd and any(x.endswith('/metrics_monitor.py') for x in cmd):
                row=dict(pid=proc.pid,created=proc.create_time(),label='prometheus');known[proc.pid]=row;capture(row)
        except psutil.NoSuchProcess:pass
    gpu=subprocess.run(['nvidia-smi','--query-compute-apps=gpu_uuid,pid,used_memory','--format=csv,noheader,nounits'],capture_output=True,text=True)
    apps=[]
    if gpu.returncode==0:
        for row in csv.reader(io.StringIO(gpu.stdout)):
            if row and row[0].strip() in UUIDS:apps.append(dict(gpu_uuid=row[0].strip(),pid=int(row[1]),used_memory_mib=row[2].strip()))
    device=subprocess.run(['nvidia-smi','--query-gpu=index,uuid,memory.used,utilization.gpu,power.draw','--format=csv,noheader,nounits'],capture_output=True,text=True)
    active=[r for r in records if r.get('identity_matches') and r.get('status')!=psutil.STATUS_ZOMBIE]
    model=[r for r in active if r['label'].startswith('vllm_') or r['label']=='model_child']
    ours={r['pid'] for r in active};own_apps=[r for r in apps if r['pid'] in ours]
    listening=[]
    for c in psutil.net_connections(kind='inet'):
        if c.status==psutil.CONN_LISTEN and c.laddr.port in PORTS:listening.append(dict(port=c.laddr.port,pid=c.pid,verified_owned=c.pid in ours))
    states={}
    for role in 'SD':
        f=service/('state_'+role)/'state.json'
        if f.exists():
            s=json.loads(f.read_text());states[role]={k:s.get(k) for k in ['pid','wall_ns','monotonic_s','mode','running','waiting','holds','migration_tasks','migration_finish']}
            states[role]['request_ids']=[r['request_id'] for r in s.get('requests',[])]
    result=dict(wall_ns=time.time_ns(),monotonic_s=time.monotonic(),root=str(root),verified_identities=verified,
        processes=records,owned_residual_pids=sorted(ours),model_residual_pids=[r['pid'] for r in model],
        compute_query_returncode=gpu.returncode,compute_query_stderr=gpu.stderr,selected_gpu_compute_apps=apps,
        verified_owned_gpu_apps=own_apps,gpu_released=gpu.returncode==0 and not own_apps and not model,
        auxiliary_released=not active and not any(r['verified_owned'] for r in listening),
        selected_devices=[line for line in device.stdout.splitlines() if any(u in line for u in UUIDS)],
        listening_experiment_ports=listening,last_engine_states=states,
        note='No signals sent. New unrelated processes on released devices/ports are not owned and must not be stopped.')
    with Path(a.output).open('x',encoding='utf-8') as out:json.dump(result,out,ensure_ascii=False,indent=2)
    print(json.dumps({k:result[k] for k in ['gpu_released','auxiliary_released','owned_residual_pids','selected_gpu_compute_apps']}))


if __name__=='__main__':main()
