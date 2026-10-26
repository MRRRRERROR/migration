"""Preserve owned resources and stop only identity-verified idle helpers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import time

import psutil
from two_gpu_nvml_monitor import NVML


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);args=parser.parse_args()
    root=Path(args.root);folder=root/'acceptance_C';repair=root.parent/'connector_shutdown_repair_20261007_01'
    roots=json.loads((folder/'owned_processes.json').read_text())['roots']
    pids={row['pid'] for row in roots}
    for role in 'SD':
        for path in (folder/f'state_{role}').glob('*.json'):
            pids.add(json.loads(path.read_text())['pid'])
    for name in ('native_socket_probe_gdb.log','native_socket_probe_explicit_gdb.log'):
        path=repair/name
        if path.exists():
            pids.update(int(pid) for pid in re.findall(r'KVT_SHUTDOWN event=\S+ pid=(\d+)',path.read_text(errors='replace')))
    kept=root/'preserved_barex_shared_memory';kept.mkdir(exist_ok=True)
    resources=[]
    for path in Path('/dev/shm').iterdir():
        if any(re.search(r'^(stats\.'+str(pid)+r'\.|SHM_BAREX_LOG_'+str(pid)+r'$|barexlock\..*\.'+str(pid)+r'$)',path.name) for pid in pids):
            try:
                shutil.copy2(path,kept/path.name)
                resources.append({'path':str(path),'bytes':path.stat().st_size,'copy':str(kept/path.name)})
            except OSError as error:
                resources.append({'path':str(path),'error':repr(error)})
    helpers=[]
    for row in roots:
        if row['name'] not in ('redis','nvml'):continue
        try:
            proc=psutil.Process(row['pid'])
            if proc.create_time()!=row['created']:raise RuntimeError('PID reused; leave process untouched')
            before=[{'pid':p.pid,'cmd':p.cmdline(),'created':p.create_time()} for p in [proc]+proc.children(recursive=True)]
            os.killpg(proc.pid,signal.SIGTERM)
            deadline=time.monotonic()+15
            while proc.is_running() and proc.status()!=psutil.STATUS_ZOMBIE and time.monotonic()<deadline:time.sleep(.1)
            helpers.append({'name':row['name'],'before':before,'signal':'SIGTERM','remaining':proc.is_running() and proc.status()!=psutil.STATUS_ZOMBIE})
        except psutil.NoSuchProcess:
            helpers.append({'name':row['name'],'already_exited':True})
    nv=NVML()
    identities={int(row['pid']):row['created'] for row in roots}
    safe=json.loads((folder/'source_safe_exit_evidence.json').read_text())
    identities.update({int(pid):created for pid,created in safe['owned_pids'].items()})
    live_owned=[]
    for pid,created in identities.items():
        try:
            process=psutil.Process(pid)
            if process.create_time()==created and process.status()!=psutil.STATUS_ZOMBIE:
                live_owned.append({'pid':pid,'status':process.status(),'cmd':process.cmdline()})
        except psutil.NoSuchProcess:pass
    library=Path('/data/home/zylu/.conda/envs/llumnix5090_vllm018/lib/libaccl_barex.so.1').resolve()
    report={'wall_ns':time.time_ns(),'status':'blocked; C target teardown segfault; explicit cleanup candidate rejected',
        'physical_GPUs':[nv.row(0,'S'),nv.row(1,'D')],'owned_pids':sorted(pids),'preserved_native_resources':resources,
        'helper_shutdown':helpers,'native_resources_deleted':False,'sigkill_used':False,
        'matching_operational_owned_processes':live_owned,
        'barex_library':{'path':str(library),'sha256':hashlib.sha256(library.read_bytes()).hexdigest()},
        'full_BC_acceptance_passed':False,'formal_rounds_executed':0}
    (root/'blocked_scene_evidence.json').write_text(json.dumps(report,indent=2))
    revoked=json.dumps({'basic_six_case_validation_passed':True,
        'migration_and_full_shutdown_validation_passed':False,'failed_result_directory':str(root),
        'candidate_lifecycle2_rejected':True,'reason':'C target segfault; GDB TCP context close/destruction failure with IO threads alive'},indent=2)
    (repair/'FREEZE_MIGRATION_VALIDATION_REVOKED.json').write_text(revoked)
    native_repair=Path('/data/home/zylu/llumnix-kvt500-2gpu/shutdown_repair_20261007_01')
    (native_repair/'FREEZE_MIGRATION_VALIDATION_REVOKED.json').write_text(revoked)
    print(json.dumps({'resources_preserved':len(resources),'gpu_compute_processes':[row['compute_pids'] for row in report['physical_GPUs']],'helpers':helpers}))


if __name__=='__main__':main()
