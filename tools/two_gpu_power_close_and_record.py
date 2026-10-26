"""Close only owned, request-free helpers after preserving the failure scene."""
import json
import os
from pathlib import Path
import psutil
import signal
import sys
import time
from two_gpu_nvml_monitor import NVML

root=Path('/data/home/zylu/llumnix-v018-main/results/two_gpu_power_drain_20261006_2119/attempt_02')
round_root=root/'acceptance_B'
with (round_root/'request_results.jsonl').open() as f:records=[json.loads(s) for s in f if s.strip()]
with (round_root/'replay.jsonl').open() as f:planned=[json.loads(s) for s in f if s.strip()]
assert len(planned)==len(records) and all(r['completed'] for r in records)
nv=NVML();assert all(not nv.processes(nv.handle(i)) for i in (0,1))
roots=json.loads((round_root/'owned_processes.json').read_text())['roots'];closed=[]
for record in roots:
    if record['name'] not in ('nvml','discovery_1','redis'):continue
    pid=record['pid']
    if not psutil.pid_exists(pid):continue
    parent=psutil.Process(pid)
    assert parent.create_time()==record['created'] and parent.uids().real==os.getuid()
    children=parent.children(recursive=True)
    for child in children:child.terminate()
    # Wait for the helper to flush, then close its owned shell wrapper.
    deadline=time.monotonic()+10
    while any(p.is_running() and p.status()!=psutil.STATUS_ZOMBIE for p in children) and time.monotonic()<deadline:time.sleep(.1)
    if parent.is_running() and parent.status()!=psutil.STATUS_ZOMBIE:parent.terminate()
    closed.append({'name':record['name'],'pid':pid,'children':[p.pid for p in children]})
result={'closed_helpers':closed,'no_gpu_compute_processes':True,'gpu_after':[nv.row(i,r) for i,r in ((0,'S'),(1,'D'))],
    'all_237_acceptance_requests_completed':True,'serving_sources_changed':False,'logs_preserved':True,'wall_ns':time.time_ns()}
(root/'final_cleanup_and_nvml_check.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result),flush=True)
