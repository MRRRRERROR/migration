"""One-shot safe controller refresh between calibration and acceptance."""
import json
import os
from pathlib import Path
import psutil
import signal
import subprocess
import time

root=Path('/data/home/zylu/llumnix-v018-main/results/two_gpu_power_drain_20261006_2119/attempt_02')
controller=psutil.Process(4049475);created=controller.create_time()
assert 'two_gpu_power_drain.py' in ' '.join(controller.cmdline())
deadline=time.monotonic()+180
while not (root/'calibration_0_D/summary.json').exists():
    if not controller.is_running():raise RuntimeError('Controller stopped before calibration completed')
    if time.monotonic()>deadline:raise RuntimeError('Calibration still in progress: do not interrupt streams')
    time.sleep(.2)
os.kill(controller.pid,signal.SIGSTOP)
try:
    assert controller.create_time()==created
    # A successful round summary is written only after every client has finished.
    summary=json.loads((root/'calibration_0_D/summary.json').read_text())
    assert summary['planned']==summary['completed']
    active=[]
    for folder in root.iterdir():
        if not folder.is_dir() or (folder/'summary.json').exists():continue
        if (folder/'request_results.jsonl').exists():raise RuntimeError('Next acceptance already started: refuse refresh')
        owners=folder/'owned_processes.json'
        if owners.exists():active.extend(json.loads(owners.read_text())['roots'])
    for r in active:
        if not psutil.pid_exists(r['pid']):continue
        p=psutil.Process(r['pid']);assert p.create_time()==r['created'] and p.uids().real==os.getuid()
        # No replay has started in this phase; retain its logs before teardown.
        children=p.children(recursive=True)
        for child in children:child.terminate()
        p.terminate();_,alive=psutil.wait_procs(children+[p],timeout=30)
        alive=[p for p in alive if p.status()!=psutil.STATUS_ZOMBIE]
        if alive:raise RuntimeError('Own empty setup processes did not exit gracefully')
    os.kill(controller.pid,signal.SIGTERM);os.kill(controller.pid,signal.SIGCONT)
    psutil.wait_procs([controller],timeout=10)
    for folder in list(root.iterdir()):
        if folder.is_dir() and (folder/'round_config.json').exists() and not (folder/'summary.json').exists():
            folder.rename(folder.with_name(folder.name+'_setup_before_controller_refresh'))
    reason={'reason':'Before formal freeze: exclude logged prompt error strings from runtime faults; preserve streams on timeout; wait for all owned child processes on graceful exit.',
        'completed_calibration_reused':True,'replay_requests_interrupted':0,'previous_controller_pid':controller.pid,'wall_ns':time.time_ns()}
    (root/'controller_refresh.json').write_text(json.dumps(reason,indent=2))
    log=(root/'controller_final.log').open('x')
    command=['bash','/data/home/zylu/llumnix-v018-main/tools/start_two_gpu_power_drain.sh','--root',str(root)]
    p=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    (root/'controller_final.pid').write_text(str(p.pid))
    print('CONTROLLER_REFRESHED',p.pid,flush=True)
except BaseException:
    try:os.kill(controller.pid,signal.SIGCONT)
    except ProcessLookupError:pass
    raise
