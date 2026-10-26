"""Test API-led graceful shutdown on the empty, owned acceptance destination."""
import json
from pathlib import Path
import psutil
import signal
import time

root=Path('/data/home/zylu/llumnix-v018-main/results/two_gpu_power_drain_20261006_2119/attempt_02/acceptance_B')
with (root/'replay.jsonl').open() as f:planned=[json.loads(s) for s in f if s.strip()]
with (root/'request_results.jsonl').open() as f:finished=[json.loads(s) for s in f if s.strip()]
assert len(planned)==len(finished) and all(r['completed'] for r in finished)
state=max((json.loads(f.read_text()) for f in (root/'state_D').glob('*.json')),key=lambda s:s['monotonic_s'])
assert not state['requests'] and state['migration_tasks']==0 and not state['migrating_request_ids']
api=psutil.Process(4061828);command=' '.join(api.cmdline())
assert '--port 8101' in command and 'power_acceptance_B_D' in command
children=api.children(recursive=True)
before=(root/'vllm_1.log').stat().st_size
evidence={'planned':len(planned),'completed':len(finished),'state':state,'api_pid':api.pid,'api_created':api.create_time(),
    'method':'SIGTERM only the API process; allow its own shutdown to order descendants. No group signal, SIGKILL or request abort.',
    'start_monotonic_s':time.monotonic(),'children_before':[{'pid':p.pid,'created':p.create_time()} for p in children]}
(root/'shutdown_probe_start.json').write_text(json.dumps(evidence,indent=2))
api.send_signal(signal.SIGTERM)
deadline=time.monotonic()+60
def alive():
    found=[]
    for p in [api]+children:
        try:
            if p.is_running() and p.status()!=psutil.STATUS_ZOMBIE:found.append(p.pid)
        except psutil.Error:pass
    return found
while alive() and time.monotonic()<deadline:time.sleep(.2)
with (root/'vllm_1.log').open() as f:f.seek(before);tail=f.read()
evidence.update({'end_monotonic_s':time.monotonic(),'remaining_pids':alive(),'new_log':tail,
    'clean':not alive() and not any(m in tail for m in ('Segfault encountered','AssertFailed','Fatal Python error','EngineCore encountered a fatal error'))})
(root/'shutdown_probe_result.json').write_text(json.dumps(evidence,indent=2))
print(json.dumps({k:evidence[k] for k in ('clean','remaining_pids','start_monotonic_s','end_monotonic_s')}),flush=True)
