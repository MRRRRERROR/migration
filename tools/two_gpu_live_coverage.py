"""Fixed coverage with an actual 65-second NVML pre-baseline, services alive."""
import argparse
import asyncio
import csv
import hashlib
import json
from pathlib import Path
import signal
import sys
import time
import traceback
from two_gpu_live_load import Experiment, P, dump, readrows, poisson_trace
from two_gpu_live_nvml import NVML

class CoverageExperiment(Experiment):
    def __init__(self,args):
        self.args=args;self.root=Path(args.root);self.gpus=[int(x) for x in args.gpus.split(',')]
        self.nv=NVML();self.prompts=readrows(self.root/'inputs/prompts.jsonl');self.processes=[];self.identities={}
        for row in json.loads((self.root/'version_manifest.json').read_text()):
            p=Path(row['path'])
            # Report scripts may be refined; serving and control files stay frozen.
            if p.name in ('two_gpu_live_report.py','two_gpu_live_guard.py'):continue
            expected=row.get('actual_sha256') or row['after_sha256']
            if hashlib.sha256(p.read_bytes()).hexdigest()!=expected:raise RuntimeError('Frozen runtime/control file changed: '+str(p))
        path=Path(__file__)
        dump(self.root/'coverage_adapter_version.json',dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
             reason='Collect a full 60s pre-t0 power window; idle source baseline is reported separately from main steady-state baseline'))

    async def deploy(self,folder,mode):
        await super().deploy(folder,mode)
        pre=self.start('nvml_pre',f'{sys.executable} -u tools/two_gpu_live_nvml.py --gpus {self.args.gpus} --output {folder}/power_pre.csv',folder)
        begin=time.monotonic();await asyncio.sleep(65)
        pre.send_signal(signal.SIGTERM);await asyncio.to_thread(pre.wait,10)
        dump(folder/'coverage_power_baseline.json',dict(start_monotonic_s=begin,end_monotonic_s=time.monotonic(),source_state='idle warmed service before 16 simultaneous requests',
            background_start='with coverage replay, after this baseline',models_and_engines_alive=True))

    async def run(self,*args,**kwargs):
        result=await super().run(*args,**kwargs)
        folder=self.root/args[0];rows=[]
        for path in (folder/'power_pre.csv',folder/'power.csv'):
            with path.open() as f:rows.extend(csv.DictReader(f))
        original=folder/'power.csv';original.rename(folder/'power_replay_only.csv')
        with original.open('x',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(sorted(rows,key=lambda x:float(x['monotonic_s'])))
        return result

async def run(args):
    e=CoverageExperiment(args);root=e.root;frozen=json.loads((root/'frozen_load.json').read_text());dr=frozen['background_rate']
    indices=json.loads((root/'coverage_prompt_indices.json').read_text())
    if len(indices)!=16:
        dump(root/'coverage_insufficient.json',dict(reason='Fewer than 16 eligible long prompts',available=len(indices)));return
    fixed=[dict(request_id=f'coverage-S-{i:02d}',initial_role='S',planned_offset_s=0.,prompt_index=p,seed=101) for i,p in enumerate(indices)]
    bg=[r for r in poisson_trace(e.prompts,101,180,.000000001,dr) if r['initial_role']=='D']
    trace=sorted(fixed+bg,key=lambda x:x['planned_offset_s']);dump(root/'frozen_coverage_trace.json',trace)
    order=json.loads((root/'scheme_order.json').read_text())
    for mode in ['B']+[m for m in order if m!='B']:await e.run('coverage_'+mode,mode,trace,180,0,True)
    dump(root/'completed.json',dict(completed=True,formal_seeds_run=False,normal_unload_qualified=False,wall_ns=time.time_ns(),
        coverage_prebaseline_s=65,measurement_scope='service alive, no model unloading'))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);a=p.parse_args()
    try:asyncio.run(run(a))
    except BaseException as exc:dump(Path(a.root)/'coverage_blocked.json',dict(error=repr(exc),traceback=traceback.format_exc()));raise
