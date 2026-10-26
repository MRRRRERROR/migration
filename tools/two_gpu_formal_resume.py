"""Resume uncompleted frozen cells after the user's GPU transition authorization."""
import argparse
import asyncio
import json
import shutil
import time
import traceback
from pathlib import Path
from two_gpu_formal_load import FormalExperiment, ORDERS, frozen_trace
from two_gpu_live_load import P, dump, capacity, readrows
from two_gpu_live_nvml import NVML

class ContinuedExperiment(FormalExperiment):
    def __init__(self,args):
        self.args=args;self.root=Path(args.root);self.base=self.root
        self.gpus=[int(x) for x in args.gpus.split(',')];self.nv=NVML()
        self.prompts=readrows(self.root/'inputs/prompts.jsonl');self.processes=[];self.identities={};self.scenario=None
        self.completed=list(json.loads((self.root/'progress.json').read_text())['completed_formal_rounds'])

async def execute(args):
    e=ContinuedExperiment(args);root=e.base
    first=e.gpu_check();await asyncio.sleep(10);second=e.gpu_check()
    archive=root/'interruptions/A_C_gpu_interference';archive.mkdir(parents=True,exist_ok=False)
    partial=root/'A/main_C'
    if partial.exists():
        assert partial.resolve().is_relative_to(root.resolve())
        cleanup=json.loads((partial/'interrupted_shutdown.json').read_text())
        if any(not r.get('exited') or r.get('remaining_children') for r in cleanup):raise RuntimeError('Prior experiment cleanup not complete')
        partial.rename(archive/'main_C')
    for name in ('blocked.json','report_ready.json','report.html','finish_audit.json','formal_analysis_index.json'):
        p=root/name
        if p.exists():p.rename(archive/name)
    dump(root/'hardware_transition_01.json',dict(from_gpus=[1,3],to_gpus=e.gpus,
        authorization='User explicitly said: 1,3 occupied, use the other two GPUs',prestart=[first,second],wall_ns=time.time_ns(),
        completed_on_previous_gpus=e.completed,interrupted_cell='A_C',reason='Foreign computation entered fixed GPUs; interrupted attempt retained and same frozen cell resumed'))
    for scenario in ('A','L','H'):
        path=root/scenario;path.mkdir(exist_ok=True);e.root=path;e.scenario=scenario
        for name in ('workload_manifest.json','dependencies.json','model_configuration_hashes.json','effective_common_config.json','version_manifest.json'):
            if not (path/name).exists():shutil.copy2(root/name,path/name)
        dump(path/'scheme_order.json',ORDERS[scenario]);frozen_file=path/'frozen_load.json'
        if frozen_file.exists():
            frozen=json.loads(frozen_file.read_text());sr,dr=frozen['source_rate'],frozen['background_rate']
            trace=json.loads((path/'frozen_main_trace.json').read_text())
        else:
            cal=[]
            for i,rate in enumerate(([1.,.75,.5] if scenario=='L' else [.9,.6,.45]),1):
                sr,dr=(rate,.3) if scenario=='L' else (1.,rate)
                trace,seeds=frozen_trace(e.prompts,scenario,300,sr,dr,'calibration')
                dump(path/f'calibration_trace_{i}.json',dict(requests=trace,seeds=seeds));e.scenario=None
                dump(root/'progress.json',dict(status='calibrating',scenario=scenario,candidate=i,completed_formal_rounds=e.completed,gpus=e.gpus))
                summary,records,samples=await e.run(f'calibration_{i}','calibration',trace,300,300)
                e.scenario=scenario;measure=capacity(summary,records,samples,sr+dr)
                cal.append(dict(candidate=i,source_rate=sr,background_rate=dr,capacity=measure));dump(path/'capacity_calibration.json',cal)
                print('CAPACITY',scenario,i,json.dumps(measure),flush=True)
                if measure['stable']:break
            if not cal[-1]['capacity']['stable']:
                dump(path/'capacity_blocked.json',dict(status='no capacity configuration within three frozen candidates',candidates=cal));continue
            trace,seeds=frozen_trace(e.prompts,scenario,600,sr,dr)
            dump(path/'frozen_main_trace.json',trace);dump(path/'derived_seeds.json',seeds)
            dump(path/'frozen_load.json',dict(source_rate=sr,background_rate=dr,seed=101,duration_s=600,t0_s=300,basis=cal[-1],scenario=scenario))
        for mode in ORDERS[scenario]:
            cell=scenario+'_'+mode
            if cell in e.completed:continue
            dump(root/'progress.json',dict(status='running',scenario=scenario,mode=mode,completed_formal_rounds=e.completed,gpus=e.gpus))
            await e.run('main_'+mode,mode,trace);e.compare_to_B();e.completed.append(cell)
            dump(root/'progress.json',dict(status='between_rounds',completed_formal_rounds=e.completed,gpus=e.gpus))
    dump(root/'completed.json',dict(completed_formal_rounds=e.completed,planned=12,formal_five_seed_experiment=False,normal_unload_qualified=False,wall_ns=time.time_ns()))
    dump(root/'progress.json',dict(status='finished',completed_formal_rounds=e.completed,gpus=e.gpus))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);a=p.parse_args()
    try:asyncio.run(execute(a))
    except BaseException as exc:
        dump(Path(a.root)/'blocked.json',dict(error=repr(exc),traceback=traceback.format_exc(),wall_ns=time.time_ns()));raise
