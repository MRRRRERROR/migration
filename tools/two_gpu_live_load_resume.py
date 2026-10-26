"""Resume the finite acceptance after the recorded post-exit idle-check block."""
import argparse
import asyncio
import json
from pathlib import Path
import time
import traceback
from two_gpu_live_load import Experiment, dump, poisson_trace
from two_gpu_live_coverage import CoverageExperiment

class ResumedExperiment(CoverageExperiment):
    async def deploy(self, folder, mode):
        if folder.name.startswith('main_'):
            return await Experiment.deploy(self, folder, mode)
        return await super().deploy(folder, mode)

    async def cleanup(self, folder, active=False):
        try:
            await super().cleanup(folder, active)
        except RuntimeError as exc:
            # Only retry an idle observation after proven complete process cleanup.
            if active or not str(exc).startswith('Authorized GPU busy/query unavailable:'):
                raise
            records=json.loads((folder/'shutdown.json').read_text())
            if any(r.get('remaining_children') or r.get('returncode') != 0 for r in records):
                raise
            observations=[dict(error=str(exc),monotonic_s=time.monotonic())]
            for _ in range(3):
                await asyncio.sleep(10)
                try:
                    rows=self.gpu_check()
                    observations.append(dict(rows=rows,monotonic_s=time.monotonic()))
                    dump(folder/'post_shutdown_gpu_rechecks.json', observations)
                    dump(folder/'post_shutdown_gpu.json',rows)
                    return
                except RuntimeError as retry:
                    observations.append(dict(error=str(retry),monotonic_s=time.monotonic()))
                    dump(folder/'post_shutdown_gpu_rechecks.json',observations)
            raise

    async def run(self, *args, **kwargs):
        if args[0].startswith('main_'):
            return await Experiment.run(self,*args,**kwargs)
        return await super().run(*args,**kwargs)

async def main(args):
    e=ResumedExperiment(args);root=e.root
    if (root/'semantic_blocked.json').exists():raise RuntimeError('Semantic block requires investigation')
    for name in ('main_B','main_E','main_D'):
        summary=json.loads((root/name/'summary.json').read_text())
        guard=json.loads((root/name/'semantic_guard.json').read_text())
        if summary['critical'] or not summary['cleanup_safe'] or not summary['kv_restored'] or guard['errors']:
            raise RuntimeError('Prior round is not valid: '+name)
    before=e.gpu_check();await asyncio.sleep(10);after=e.gpu_check()
    interruption=root/'interruptions';interruption.mkdir(exist_ok=True)
    block=root/'blocked.json'
    if block.exists():
        data=json.loads(block.read_text())
        if 'Authorized GPU busy/query unavailable' not in data['error'] or 'await self.cleanup' not in data['traceback']:
            raise RuntimeError('Unexpected previous block')
        block.rename(interruption/'post_main_D_gpu_observation_block.json')
    dump(interruption/'resume_evidence.json',dict(before=before,after=after,wall_ns=time.time_ns(),
        reason='No experiment processes remain; all prior requests and shutdown succeeded; selected GPUs now idle'))
    trace=json.loads((root/'frozen_main_trace.json').read_text())
    await e.run('main_C','C',trace)
    indices=json.loads((root/'coverage_prompt_indices.json').read_text())
    if len(indices)!=16:raise RuntimeError('Need 16 fixed long-history prompts')
    frozen=json.loads((root/'frozen_load.json').read_text())
    fixed=[dict(request_id=f'coverage-S-{i:02d}',initial_role='S',planned_offset_s=0.,prompt_index=p,seed=101) for i,p in enumerate(indices)]
    bg=[r for r in poisson_trace(e.prompts,101,180,.000000001,frozen['background_rate']) if r['initial_role']=='D']
    trace=sorted(fixed+bg,key=lambda x:x['planned_offset_s']);dump(root/'frozen_coverage_trace.json',trace)
    for mode in ('B','E','D','C'):await e.run('coverage_'+mode,mode,trace,180,0,True)
    dump(root/'completed.json',dict(completed=True,formal_seeds_run=False,normal_unload_qualified=False,wall_ns=time.time_ns(),coverage_prebaseline_s=65))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);args=p.parse_args()
    try:asyncio.run(main(args))
    except BaseException as exc:
        dump(Path(args.root)/'blocked.json',dict(error=repr(exc),traceback=traceback.format_exc(),wall_ns=time.time_ns()));raise
