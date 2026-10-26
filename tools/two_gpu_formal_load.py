"""Execute the frozen three-scenario, twelve-round experiment; no design tuning."""
import argparse
import asyncio
import collections
import csv
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import signal
import statistics
import sys
import time
import traceback
import aiohttp
import psutil
from two_gpu_live_load import Experiment, P, MODES, CRITICAL, dump, append, snapshot, stats, capacity
from two_gpu_live_report import first_difference

ORDERS={'A':['B','E','D','C'],'L':['C','D','E','B'],'H':['D','C','B','E']}
GENERATE=dict(max_tokens=4096,temperature=0,n=1,ignore_eos=False)

def frozen_trace(prompts,scenario,duration,sr,dr,phase='formal'):
    result=[];seeds={};full=list(range(len(prompts)));longs=[i for i,p in enumerate(prompts) if 2048<=p['input_tokens']<=4096]
    for role,rate in [('S',sr),('D',dr)]:
        # Scenario never changes a role's random stream; prompt and arrival draws are independent.
        arrival_seed=10100+(0 if role=='S' else 1)+(100000 if phase=='calibration' else 0)
        prompt_seed=10200+(0 if role=='S' else 1)+(100000 if phase=='calibration' else 0)
        arrival=random.Random(arrival_seed);mark=random.Random(prompt_seed);seeds[role]=dict(arrival=arrival_seed,prompt=prompt_seed)
        pool=longs if scenario=='L' and role=='S' else full
        t=arrival.expovariate(rate);i=0
        while t<duration:
            index=pool[mark.randrange(len(pool))];p=prompts[index]
            result.append(dict(request_id=f'p101-{role}-{i:05d}',planned_offset_s=t,initial_role=role,prompt_index=index,
                seed=101,input_sha256=p['input_sha256'],token_ids=p['token_ids'],generation=GENERATE))
            t+=arrival.expovariate(rate);i+=1
    return sorted(result,key=lambda r:r['planned_offset_s']),seeds

class FormalExperiment(Experiment):
    def __init__(self,args):
        super().__init__(args);self.base=self.root;self.scenario=None
        dump(self.base/'frozen_plan.json',dict(planned_rounds=12,scenarios=ORDERS,master_seed=101,duration=600,t0=300,
            L_candidates=[1.,.75,.5],L_background=.3,H_source=1.,H_candidates=[.9,.6,.45],
            calibration_duration=300,calibration_windows=dict(early=[60,180],late=[180,300]),
            post_measurement_alive_s=30,basis='user pasted frozen design; no design extension',formal_repeats_per_cell=1))
        self.completed=[]
        dump(self.base/'progress.json',dict(status='starting',completed_formal_rounds=[]))

    def coverage_state(self,folder,timeline):
        if self.scenario is None:return
        s=snapshot(folder/'state_S');d=snapshot(folder/'state_D')
        decoding=sum(r['generated_tokens']>0 for r in s['requests']);background_running=sum(r['state']=='running' for r in d['requests'])
        ok=(decoding>=8 and background_running>=4) if self.scenario=='L' else background_running>=4 if self.scenario=='H' else True
        timeline['coverage']=dict(scenario=self.scenario,source_decoding=decoding,target_running=background_running,passed=ok)

    def check_output(self,folder):
        result_path=folder/'request_results.jsonl'
        if not result_path.exists():
            summary=json.loads((folder/'summary.json').read_text())
            if summary['sent']!=0:raise RuntimeError('Missing client results after requests were sent')
            dump(folder/'semantic_guard.json',dict(checked_completed=0,errors=[],coverage='no requests sent; not a correctness pass'))
            return
        rows=[json.loads(l) for l in result_path.open() if l.strip()]
        seq=collections.defaultdict(list)
        outputs=sorted([json.loads(l) for p in folder.glob('outputs_*.jsonl') for l in p.open() if l.strip()],key=lambda r:r['time_ns'])
        for x in outputs:seq[x['request_id']].extend(x['tokens'])
        errors=[]
        for r in rows:
            if not r.get('completed'):continue
            actual=seq[(r.get('response_id') or '')+'-0'];index=first_difference(actual,r['token_ids'])
            if index is not None:errors.append(dict(request_id=r['request_id'],reason='Core/API mismatch',first_token=index))
        result=dict(checked_completed=sum(r.get('completed',False) for r in rows),errors=errors)
        dump(folder/'semantic_guard.json',result)
        if errors:raise RuntimeError('Output correctness hard block: '+str(folder))

    def compare_to_B(self):
        b=self.root/'main_B/request_results.jsonl'
        if not b.exists():return
        reference={r['request_id']:r for r in [json.loads(l) for l in b.open() if l.strip()]}
        evidence=[];errors=[]
        for m in 'CDE':
            p=self.root/('main_'+m)/'request_results.jsonl'
            if not p.exists():continue
            for r in [json.loads(l) for l in p.open() if l.strip()]:
                before=reference.get(r['request_id'])
                if not before or not before.get('completed') or not r.get('completed'):continue
                index=first_difference(before['token_ids'],r['token_ids'])
                row=dict(mode=m,request_id=r['request_id'],first_token=index,text_equal=before['text']==r['text'],finish_equal=before['finish_reason']==r['finish_reason'],input_equal=before['input_sha256']==r['input_sha256'],
                    baseline_token=before['token_ids'][index] if index is not None and index<len(before['token_ids']) else None,
                    actual_token=r['token_ids'][index] if index is not None and index<len(r['token_ids']) else None)
                evidence.append(row)
                if index is not None or not all(row[k] for k in ('text_equal','finish_equal','input_equal')):errors.append(row)
        dump(self.root/'paired_output_comparisons.json',dict(comparisons=evidence,errors=errors))
        if errors:raise RuntimeError('Natural drain reference mismatch; later rounds stopped')

    async def cleanup(self,folder,active=False):
        try:await super().cleanup(folder,active)
        except RuntimeError as exc:
            if active or not str(exc).startswith('Authorized GPU busy/query unavailable:'):raise
            records=json.loads((folder/'shutdown.json').read_text())
            if any(r.get('remaining_children') or r.get('returncode')!=0 for r in records):raise
            readings=[dict(error=str(exc),monotonic_s=time.monotonic())]
            for _ in range(3):
                await asyncio.sleep(10)
                try:
                    rows=self.gpu_check();readings.append(dict(rows=rows,monotonic_s=time.monotonic()))
                    dump(folder/'post_shutdown_gpu_rechecks.json',readings);dump(folder/'post_shutdown_gpu.json',rows);return
                except RuntimeError as retry:
                    readings.append(dict(error=str(retry),monotonic_s=time.monotonic()));dump(folder/'post_shutdown_gpu_rechecks.json',readings)
            raise

    async def run(self,name,mode,trace,duration=600,t0=300,coverage=False):
        folder=self.root/name;folder.mkdir(exist_ok=False)
        timeline=dict(mode=mode,migration_mode=MODES.get(mode,'kv'),duration_s=duration,t0_offset_s=t0,coverage=coverage)
        dump(folder/'round_config.json',timeline)
        await self.deploy(folder,timeline['migration_mode'])
        records=[dict(r) for r in trace];dump(folder/'replay.json',records)
        monitor=self.start('nvml',f'{shlex.quote(sys.executable)} -u tools/two_gpu_live_nvml.py --gpus {self.args.gpus} --output {folder}/power.csv',folder)
        start=time.monotonic();timeline['start_monotonic_s']=start;timeline['start_wall_ns']=time.time_ns()
        dump(folder/'timeline.json',timeline);redirect=asyncio.Event();done=asyncio.Event();critical=[];positions={};tasks=[];samples=[]
        async def observe():
            while not done.is_set():
                now=time.monotonic();item=dict(monotonic_s=now,elapsed_s=now-start)
                owned=self.owned()
                for i,role in zip(self.gpus,('S','D')):
                    state=snapshot(folder/('state_'+role));item[role]=state
                    gpu=self.nv.row(i,role)
                    if any(x['pid'] not in owned for x in json.loads(gpu['compute_pids'])):critical.append(dict(kind='foreign_gpu_process',gpu=gpu))
                for path in folder.glob('vllm_*.log'):
                    with path.open(errors='replace') as f:f.seek(positions.get(path,0));new=f.read();positions[path]=f.tell()
                    lines='\n'.join(l for l in new.splitlines() if ' prompt: ' not in l and '[entrypoints/logger.py:' not in l)
                    if CRITICAL.search(lines):critical.append(dict(kind='runtime_critical',path=str(path),evidence=lines[-12000:]))
                if monitor.poll() is not None:critical.append(dict(kind='nvml_exited'))
                if any(p.poll() is not None for l,p,_ in self.processes if l.startswith('vllm')):critical.append(dict(kind='service_exited_during_measurement'))
                samples.append(item);append(folder/'instance_samples.jsonl',item)
                await asyncio.sleep(1)
        observer=asyncio.create_task(observe())
        if mode=='calibration':redirect.set()
        async def trigger():
            if mode=='calibration':return
            if coverage:
                deadline=start+60
                while time.monotonic()<deadline:
                    state=snapshot(folder/'state_S')
                    decoding=[r for r in (state or {}).get('requests',[]) if r['generated_tokens']>0]
                    if len(decoding)>=8:break
                    if critical:return
                    await asyncio.sleep(.05)
                else:
                    timeline['coverage_insufficient']='Fewer than 8 simultaneous decoding requests within 60s'
                    dump(folder/'timeline.json',timeline);return
            else:await asyncio.sleep(max(0,start+t0-time.monotonic()))
            timeline['control_action_start_monotonic_s']=time.monotonic()
            redirect.set();timeline['redirect_effective_monotonic_s']=time.monotonic()
            timeline['t0_monotonic_s']=timeline['control_action_start_monotonic_s']
            dump(folder/'t0_snapshot.json',dict(S=snapshot(folder/'state_S'),D=snapshot(folder/'state_D'),
                source_client_inflight=[r['request_id'] for r in records if r.get('route_role')=='S' and not r.get('finish_monotonic_s')]))
            self.coverage_state(folder,timeline)
            dump(folder/'timeline.json',timeline);print('T0',self.root.name,name,flush=True)
            if mode in 'CDE':await self.migrate(folder,timeline)
        trigger_task=asyncio.create_task(trigger())
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            for row in records:
                await asyncio.sleep(max(0,start+row['planned_offset_s']-time.monotonic()))
                if critical:break
                tasks.append(asyncio.create_task(self.send(session,row,folder,start,redirect)))
            await asyncio.sleep(max(0,start+duration-time.monotonic()) if not critical else 0)
            timeline['send_cutoff_monotonic_s']=time.monotonic();dump(folder/'timeline.json',timeline)
            pending={t for t in tasks if not t.done()};deadline=time.monotonic()+600
            while pending and time.monotonic()<deadline and not critical:
                _,pending=await asyncio.wait(pending,timeout=1)
            timeline['work_completion_monotonic_s']=max((r.get('finish_monotonic_s') or 0 for r in records),default=0) if all(r.get('completed') for r in records) else None
            timeline['measurement_cutoff_monotonic_s']=time.monotonic();timeline['critical']=critical
            timeline['unfinished_at_cutoff']=[r['request_id'] for r in records if r.get('actual_send_monotonic_s') and not r.get('finish_monotonic_s')]
            dump(folder/'measurement_cutoff.json',timeline);dump(folder/'unfinished_requests.json',[r for r in records if r['request_id'] in timeline['unfinished_at_cutoff']])
            # After measurement is closed, cancellation is explicit and reported.
            for task in pending:task.cancel()
            if pending:await asyncio.gather(*pending,return_exceptions=True)
        if not trigger_task.done():trigger_task.cancel()
        await asyncio.gather(trigger_task,return_exceptions=True)
        if mode!='calibration' and not critical and not pending:
            await asyncio.sleep(30)
            timeline['post_measurement_observation_end_monotonic_s']=time.monotonic()
            dump(folder/'service_alive_observation.json',dict(end_monotonic_s=time.monotonic(),seconds=30,critical=critical))
            dump(folder/'timeline.json',timeline)
        done.set();await observer
        nvlog=next(x for x in self.processes if x[0]=='nvml');nvlog[1].send_signal(signal.SIGTERM)
        await asyncio.to_thread(nvlog[1].wait,10)
        cleanup=[];deadline=time.monotonic()+30
        while True:
            states=[snapshot(folder/('state_'+r)) for r in ('S','D')];cleanup.append(dict(monotonic_s=time.monotonic(),states=states))
            safe=all(s and not s['requests'] and not (s['sessions'] or {}) and not (s['sources'] or {}) and not s['migration_tasks'] for s in states)
            kv_restored=all(s and s['free_kv_blocks']>=b['free_kv_blocks'] for s,b in zip(states,self.baseline))
            if safe and kv_restored:break
            if time.monotonic()>deadline:break
            await asyncio.sleep(.2)
        dump(folder/'cleanup_observations.json',cleanup)
        summary=dict(name=name,mode=mode,records=len(records),sent=len(tasks),completed=sum(r.get('completed',False) for r in records),
            errors=[dict(request_id=r['request_id'],error=r.get('error'),status=r.get('http_status')) for r in records if r.get('error')],
            critical=critical,cleanup_safe=safe,kv_restored=kv_restored,timing=timeline,send_lag=stats([r.get('send_lag_s') for r in records]),
            ttft=stats([r.get('ttft_s') for r in records]),e2e=stats([r.get('e2e_s') for r in records]))
        dump(folder/'summary.json',summary)
        self.check_output(folder)
        await self.cleanup(folder,active=not safe)
        if critical or not safe or not kv_restored:raise RuntimeError('Measurement/state invalid; subsequent rounds paused: '+name)
        print('ROUND_DONE',self.root.name,name,summary['completed'],flush=True)
        return summary,records,samples

async def execute(args):
    e=FormalExperiment(args);root=e.base
    old=P/'results/two_gpu_service_alive_20261009_03'
    source=json.loads((old/'coverage_E/analysis.json').read_text())
    unclassified=[a for a in source['attempts'] if a['state']=='unclassified']
    dump(root/'prior_unclassified_audit.json',dict(requests=[dict(request_id=a['request_id'],attempt_id=a['attempt_id'],events=a['events'],classification='unconfirmed; hello-only evidence does not identify terminal migration outcome') for a in unclassified],scope='existing evidence only, no old rerun'))
    for scenario in ('A','L','H'):
        path=root/scenario;path.mkdir();e.root=path;e.scenario=scenario
        for name in ('workload_manifest.json','dependencies.json','model_configuration_hashes.json','effective_common_config.json','version_manifest.json'):
            shutil.copy2(root/name,path/name)
        dump(path/'scheme_order.json',ORDERS[scenario]);cal=[]
        if scenario=='A':
            frozen=json.loads((old/'frozen_load.json').read_text());sr,dr=1.,.3
            cal=[dict(candidate=1,source_rate=sr,background_rate=dr,capacity=frozen['basis']['capacity'],reused_from=str(old/'capacity_calibration.json'),identity_evidence=str(root/'minimal_preflight.json'))]
        else:
            for i,rate in enumerate(([1.,.75,.5] if scenario=='L' else [.9,.6,.45]),1):
                sr,dr=(rate,.3) if scenario=='L' else (1.,rate)
                trace,seeds=frozen_trace(e.prompts,scenario,300,sr,dr,'calibration')
                dump(path/f'calibration_trace_{i}.json',dict(requests=trace,seeds=seeds))
                e.scenario=None
                dump(root/'progress.json',dict(status='calibrating',scenario=scenario,candidate=i,completed_formal_rounds=e.completed))
                summary,records,samples=await e.run(f'calibration_{i}','calibration',trace,300,300)
                e.scenario=scenario;measure=capacity(summary,records,samples,sr+dr)
                cal.append(dict(candidate=i,source_rate=sr,background_rate=dr,capacity=measure));dump(path/'capacity_calibration.json',cal)
                print('CAPACITY',scenario,i,json.dumps(measure),flush=True)
                if measure['stable']:break
            if not cal[-1]['capacity']['stable']:
                dump(path/'capacity_blocked.json',dict(status='no capacity configuration within three frozen candidates',candidates=cal));continue
        dump(path/'capacity_calibration.json',cal)
        trace,seeds=frozen_trace(e.prompts,scenario,600,sr,dr)
        dump(path/'frozen_main_trace.json',trace);dump(path/'derived_seeds.json',seeds)
        dump(path/'frozen_load.json',dict(source_rate=sr,background_rate=dr,seed=101,duration_s=600,t0_s=300,basis=cal[-1],scenario=scenario))
        for mode in ORDERS[scenario]:
            dump(root/'progress.json',dict(status='running',scenario=scenario,mode=mode,completed_formal_rounds=e.completed))
            await e.run('main_'+mode,mode,trace)
            e.compare_to_B();e.completed.append(scenario+'_'+mode)
            dump(root/'progress.json',dict(status='between_rounds',completed_formal_rounds=e.completed))
    dump(root/'completed.json',dict(completed_formal_rounds=e.completed,planned=12,formal_five_seed_experiment=False,normal_unload_qualified=False,wall_ns=time.time_ns()))
    dump(root/'progress.json',dict(status='finished',completed_formal_rounds=e.completed))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);p.add_argument('--authorization',required=True);a=p.parse_args()
    try:asyncio.run(execute(a))
    except BaseException as exc:
        dump(Path(a.root)/'blocked.json',dict(error=repr(exc),traceback=traceback.format_exc(),wall_ns=time.time_ns()));raise
