"""One finite, service-alive B/C/D/E acceptance; no formal seed expansion."""
import argparse
import asyncio
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import time
import traceback

import aiohttp
import grpc
import psutil
from tests.local import utils
from llumnix.server.proto import llumlet_server_pb2 as pb, llumlet_server_pb2_grpc as rpc
from two_gpu_live_nvml import NVML
from two_gpu_power_metrics import poisson_trace

P=Path('/data/home/zylu/llumnix-v018-main');MODEL='/data/models/Qwen2.5-7B-Instruct'
APIS=(18100,18101);KVTS=(41100,41101);GRPCS=(59000,59001);REDIS=16379
PORTS=(REDIS,*APIS,*KVTS,41200,41201,*GRPCS,46218,46226)
MODES={'B':'kv','C':'kv','D':'token_stop','E':'token_live'}
CRITICAL=re.compile(r'KVTResp\(code=500|EngineCore encountered a fatal error|EngineCore failed to start|CUDA illegal|illegal memory access|Segfault encountered|AssertFailed|Fatal Python error',re.I)

def dump(path,obj):path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str)+'\n')
def append(path,obj):
    with path.open('a') as f:f.write(json.dumps(obj,ensure_ascii=False,default=str)+'\n')
def readrows(path):return [json.loads(l) for l in path.read_text().split('\n') if l.strip()]
def snapshot(folder):
    values=[json.loads(f.read_text()) for f in folder.glob('*.json')]
    return max(values,key=lambda x:x['monotonic_s']) if values else None
def stats(values):
    x=sorted(v for v in values if v is not None)
    return dict(n=len(x),mean=statistics.mean(x) if x else None,p95=x[int(.95*(len(x)-1))] if x else None,max=max(x) if x else None)

class Experiment:
    def __init__(self,args):
        self.args=args;self.root=Path(args.root);self.nv=NVML();self.gpus=[int(x) for x in args.gpus.split(',')]
        if len(set(self.gpus))!=2:raise ValueError('Exactly two distinct authorized GPUs required')
        self.prompts=readrows(self.root/'inputs/prompts.jsonl');self.processes=[];self.identities={}
        release=P/'results/token_migration_20261008_01/delivery_20261009_route_01'
        manifest=[]
        for row in json.loads((release/'manifest.json').read_text()):
            f=Path(row['path']);actual=hashlib.sha256(f.read_bytes()).hexdigest()
            if actual!=row['after_sha256']:raise RuntimeError('Delivery changed: '+str(f))
            manifest.append({**row,'actual_sha256':actual})
        for f in (P/'tools').glob('two_gpu_live*.py'):
            manifest.append(dict(path=str(f),actual_sha256=hashlib.sha256(f.read_bytes()).hexdigest()))
        f=P/'tools/live_load_observer/sitecustomize.py'
        manifest.append(dict(path=str(f),actual_sha256=hashlib.sha256(f.read_bytes()).hexdigest()))
        dump(self.root/'version_manifest.json',manifest)
        dump(self.root/'independent_acceptance_state.json',dict(basis=str(release),scope='service-alive local two-GPU drain and power response',
             normal_unload_qualified=False,old_revocation_preserved=True,status='running',formal_seeds_run=False))
        dump(self.root/'effective_common_config.json',dict(gpus=self.gpus,gpu_authorization=args.authorization,
            model=MODEL,dtype='bfloat16',context_length=9216,max_tokens=4096,temperature=0,n=1,
            batch_invariant=True,async_scheduling=False,ignore_eos=False,max_rounds=3,final_gap_tokens=16,timeout=30,
            kv_cache_memory_bytes=4294967296,automatic_scaling=False,automatic_migration=False,
            ports=PORTS,measurement_source_exit=False,fault_injection=False))

    def gpu_check(self):
        rows=[self.nv.row(i,r) for i,r in zip(self.gpus,('S','D'))]
        for row in rows:
            if json.loads(row['compute_pids']) or row['memory_mib'] is None or row['memory_mib']>100 or row['utilization_pct']!=0:
                raise RuntimeError('Authorized GPU busy/query unavailable: '+str(row))
        return rows

    def start(self,label,command,folder,env=None):
        log=(folder/(label+'.log')).open('x')
        process=subprocess.Popen(['bash','-c','exec '+command],cwd=P,env={**os.environ,**(env or {})},stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        self.processes.append((label,process,log));self.identities[process.pid]=psutil.Process(process.pid).create_time()
        dump(folder/'owned_processes.json',[dict(label=l,pid=p.pid,created=self.identities[p.pid]) for l,p,_ in self.processes])
        return process

    def owned(self):
        ids=set(self.identities)
        for _,process,_ in self.processes:
            try:
                if psutil.Process(process.pid).create_time()!=self.identities[process.pid]:raise RuntimeError('PID reuse')
                for child in psutil.Process(process.pid).children(recursive=True):
                    ids.add(child.pid);self.identities.setdefault(child.pid,child.create_time())
            except psutil.NoSuchProcess:pass
        return ids

    async def cleanup(self,folder,active=False):
        if active:
            dump(folder/'shutdown_blocked.json',dict(reason='Active requests retained; no process termination',identities=self.identities))
            raise RuntimeError('Cannot clean up with active requests; preserve services')
        records=[]
        for label,process,log in reversed(self.processes):
            children=[]
            if process.poll() is None:
                parent=psutil.Process(process.pid)
                if parent.create_time()!=self.identities[process.pid]:raise RuntimeError('PID identity changed')
                children=[(x.pid,x.create_time()) for x in parent.children(recursive=True)]
                process.send_signal(signal.SIGTERM)
                try:await asyncio.to_thread(process.wait,65)
                except subprocess.TimeoutExpired:
                    signals=[]
                    # Only verified owned children, after zero active requests and cutoff.
                    for pid,created in children:
                        try:
                            c=psutil.Process(pid)
                            if c.create_time()==created and c.status()!=psutil.STATUS_ZOMBIE:
                                c.send_signal(signal.SIGTERM);signals.append(dict(pid=pid,signal='SIGTERM'))
                        except psutil.NoSuchProcess:pass
                    records.append(dict(label=label,graceful_timeout=True,controlled_signals=signals))
                    try:await asyncio.to_thread(process.wait,30)
                    except subprocess.TimeoutExpired:
                        dump(folder/'shutdown.json',records);raise RuntimeError('Owned process cannot exit; stop subsequent rounds')
            remaining=[]
            for pid,created in children:
                try:
                    c=psutil.Process(pid)
                    if c.create_time()==created and c.status()!=psutil.STATUS_ZOMBIE:remaining.append(pid)
                except psutil.NoSuchProcess:pass
            records.append(dict(label=label,returncode=process.returncode,remaining_children=remaining))
            log.flush();log.close()
            if remaining:
                dump(folder/'shutdown.json',records);raise RuntimeError('Residual owned children')
        dump(folder/'shutdown.json',records);self.processes=[]
        # A nonzero shutdown is separate from already closed measurement validity.
        if any(x.get('returncode',0) not in (0,None) for x in records):dump(folder/'exit_fault.json',records)
        dump(folder/'post_shutdown_gpu.json',self.gpu_check())

    async def deploy(self,folder,mode):
        self.processes=[];self.identities={}
        before=self.gpu_check();await asyncio.sleep(10);after=self.gpu_check()
        if [x['gpu_uuid'] for x in before]!=[x['gpu_uuid'] for x in after]:raise RuntimeError('GPU UUID changed')
        dump(folder/'gpu_preflight.json',[before,after]);dump(folder/'gpu_mapping.json',after)
        for port in PORTS:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('127.0.0.1',port))
        self.gpu_check()
        self.start('redis',f'redis-server --port {REDIS} --save "" --appendonly no',folder)
        await asyncio.sleep(.5)
        for i,role in enumerate(('S','D')):
            command=utils.get_vllm_command('neutral',APIS[i],i,True,'alive_'+folder.name+'_'+role,'HybridConnector',migration_mode=mode)
            command=re.sub(r'CUDA_VISIBLE_DEVICES=\S+',f'CUDA_VISIBLE_DEVICES={self.gpus[i]}',command)
            command=re.sub(r'LLUMNIX_CMS_REDIS_PORT=\d+',f'LLUMNIX_CMS_REDIS_PORT={REDIS}',command)
            command=re.sub(r'BLLM_KVTRANS_PORT_BASE=\d+',f'BLLM_KVTRANS_PORT_BASE={46218+8*i}',command)
            command=re.sub(r'--max-model-len\s+\S+','--max-model-len 9216',command)
            command=re.sub(r'--max-num-batched-tokens\s+\S+','--max-num-batched-tokens 8192',command)
            if '--no-async-scheduling' not in command:command+=' --no-async-scheduling'
            command+=' --dtype bfloat16 --generation-config auto --kv-cache-memory-bytes 4294967296'
            env=dict(LLUMNIX_RPC_PORT=str(GRPCS[i]),LLUMNIX_TOKEN_TESTING='',
                LIVE_LOAD_STATE_DIR=str(folder/('state_'+role)),LLUMNIX_TOKEN_MIGRATION_LOG=str(folder/'token_events.jsonl'),
                LLUMNIX_TOKEN_VALIDATION_LOG=str(folder/('outputs_'+role+'.jsonl')),
                LLUMNIX_MIGRATION_EFFICIENCY_LOG=str(folder/'kv_events.jsonl'),LLUMNIX_ENGINE_MIGRATE_TIMEOUT='45')
            self.start('vllm_'+role,'env '+command,folder,env)
            append(folder/'launch.jsonl',dict(role=role,physical_gpu=self.gpus[i],command=command,env=env))
        async with aiohttp.ClientSession() as session:
            ready=set();deadline=time.monotonic()+400
            while len(ready)<2:
                if any(p.poll() is not None for _,p,_ in self.processes):raise RuntimeError('Service startup failed')
                for port in APIS:
                    try:
                        async with session.get(f'http://127.0.0.1:{port}/health',timeout=aiohttp.ClientTimeout(total=2)) as response:
                            if response.status==200:ready.add(port)
                    except Exception:pass
                if time.monotonic()>deadline:raise TimeoutError('Startup timeout')
                await asyncio.sleep(1)
            for port in APIS:
                async with session.post(f'http://127.0.0.1:{port}/v1/completions',json=dict(model=MODEL,prompt='Hello. Say hello briefly.',max_tokens=16,temperature=0,
                    kv_transfer_params={'ali_llumnix_disagg':False})) as response:
                    body=await response.text()
                    if response.status!=200:raise RuntimeError('Warmup failed '+body)
            deadline=time.monotonic()+30
            while True:
                states=[snapshot(folder/('state_'+r)) for r in ('S','D')]
                if all(s and not s['requests'] for s in states):break
                if time.monotonic()>deadline:raise RuntimeError('Warmup residual requests')
                await asyncio.sleep(.2)
            dump(folder/'warmup_empty.json',states);self.baseline=states
        native=[]
        for pid in self.owned():
            maps=Path(f'/proc/{pid}/maps')
            if not maps.exists():continue
            for line in maps.read_text().splitlines():
                path=line.split()[-1]
                if any(x in path for x in ('kvtransfer_ops','libaccl_barex','vllm/_C.','libtorch_cuda.so')) and Path(path).is_file():
                    native.append(dict(pid=pid,path=path,sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()))
        dump(folder/'native_loaded.json',native)
        if not any('libaccl_barex' in x['path'] for x in native):raise RuntimeError('Actual native library path not observed')
        print('READY',folder.name,flush=True)

    async def send(self,session,row,folder,start,redirect):
        now=time.monotonic();role='D' if row['initial_role']=='D' or redirect.is_set() else 'S'
        prompt=self.prompts[row['prompt_index']]
        row.update(actual_send_monotonic_s=now,planned_monotonic_s=start+row['planned_offset_s'],
            send_lag_s=now-start-row['planned_offset_s'],route_role=role,input_tokens=prompt['input_tokens'],input_sha256=prompt['input_sha256'],
            chunks=[],token_ids=[],text='',finish_reason=None,error=None,sse_done=False)
        payload=dict(model=MODEL,prompt=prompt['token_ids'],max_tokens=4096,temperature=0,n=1,stream=True,return_token_ids=True,
            stream_options={'include_usage':True,'continuous_usage_stats':True},kv_transfer_params={'ali_llumnix_disagg':False})
        try:
            async with session.post(f'http://127.0.0.1:{APIS[0] if role=="S" else APIS[1]}/v1/completions',json=payload,
                headers={'X-Request-Id':row['request_id']},timeout=aiohttp.ClientTimeout(total=None)) as response:
                row['http_status']=response.status
                if response.status!=200:
                    row['http_error_body']=await response.text();raise RuntimeError('HTTP '+str(response.status))
                async for line in response.content:
                    if not line.startswith(b'data:'):continue
                    body=line[5:].strip()
                    if body==b'[DONE]':row['sse_done']=True;continue
                    item=json.loads(body);row['response_id']=item.get('id',row.get('response_id'))
                    if item.get('usage'):row['usage']=item['usage']
                    for choice in item.get('choices',[]):
                        content=choice.get('text') or '';ids=choice.get('token_ids') or []
                        if content or ids:row['chunks'].append(dict(time=time.monotonic(),token_count=len(ids)))
                        row['text']+=content;row['token_ids'].extend(ids)
                        row['finish_reason']=choice.get('finish_reason') or row['finish_reason']
                if not row['sse_done'] or row['finish_reason'] not in ('stop','length'):raise RuntimeError('Incomplete SSE')
                if len(row['token_ids'])>4096:raise RuntimeError('Output budget exceeded')
        except asyncio.CancelledError:
            row['error']='user_cancel_after_measurement_cutoff';row['cancelled']=True;raise
        except Exception as exc:row['error']=repr(exc)
        finally:
            end=time.monotonic();row['finish_monotonic_s']=end;row['e2e_s']=end-now
            row['ttft_s']=row['chunks'][0]['time']-now if row['chunks'] else None
            row['completed']=row['error'] is None and row['sse_done']
            append(folder/'request_results.jsonl',row)

    async def migrate(self,folder,timeline):
        s=snapshot(folder/'state_S');d=snapshot(folder/'state_D')
        info=dict(start_monotonic_s=time.monotonic(),requested_mode=timeline['migration_mode'],selection='NUM_REQ/LR/all, one controller trigger',
            source_state=s,destination_state=d)
        channel=grpc.aio.insecure_channel(f'127.0.0.1:{GRPCS[0]}')
        try:
            request=pb.MigrateRequest(src_engine_id=s['instance_id'],dst_engine_id=d['instance_id'],dst_engine_ip='127.0.0.1',
                dst_engine_port=KVTS[1],migration_type='NUM_REQ',migration_req_policy='LR',num_reqs=-1,trigger_policy='service_alive_acceptance')
            reply=await rpc.LlumletStub(channel).Migrate(request,timeout=120)
            info.update(success=reply.success,message=reply.message)
        except Exception as exc:info.update(success=False,error=repr(exc))
        finally:
            info['end_monotonic_s']=time.monotonic();dump(folder/'migration_trigger.json',info);await channel.close()

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
            dump(folder/'timeline.json',timeline);print('T0',name,flush=True)
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
            timeline['measurement_cutoff_monotonic_s']=time.monotonic();timeline['critical']=critical
            timeline['unfinished_at_cutoff']=[r['request_id'] for r in records if r.get('actual_send_monotonic_s') and not r.get('finish_monotonic_s')]
            dump(folder/'measurement_cutoff.json',timeline);dump(folder/'unfinished_requests.json',[r for r in records if r['request_id'] in timeline['unfinished_at_cutoff']])
            # After measurement is closed, cancellation is explicit and reported.
            for task in pending:task.cancel()
            if pending:await asyncio.gather(*pending,return_exceptions=True)
        if not trigger_task.done():trigger_task.cancel()
        await asyncio.gather(trigger_task,return_exceptions=True)
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
        await self.cleanup(folder,active=not safe)
        if critical or not safe or not kv_restored:raise RuntimeError('Measurement/state invalid; subsequent rounds paused: '+name)
        print('ROUND_DONE',name,summary['completed'],flush=True)
        return summary,records,samples

def capacity(summary,records,samples,rate):
    q=[(s['elapsed_s'],s['D']['waiting']) for s in samples if 60<=s['elapsed_s']<=300 and s.get('D')]
    early=[w for t,w in q if t<180];late=[w for t,w in q if t>=180]
    if not early or not late:return dict(stable=False,reason='missing observed queues')
    mt=statistics.mean(t for t,w in q);mw=statistics.mean(w for t,w in q)
    slope=sum((t-mt)*(w-mw) for t,w in q)/sum((t-mt)**2 for t,w in q)
    start=summary['timing']['start_monotonic_s']
    first=[r for r in records if 60<=r.get('actual_send_monotonic_s',0)-start<180]
    second=[r for r in records if 180<=r.get('actual_send_monotonic_s',0)-start<300]
    metrics={k:dict(early=stats([r.get(k) for r in first]),late=stats([r.get(k) for r in second])) for k in ('ttft_s','e2e_s')}
    completion_rate=sum(r.get('completed') and 180<=r.get('finish_monotonic_s',0)-start<300 for r in records)/120
    preemptions=max((s['D'].get('preemptions',0) for s in samples if s.get('D')),default=None)
    stable=slope<=.01 and statistics.mean(late)-statistics.mean(early)<=2 and completion_rate>=rate*.85 and not summary['errors'] and preemptions==0
    for v in metrics.values():
        a,b=v['early']['p95'],v['late']['p95']
        stable=stable and a is not None and b is not None and b<=max(a*2,a+1)
    return dict(stable=stable,queue_slope_per_s=slope,early_queue_mean=statistics.mean(early),late_queue_mean=statistics.mean(late),
        late_completion_rate=completion_rate,offered_rate=rate,latency=metrics,preemptions=preemptions,
        rule='queue slope<=.01/s, mean growth<=2, completion>=85%, late p95<=max(2*early,early+1), no errors/preemptions')

async def main_async(args):
    e=Experiment(args);root=e.root;order=list('BCDE');random.Random(101).shuffle(order);dump(root/'scheme_order.json',order)
    calibration=[]
    for index,factor in enumerate((1.,.7,.49),1):
        sr,dr=factor,.3*factor
        trace=poisson_trace(e.prompts,101,300,.000000001,sr+dr);trace=[r for r in trace if r['initial_role']=='D']
        summary,records,samples=await e.run(f'calibration_{index}','calibration',trace,300,300)
        measure=capacity(summary,records,samples,sr+dr);calibration.append(dict(candidate=index,source_rate=sr,background_rate=dr,capacity=measure))
        dump(root/'capacity_calibration.json',calibration);print('CAPACITY',json.dumps(calibration[-1]),flush=True)
        if measure['stable']:break
    else:raise RuntimeError('No stable load within three capacity candidates')
    dump(root/'frozen_load.json',dict(source_rate=sr,background_rate=dr,seed=101,duration_s=600,t0_s=300,basis=calibration[-1]))
    trace=poisson_trace(e.prompts,101,600,sr,dr);dump(root/'frozen_main_trace.json',trace)
    for mode in order:await e.run('main_'+mode,mode,trace)
    indices=json.loads((root/'coverage_prompt_indices.json').read_text())
    if len(indices)!=16:dump(root/'coverage_insufficient.json',dict(reason='Fewer than 16 eligible long prompts',available=len(indices)))
    else:
        fixed=[dict(request_id=f'coverage-S-{i:02d}',initial_role='S',planned_offset_s=0.,prompt_index=p,seed=101) for i,p in enumerate(indices)]
        bg=[r for r in poisson_trace(e.prompts,101,180,.000000001,dr) if r['initial_role']=='D']
        cover=sorted(fixed+bg,key=lambda x:x['planned_offset_s']);dump(root/'frozen_coverage_trace.json',cover)
        for mode in ['B']+[m for m in order if m!='B']:await e.run('coverage_'+mode,mode,cover,180,0,True)
    dump(root/'completed.json',dict(completed=True,formal_seeds_run=False,normal_unload_qualified=False,wall_ns=time.time_ns()))

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--gpus',required=True);p.add_argument('--authorization',required=True);args=p.parse_args()
    try:asyncio.run(main_async(args))
    except BaseException as exc:
        dump(Path(args.root)/'blocked.json',dict(error=repr(exc),traceback=traceback.format_exc(),wall_ns=time.time_ns()))
        print(traceback.format_exc(),flush=True);raise
if __name__=='__main__':main()
