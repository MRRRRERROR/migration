#!/usr/bin/env python3
"""Finite paired two-GPU real experiment; fail closed, retain failed rounds."""
from __future__ import annotations
import argparse
import asyncio
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import statistics
import subprocess
import sys
import time
import traceback
from collections import Counter

import aiohttp
import grpc
import psutil
import redis
from llumnix.cms.proto.cms_pb2 import InstanceMetadata, InstanceStatus
from llumnix.server.proto import llumlet_server_pb2 as pb, llumlet_server_pb2_grpc as rpc
from tests.local import utils
from two_gpu_nvml_monitor import NVML
from two_gpu_power_metrics import poisson_trace,power_metrics

PROJECT=Path(__file__).resolve().parents[1]
MODEL=Path('/data/models/Qwen2.5-7B-Instruct')
OVERLAY=Path('/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay')
FROZEN=Path(os.environ.get('LLUMNIX_SERVING_FROZEN_DIR','/data/home/zylu/llumnix-kvt500-2gpu/repair_20261002/frozen_cancel_scope_20261006'))
PORTS=(6379,8100,8101,31100,31101,36218,36226,49000,49001)
CRITICAL=re.compile(r'KVTResp\(code=500|EngineCore encountered a fatal error|EngineCore failed to start|CUDA error|CUDA illegal|illegal memory access|Segfault encountered|AssertFailed|Fatal Python error',re.I)

def dump(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str)+'\n')
def rows(path):
    with path.open() as stream:return [json.loads(s) for s in stream if s.strip()]
def csvwrite(path,data):
    if not data:return
    keys=list(dict.fromkeys(k for r in data for k in r))
    with path.open('w',newline='') as stream:
        w=csv.DictWriter(stream,fieldnames=keys);w.writeheader()
        for r in data:w.writerow({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def stats(values):
    v=sorted(x for x in values if x is not None)
    return {'n':len(v),'mean':statistics.mean(v) if v else None,'p95':v[min(len(v)-1,int(.95*(len(v)-1)))] if v else None,'max':max(v) if v else None}

def verify_version(diagnostic=False):
    revoked=FROZEN.parent/'FREEZE_MIGRATION_VALIDATION_REVOKED.json'
    if revoked.exists() and not diagnostic:
        raise RuntimeError('Frozen version only passed basic exit cases; migration shutdown validation was revoked: '+str(revoked))
    changes=[];files=[]
    for frozen in FROZEN.rglob('*'):
        if not frozen.is_file() or '__pycache__' in frozen.parts or frozen.suffix not in ('.py','.so'):continue
        relative=frozen.relative_to(FROZEN)
        actual=OVERLAY/'blade_kvt'/relative
        if not actual.exists() or sha(actual)!=sha(frozen):changes.append(str(relative))
        files.append({'path':str(actual),'sha256':sha(actual) if actual.exists() else None})
    if changes:raise RuntimeError('Frozen KVT version mismatch: '+str(changes))
    # Include model/tokenizer, serving and migration sources plus worktree patch.
    for folder in (PROJECT/'python/llumnix/llumnix',OVERLAY/'blade_kvt/hybrid_connector'):
        files.extend({'path':str(p),'sha256':sha(p)} for p in folder.rglob('*.py'))
    files.extend({'path':str(p),'sha256':sha(p)} for p in MODEL.glob('*.json'))
    return files

def preflight(nv):
    state=[nv.row(i,r) for i,r in ((0,'S'),(1,'D'))]
    for row in state:
        if json.loads(row['compute_pids']):raise RuntimeError('Approved GPU busy: '+str(row))
    for port in PORTS:
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            try:s.bind(('127.0.0.1',port))
            except OSError:raise RuntimeError(f'Port {port} occupied; leave owner untouched')
    return state

def snapshot(folder):
    found=[]
    for p in folder.glob('*.json'):
        try:found.append(json.loads(p.read_text()))
        except (OSError,json.JSONDecodeError):pass
    return max(found,key=lambda r:r['monotonic_s']) if found else None

class Experiment:
    def __init__(self,root,diagnostic=False):
        self.diagnostic=diagnostic
        self.root=root;self.nv=NVML();self.prompts=rows(root/'inputs/prompts.jsonl');self.version=verify_version(diagnostic=diagnostic)
        if any(not 128<=p['input_tokens']<=4096 for p in self.prompts):raise RuntimeError('Actual model input length outside 128..4096; prepare a new directory and recalibrate')
        self.owned={};self.processes=[];self.failed=False
        self.idle_memory={i:self.nv.row(i,role)['memory_mib'] for i,role in ((0,'S'),(1,'D'))}
    def start(self,name,cmd,folder,env=None):
        log=(folder/(name+'.log')).open('x')
        proc=subprocess.Popen(cmd,shell=True,cwd=PROJECT,env={**os.environ,**(env or {})},stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        self.processes.append((name,proc,log));self.owned[proc.pid]=psutil.Process(proc.pid).create_time()
        dump(folder/'owned_processes.json',{'roots':[{'name':n,'pid':p.pid,'created':self.owned[p.pid]} for n,p,_ in self.processes]})
        return proc
    def record_owned(self):
        for _,proc,_ in self.processes:
            try:
                parent=psutil.Process(proc.pid)
                for child in parent.children(recursive=True):self.owned[child.pid]=child.create_time()
            except psutil.Error:pass
    async def terminate(self,proc):
        if proc.poll() is not None:return
        parent=psutil.Process(proc.pid)
        if parent.create_time()!=self.owned[proc.pid]:raise RuntimeError('PID identity changed')
        targets=[(p.pid,p.create_time()) for p in parent.children(recursive=True)]+[(parent.pid,parent.create_time())]
        if any(name.startswith('vllm_') and owned is proc for name,owned,_ in self.processes):
            api=next(p for p in parent.children(recursive=True) if '/bin/vllm' in ' '.join(p.cmdline()))
            api.send_signal(signal.SIGTERM)
        else:
            os.killpg(proc.pid,signal.SIGTERM)
        deadline=time.monotonic()+60
        def alive():
            for pid,created in targets:
                try:
                    p=psutil.Process(pid)
                    if p.create_time()==created and p.status()!=psutil.STATUS_ZOMBIE:return True
                except psutil.Error:pass
            return False
        while alive() and time.monotonic()<deadline:await asyncio.sleep(.2)
        if alive():raise RuntimeError('Graceful shutdown timeout; preserve process')
        proc.poll()
        for name,owned,log in self.processes:
            if owned is proc and name.startswith('vllm_'):
                log.flush();text=Path(log.name).read_text(errors='replace')
                runtime='\n'.join(line for line in text.splitlines() if '[entrypoints/logger.py:' not in line and ' prompt: ' not in line)
                if CRITICAL.search(runtime):raise RuntimeError('Fatal serving/native shutdown evidence; preserve scene: '+log.name)
    def registry(self):
        db=redis.Redis(host='127.0.0.1',port=6379,socket_timeout=2);out={}
        for key in db.scan_iter('llumnix:instance_metadata:*'):
            raw=db.get(key)
            if not raw:continue
            md=InstanceMetadata();md.ParseFromString(raw)
            st=InstanceStatus();raw=db.get(b'llumnix:instance_status:'+key.split(b':')[-1])
            if raw:st.ParseFromString(raw)
            out[md.api_server_port]=(md,st)
        return out
    async def deploy(self,folder):
        deadline=time.monotonic()+60
        while True:
            active=[p for i in (0,1) for p in self.nv.processes(self.nv.handle(i))]
            if not active:break
            if any(p['pid'] not in self.owned for p in active) or time.monotonic()>deadline:raise RuntimeError('GPU unavailable after cleanup: '+str(active))
            await asyncio.sleep(.5)
        preflight(self.nv);current=verify_version(diagnostic=self.diagnostic)
        if current!=self.version:raise RuntimeError('Serving code changed during experiment')
        self.processes=[];self.owned={};self.start('redis',utils.get_redis_command()+" --save '' --appendonly no",folder)
        await asyncio.sleep(.5);self.servers={};self.discovery={};commands=[]
        for i,role in ((0,'S'),(1,'D')):
            cmd=utils.get_vllm_command('neutral',8100+i,i,True,f'power_{folder.name}_{role}','HybridConnector')
            cmd=re.sub(r'--max-model-len\s+\S+','--max-model-len 9216',cmd)
            cmd=re.sub(r'--max-num-batched-tokens\s+\S+','--max-num-batched-tokens 8192',cmd)
            cmd+=' --dtype bfloat16 --generation-config auto --kv-cache-memory-bytes 4294967296'
            env={'POWER_DRAIN_STATE_DIR':str(folder/('state_'+role)),
                 'LLUMNIX_RPC_PORT':str(49000+i),'LLUMNIX_MIGRATION_EFFICIENCY_LOG':str(folder/'migration_events.jsonl'),
                 'KVT500_MINIMAL_TRACE':str(folder/'kvt_attempt_trace.jsonl')}
            self.servers[role]=self.start('vllm_'+str(i),cmd,folder,env)
            disc=utils.get_discovery_command('neutral',8100+i)
            self.discovery[role]=self.start('discovery_'+str(i),disc,folder)
            commands.append({'role':role,'gpu':i,'command':cmd,'discovery':disc,'env':env})
        dump(folder/'launch.json',commands)
        async with aiohttp.ClientSession() as session:
            deadline=time.monotonic()+600
            ready=set()
            while len(ready)<2:
                self.record_owned()
                for i,role in ((0,'S'),(1,'D')):
                    if self.servers[role].poll() is not None:raise RuntimeError(role+' exited during startup')
                    try:
                        async with session.get(f'http://127.0.0.1:{8100+i}/health',timeout=aiohttp.ClientTimeout(total=2)) as r:
                            if r.status==200:ready.add(role)
                    except Exception:pass
                if time.monotonic()>deadline:raise RuntimeError('Startup timeout')
                await asyncio.sleep(1)
            for i,role in ((0,'S'),(1,'D')):
                async with session.post(f'http://127.0.0.1:{8100+i}/v1/completions',json={
                    'model':str(MODEL),'prompt':'Hello. Say hello briefly.','max_tokens':16,'temperature':0,
                    'kv_transfer_params':{'ali_llumnix_disagg':False}}) as resp:
                    body=await resp.text()
                    if resp.status!=200:raise RuntimeError('Warmup failed '+body)
            deadline=time.monotonic()+60
            while True:
                reg=self.registry();snaps=[snapshot(folder/('state_'+r)) for r in ('S','D')]
                if all(p in reg for p in (8100,8101)) and all(s and s['frontend_observed'] and not s['requests'] for s in snaps):break
                if time.monotonic()>deadline:raise RuntimeError('CMS or real engine observer not ready: '+str(snaps))
                await asyncio.sleep(.2)
            dump(folder/'initial_registry.json',{str(p):{'metadata':str(md),'status':str(st)} for p,(md,st) in reg.items()})
        print('DEPLOY_READY '+str(folder),flush=True)

    def lease_state(self,folder):
        held=set();acquired=returned=0
        for p in folder.glob('vllm_*.log'):
            for line in p.read_text(errors='replace').splitlines():
                if 'KVT_BLOCK_LEASE {' not in line:continue
                row=json.loads(line.split('KVT_BLOCK_LEASE ',1)[1]);key=(str(p),row['lease_id'])
                if row['event']=='acquire':held.add(key);acquired+=1
                elif row['event']=='release':held.discard(key);returned+=1
        return {'held':sorted(held),'acquired':acquired,'returned':returned}

    async def migrate(self,folder):
        reg=self.registry();source=reg[8100][0];dest=reg[8101][0]
        channel=grpc.aio.insecure_channel(f'127.0.0.1:{source.llumlet_port}')
        request=pb.MigrateRequest(src_engine_id=source.instance_id,dst_engine_id=dest.instance_id,
            dst_engine_ip='127.0.0.1',dst_engine_port=dest.kvt_port,migration_type='NUM_REQ',
            migration_req_policy='LR',num_reqs=-1,trigger_policy='power_drain')
        info={'start_monotonic_s':time.monotonic(),'start_wall_ns':time.time_ns(),
              'rules':'Existing Llumnix pre-stop: 2 sequential sends; existing engine-client 3 retries, frontend 10s and prestop 5s. No extra controller retry.'}
        try:
            resp=await rpc.LlumletStub(channel).Migrate(request,timeout=120)
            info.update({'rpc_success':resp.success,'message':resp.message})
        except Exception as exc:info.update({'rpc_success':False,'error':repr(exc)})
        finally:
            info['end_monotonic_s']=time.monotonic();dump(folder/'migration_trigger.json',info);await channel.close()

    async def send(self,session,record,folder,start):
        now=time.monotonic();role=record['initial_role']
        if record['mode'] in 'BC' and now>=start+record['t0_offset_s']:role='D'
        record.update({'actual_send_monotonic_s':now,'actual_send_wall_ns':time.time_ns(),'route_role':role,
            'planned_monotonic_s':start+record['planned_offset_s'],
            'send_delay_s':now-start-record['planned_offset_s'],'output_blocks':[]})
        prompt=self.prompts[record['prompt_index']];record['input_tokens']=prompt['input_tokens'];record['prompt_sha256']=prompt['sha256']
        payload={'model':str(MODEL),'prompt':prompt['prompt'],'max_tokens':4096,'temperature':0,
            'seed':record['seed'],'stream':True,'stream_options':{'include_usage':True,'continuous_usage_stats':True},
            'kv_transfer_params':{'ali_llumnix_disagg':False}}
        text=[];record['finish_reason']=None;record['completion_tokens']=None;record['error']=None
        try:
            async with session.post(f'http://127.0.0.1:{8100 if role=="S" else 8101}/v1/completions',json=payload,
                headers={'X-Request-Id':record['request_id']},timeout=aiohttp.ClientTimeout(total=None)) as resp:
                record['http_status']=resp.status
                if resp.status!=200:raise RuntimeError((await resp.text())[:2000])
                buffer=b''
                async for chunk in resp.content.iter_chunked(8192):
                    buffer+=chunk
                    while b'\n' in buffer:
                        line,buffer=buffer.split(b'\n',1);line=line.decode(errors='replace').strip()
                        if not line.startswith('data:'):continue
                        body=line[5:].strip()
                        if body=='[DONE]':record['sse_done']=True;continue
                        item=json.loads(body);record['response_id']=item.get('id',record.get('response_id'))
                        usage=item.get('usage') or {}
                        if usage.get('completion_tokens') is not None:record['completion_tokens']=usage['completion_tokens']
                        if usage.get('prompt_tokens') is not None:record['server_input_tokens']=usage['prompt_tokens']
                        for choice in item.get('choices') or []:
                            content=choice.get('text') or (choice.get('delta') or {}).get('content')
                            if content:text.append(content);record['output_blocks'].append(time.monotonic())
                            record['finish_reason']=choice.get('finish_reason') or record['finish_reason']
                if not record['finish_reason']:raise RuntimeError('SSE ended without finish_reason')
        except asyncio.CancelledError:
            record['error']='unfinished; retained at tail timeout';raise
        except Exception as exc:record['error']=repr(exc)
        finally:
            end=time.monotonic();record['finish_monotonic_s']=end;record['finish_wall_ns']=time.time_ns()
            blocks=record['output_blocks'];tokens=record['completion_tokens']
            record.update({'first_output_monotonic_s':blocks[0] if blocks else None,
                'ttft_s':blocks[0]-now if blocks else None,'e2e_s':end-now,
                'tpot_s':(blocks[-1]-blocks[0])/(tokens-1) if blocks and tokens and tokens>1 else None,
                'max_output_block_gap_s':max((b-a for a,b in zip(blocks,blocks[1:])),default=None),
                'output_text_sha256':hashlib.sha256(''.join(text).encode()).hexdigest(),
                'completed':record['error'] is None and record['finish_reason'] in ('stop','length')})
            with (folder/'request_results.jsonl').open('a') as f:f.write(json.dumps(record,ensure_ascii=False)+'\n')

    async def run_round(self,name,mode,seed,srate,drate,duration=900,t0_offset=300,trace=None):
        existing=self.root/name
        if (existing/'summary.json').exists():
            return json.loads((existing/'summary.json').read_text()),rows(existing/'request_results.jsonl'),rows(existing/'instance_samples.jsonl')
        folder=self.root/name;folder.mkdir(exist_ok=False);dump(folder/'round_config.json',locals_config(mode,seed,srate,drate,duration,t0_offset))
        await self.deploy(folder)
        if trace is None:trace=poisson_trace(self.prompts,seed,duration,srate,drate)
        with (folder/'replay.jsonl').open('x') as f:
            for row in trace:f.write(json.dumps(row)+'\n')
        records=[{**r,'mode':mode,'t0_offset_s':t0_offset} for r in trace]
        mon=self.start('nvml',f'{sys.executable} -u tools/two_gpu_nvml_monitor.py --output {folder}/power.csv',folder)
        start=time.monotonic();wall=time.time_ns();t0=start+t0_offset
        timeline={'start_monotonic_s':start,'start_wall_ns':wall,'t0_planned_monotonic_s':t0,'duration_s':duration}
        dump(folder/'timeline.json',timeline);samples=[];done=asyncio.Event();critical=[];positions={};tasks=[]
        async def monitor():
            with (folder/'instance_samples.jsonl').open('x') as stream:
                while not done.is_set():
                    self.record_owned();now=time.monotonic();item={'monotonic_s':now,'elapsed_s':now-start}
                    for i,role in ((0,'S'),(1,'D')):
                        snap=snapshot(folder/('state_'+role));item[role]=snap
                        gp=self.nv.row(i,role)
                        alien=[p for p in json.loads(gp['compute_pids']) if p['pid'] not in self.owned]
                        if alien:critical.append({'kind':'GPU_interference','role':role,'evidence':alien,'monotonic_s':now})
                    for path in folder.glob('vllm_*.log'):
                        with path.open(errors='replace') as f:
                            f.seek(positions.get(path,0));new=f.read();positions[path]=f.tell()
                        runtime_lines=[line for line in new.splitlines() if '[entrypoints/logger.py:' not in line and ' prompt: ' not in line]
                        hits=CRITICAL.findall('\n'.join(runtime_lines))
                        if hits:critical.append({'kind':'runtime_critical','evidence':new[-10000:],'monotonic_s':now})
                    samples.append(item);stream.write(json.dumps(item)+'\n');stream.flush()
                    if mon.poll() is not None:critical.append({'kind':'power_monitor_exit','time':now})
                    await asyncio.sleep(.5)
        sampler=asyncio.create_task(monitor());migration_task=None
        async def trigger():
            nonlocal migration_task
            await asyncio.sleep(max(0,t0-time.monotonic()))
            timeline['t0_monotonic_s']=time.monotonic();timeline['t0_wall_ns']=time.time_ns()
            dump(folder/'t0_snapshot.json',{'S':snapshot(folder/'state_S'),'D':snapshot(folder/'state_D'),
                'client_source_inflight':[r['request_id'] for r in records if r.get('route_role')=='S' and not r.get('finish_monotonic_s')]})
            dump(folder/'timeline.json',timeline)
            print('T0 '+name,flush=True)
            if mode=='C':migration_task=asyncio.create_task(self.migrate(folder))
        instruction=asyncio.create_task(trigger())
        async def retire():
            if mode not in 'BC':return
            await instruction
            if migration_task:await migration_task
            stable=0
            while not done.is_set():
                snap=snapshot(folder/'state_S')
                if snap and not snap['requests'] and 'source_requests_empty_s' not in timeline:
                    timeline['source_requests_empty_s']=time.monotonic()-timeline['t0_monotonic_s']
                # Original source API is an output forwarding dependency. Keep it
                # until its HTTP responses complete; never infer handoff from count=0.
                unfinished=[r for r in records if r.get('route_role')=='S' and not r.get('finish_monotonic_s')]
                safe=snap and time.monotonic()-snap['monotonic_s']<2 and not snap['requests'] and not unfinished and snap['frontend_observed'] and snap['migration_tasks']==0 and not snap['migrating_request_ids'] and snap['migration_finish_set']
                if safe:
                    lease=await asyncio.to_thread(self.lease_state,folder)
                    safe=not lease['held'] and lease['acquired']==lease['returned']
                stable=stable+1 if safe else 0
                if stable>=3:
                    dump(folder/'source_safe_exit_evidence.json',{'snapshot':snap,'lease':lease,'client_source_streams_unfinished':0,'owned_pids':self.owned})
                    timeline['source_safe_ready_s']=time.monotonic()-timeline['t0_monotonic_s']
                    await self.terminate(self.discovery['S']);await self.terminate(self.servers['S'])
                    timeline['source_process_exit_s']=time.monotonic()-timeline['t0_monotonic_s']
                    deadline=time.monotonic()+60
                    while self.nv.row(0,'S')['memory_mib']>self.idle_memory[0]+64 or self.nv.processes(self.nv.handle(0)):
                        if time.monotonic()>deadline:raise RuntimeError('Source GPU memory release timeout')
                        await asyncio.sleep(.2)
                    timeline['source_memory_idle_s']=time.monotonic()-timeline['t0_monotonic_s'];dump(folder/'timeline.json',timeline)
                    print('SOURCE_EXIT '+name,flush=True);return
                await asyncio.sleep(.5)
        retiring=asyncio.create_task(retire())
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            for record in records:
                await asyncio.sleep(max(0,start+record['planned_offset_s']-time.monotonic()))
                if critical:break
                tasks.append(asyncio.create_task(self.send(session,record,folder,start)))
            await asyncio.sleep(max(0,start+duration-time.monotonic()) if not critical else 0)
            timeline['replay_end_monotonic_s']=time.monotonic()
            fixed=[dict(r) for r in records if r.get('actual_send_monotonic_s')]
            dump(folder/'fixed_window_requests.json',fixed)
            pending={t for t in tasks if not t.done()};deadline=start+duration+600
            while pending and time.monotonic()<deadline and not critical:
                _,pending=await asyncio.wait(pending,timeout=.5)
            if pending or critical:
                dump(folder/'blocked.json',{'critical':critical,'unfinished':[r['request_id'] for r in records if r.get('actual_send_monotonic_s') and not r.get('finish_monotonic_s')],
                                          'owned_pids':self.owned,'preserve_scene':True})
                done.set();await sampler
                # Retain existing HTTP streams. Do not cancel unfinished user
                # requests merely to stop the controller's formal sequence.
                if pending:
                    await asyncio.gather(*pending,return_exceptions=True)
                raise RuntimeError('Critical error or tail timeout: preserve serving processes; formal rounds paused')
            for t in tasks:await t
            timeline['tail_finished_monotonic_s']=time.monotonic()
            if mode in 'BC':
                try:await asyncio.wait_for(asyncio.shield(retiring),timeout=90)
                except asyncio.TimeoutError:raise RuntimeError('Safety proof unavailable: preserve scene')
        done.set();await sampler;await instruction
        # All client streams are complete; check physical lease returns before cleanup.
        lease=await asyncio.to_thread(self.lease_state,folder);dump(folder/'final_leases.json',lease)
        if lease['held'] or lease['acquired']!=lease['returned']:raise RuntimeError('Unreturned physical lease: preserve scene')
        for name2,proc,_ in reversed(self.processes):
            if name2!='nvml':await self.terminate(proc)
        await asyncio.sleep(1);await self.terminate(mon)
        for _,_,f in self.processes:f.close()
        timeline['cleanup_complete_monotonic_s']=time.monotonic();dump(folder/'timeline.json',timeline)
        csvwrite(folder/'request_metrics.csv',records)
        csvwrite(folder/'queue_metrics.csv',[{'monotonic_s':s['monotonic_s'],'role':r,'running':(s[r] or {}).get('running'),
            'waiting':(s[r] or {}).get('waiting')} for s in samples for r in ('S','D')])
        completed=[r for r in records if r.get('completed')]
        power=list(csv.DictReader((folder/'power.csv').open()))
        result={'name':name,'mode':mode,'seed':seed,'planned':len(records),'sent':len(tasks),'completed':len(completed),
            'completion_rate':len(completed)/len(records),'completed_in_900s':sum(r.get('completed',False) and r['finish_monotonic_s']<=start+duration for r in records),
            'unfinished':len(records)-len(completed),'finish_reasons':dict(Counter(r.get('finish_reason') or 'unfinished' for r in records)),
            'send_delay_s':stats([r.get('send_delay_s') for r in records]),'timing':timeline,
            'ttft_s':stats([r.get('ttft_s') for r in completed]),'tpot_s':stats([r.get('tpot_s') for r in completed]),'e2e_s':stats([r.get('e2e_s') for r in completed]),
            'max_output_block_gap_s':stats([r.get('max_output_block_gap_s') for r in completed]),
            'power':power_metrics(power,timeline['t0_monotonic_s']),
            'metric_scope':'TPOT=(last nonempty SSE output block-first nonempty block)/(completion_tokens-1). SSE gaps are output block intervals, not token ITL.',
            'safe_exit_scope':'source API retained while original HTTP streams depend on it; migration does not imply forwarding independence'}
        for group,predicate in (('D_background',lambda r:r['initial_role']=='D'),('new_transferred',lambda r:r['initial_role']=='S' and r.get('route_role')=='D'),('t0_S_inflight',lambda r:r.get('route_role')=='S' and r.get('actual_send_monotonic_s',1e30)<t0 and r.get('finish_monotonic_s',0)>t0)):
            selected=[r for r in completed if predicate(r)];result[group]={'n':len(selected),'ttft_s':stats([r.get('ttft_s') for r in selected]),'tpot_s':stats([r.get('tpot_s') for r in selected]),'e2e_s':stats([r.get('e2e_s') for r in selected])}
        text='\n'.join(p.read_text(errors='replace') for p in folder.glob('vllm_*.log'))
        runtime_text='\n'.join(line for line in text.splitlines() if '[entrypoints/logger.py:' not in line and ' prompt: ' not in line)
        result['raw_counts']={'kvt500':text.count('KVTResp(code=500'),'kvt404':text.count('KVTResp(code=404'),
            'cancel':len(re.findall(r'(?im)^.*abort req.*$',text)), 'traceback':text.count('Traceback (most recent call last)'),
            'committed_migration':text.count('"event_name": "migration_done"'), 'preemption':len(re.findall(r'(?i)preempted by',text)), 'runtime_error':text.count('RuntimeError:')}
        result['raw_strict_validity']='invalid' if result['raw_counts']['cancel'] or result['raw_counts']['kvt404'] or result['unfinished'] else 'valid'
        result['runtime_only_counts']={'kvt500':runtime_text.count('KVTResp(code=500'),'traceback':runtime_text.count('Traceback (most recent call last)'),
            'runtime_error':runtime_text.count('RuntimeError:'),'scope':'request prompt logging excluded; original raw counts retained'}
        for r in records:
            blocks=r.get('output_blocks',[])
            r['t0_crossing_output_block_gap_s']=max((b-a for a,b in zip(blocks,blocks[1:]) if a<=timeline['t0_monotonic_s']<=b),default=None)
        if result['t0_S_inflight']['n']:
            selected=[r for r in completed if r.get('route_role')=='S' and r.get('actual_send_monotonic_s',1e30)<t0<r.get('finish_monotonic_s',0)]
            result['t0_S_inflight']['post_t0_completion_s']=stats([r['finish_monotonic_s']-timeline['t0_monotonic_s'] for r in selected])
            result['t0_S_inflight']['crossing_output_block_gap_s']=stats([r['t0_crossing_output_block_gap_s'] for r in selected])
        csvwrite(folder/'request_metrics.csv',records)
        result['power_readout_characteristics']={r:{'requested_interval_s':.2,'identical_adjacent_fraction':sum(a['power_w']==b['power_w'] for a,b in zip([x for x in power if x['role']==r],[x for x in power if x['role']==r][1:]))/max(1,len(power)//2-1),
            'missing_readings':sum(x['power_w']=='' for x in power if x['role']==r), 'note':'NVML polling rate does not establish sensor measurement update rate'} for r in ('S','D')}
        dump(folder/'summary.json',result);print('ROUND_DONE '+json.dumps({k:result[k] for k in ('name','sent','completed','raw_counts')},ensure_ascii=False),flush=True)
        if result['unfinished'] or result['raw_counts']['kvt500']:raise RuntimeError('Request loss or KVT500; no further formal rounds')
        return result,records,samples

def locals_config(mode,seed,srate,drate,duration,t0):
    return {'mode':mode,'seed':seed,'source_rate':srate,'destination_rate':drate,'duration_s':duration,'t0_offset_s':t0,
        'model':str(MODEL),'dtype':'bfloat16','context_length':9216,'output_cap':4096,'temperature':0,
        'ignore_eos':'omitted','generation_config':'auto','kv_cache_bytes':4294967296,
        'migration':'existing pre-stop RPC; auto scaling and ordinary scheduler migration absent',
        'topology':'two GPU logical domains on one server; physical GPUs 0 and 1 only'}

def capacity(records,samples,role,start,duration,rate):
    observations=[(s['elapsed_s'],(s.get(role) or {}).get('waiting'),(s.get(role) or {}).get('running')) for s in samples if 60<=s['elapsed_s']<=duration]
    queue=[(t,w) for t,w,_ in observations if w is not None]
    late=[w for t,w in queue if t>=duration-120];early=[w for t,w in queue if 60<=t<180]
    if not late or not early:return {'stable':False,'reason':'missing real queue observations'}
    avg_t=statistics.mean(t for t,w in queue);avg_w=statistics.mean(w for t,w in queue)
    slope=sum((t-avg_t)*(w-avg_w) for t,w in queue)/sum((t-avg_t)**2 for t,w in queue)
    selected=[r for r in records if r.get('route_role')==role]
    completion_rate=sum(r.get('completed',False) and start+duration-120<=r.get('finish_monotonic_s',0)<=start+duration for r in selected)/120
    late_ttft=stats([r.get('ttft_s') for r in selected if r.get('actual_send_monotonic_s',0)>=start+duration-120])
    early_ttft=stats([r.get('ttft_s') for r in selected if start+60<=r.get('actual_send_monotonic_s',0)<start+180])
    late_tpot=stats([r.get('tpot_s') for r in selected if r.get('actual_send_monotonic_s',0)>=start+duration-120])
    early_tpot=stats([r.get('tpot_s') for r in selected if start+60<=r.get('actual_send_monotonic_s',0)<start+180])
    stable=slope<=.01 and statistics.mean(late)-statistics.mean(early)<=2 and completion_rate>=rate*.85
    if late_ttft['p95'] is not None and early_ttft['p95'] is not None:stable=stable and late_ttft['p95']<=max(early_ttft['p95']*2,early_ttft['p95']+1)
    if late_tpot['p95'] is not None and early_tpot['p95'] is not None:stable=stable and late_tpot['p95']<=max(early_tpot['p95']*1.5,early_tpot['p95']+.01)
    return {'stable':stable,'queue_slope_per_s':slope,'early_waiting_mean':statistics.mean(early),'late_waiting_mean':statistics.mean(late),
        'late_completion_rate':completion_rate,'offered_rate':rate,'active_fraction':sum(bool(r or w) for _,w,r in observations)/len(observations),
        'early_ttft':early_ttft,'late_ttft':late_ttft,'early_tpot':early_tpot,'late_tpot':late_tpot}

async def main_async(args):
    root=Path(args.root);e=Experiment(root);dump(root/'version_manifest.json',e.version)
    dump(root/'preflight.json',preflight(e.nv));final=[];cal=[];factor=1.
    for point in range(5):
        srate,drate=1.*factor,.3*factor
        for role,rs,rd in (('S',srate,drate),('D',0.,srate+drate)):
            if rs==0:
                trace=poisson_trace(e.prompts,700+point,300,.000001,rd);trace=[r for r in trace if r['initial_role']=='D']
            else:trace=None
            result,records,samples=await e.run_round(f'calibration_{point}_{role}','A',700+point,rs,rd,300,150,trace)
            measure=capacity(records,samples,role,result['timing']['start_monotonic_s'],300,rs if role=='S' else rd)
            measure['preemptions']=result['raw_counts']['preemption'];measure['errors']=result['unfinished']
            measure['stable']=measure['stable'] and not measure['preemptions'] and not measure['errors']
            cal.append({'point':point,'factor':factor,'role':role,'capacity':measure,'summary':result['name']})
            dump(root/'capacity_calibration.json',cal)
        source,dest=cal[-2]['capacity'],cal[-1]['capacity']
        print('CAPACITY '+json.dumps(cal[-2:],ensure_ascii=False),flush=True)
        if source['stable'] and dest['stable'] and source.get('active_fraction',0)>=.5:break
        factor*=.7 if not source['stable'] or not dest['stable'] else 1.25
    else:raise RuntimeError('No stable calibrated capacity after five candidate points')
    frozen=locals_config('paired',0,srate,drate,900,300);frozen['capacity_basis']=cal[-2:]
    dump(root/'final_config.json',frozen)
    replay_dir=root/'replays';replay_dir.mkdir(exist_ok=True)
    traces={}
    for seed in (101,102,103,104,105):
        trace=poisson_trace(e.prompts,seed,900,srate,drate);traces[seed]=trace
        file=replay_dir/f'seed_{seed}.jsonl'
        if file.exists():
            if rows(file)!=trace:raise RuntimeError('Frozen trace changed on resume')
        else:
            with file.open('x') as f:
                for row in trace:f.write(json.dumps(row)+'\n')
    dump(root/'replay_counts.json',{s:len(t) for s,t in traces.items()})
    # Acceptance exercises the actual B/C route transition, power and safe retirement.
    for mode in 'BC':
        result,records,_=await e.run_round('acceptance_'+mode,mode,999,srate,drate,180,60)
        if not result['timing'].get('source_safe_ready_s') or result['send_delay_s']['max']>1 or any(r.get('route_role')=='S' and r.get('actual_send_monotonic_s',0)>=result['timing']['t0_monotonic_s'] for r in records):raise RuntimeError('Short acceptance failed')
    dump(root/'acceptance_passed.json',{'passed':True,'modes':['B','C'],'counts_as_formal':False})
    if args.stop_after_acceptance:
        print('CALIBRATION_AND_BC_ACCEPTANCE_COMPLETE',flush=True)
        return
    for index,item in enumerate(json.loads((root/'formal_order.json').read_text()),1):
        result,_,_=await e.run_round(f'formal_{index:02d}_{item["mode"]}_seed{item["seed"]}',item['mode'],item['seed'],srate,drate,trace=traces[item['seed']])
        final.append(result);dump(root/'formal_summaries.json',final)
        subprocess.run([sys.executable,str(PROJECT/'tools/two_gpu_power_report.py'),'--root',str(root)],check=True)
    dump(root/'completed.json',{'formal_rounds':15,'completed':True,'wall_ns':time.time_ns()})

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--stop-after-acceptance',action='store_true');args=p.parse_args()
    try:asyncio.run(main_async(args))
    except BaseException as exc:
        root=Path(args.root);dump(root/'experiment_blocked.json',{'error':repr(exc),'traceback':traceback.format_exc(),'scene_preserved':True})
        print(traceback.format_exc(),flush=True);raise

if __name__=='__main__':main()
