"""Real two-GPU correctness gate and common persistent service deployment."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shlex
import sys
import time
import traceback
import uuid
import fcntl

import aiohttp
import grpc
import psutil
import experiment_base as base
from engine_control import atomic,append

base.APIS=(18270,18271);base.KVTS=(41270,41271);base.GRPCS=(59170,59171);base.REDIS=16579
base.PORTS=(16579,18270,18271,41270,41271,41370,41371,59170,59171,46570,46578,18370)
ROOT=Path('/data/home/zylu/llumnix-v018-main/results/incremental_runtime_migration_20261027_01')


class AttachedProcess:
    """Identity-checked handle to this experiment's surviving child only."""
    def __init__(self,pid,created):self.pid=pid;self.created=created;self.returncode=None
    def checked(self):
        p=psutil.Process(self.pid)
        if p.create_time()!=self.created:raise RuntimeError('Attached PID was reused')
        return p
    def poll(self):
        try:
            p=self.checked()
            if p.status()==psutil.STATUS_ZOMBIE:return -1
            return None
        except psutil.NoSuchProcess:return -1
    def send_signal(self,sig):self.checked().send_signal(sig)
    def wait(self,timeout=None):
        try:result=self.checked().wait(timeout)
        except psutil.TimeoutExpired:raise base.subprocess.TimeoutExpired(str(self.pid),timeout)
        # An attached non-child's exit code is unavailable, never imply zero.
        self.returncode=result if result is not None else 'unavailable_non_child'
        return self.returncode


class Harness(base.Experiment):
    def __init__(self,root=ROOT,attach=False):
        from two_gpu_live_nvml import NVML
        self.root=Path(root);self.nv=NVML();self.gpus=[0,1]
        self.args=argparse.Namespace(root=str(root),gpus='0,1',authorization='user authorized exactly two idle physical GPUs; fixed 0/1')
        self.prompts=base.readrows(self.root/'inputs/prompts.jsonl');self.processes=[];self.identities={}
        self.service=self.root/'services_01'
        if not attach:self.service.mkdir(exist_ok=False)
        self.controller_lock=(self.service/'controller_cpu.lock').open('a')
        try:fcntl.flock(self.controller_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('These experiment instances already have an active controller; do not duplicate launch')
        self.controller_lock.write(json.dumps(dict(pid=os.getpid(),wall_ns=time.time_ns(),root=str(self.root)))+'\n');self.controller_lock.flush()
        if attach:
            for row in json.loads((self.service/'owned_processes.json').read_text()):
                p=psutil.Process(row['pid'])
                if p.create_time()!=row['created']:raise RuntimeError('Cannot attach a reused PID')
                cmd=' '.join(p.cmdline())
                if row['label'].startswith('vllm_') and 'alive_services_01_' not in cmd:raise RuntimeError('Service identity tag missing')
                if row['label']=='redis' and '16579' not in cmd:raise RuntimeError('Redis identity port mismatch')
                self.processes.append((row['label'],AttachedProcess(p.pid,p.create_time()),(self.service/(row['label']+'.log')).open('a')))
                self.identities[p.pid]=p.create_time()
            self.owned()
        self.serial=0;self.rows=[]

    def start(self,label,command,folder,env=None):
        env=dict(env or {})
        if label.startswith('vllm_'):
            role=label[-1]
            command=command.replace('BLLM_KVTRANS_PORT_BASE=46418','BLLM_KVTRANS_PORT_BASE=46570').replace('BLLM_KVTRANS_PORT_BASE=46426','BLLM_KVTRANS_PORT_BASE=46578')
            env.update(INCREMENTAL_ENGINE_DIR=str(folder/('state_'+role)),
                VLLM_BATCH_INVARIANT='1',LLUMNIX_TOKEN_TESTING='',MODEL_PATH=base.MODEL,
                PYTHONPATH=str(self.root/'execution_sources')+':'+str(base.P/'tools')+':'+os.environ.get('PYTHONPATH',''),
                KVT_LEASE_TRACE='1',KVT_LEASE_RECHECK='1',KVT_LEASE_FAULT='',KVT500_TRACE_NATIVE='1',
                LLUMNIX_ENABLE_STRUCTURED_MIGRATION_LOG='1',VLLM_HOST_IP='127.0.0.1')
        append(folder/'effective_environments.jsonl',dict(label=label,command=command,env=env))
        return super().start(label,command,folder,env)

    def state(self,i):
        path=self.service/('state_'+('S','D')[i])/'state.json'
        return json.loads(path.read_text()) if path.exists() else None

    async def command(self,i,op,timeout=15,**fields):
        self.serial+=1;name=f'{self.serial:08d}_{uuid.uuid4().hex}.json'
        root=self.service/('state_'+('S','D')[i]);value=dict(op=op,**fields)
        atomic(root/'commands'/name,value);deadline=time.monotonic()+timeout
        while not (root/'acks'/name).exists():
            if time.monotonic()>deadline:raise TimeoutError(f'Engine command {op} ambiguous; preserve ownership and services')
            self.check_critical();await asyncio.sleep(.05)
        reply=json.loads((root/'acks'/name).read_text())
        append(self.root/'command_results.jsonl',dict(engine=i,command={k:v for k,v in value.items() if k!='payload'},reply=reply))
        return reply

    async def require(self,i,op,**fields):
        reply=await self.command(i,op,**fields)
        if not reply['ok']:raise RuntimeError(f'Engine {i} {op}: '+reply['error'])
        return reply['result']

    def check_critical(self):
        for role in ('S','D'):
            p=self.service/('state_'+role)/'observer_failure.json'
            if p.exists():raise RuntimeError('Observer failed; preserve service: '+p.read_text())
        for f in self.service.glob('vllm_*.log'):
            # Finite correctness gate; the continuous runner uses tail offsets.
            text=f.read_text(errors='replace')
            found=base.CRITICAL.search(text)
            if found:raise RuntimeError('Critical serving failure; preserve scene: '+str(f)+' '+text[max(0,found.start()-200):found.end()+1000])
        for label,p,_ in self.processes:
            if label.startswith('vllm_') and p.poll() is not None:raise RuntimeError('Owned service exited: '+label)

    async def wait(self,predicate,timeout=60):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            self.check_critical();result=predicate()
            if result:return result
            await asyncio.sleep(.05)
        raise TimeoutError('Correctness gate condition not reached')

    async def configure(self,mode,hold=()):
        await self.wait(lambda:all(self.state(i) and not self.state(i)['requests'] and not self.state(i)['holds'] and not self.state(i)['migration_tasks'] for i in range(2)))
        for i in range(2):await self.require(i,'configure',mode=mode,test_hold_ids=list(hold))

    async def transfer(self,source,destination,req,folder,rollback=False,verify_duplicate=False):
        tx=uuid.uuid4().hex;rid=req['request_id'];fields=dict(tx=tx,request_id=rid)
        append(folder/'waiting_transfers.jsonl',dict(event='begin',source=source,destination=destination,request=req,**fields,monotonic_s=time.monotonic()))
        await self.require(destination,'reserve',**fields)
        claim=await self.command(source,'detach',**fields)
        if not claim['ok']:
            if any(tx in self.state(i)['holds'] for i in range(2)):
                raise RuntimeError('Ambiguous failed detach; retain holds')
            await self.require(destination,'abort_reservation',**fields)
            append(folder/'waiting_transfers.jsonl',dict(event='atomic_claim_rejected',reply=claim,**fields))
            raise RuntimeError('Atomic never-executed claim rejected: '+claim['error'])
        detached=claim['result']
        if rollback:
            aborted=await self.require(destination,'abort_reservation',**fields)
            await self.require(source,'rollback_source',destination_state=aborted['state'],**fields)
            return detached
        try:
            admitted=await self.require(destination,'admit',payload=detached['payload'],**fields)
        except TimeoutError:
            # Query only. Never execute a source rollback after an ambiguous add.
            admitted=await self.require(destination,'status',tx=tx)
            if admitted['state']!='committed':raise RuntimeError('Ambiguous destination ownership; retain source hold')
        await self.require(source,'release_source',destination_state=admitted['state'],**fields)
        if verify_duplicate:
            duplicate=await self.require(destination,'admit',payload=detached['payload'],**fields)
            assert duplicate['state']=='committed'
        append(folder/'waiting_transfers.jsonl',dict(event='committed',source=source,destination=destination,**fields,monotonic_s=time.monotonic()))
        return detached

    async def send_direct(self,session,rid,index,engine=0):
        row=dict(request_id=rid,prompt_index=index,planned_offset_s=0,initial_role=('S','D')[engine])
        class Route:
            def is_set(self):return engine==1
        await super().send(session,row,self.service,time.monotonic(),Route())
        self.rows.append(row);return row

    async def trigger_kv(self,source,destination,req,folder):
        s=self.state(source);d=self.state(destination)
        atomic(self.service/('state_'+('S','D')[source])/'selected_running.json',dict(request_id=req['request_id'],valid_until=time.monotonic()+60,mode='C'))
        request=base.pb.MigrateRequest(src_engine_id=s['instance_id'],dst_engine_id=d['instance_id'],
            dst_engine_ip='127.0.0.1',dst_engine_port=base.KVTS[destination],migration_type='NUM_REQ',
            migration_req_policy='SR',num_reqs=1,trigger_policy='incremental_running_baseline')
        channel=grpc.aio.insecure_channel(f'127.0.0.1:{base.GRPCS[source]}')
        cursors={}
        for key,path in [('lease',self.service/('vllm_'+('S','D')[source]+'.log')),('events',self.service/'kv_events.jsonl')]:
            info=path.stat() if path.exists() else None
            boundary=True
            if info and info.st_size:
                with path.open('rb') as inp:inp.seek(info.st_size-1);boundary=inp.read(1)==b'\n'
            cursors[key]=dict(offset=info.st_size if info else 0,inode=info.st_ino if info else None,starts_at_line_boundary=boundary)
        value=dict(request_id=req['request_id'],source=source,destination=destination,started=time.monotonic(),started_wall_ns=time.time_ns(),parameters=str(request),evidence_cursors=cursors)
        try:
            response=await base.rpc.LlumletStub(channel).Migrate(request,timeout=65)
            value.update(rpc_success=response.success,message=response.message)
        except Exception as error:value['error']=repr(error)
        finally:
            value['ended']=time.monotonic();append(folder/'running_attempts.jsonl',value);await channel.close()
        return value

    async def correctness(self):
        folder=self.root/'correctness_01';folder.mkdir(exist_ok=False)
        gates=[]
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as session:
            # An explicit correctness fixture holds only these IDs before schedule.
            # It is not present in calibration or measured rounds.
            for name,rollback in [('queue_handoff',False),('queue_rollback',True)]:
                rid='check_'+name;await self.configure('correctness',[rid])
                task=asyncio.create_task(self.send_direct(session,rid,0))
                req=await self.wait(lambda:next((r for r in self.state(0)['requests'] if r['logical_id']==rid),None))
                assert not req['ever_scheduled'] and req['computed_tokens']==0 and req['kv_blocks']==0
                detached=await self.transfer(0,1,req,folder,rollback,verify_duplicate=True)
                owner=0 if rollback else 1
                observed=await self.wait(lambda:next((r for r in self.state(owner)['requests'] if r['request_id']==req['request_id']),None))
                assert observed['arrival_time']==req['arrival_time']
                assert observed['queue_server_address']==req['queue_server_address']
                assert not observed['ever_scheduled'] and observed['computed_tokens']==0
                for i in range(2):await self.require(i,'release_test_hold')
                row=await asyncio.wait_for(asyncio.shield(task),180)
                assert row['completed'] and row['response_id']+'-0'==req['request_id']
                gates.append(dict(name=name,passed=True,request_id=rid,source_proof=detached['proof'],destination_proof=observed,
                    completion_tokens=len(row['token_ids']),finish_reason=row['finish_reason']))
                atomic(folder/'gates.json',gates)

            # A must reject even an otherwise well-formed reservation.
            await self.configure('A')
            refused=await self.command(1,'reserve',tx='forbidden_A',request_id='not_submitted')
            assert not refused['ok'];gates.append(dict(name='A_permissions',passed=True,evidence=refused))
            await self.configure('B')
            # KV selector checks mode C; prove no selected ID can bypass B gate.
            atomic(self.service/'state_S'/'selected_running.json',dict(mode='B',request_id='not_submitted',valid_until=time.monotonic()+60))
            gates.append(dict(name='B_running_permissions',passed=True,evidence='selector requires mode=C; controller mode gate tested separately'))

            # Reference uses identical full generation parameters and real prompt.
            await self.configure('C');reference=await self.send_direct(session,'check_reference',0)
            assert reference['completed']
            task=asyncio.create_task(self.send_direct(session,'check_running',0))
            req=await self.wait(lambda:next((r for r in self.state(0)['requests'] if r['logical_id']=='check_running' and r['generated_tokens']>=8 and r['state']=='running'),None))
            rejected=await self.command(0,'detach',tx='must_reject_running',request_id=req['request_id'])
            assert not rejected['ok'] and not self.state(0)['holds']
            result=await self.trigger_kv(0,1,req,folder)
            row=await asyncio.wait_for(asyncio.shield(task),180)
            assert row['completed'],row
            await self.wait(lambda:all(not self.state(i)['requests'] and not self.state(i)['migration_tasks'] for i in range(2)),90)
            events=base.readrows(self.service/'kv_events.jsonl')
            related=[e for e in events if req['request_id'] in json.dumps(e)]
            atomic(folder/'running_related_events.json',related)
            names={e.get('event_name',e.get('event')) for e in related}
            transferred=bool({'dst_resume','migration_done'}&names)
            equal=row['token_ids']==reference['token_ids']
            gates.append(dict(name='running_resume',passed=row['completed'] and transferred and equal,
                rpc=result,output_equal=equal,native_events=sorted(str(n) for n in names),running_detach_rejected=rejected))
            atomic(folder/'gates.json',gates)
            if not gates[-1]['passed']:raise RuntimeError('Real KV continuation gate failed or not evidenced')
        atomic(folder/'result.json',dict(passed=True,gates=gates,fixture_only=True,measurement_rounds=0))
        return gates


async def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',default=str(ROOT));parser.add_argument('--keep-alive',action='store_true');args=parser.parse_args()
    h=Harness(args.root)
    atomic(h.root/'progress.json',dict(phase='deploying_correctness_gate',wall_ns=time.time_ns()))
    try:
        await h.deploy(h.service,'kv')
        monitor=h.start('nvml',f'{shlex.quote(sys.executable)} -u tools/two_gpu_live_nvml.py --gpus 0,1 --output {h.service}/power.csv',h.service)
        await h.correctness()
        atomic(h.root/'progress.json',dict(phase='correctness_passed',wall_ns=time.time_ns(),next='capacity calibration required'))
        if args.keep_alive:
            atomic(h.root/'service_owner.json',dict(pid=os.getpid(),created=psutil.Process().create_time(),gpus=[0,1]))
            while not (h.root/'STOP_AFTER_IDLE').exists():
                h.check_critical();await asyncio.sleep(1)
        await h.cleanup(h.service)
    except BaseException as error:
        atomic(h.root/'failure_01.json',dict(error=repr(error),traceback=traceback.format_exc(),wall_ns=time.time_ns(),
            states=[h.state(i) for i in range(2)],owned=h.identities,preserve_scene=True))
        atomic(h.root/'progress.json',dict(phase='blocked_correctness',error=repr(error),wall_ns=time.time_ns()))
        raise

if __name__=='__main__':asyncio.run(main())
