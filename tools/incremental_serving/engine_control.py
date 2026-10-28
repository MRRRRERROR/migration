"""Experiment-only EngineCore boundary control. No scheduler access from threads.

Trusted local command spool; payloads are never accepted from HTTP clients.
The existing Hybrid abort path releases zero-state queue entries without
sending an abort to the original SSE output processor.
"""
import base64
import copy
from collections import deque
import json
import os
from pathlib import Path
import pickle
import re
import threading
import time
import traceback

from policy import TransferLedger, never_executed


def atomic(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,default=str));tmp.replace(path)


def append(path,value):
    with Path(path).open('a') as f:f.write(json.dumps(value,ensure_ascii=False,default=str)+'\n')


def logical_id(rid):
    return re.sub(r'-\d+$','',rid.removeprefix('cmpl-'))


class BoundaryControl:
    def __init__(self,core):
        self.core=core;self.root=Path(os.environ['INCREMENTAL_ENGINE_DIR'])
        self.root.mkdir(parents=True,exist_ok=True)
        for name in ('commands','acks','payloads'):(self.root/name).mkdir(exist_ok=True)
        self.ledger=TransferLedger();self.holds={};self.ever=set();self.steps=deque(maxlen=100)
        self.mode='A';self.last_snapshot=0.;self.stop=threading.Event();self.hold_ids=set()
        # This thread only enqueues the existing WAKEUP enum on a threadsafe
        # queue. All reads, writes, serialization and commands run on core.
        from vllm.v1.engine import EngineCoreRequestType
        def wake():
            while not self.stop.wait(.25):
                if not core.is_running():return
                core.input_queue.put_nowait((EngineCoreRequestType.WAKEUP,None))
        self.thread=threading.Thread(target=wake,name='experiment-core-wakeup',daemon=True)
        self.thread.start()

    def event(self,kind,**fields):
        append(self.root/'transactions.jsonl',dict(event=kind,monotonic_s=time.monotonic(),wall_ns=time.time_ns(),pid=os.getpid(),mode=self.mode,**fields))

    def request_state(self,req):
        s=self.core.scheduler;rid=req.request_id
        groups=s.kv_cache_manager.get_blocks(rid)
        groups=getattr(groups,'blocks',groups)
        h=getattr(s.get_kv_connector(),'_sched',None)
        active=any(rid in getattr(h,name,{}) for name in ('_loading','_saving'))
        running={r.request_id for r in s.running};waiting={r.request_id for r in s.waiting}
        return dict(request_id=rid,logical_id=logical_id(rid),input_tokens=len(req.prompt_token_ids or []),
            generated_tokens=req.num_output_tokens,computed_tokens=req.num_computed_tokens,
            arrival_time=req.arrival_time,status=req.status.name,
            state='running' if rid in running else 'waiting' if rid in waiting else req.status.name,
            kv_blocks=sum(len(g) for g in groups),preemptions=req.num_preemptions,
            ever_scheduled=rid in self.ever,transfer_active=active,
            queue_server_address=req.queue_server_address,client_index=req.client_index)

    def snapshot(self,force=False):
        now=time.monotonic()
        if not force and now-self.last_snapshot<.25:return
        self.last_snapshot=now;s=self.core.scheduler;pool=s.kv_cache_manager.block_pool
        front=self.core.llumlet_proxy.migration_frontend
        free=pool.get_num_free_blocks();total=pool.num_gpu_blocks
        value=dict(monotonic_s=now,wall_ns=time.time_ns(),pid=os.getpid(),mode=self.mode,
            instance_id=self.core.vllm_config.instance_id,
            requests=[self.request_state(r) for r in s.requests.values()],
            running=len(s.running),waiting=len(s.waiting),free_kv_blocks=free,total_kv_blocks=total,
            kv_usage=1-free/total,block_size=s.block_size,
            reserved=sum(r['state']=='reserved' for r in self.ledger.records.values()),
            holds=list(self.holds),migration_tasks=len(front._migration_out_tasks),
            migrating_request_ids=list(front.migrating_reqs),
            migration_finish=front.migration_finish_event.is_set(),
            active_step_s=list(self.steps),ever_scheduled_count=len(self.ever),
            loaded_files={n:getattr(__import__('sys').modules.get(n),'__file__',None) for n in (
                'vllm.v1.engine.core','vllm.v1.core.sched.scheduler','llumnix.vllm_llumlet_proxy',
                'llumnix.migration_frontend.vllm_v1.kvt_migration_frontend')})
        atomic(self.root/'state.json',value);append(self.root/'states.jsonl',value)

    def permission(self):
        if self.mode not in ('B','C','correctness'):raise ValueError('Waiting transfer forbidden in mode '+self.mode)

    def perform(self,c):
        op=c['op'];s=self.core.scheduler
        if op=='configure':
            if c['mode'] not in ('A','B','C','correctness'):raise ValueError('Unknown scheme')
            if s.requests or self.holds or any(r['state']=='reserved' for r in self.ledger.records.values()):
                raise ValueError('Cannot reset a nonempty engine')
            self.mode=c['mode'];self.ever.clear();self.ledger=TransferLedger();self.steps.clear()
            self.hold_ids=set(c.get('test_hold_ids',[]))
            if self.hold_ids and self.mode!='correctness':raise ValueError('Test hold is forbidden in measurements')
            if not self.core.reset_prefix_cache():raise ValueError('Prefix cache reset refused')
            return dict(mode=self.mode,cache_reset=True)
        if op=='release_test_hold':self.hold_ids.clear();return dict(released=True)
        if op=='snapshot':self.snapshot(True);return dict(ok=True)
        if op=='status':return self.ledger.status(c['tx'])
        self.permission();tx=c['tx'];rid=c['request_id']
        if op=='reserve':
            if rid in s.requests:raise ValueError('Destination already owns request')
            if any(k!=tx and r['request_id']==rid and r['state']!='aborted' for k,r in self.ledger.records.items()):
                raise ValueError('Conflicting ownership reservation')
            return self.ledger.reserve(tx,rid)
        if op=='abort_reservation':return self.ledger.abort(tx)
        if op=='detach':
            if tx in self.holds:return self.holds[tx]['reply']
            req=s.requests.get(rid)
            if req is None:raise ValueError('Request absent or finished before claim')
            proof=self.request_state(req)
            if not never_executed(proof):raise ValueError('Atomic never-executed check rejected: '+json.dumps(proof))
            from llumnix.compat.hybrid_connector_compat import req2corereq
            clone=copy.copy(req);clone.sampling_params=copy.deepcopy(req.sampling_params)
            dto=req2corereq(clone)
            if hasattr(dto,'client_index'):dto.client_index=req.client_index
            blob=pickle.dumps(dto,protocol=5)
            # Serialize and validate BEFORE changing ownership.
            check=pickle.loads(blob)
            assert check.request_id==rid and check.arrival_time==req.arrival_time
            assert check.queue_server_address==req.queue_server_address
            conn=s.get_kv_connector();conn.on_abort_req(rid,'experiment.waiting_transfer',False,True)
            outputs=conn._sched._step_aborting()
            if outputs:
                from vllm.v1.engine import EngineCoreOutputs
                self.core.llumlet_proxy.forward_outputs({i:EngineCoreOutputs(outputs=o) for i,o in outputs.items()})
            if rid in s.requests:raise RuntimeError('Detach did not remove scheduler ownership')
            reply=dict(request_id=rid,proof=proof,payload=base64.b64encode(blob).decode())
            self.holds[tx]=dict(blob=blob,reply=reply,request_id=rid)
            self.event('source_detached',tx=tx,request_id=rid,proof=proof)
            return reply
        if op=='admit':
            record=self.ledger.status(tx)
            if record['state']=='committed':return record
            if record['state']!='reserved' or record['request_id']!=rid:raise ValueError('No matching destination reservation')
            if rid in s.requests:raise ValueError('Destination request identity collision')
            dto=pickle.loads(base64.b64decode(c['payload']))
            if dto.request_id!=rid:raise ValueError('Payload identity mismatch')
            req,wave=self.core.preprocess_add_request(dto)
            if req.num_computed_tokens or req.num_output_tokens:raise ValueError('Nonempty transferred request')
            # A thrown add may have partially changed engine state. Mark intent
            # first so an ambiguous failure can never authorize source rollback.
            self.ledger.records[tx]['state']='admitting'
            self.core.add_request(req,wave)
            result=self.ledger.commit(tx)
            self.event('destination_admitted',tx=tx,request_id=rid,arrival_time=req.arrival_time,
                queue_server_address=req.queue_server_address,client_index=req.client_index)
            return result
        if op=='release_source':
            if c.get('destination_state')!='committed':raise ValueError('Destination proof required')
            held=self.holds.pop(tx,None)
            self.event('source_released',tx=tx,request_id=rid,already_released=held is None)
            return dict(released=True)
        if op=='rollback_source':
            if c.get('destination_state')!='aborted':raise ValueError('Confirmed destination abort required')
            held=self.holds[tx]
            if rid in s.requests:raise ValueError('Rollback identity collision')
            dto=pickle.loads(held['blob']);req,wave=self.core.preprocess_add_request(dto)
            self.core.add_request(req,wave);del self.holds[tx]
            self.event('source_rolled_back',tx=tx,request_id=rid,arrival_time=req.arrival_time)
            return dict(restored=True)
        raise ValueError('Unknown operation '+op)

    def boundary(self):
        for path in sorted((self.root/'commands').glob('*.json')):
            ack=self.root/'acks'/path.name
            if ack.exists():continue
            try:
                command=json.loads(path.read_text());result=self.perform(command)
                reply=dict(ok=True,result=result)
            except Exception as error:
                reply=dict(ok=False,error=repr(error),traceback=traceback.format_exc())
            reply.update(monotonic_s=time.monotonic(),pid=os.getpid())
            self.event('command',command={k:v for k,v in command.items() if k!='payload'},reply={k:v for k,v in reply.items() if k!='result'})
            atomic(ack,reply)
        try:self.snapshot()
        except Exception as error:
            # A measurement failure must block the experiment without crashing
            # the inference process or discarding live requests.
            atomic(self.root/'observer_failure.json',dict(error=repr(error),traceback=traceback.format_exc(),monotonic_s=time.monotonic()))
